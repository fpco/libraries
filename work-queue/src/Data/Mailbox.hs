{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
module Data.Mailbox
    ( Mailbox
    , withMailbox
    , mailboxSelect
    , mailboxWrite
    ) where

import ClassyPrelude
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import qualified Control.Concurrent.STM as STM
import Control.Monad.Logger
import Control.Monad.Trans.Control (MonadBaseControl)

import FP.Redis (MonadConnect)

data Mailbox m iSend youSend = Mailbox
    { mboxReceived :: !(TChan youSend)
    , mboxWrite :: !(MVar (iSend -> m ()))
    , mboxException :: !(TVar (Maybe SomeException))
    }

withMailbox :: forall m youSend iSend b.
       (MonadConnect m)
    => m youSend
    -> (iSend -> m ())
    -> (Mailbox m iSend youSend -> m b)
    -> m b
withMailbox read write cont = do
    chan :: TChan a <- liftIO newTChanIO
    exc :: TVar (Maybe SomeException) <- liftIO (newTVarIO Nothing)
    let recvLoop = do
            mbMsg <- try read
            case mbMsg of
                Left (err :: SomeException) -> do
                    $logWarn ("Got exception " ++ tshow err ++ " when receving message for mailbox, exiting receive loop")
                    atomically (writeTVar exc (Just err))
                Right msg -> do
                    atomically (writeTChan chan msg)
                    recvLoop
    writeVar :: MVar (iSend -> m ()) <- newMVar write
    let mbox = Mailbox{mboxReceived = chan, mboxException = exc, mboxWrite = writeVar}
    Async.withAsync recvLoop (\_ -> cont mbox)

mailboxSelect :: forall iSend youSend a m.
       Mailbox m iSend youSend
    -> (youSend -> Maybe a) -> STM (Either SomeException a)
mailboxSelect Mailbox{..} match =
    STM.orElse (Right <$> getMessage) (Left <$> getException)
    where
        getMessage :: STM a
        getMessage = do
            msg <- readTChan mboxReceived
            case match msg of
                Nothing -> do
                    x <- getMessage
                    unGetTChan mboxReceived msg
                    return x
                Just x -> return x

        getException :: STM SomeException
        getException = do
            mbExc <- readTVar mboxException
            case mbExc of
                Nothing -> STM.retry
                Just exc -> return exc

mailboxWrite :: forall iSend youSend m.
       (MonadBaseControl IO m)
    => Mailbox m iSend youSend -> iSend -> m ()
mailboxWrite Mailbox{..} msg = withMVar mboxWrite (\write -> write msg)
