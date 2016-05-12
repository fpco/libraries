{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ViewPatterns #-}
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
import Control.Exception (AsyncException)

import FP.Redis (MonadConnect)

data Mailbox m iSend youSend = Mailbox
    { mboxReceived :: !(TChan youSend)
    , mboxReadException :: !(TVar (Maybe SomeException))
    , mboxWrite :: !(MVar (iSend -> m ()))
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
    -- When running the receive loop filling the TChan, keep reading until
    -- you encounter an exception. Then fill the TVar with the received exception.
    -- However, if the exception is an 'AsyncException', just die directly.
    let recvLoop = do
            mbMsg <- try read
            case mbMsg of
                Left (fromException -> Just (err :: AsyncException)) ->
                    throwIO err
                Left (err :: SomeException) -> do
                    $logWarn ("Got exception " ++ tshow err ++ " when receving message for mailbox, exiting receive loop")
                    atomically (writeTVar exc (Just err))
                Right msg -> do
                    atomically (writeTChan chan msg)
                    recvLoop
    writeVar :: MVar (iSend -> m ()) <- newMVar write
    let mbox = Mailbox{mboxReceived = chan, mboxReadException = exc, mboxWrite = writeVar}
    -- We wait until the recv loop throws an exception or
    -- the continuation throws an exception or terminates.
    Async.withAsync recvLoop $ \recvLoopAsync -> Async.withAsync (cont mbox) $ \contAsync -> do
        mbExc :: Either SomeException b <- atomically $ do
            whichExc <- STM.orElse
                (Left <$> Async.waitCatchSTM recvLoopAsync)
                (Right <$> Async.waitCatchSTM contAsync)
            case whichExc of
                Left (Left err) -> return (Left err)
                Left (Right ()) -> Async.waitCatchSTM contAsync
                Right mbExc -> return mbExc
        either (liftIO . throwIO) return mbExc

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
            mbExc <- readTVar mboxReadException
            case mbExc of
                Nothing -> STM.retry
                Just exc -> return exc

mailboxWrite :: forall iSend youSend m.
       (MonadBaseControl IO m)
    => Mailbox m iSend youSend -> iSend -> m ()
mailboxWrite Mailbox{..} msg = withMVar mboxWrite (\write -> write msg)
