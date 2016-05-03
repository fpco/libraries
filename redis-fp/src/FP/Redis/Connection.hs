{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns, TupleSections #-}

-- | Redis connection handling.

module FP.Redis.Connection
    ( ConnectInfo (..)
    , Connection
    , connectInfo
    , connect
    , disconnect
    , withConnection
    , connectionInfo
    ) where

-- TODO OPTIONAL: Add a HasConnection class so clients don't need to pass Connections explicitly

import ClassyPrelude.Conduit hiding (Builder, connect)
import Blaze.ByteString.Builder (Builder)
import qualified Blaze.ByteString.Builder as Builder
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import Control.Concurrent.STM (retry)
import Control.Exception (AsyncException(ThreadKilled))
import Control.Monad.Catch (Handler(..))
import Control.Monad.Extra
import Control.Monad.Logger
import Control.Monad.Trans.Unlift (UnliftBase (..), askUnliftBase)
import Data.Attoparsec.ByteString (takeTill)
import Data.Attoparsec.ByteString.Char8
    (Parser, choice, char, isEndOfLine, endOfLine, decimal, signed, take, count)
import Data.Conduit.Attoparsec (conduitParser, PositionRange)
import Data.Conduit.Blaze (unsafeBuilderToByteString, allocBuffer)
import qualified Data.Conduit.Network as CN
import qualified Data.DList as DList

import FP.Redis.Command
import FP.Redis.Internal
import FP.Redis.Types.Internal
import Control.Concurrent.STM.TSQueue
import FP.ThreadFileLogger

-- | Connects to Redis server and runs the inner action.  When the inner action returns,
-- the connection is terminated.
withConnection :: forall a m. (MonadConnect m)
               => ConnectInfo -- ^ Connection information
               -> (Connection -> m a) -- ^ Inner action
               -> m a
withConnection cinfo inner =
    bracket (connect cinfo)
            disconnect
            inner

-- | Connects to Redis server.
connect :: forall m. (MonadConnect m)
        => ConnectInfo -- ^ Connection information
        -> m Connection
connect cinfo = do
    initialTag <- getLogTag
    connectionMVar <- newEmptyMVar
    thread <- Async.async (clientThread initialTag connectionMVar)
    eConnection <- takeMVarE ConnectionFailedException connectionMVar
    case eConnection of
        Left exception -> throwM (ConnectionFailedException exception)
        Right connection -> return (connection thread)
  where

    clientThread :: LogTag -> ConnectionMVar -> m ()
    clientThread initialTag connectionMVar =
        showEx "clientThread" $
        catch clientThread' outerHandler
      where
        clientThread' = do
            (reqQueue, reqVar) <-
                atomically ((,)
                            <$> (newTVar =<< RQConnected <$> newTSQueue)
                            <*> newEmptyTMVar)
            let runClient :: m () -> m ()
                runClient resetRetries = do
                    UnliftBase runInIO <- askUnliftBase
                    liftIO $ CN.runTCPClient
                        (CN.clientSettings (connectPort cinfo) (connectHost cinfo))
                        (\appData -> runInIO $ do
                            resetRetries
                            app initialTag reqQueue reqVar connectionMVar appData)
            case connectRetryPolicy cinfo of
                Just retryPolicy ->
                    forever (recoveringWithReset
                               retryPolicy
                               [\_ -> Handler retryHandler]
                               (\resetRetries -> runClient resetRetries))
                Nothing -> runClient (return ())
        retryHandler :: IOException -> m Bool
        retryHandler e = do
            logErrorNS logSource ("Exception in Redis connection thread: " ++ tshow e)
            return True
        outerHandler :: IOException -> m ()
        outerHandler exception = do
            _ <- tryPutMVar connectionMVar (Left (toException exception))
            throwM exception

    app :: LogTag -> RequestQueue -> TMVar Request -> ConnectionMVar -> CN.AppData -> m ()
    app initialTag reqQueue reqVar connectionMVar appData = showEx "app" $ do
        initialRequestPairs <- mapM commandToRequestPair (connectInitialCommands cinfo)
        let requeue :: TSQueue Request -> [Request] -> STM ()
            requeue queue reqs = mapM_ (unGetTSQueue queue) reqs
            requeueRequests' :: TSQueue Request -> STM ()
            requeueRequests' queue = do
                requeue queue (map unSubscriptionRequest (reverse (connectInitialSubscriptions cinfo)))
                requeue queue (reverse (DList.toList (concat (map fst initialRequestPairs))))
            requeueRequests :: STM ()
            requeueRequests = do
                rqs <- readTVar reqQueue
                case rqs of
                    RQConnected queue ->
                        requeueRequests' queue
                    RQLostConnection queue -> do
                        requeueRequests' queue
                        writeTVar reqQueue (RQConnected queue)
                    RQFinal _ -> throwSTM DisconnectedException
                    RQDisconnect -> throwSTM DisconnectedException
        atomically requeueRequests
        runThreads initialRequestPairs
      where
        runThreads :: [((DList.DList Request),IO ())] -> m ()
        runThreads initialRequestPairs = do
            Async.withAsync (Async.race_
                                (setLogTag initialTag >> reqThread)
                                (setLogTag initialTag >> respThread))
                            (waitInitialResponses (map snd initialRequestPairs))
        reqThread :: m ()
        reqThread = showEx "reqThread" $
                    reqSource reqQueue reqVar
                    =$ unsafeBuilderToByteString (allocBuffer 4096)
                    $$ CN.appSink appData
        respThread :: m ()
        respThread = showEx "respThread" $
                     CN.appSource appData
                     $= conduitParser responseParser
                     $$ respSink reqQueue reqVar
        waitInitialResponses :: [IO ()] -> Async.Async () -> m ()
        waitInitialResponses initialResponseActions async = showEx "waitInitialResponses" $ do
            _ <- catch (liftIO (sequence initialResponseActions))
                       initialResponseHandler
            -- Using `tryPutMVar' so we don't block when recovering after a disconnection
            _ <- tryPutMVar connectionMVar (Right (Connection cinfo reqQueue))
            Async.wait async
        initialResponseHandler :: RedisException -> m [()]
        initialResponseHandler exception = do
            _ <- tryPutMVar connectionMVar (Left (toException exception))
            throwM exception

    reqSource :: RequestQueue -> TMVar Request -> Source m Builder
    reqSource reqQueue reqVar =
        forever loopBatch
      where
        loopBatch :: Source m Builder
        loopBatch = do
            -- Still space in batch so try to batch another request from the queue
            req <- atomically getNextRequest
            yieldReq req
        getNextRequest :: STM Request
        getNextRequest = do
            rqs <- readTVar reqQueue
            (req, isFinal) <-
                case rqs of
                    RQConnected queue -> (, False) <$> readTSQueue queue
                    RQLostConnection _ -> retry
                    RQFinal req -> return (req, True)
                    RQDisconnect -> retry
            when isFinal (writeTVar reqQueue RQDisconnect)
            putTMVar reqVar req
            return req
        yieldReq req = do
            let builder = requestBuilder req
                bs = Builder.toByteString builder
            logDebugNS logSource ("reqSource: yielding: " ++ tshow bs)
            yield (builder ++ Builder.flush)

    respSink :: RequestQueue -> TMVar Request -> Sink (PositionRange, Response) m ()
    respSink reqQueue reqVar = loop Normal
      where
        loop :: Mode -> Sink (PositionRange, Response) m ()
        loop mode = do
            mresp <- await
            logDebugNS logSource
                       ("respSink (mode=" ++ tshow mode ++ "): received: " ++ tshow mresp)
            case (mresp, mode) of
                (Nothing, _) -> atomically $ do
                    -- This ensures that no more requests will be attempted to
                    -- be sent over the dead connection.
                    rqs <- readTVar reqQueue
                    case rqs of
                        RQConnected queue -> writeTVar reqQueue (RQLostConnection queue)
                        RQLostConnection _ -> return ()
                        RQFinal _ -> writeTVar reqQueue RQDisconnect
                        RQDisconnect -> return ()
                (Just (_, resp), Normal) -> handleNormal resp
                (Just (_, resp), Subscribed) -> handleSubscribed resp
        handleNormal :: Response -> Sink (PositionRange, Response) m ()
        handleNormal resp = do
            -- `mask' so that an async exception can't get inbetween reading from the pending
            -- response queue and passing the result to the user.
            req <- liftIO (mask (sendResponse resp))
            case req of
                Command _ _ -> loop Normal
                Subscription _ -> handleSubscribed resp
        handleSubscribed :: Response -> Sink (PositionRange, Response) m ()
        handleSubscribed resp =
            case resp of
                Array (Just message) -> do
                    forM_ (connectSubscriptionCallback cinfo) $ \cb -> liftIO (cb message)
                    case message of
                        [BulkString (Just "unsubscribe"), _, Integer 0] -> loop Normal
                        _ -> loop Subscribed
                _ -> throwM ProtocolException
        sendResponse :: Response -> (forall b. IO b -> IO b) -> IO Request
        sendResponse resp restore = do
            req <- atomically (takeTMVar reqVar)
            case req of
                Command _ respAction -> respAction restore resp
                _ -> skip
            return req

    showEx :: forall a b. (MonadConnect a) => Text -> a b -> a b
    showEx s i = do
        r <- catch i
            (\(e::SomeException) -> do
                case fromException e of
                    Just ThreadKilled -> return ()
                    _ -> logDebugNS logSource (s ++ ": EXCEPTION " ++ tshow e)
                throwM e)
        logDebugNS logSource (s ++ ": returning")
        return r

    logSource = connectLogSource cinfo

-- | Disconnect from Redis server, sending a QUIT command before terminating.
disconnect :: MonadCommand m => Connection -> m ()
disconnect conn =
    disconnect' conn (Just quit)

-- | Default Redis server connection info.
connectInfo :: ByteString -- ^ Server's hostname
            -> ConnectInfo
connectInfo host = ConnectInfo { connectHost = host
                               , connectPort = 6379
                               , connectInitialCommands = []
                               , connectInitialSubscriptions = []
                               , connectSubscriptionCallback = Nothing
                               , connectRetryPolicy = Nothing
                               , connectLogSource = "REDIS"
                               , connectMaxRequestQueue = 100 }

-- | Get original connect info from connection.
connectionInfo :: Connection -> ConnectInfo
connectionInfo = connectionInfo_

-- | Redis Response protocol parser (adapted from hedis)
responseParser :: Parser Response
responseParser = response
  where
    response = choice [simpleString
                      ,integer
                      ,bulkString
                      ,array
                      ,error_]
    simpleString = SimpleString <$> (char '+' *> takeTill isEndOfLine <* endOfLine)
    error_ = Error <$> (char '-' *> takeTill isEndOfLine <* endOfLine)
    integer = Integer <$> (char ':' *> signed decimal <* endOfLine)
    bulkString = BulkString <$> do
        len <- char '$' *> signed decimal <* endOfLine
        if len < 0
            then return Nothing
            else Just <$> Data.Attoparsec.ByteString.Char8.take len <* endOfLine
    array = Array <$> do
        len <- char '*' *> signed decimal <* endOfLine
        if len < 0
            then return Nothing
            else Just <$> count len response
