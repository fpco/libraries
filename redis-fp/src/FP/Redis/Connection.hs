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
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.STM (retry)
import Control.Monad.Catch (Handler(..))
import Control.Monad.Extra
import Control.Monad.Logger
import Control.Monad.Trans.Control (control, RunInBase, StM)
import Data.Attoparsec.ByteString (takeTill)
import Data.Attoparsec.ByteString.Char8
    (Parser, choice, char, isEndOfLine, endOfLine, decimal, signed, take, count)
import Data.Conduit.Attoparsec (conduitParser, PositionRange)
import Data.Conduit.Blaze (unsafeBuilderToByteString, allocBuffer)
import qualified Data.Conduit.Network as CN
import qualified Data.DList as DList
import qualified Data.Sequence as Seq

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
    thread <- control $ \runInIO -> do
        async <- Async.async (void (runInIO (clientThread initialTag connectionMVar)))
        runInIO (return async)
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
            (reqQueue, pendingRespQueue) <-
                atomically ((,)
                            <$> (newTMVar =<< RQConnected <$> newTSQueue)
                            <*> newTSQueue)
            let runClient :: m () -> RunInBase m IO -> IO (StM m ())
                runClient resetRetries runInIO =
                    CN.runTCPClient
                        (CN.clientSettings (connectPort cinfo) (connectHost cinfo))
                        (\appData -> runInIO $ do
                            resetRetries
                            app initialTag reqQueue pendingRespQueue connectionMVar appData)
            case connectRetryPolicy cinfo of
                Just retryPolicy ->
                    forever (recoveringWithReset
                               retryPolicy
                               [\_ -> Handler retryHandler]
                               (\resetRetries -> control (runClient resetRetries) :: m ()))
                Nothing ->
                    control (runClient (return ()))
        retryHandler :: IOException -> m Bool
        retryHandler e = do
            --TODO SHOULD: improve logging
            logWarnNS logSource ("connect clientThread retryHandler exception: " ++ tshow e)
            return True
        outerHandler :: IOException -> m ()
        outerHandler exception = do
            _ <- tryPutMVar connectionMVar (Left (toException exception))
            throwM exception

    app :: LogTag -> RequestQueue -> PendingResponseQueue -> ConnectionMVar -> CN.AppData -> m ()
    app initialTag reqQueue pendingRespQueue connectionMVar appData = showEx "app" $ do
        initialRequestPairs <- mapM commandToRequestPair (connectInitialCommands cinfo)
        let requeue :: TSQueue Request -> [Request] -> STM ()
            requeue queue reqs = mapM_ (unGetTSQueue queue) reqs
            requeueRequests' :: TSQueue Request -> STM ()
            requeueRequests' queue = do
                reqsReversed <- readAllReversedTSQueue pendingRespQueue
                requeue queue reqsReversed
                requeue queue (map unSubscriptionRequest (reverse (connectInitialSubscriptions cinfo)))
                requeue queue (reverse (DList.toList (concat (map fst initialRequestPairs))))
            requeueRequests :: STM ()
            requeueRequests =
                modifyTMVarSTM reqQueue $ \rqs -> do
                    case rqs of
                        RQConnected queue -> do
                            requeueRequests' queue
                            return (rqs,())
                        RQLostConnection queue -> do
                            requeueRequests' queue
                            return (RQConnected queue,())
                        RQFinal _ -> throwSTM DisconnectedException
                        RQDisconnect -> throwSTM DisconnectedException
        atomically requeueRequests
        control (runThreads initialRequestPairs)
      where
        runThreads :: [((DList.DList Request),IO ())] -> (RunInBase m IO) -> IO (StM m ())
        runThreads initialRequestPairs runInIO =
            Async.withAsync (Async.race_
                                (runInIO (setLogTag initialTag >> reqThread))
                                (runInIO (setLogTag initialTag >> respThread)))
                            (runInIO . waitInitialResponses (map snd initialRequestPairs))
        reqThread :: m ()
        reqThread = showEx "reqThread" $
                    reqSource reqQueue pendingRespQueue
                    =$ unsafeBuilderToByteString (allocBuffer 4096)
                    $$ CN.appSink appData
        respThread :: m ()
        respThread = showEx "respThread" $
                     CN.appSource appData
                     $= conduitParser responseParser
                     $$ respSink reqQueue pendingRespQueue
        waitInitialResponses :: [IO ()] -> Async.Async () -> m ()
        waitInitialResponses initialResponseActions async = showEx "waitInitialResponses" $ do
            _ <- catch (liftIO (sequence initialResponseActions))
                       initialResponseHandler
            -- Using `tryPutMVar' so we don't block when recovering after a disconnection
            _ <- tryPutMVar connectionMVar (Right (Connection cinfo reqQueue pendingRespQueue))
            liftIO (Async.wait async)
        initialResponseHandler :: RedisException -> m [()]
        initialResponseHandler exception = do
            _ <- tryPutMVar connectionMVar (Left (toException exception))
            throwM exception

    reqSource :: RequestQueue -> PendingResponseQueue -> Source m Builder
    reqSource reqQueue pendingRespQueue =
        forever (loopBatch Seq.empty)
      where
        loopBatch :: Seq Request -> Source m Builder
        loopBatch reqs =
            -- Combine multiple queued requests into batches
            let reqsLen = Seq.length reqs
            in if reqsLen >= requestsPerBatch
                then -- Batch is full so send what we've batched
                     yieldReqs reqs
                else do
                    -- Still space in batch so try to batch another request from the queue
                    mreq <- atomically (getNextRequest (reqsLen > 0))
                    case mreq of
                        NoRequest -> do
                            -- No more requests in queue, so yield send what we've batched
                            yieldReqs reqs
                            atomically waitPendingResponseSpace
                        NextRequest req ->
                            -- Another request in the queue, so batch it up
                            loopBatch (reqs Seq.|> req)
                        FinalRequest req -> do
                            -- Only final QUIT command, so forget anything batched up
                            -- and just send it.
                            yieldReqs (Seq.singleton req)
        getNextRequest :: Bool -> STM NextRequest
        getNextRequest hasBatch = do
            modifyTMVarSTM reqQueue $ \rqs -> do
                pendingRespLen <- lengthTSQueue pendingRespQueue
                let pendingRespFull = pendingRespLen >= maxPendingResponses
                mreq <- if hasBatch
                    then if pendingRespFull
                        then -- If too many pending responses, don't batch any more requests.
                             return NoRequest
                        else case rqs of
                            RQConnected queue -> do
                                mreq <- tryReadTSQueue queue
                                case mreq of
                                    Nothing -> return NoRequest
                                    Just req -> return (NextRequest req)
                            RQLostConnection _ -> retry
                            RQFinal req -> return (FinalRequest req)
                            RQDisconnect -> retry
                    else do
                        when pendingRespFull retry
                        case rqs of
                            RQConnected queue -> NextRequest <$> readTSQueue queue
                            RQLostConnection _ -> retry
                            RQFinal req -> return (FinalRequest req)
                            RQDisconnect -> retry
                case mreq of
                    NoRequest -> return (rqs, mreq)
                    NextRequest req -> do
                        writeTSQueue pendingRespQueue req
                        return (rqs, mreq)
                    FinalRequest req -> do
                        writeTSQueue pendingRespQueue req
                        return (RQDisconnect, mreq)
        waitPendingResponseSpace :: STM ()
        waitPendingResponseSpace = do
            -- This waits until there is extra space in the pending response queue, so that
            -- if the connection is saturated we still send multiple requests in batches.
            size <- lengthTSQueue pendingRespQueue
            when (size > maxPendingResponses - requestsPerBatch) retry
        yieldReqs reqs = do
            logDebugNS logSource
                       ("reqSource: yielding: " ++
                        tshow (map (Builder.toByteString . requestBuilder) reqs))
            yield (concatMap requestBuilder reqs ++ Builder.flush)

    respSink :: RequestQueue -> PendingResponseQueue -> Sink (PositionRange, Response) m ()
    respSink reqQueue pendingRespQueue = loop Normal
      where
        loop :: Mode -> Sink (PositionRange, Response) m ()
        loop mode = do
            mresp <- await
            logDebugNS logSource
                       ("respSink (mode=" ++ tshow mode ++ "): received: " ++ tshow mresp)
            case (mresp, mode) of
                (Nothing, _) ->
                    -- This ensures that no more requests will be attempted to
                    -- be sent over the dead connection.
                    atomically $ modifyTMVarSTM reqQueue $ \rqs ->
                        case rqs of
                            RQConnected queue -> return (RQLostConnection queue, ())
                            RQLostConnection _ -> return (rqs, ())
                            RQFinal _ -> return (RQDisconnect, ())
                            RQDisconnect -> return (rqs, ())
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
        handleSubscribed resp = do
            -- We don't actually need the pendingRespQueue in pub/sub mode, so we just try to
            -- read from it (and ignore the result) to ensure it doesn't grow.
            _ <- atomically (tryReadTSQueue pendingRespQueue)
            case resp of
                Array (Just message) -> do
                    forM_ (connectSubscriptionCallback cinfo) $ \cb -> liftIO (cb message)
                    case message of
                        [BulkString (Just "unsubscribe"), _, Integer 0] -> loop Normal
                        _ -> loop Subscribed
                _ -> throwM ProtocolException
        sendResponse :: Response -> (forall b. IO b -> IO b) -> IO Request
        sendResponse resp restore = do
            req <- atomically (readTSQueue pendingRespQueue)
            case req of
                Command _ respAction -> respAction restore resp
                _ -> skip
            return req

    showEx :: forall a b. (MonadConnect a) => Text -> a b -> a b
    showEx s i = do
        r <- catch i
            (\(e::SomeException) -> do
                logDebugNS logSource (s ++ ": EXCEPTION " ++ tshow e)
                throwM e)
        logDebugNS logSource (s ++ ": returning")
        return r

    logSource = connectLogSource cinfo

    requestsPerBatch = connectRequestsPerBatch cinfo

    maxPendingResponses = connectMaxPendingResponses cinfo

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
                               , connectRequestsPerBatch = 100
                               , connectMaxPendingResponses = 1000 }

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
