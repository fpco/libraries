{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Handler.Home where

import Control.Monad.Logger
import Data.Either
import Data.Time.Clock
import Data.Time.Clock.POSIX
import Distributed.JobQueue.Client (cancelRequest)
import Distributed.JobQueue.Status
import Distributed.RedisQueue
import FP.Redis
import Import
import Yesod.Form.Bootstrap3

getHomeR :: Handler Html
getHomeR = do
    -- If the settings have been specified by env vars, skip configuration.
    settings <- appSettings <$> getYesod
    mnoRedir <- lookupGetParam "no-redirect"
    when (appHpcRedisPrefix settings /= "" && isNothing mnoRedir) $
        redirect StatusR
    -- Otherwise, ask the user to configure the connection.
    (formWidget, formEncType) <- generateFormGet' =<< configForm
    defaultLayout $ do
        setTitle "Compute Tier Status Connection Setup"
        $(widgetFile "homepage")

data Config = Config
    { redisHost :: !ByteString
    , redisPort :: !Int
    , redisPrefix :: !ByteString
    } deriving (Show, Read)

configForm :: Handler (Form Config)
configForm = do
    AppSettings {..} <- appSettings <$> getYesod
    return $ renderBootstrap3 BootstrapBasicForm $ Config
        <$> fmap encodeUtf8 (areq textField (withLargeInput $ bfs ("Redis host" :: Text)) (Just appHpcRedisHost))
        <*> areq intField (withSmallInput $ bfs ("Redis port" :: Text)) (Just appHpcRedisPort)
        <*> fmap encodeUtf8 (areq textField (withLargeInput $ bfs ("Redis key prefix" :: Text)) (Just appHpcRedisPrefix))

ensureConfig :: Handler Config
ensureConfig = do
    ((res, _), _) <- runFormGet =<< configForm
    case res of
        FormSuccess config -> return config
        _ -> do
            -- When there's a redirect from HomeR due to having a
            -- configuration, there are no get params.
            --
            -- NOTE: ideally we'd have a way to go from a form + value
            -- to a list of query params, then this would be
            -- unnecessary.
            settings <- appSettings <$> getYesod
            gps <- reqGetParams <$> getRequest
            if null gps
                then return Config
                    { redisHost = encodeUtf8 (appHpcRedisHost settings)
                    , redisPort = appHpcRedisPort settings
                    , redisPrefix = encodeUtf8 (appHpcRedisPrefix settings)
                    }
                else do
                    setMessageI (pack (show res) :: Text)
                    redirect HomeR

getStatusR :: Handler Html
getStatusR = do
    config <- ensureConfig
    setUltDestCurrent
    start <- liftIO getCurrentTime
    (jqs, workers, pending) <- withRedis' config $ \r -> do
        jqs <- getJobQueueStatus r
        workers <- forM (jqsWorkers jqs) $ \ws ->
            (decodeUtf8 (unWorkerId (wsWorker ws)), ) <$>
            case wsRequests ws of
                [k] -> Left <$> getAndRenderRequest start r k
                _ | wsHeartbeatFailure ws -> return $ Right "worker failed its heartbeat check and its work was re-enqueued"
                  | otherwise -> return $ Right ("either idle or slave of another worker" :: Text)
        pending <- forM (jqsPending jqs) (getAndRenderRequest start r)
        return (jqs, sortBy (comparing snd) workers, sort pending)
    defaultLayout $ do
        setTitle "Compute Tier Status"
        $(widgetFile "status")

getAndRenderRequest :: (MonadIO m, MonadBaseControl IO m) =>
    UTCTime -> RedisInfo -> RequestId -> m (Text, Text, Text)
getAndRenderRequest start r k = do
    mrs <- getRequestStats r k
    let shownId = decodeUtf8 (unRequestId k)
    case mrs of
        Nothing -> return ("?", "?", shownId)
        Just rs -> return
            ( tshow (start `diffUTCTime` rsEnqueueTime rs)
            , tshow (rsReenqueueCount rs)
            , shownId
            )

postStatusR :: Handler Html
postStatusR = do
    ((res, _), _) <- runFormGet =<< configForm
    case res of
        FormSuccess config -> do
            (postParams, _) <- runRequestBody
            let (cmds, others) = partition (\(k, v) -> k `elem` ["cancel"] && v == "true") postParams
                (reqs', others') = partition (\(k, _) -> "jqr:" `isPrefixOf` k) others
                reqs = mapMaybe
                    (\(k, v) ->
                        (, (WorkerId . encodeUtf8 <$> stripPrefix "wid:" v))
                        <$> (RequestId . encodeUtf8 <$> stripPrefix "jqr:" k))
                    reqs'
            when (not (null others')) $ invalidArgs (map fst others')
            case map fst cmds of
                ["cancel"] -> do
                    (successes, failures) <- fmap (partition snd) $
                        withRedis' config $ \redis ->
                        forM reqs $ \(rid, mwid) -> do
                            success <- cancelRequest (Seconds 60) redis rid mwid
                            return (rid, success)
                    let takesAWhile :: Text
                        takesAWhile = "NOTE: it may take a while for computations to cancel, so they will likely still appear as active work items"
                        failuresList = pack (show (map (unRequestId . fst) failures))
                    setMessageI $ case (null successes, null failures) of
                        (True, True) -> "No cancellations selected."
                        (False, True) -> "Cancellation request applied.  " <> takesAWhile
                        (True, False) ->
                            "Failed to cancel any requests, couldn't find the following: " <>
                            failuresList <> "\n" <> takesAWhile
                        (False, False) ->
                            "Some cancellations applied, couldn't find the following: " <>
                            failuresList <> "\n" <> takesAWhile
                _ -> invalidArgs (map fst cmds)
            redirectUltDest HomeR
        _ -> do
          setMessageI (pack (show res) :: Text)
          redirect HomeR

getRequestsR :: Handler Html
getRequestsR = do
    config <- ensureConfig
    rs <- withRedis' config getAllRequests
    $logInfo (tshow rs)
    requests <- withRedis' config getAllRequestStats
    defaultLayout $ do
        setTitle "Compute Tier Requests"
        $(widgetFile "requests")

withRedis' :: (MonadIO m, MonadCatch m, MonadBaseControl IO m)
           => Config -> (RedisInfo -> LoggingT m a) -> m a
withRedis' config = runStdoutLoggingT . withRedis (redisPrefix config) ci
  where
    ci = (connectInfo (redisHost config)) { connectPort = redisPort config }
