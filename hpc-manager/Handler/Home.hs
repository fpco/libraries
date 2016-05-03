{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Handler.Home where

import Control.Monad.Logger
import Control.Monad.Trans.Unlift (MonadBaseUnlift)
import Data.Either
import Data.Time.Clock
import Distributed.JobQueue.Client (cancelRequest)
import Distributed.Types
import Distributed.JobQueue.Status
import FP.Redis
import Import
import Yesod.Form.Bootstrap3
import Distributed.Redis

--------------------------------------------------------------------------------
-- Connection Configuration

getHomeR :: Handler Html
getHomeR = do
    (formWidget, formEncType) <- generateFormPost =<< configForm
    defaultLayout $ do
        setTitle "Compute Tier Status Connection Setup"
        $(widgetFile "homepage")

postHomeR :: Handler Html
postHomeR = do
    ((res, _), _) <- runFormPost =<< configForm
    case res of
        FormSuccess config -> do
            storeConfig config
            redirect StatusR
        _ -> do
            setMessageI (pack (show res) :: Text)
            redirect HomeR

postDefaultSettingsR :: Handler Html
postDefaultSettingsR = clearSession >> redirect StatusR

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

storeConfig :: Config -> Handler ()
storeConfig config = do
    setSessionBS "redis-host" (redisHost config)
    setSession "redis-port" (pack (show (redisPort config)))
    setSessionBS "redis-prefix" (redisPrefix config)

clearConfig :: Handler ()
clearConfig = do
    deleteSession "redis-host"
    deleteSession "redis-port"
    deleteSession "redis-prefix"

getConfig :: Handler Config
getConfig = do
    mredisHost <- lookupSessionBS "redis-host"
    mredisPort <- lookupSession "redis-port"
    mredisPrefix <- lookupSessionBS "redis-prefix"
    case (mredisHost, mredisPort, mredisPrefix) of
        (Just redisHost, Just (readMay . unpack -> Just redisPort), Just redisPrefix) ->
            return Config {..}
        (Nothing, Nothing, Nothing) -> do
            settings <- appSettings <$> getYesod
            return Config
                { redisHost = encodeUtf8 (appHpcRedisHost settings)
                , redisPort = appHpcRedisPort settings
                , redisPrefix = encodeUtf8 (appHpcRedisPrefix settings)
                }
        _ -> do
            setMessageI ("Unexpected session state.  Please reconfigure connection." :: Text)
            redirect HomeR

--------------------------------------------------------------------------------
-- Worker status

getStatusR :: Handler Html
getStatusR = do
    config <- getConfig
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

getAndRenderRequest :: (MonadIO m, MonadBaseUnlift IO m) =>
    UTCTime -> Redis -> RequestId -> m (Text, Text, Text)
getAndRenderRequest start r k = do
    mrs <- getRequestStats r k
    let shownId = decodeUtf8 (unRequestId k)
    case mrs of
        Nothing -> return ("?", "?", shownId)
        Just rs -> return
            ( fromMaybe "Missing?!?" (fmap (tshow . (start `diffUTCTime`)) (rsEnqueueTime rs))
            , tshow (rsReenqueueCount rs)
            , shownId
            )

postStatusR :: Handler Html
postStatusR = do
    config <- getConfig
    (postParams, _) <- runRequestBody
    let (cmds, others) = partition (\(k, v) -> k `elem` ["cancel", "clear-heartbeats"] && v == "true") postParams
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
        ["clear-heartbeats"] -> withRedis' config $ \redis ->
            mapM_ (clearHeartbeatFailure redis) =<< getActiveWorkers redis
        _ -> invalidArgs (map fst cmds)
    redirectUltDest HomeR

--------------------------------------------------------------------------------
-- Requests status

getRequestsR :: Handler Html
getRequestsR = do
    config <- getConfig
    rs <- withRedis' config getAllRequests
    $logInfo (tshow rs)
    requests <- withRedis' config getAllRequestStats
    defaultLayout $ do
        setTitle "Compute Tier Requests"
        $(widgetFile "requests")

--------------------------------------------------------------------------------
-- Utilities

withRedis' :: (MonadIO m, MonadCatch m, MonadBaseUnlift IO m)
           => Config -> (Redis -> LoggingT m a) -> m a
withRedis' config =
    handleMismatchedSchema . runStdoutLoggingT . withRedis rc
  where
    rc = RedisConfig
        { rcConnectInfo = (connectInfo (redisHost config)) { connectPort = redisPort config }
        , rcKeyPrefix = redisPrefix config
        }

handleMismatchedSchema :: (MonadIO m, MonadCatch m, MonadBaseUnlift IO m) => m a -> m a
handleMismatchedSchema = flip catch $ \ex ->
    case ex of
        MismatchedRedisSchemaVersion { actualRedisSchemaVersion = "" } ->
            fail $ "Either the redis prefix key is incorrect, or the hpc-manager's redis schema is mismatched: "
                ++ show ex
        _ -> throwM ex
