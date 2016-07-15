{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Handler.Home where

import Control.Monad.Logger
import Data.Either
import Data.Time.Clock
import           Distributed.Heartbeat (clearHeartbeatFailures)
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
            case wsRequest ws of
                Just k -> Left <$> getAndRenderRequest start r k
                Nothing -> return $ Right ("either idle or slave of another worker" :: Text)
        pending <- forM (jqsPending jqs) (getAndRenderRequest start r)
        return (jqs, sortBy (comparing snd) workers, sort pending)
    defaultLayout $ do
        setTitle "Compute Tier Status"
        $(widgetFile "status")

getAndRenderRequest :: MonadConnect m =>
    UTCTime -> Redis -> RequestId -> m (Text, Text, Text)
getAndRenderRequest start r k = do
    mrs <- getRequestStats r k
    let shownId = decodeUtf8 (unRequestId k)
    case mrs of
        Nothing -> return ("?", "?", shownId)
        Just rs -> return
            ( maybe "Missing?!?" (tshow . (start `diffUTCTime`)) (rsEnqueueTime rs)
            , reenqueueText
            , shownId
            )
          where reenqueueText = unwords
                    [tshow (  rsReenqueueByWorkerCount rs
                            + rsReenqueueByHeartbeatCount rs
                            + rsReenqueueByStaleKeyCount rs)
                    , "("
                    ++        tshow (rsReenqueueByWorkerCount rs)
                    ++ "/" ++ tshow (rsReenqueueByHeartbeatCount rs)
                    ++ "/" ++ tshow (rsReenqueueByStaleKeyCount rs)
                    ++ ")"]

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
    unless (null others') $ invalidArgs (map fst others')
    case map fst cmds of
        ["cancel"] -> do
            withRedis' config $ \redis ->
                forM reqs $ \(rid, mwid) ->
                    cancelRequest (Seconds 60) redis rid
            let takesAWhile :: Text
                takesAWhile = "NOTE: it may take a while for computations to cancel, so they will likely still appear as active work items"
            setMessageI $ "Cancellation request applied.  " <> takesAWhile
        ["clear-heartbeats"] -> withRedis' config $ \redis ->
            clearHeartbeatFailures redis
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

withRedis' :: (MonadCatch m, MonadCommand m, MonadMask m)
           => Config -> (Redis -> LoggingT m a) -> m a
withRedis' config =
    handleMismatchedSchema . runStdoutLoggingT . withRedis rc
  where
    rc = (defaultRedisConfig (redisPrefix config))
        { rcHost = redisHost config
        , rcPort = redisPort config
        }

handleMismatchedSchema :: (MonadIO m, MonadCatch m, MonadBaseControl IO m) => m a -> m a
handleMismatchedSchema = flip catch $ \ex ->
    case ex of
        MismatchedRedisSchemaVersion { actualRedisSchemaVersion = "" } ->
            fail $ "Either the redis prefix key is incorrect, or the hpc-manager's redis schema is mismatched: "
                ++ show ex
        _ -> throwM ex
