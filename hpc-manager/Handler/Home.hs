{-# LANGUAGE NoImplicitPrelude #-}
module Handler.Home where

import Control.Monad.Logger
import Distributed.JobQueue.Status
import Distributed.JobQueue.Client (cancelRequest)
import Distributed.RedisQueue
import FP.Redis
import Import
import Yesod.Form.Bootstrap3

getHomeR :: Handler Html
getHomeR = do
    (formWidget, formEncType) <- generateFormGet' configForm
    defaultLayout $ do
        setTitle "Compute Tier Status Connection Setup"
        $(widgetFile "homepage")

data Config = Config
    { redisHost :: !ByteString
    , redisPort :: !Int
    , redisPrefix :: !ByteString
    } deriving (Show)

configForm :: Form Config
configForm = renderBootstrap3 BootstrapBasicForm $ Config
    <$> fmap encodeUtf8 (areq textField (withLargeInput $ bfs ("Redis host" :: Text)) (Just "localhost"))
    <*> areq intField (withSmallInput $ bfs ("Redis port" :: Text)) (Just 6379)
    <*> fmap encodeUtf8 (areq textField (withLargeInput $ bfs ("Redis key prefix" :: Text)) (Just "fpco:job-queue-test:"))

getStatusR :: Handler Html
getStatusR = do
    ((res, _), _) <- runFormGet configForm
    case res of
        FormSuccess config -> do
            setUltDestCurrent
            displayStatus config
        _ -> do
            setMessageI (pack (show res) :: Text)
            redirect HomeR

displayStatus :: Config -> Handler Html
displayStatus config = do
     jqs <- withRedis' config getJobQueueStatus
     let pendingCount = length (jqsPending jqs)
         activeCount = length (filter (not . null . wsRequests) (jqsWorkers jqs))
         workerCount = length (jqsWorkers jqs)
     defaultLayout $ do
         setTitle "Compute Tier Status"
         $(widgetFile "status")

postStatusR :: Handler Html
postStatusR = do
    ((res, _), _) <- runFormGet configForm
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

withRedis' :: (MonadIO m, MonadCatch m, MonadBaseControl IO m)
           => Config -> (RedisInfo -> LoggingT m a) -> m a
withRedis' config = runStdoutLoggingT . withRedis (redisPrefix config) ci
  where
    ci = (connectInfo (redisHost config)) { connectPort = redisPort config }
