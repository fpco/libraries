{-# LANGUAGE NoImplicitPrelude #-}
module Handler.Home where

import Control.Monad.Logger
import Distributed.JobQueue.Status
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
displayStatus Config {..} = do
    let ci = (connectInfo redisHost) { connectPort = redisPort }
    jqs <- runStdoutLoggingT $ withRedis redisPrefix ci getJobQueueStatus
    let pendingCount = length (jqsPending jqs)
        activeCount = length (filter (not . null . wsRequests) (jqsWorkers jqs))
        workerCount = length (jqsWorkers jqs)
    defaultLayout $ do
        setTitle "Compute Tier Status"
        $(widgetFile "status")

postStatusR :: Handler Html
postStatusR = do
    (postParams, _) <- runRequestBody
    let (cmds, others) = partition (\(k, v) -> k `elem` ["cancel"] && v == "true") postParams
        (reqs, others') = partition (\(k, v) -> v == "jqr") others
    when (not (null others')) $ invalidArgs (map fst others')
    cmd <- case map fst cmds of
        ["cancel"] -> do
            --TODO: implement
            setMessageI ("Note: it may take a while for computations to cancel, so they will likely still appear as active work items" :: Text)
        _ -> invalidArgs (map fst cmds)
    redirectUltDest HomeR
