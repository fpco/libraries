{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module Distributed.TestUtil where

import ClassyPrelude hiding (keys)
import Data.List.NonEmpty (nonEmpty)
import FP.Redis
import FP.ThreadFileLogger

clearRedisKeys :: MonadIO m => m ()
clearRedisKeys =
    liftIO $ runThreadFileLoggingT $ logNest "clearRedisKeys" $ withConnection localhost $ \redis -> do
        matches <- runCommand redis $ keys (redisTestPrefix <> "*")
        mapM_ (runCommand_ redis . del) (nonEmpty matches)

redisTestPrefix :: ByteString
redisTestPrefix = "fpco:job-queue-test:"

localhost :: ConnectInfo
localhost = connectInfo "localhost"
