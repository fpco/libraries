{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module Distributed.TestUtil where

import ClassyPrelude hiding (keys)
import Control.Monad.Trans.Control (MonadBaseControl)
import Data.List.NonEmpty (nonEmpty)
import FP.Redis
import FP.ThreadFileLogger
import Test.Hspec.Expectations

clearRedisKeys :: MonadIO m => m ()
clearRedisKeys =
    liftIO $ runThreadFileLoggingT $ logNest "clearRedisKeys" $ withConnection localhost $ \redis -> do
        matches <- runCommand redis $ keys (redisTestPrefix <> "*")
        mapM_ (runCommand_ redis . del) (nonEmpty matches)

redisTestPrefix :: ByteString
redisTestPrefix = "fpco:job-queue-test:"

localhost :: ConnectInfo
localhost = connectInfo "localhost"

shouldThrow :: (Exception e, MonadBaseControl IO m, MonadThrow m) => m () -> Selector e -> m ()
shouldThrow f s = f `catch` \e -> unless (s e) $ throwM e
