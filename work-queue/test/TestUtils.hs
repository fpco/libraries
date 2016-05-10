{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module TestUtils (redisIt) where

import           ClassyPrelude hiding (keys)
import           Test.Hspec (Spec, SpecWith, it, runIO, hspec)
import           FP.Redis
import           FP.ThreadFileLogger
import qualified Data.List.NonEmpty as NE
import           Control.Monad.Logger (runStdoutLoggingT, filterLogger, logInfo)
import qualified Data.Text as T

import           Distributed.Redis

redisConfig :: RedisConfig
redisConfig = defaultRedisConfig
    { rcKeyPrefix = "test:" }

clearRedisKeys :: (MonadConnect m) => Redis -> m ()
clearRedisKeys redis = do
    matches <- run redis (keys "test:*")
    mapM_ (run_ redis . del) (NE.nonEmpty matches)

redisIt :: String -> (forall m. (MonadConnect m) => Redis -> m ()) -> Spec
redisIt msg cont = it msg x
  where
    x :: IO ()
    x = runStdoutLoggingT $ filterLogger (\_ _ -> True) $ do
        $logInfo (T.pack msg)
        withRedis redisConfig (\redis -> finally (cont redis) (clearRedisKeys redis))
