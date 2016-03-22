{-# LANGUAGE OverloadedStrings #-}

module Distributed.Heartbeat.Internal where

import Distributed.Types
import FP.Redis
import Data.Monoid

-- | A sorted set of 'WorkerId's that are currently thought to be running. The
-- score of each is its timestamp.
heartbeatActiveKey :: Redis -> ZKey
heartbeatActiveKey r = ZKey $ Key $ redisKeyPrefix r <> "heartbeat:active"

-- | Timestamp for the last time the heartbeat check successfully ran.
heartbeatLastCheckKey :: Redis -> VKey
heartbeatLastCheckKey r = VKey $ Key $ redisKeyPrefix r <> "heartbeat:last-check"

-- | Key used as a simple mutex to signal that some server is currently doing
-- the heartbeat check. Note that this is just to avoid unnecessary work, and
-- correctness doesn't rely on it.
heartbeatMutexKey :: Redis -> VKey
heartbeatMutexKey r = VKey $ Key $ redisKeyPrefix r <> "heartbeat:mutex"
