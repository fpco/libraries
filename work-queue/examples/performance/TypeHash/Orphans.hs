{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DataKinds #-}
module TypeHash.Orphans where

import           ClassyPrelude
import           Data.Store.TypeHash (mkManyHasTypeHash)
import qualified Data.Vector as V
import           Distributed.Stateful.Master (SlaveProfiling (..))

$(mkManyHasTypeHash [ [t| ByteString |]
                    , [t| Double |]
                    , [t| V.Vector Double |]
                    , [t| SlaveProfiling |]
                    ])
