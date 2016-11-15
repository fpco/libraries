{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DataKinds #-}
module Data.TypeHashSpec where

import           ClassyPrelude
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import           Data.TypeHash
import           Data.TypeHash.Orphans ()
import           Test.Hspec (Spec)

spec :: Spec
spec = return ()

$(mkManyHasTypeHash
    [ [t| Bool |]
    , [t| Maybe Bool |]
    , [t| LBS.ByteString |]
    , [t| BS.ByteString |]
    , [t| Vector [Int] |]
    , [t| Int |]
    , [t| [Int] |]
    , [t| String |]
    , [t| [Either Int String] |]
    ])
