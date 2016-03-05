{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE FlexibleInstances #-}

module Data.TypeFingerprintSpec where

import           ClassyPrelude
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import           Data.TypeFingerprint
import           Test.Hspec (Spec)

spec :: Spec
spec = return ()

$(mkManyHasTypeFingerprint
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
