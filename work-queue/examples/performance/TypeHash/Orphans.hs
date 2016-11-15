{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DataKinds #-}
module TypeHash.Orphans where

import           ClassyPrelude
import           Data.TypeHash (mkManyHasTypeHash)
import qualified Data.Vector as V

$(mkManyHasTypeHash [ [t| ByteString |]
                    , [t| Double |]
                    , [t| V.Vector Double |]
                    ])
