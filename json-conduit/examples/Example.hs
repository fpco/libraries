{-# LANGUAGE OverloadedStrings #-}

import Data.Conduit
import Data.Conduit.Json.Encode
import Data.Scientific
import Data.Text
import Data.Conduit.ByteString.Builder (builderToByteString)
import qualified Data.Conduit.List as CL
import Data.ByteString (ByteString)

run :: IO [ByteString]
run =
    CL.sourceList [("foo", 1), ("bar", 2)] $=
    renderBytes renderKeyVals $$
    CL.consume

runSkipFirst :: IO [ByteString]
runSkipFirst =
    CL.sourceList [("foo", 1), ("bar", 2)] $$
    renderBytes (bindConsumer await (\_ -> renderKeyVals)) $=
    CL.consume

renderKeyVals :: Monad m => ValueRenderer (Text, Scientific) m ()
renderKeyVals =
    array await $ \(k, v) ->
        object
            [ ("key", string (yield k))
            , ("val", number v)
            ]
