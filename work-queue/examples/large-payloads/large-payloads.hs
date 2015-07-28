module Main where

import           Control.Concurrent (threadDelay)
import           Control.Monad
import qualified Data.ByteString as BS
import qualified Distributed.WorkQueue as WQ

main :: IO ()
main = do
  -- No initial data, no calculation.
  let getInitialData = return ()
      calc           = \() _ -> putStrLn "calc invoked"
  WQ.runArgs getInitialData calc $ \() queue -> do
    forM_ [0..5] $ \n -> do
      putStrLn $ "Sending work item " ++ show n
      [()] <- WQ.mapQueue queue [BS.replicate (1000 * 1000 * 100) n]
      putStrLn "Waiting a second"
      threadDelay (1000 * 1000)
