{-# LANGUAGE TemplateHaskell #-}

module Main where

import           Control.Concurrent.MVar (newEmptyMVar, takeMVar, putMVar)
import           Control.Concurrent.STM (atomically)
import           Control.Monad
import           Data.IORef (newIORef, atomicModifyIORef', readIORef)
import           Data.TypeFingerprint (mkHasTypeFingerprint)
import           Data.WorkQueue (queueItem)
import qualified Distributed.WorkQueue as WQ

$(mkHasTypeFingerprint =<< [t| Int |])

-- | Some function that workers can run.
fact :: Int -> Int
fact n | n < 0 = error $ "fact: negative input " ++ show n
fact 0 = 1
fact 1 = 1
fact n = n * fact (n - 1)


main :: IO ()
main = do

  let getInitialData = return ()
      calc           = \() x -> return $ fact x

  WQ.runArgs getInitialData calc $ \() queue -> do -- `runArgs` runs `calc` if we're a worker, and the
                                                   -- "work providing" function if we're a master.
    results <- newIORef []                      -- Results sent back to the master go into this list.
    doneMvar <- newEmptyMVar                    -- Put in () when all done

    let processResult i res = do                           -- What the master does when receiving a result:
          xs' <- atomicModifyIORef' results $              -- * Append to results list, atomically, since these
              \xs -> let xs' = (i,res) : xs  in (xs', xs') --   callbacks sometimes run in parallel.
          when (length xs' == 100) $ putMVar doneMvar ()   -- * When we have all results, put a () into `doneMvar`

    forM_ [1..100] $ \i ->                             -- Schedule all the work
      atomically $ queueItem queue i (processResult i) -- and specify the callback for each result.

    takeMVar doneMvar                          -- Wait until all work is done
    putStrLn "Done. Results:"
    print =<< readIORef results
