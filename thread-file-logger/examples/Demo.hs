{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}

import FP.ThreadFileLogger
import Control.Concurrent.Lifted
import Control.Monad.Logger
import Control.Monad.IO.Class (liftIO)

main = do
    runThreadFileLoggingT $ logNest "main" $ do
        $logDebug "Main thread"
        logNest "nested" $ do
            $logDebug "Nested"
        fork $ logNest "forked-1" $ do
            threadDelay (10 * 1000)
            $logDebug "Forked thread 10ms later"
        fork $ logNest "forked-2" $ do
            $logDebug "Fork #2"
            threadDelay (50 * 1000)
            $logDebug "Forked thread 50ms later"
        fork $ $logDebug "Untagged log"
        fork $ logNest "filter-example" $ filterThreadLogger (\_ l -> l >= LevelInfo) $ do
            $logDebug "Omitted"
            $logInfo "Included"
    runThreadFileLoggingT $ do
        $logDebug "Main thread again!"
        threadDelay (100 * 2000)
        $logDebug "Main thread exiting"
