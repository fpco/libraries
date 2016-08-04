{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE TemplateHaskell #-}

module FP.ThreadFileLogger
    -- The other definitions in this file could be useful, and would
    -- likely be exposed.  For now I'm just keeping it as a minimal
    -- list of things that are actually used.
    ( LogTag(..)
    , runThreadFileLoggingT
    , logNest
    , logExceptions
    , getLogTag
    , setLogTag
    , withLogTag
    , filterThreadLogger
    , logIO
    , logIODebugS
    , logIOInfoS
    , logIOWarnS
    , logIOErrorS
    , logIOOtherS
    ) where

import ClassyPrelude hiding (catch)
import Control.Monad.Base (MonadBase(liftBase))
import Control.Monad.Logger (LogSource, LogLevel(LevelInfo), LoggingT, filterLogger, runStdoutLoggingT, logDebugS, logInfoS, logWarnS, logErrorS, logOtherS)
import Control.Monad.Trans.Control (MonadBaseControl)
import Language.Haskell.TH (Q, Exp)

-- TODO: Use weak pointers for thread IDs, as otherwise threads can't
-- be GCed.

newtype LogTag = LogTag { unLogTag :: Text }
    deriving (Eq, Ord, Show, IsString)

-- Default to using standard stdout logging and omitting the effect of
-- this library.
#ifndef ENABLE_THREAD_FILE_LOGGER

runThreadFileLoggingT :: (MonadBaseControl IO m, MonadIO m) => LoggingT m a -> m a
runThreadFileLoggingT = runStdoutLoggingT . defaultFiltering

logNest :: MonadBaseControl IO m => LogTag -> m a -> m a
logNest _ = id

logExceptions :: MonadBaseControl IO m => Text -> m a -> m a
logExceptions _ = id

getLogTag :: MonadBase IO m => m LogTag
getLogTag = return $ LogTag "ThreadFileLogger disabled"

setLogTag :: MonadBase IO m => LogTag -> m ()
setLogTag _ = return ()

withLogTag :: MonadBaseControl IO m => LogTag -> m a -> m a
withLogTag _ = id

filterThreadLogger
    :: MonadBaseControl IO m
    => (LogSource -> LogLevel -> Bool) -> m a -> m a
filterThreadLogger _ = id

logIO :: MonadBase IO m => LoggingT IO a -> m a
logIO = liftBase . runStdoutLoggingT . defaultFiltering

-- Instead of jamming tons of debug info into stdout, just show info, warnings, and errors
defaultFiltering :: LoggingT m a -> LoggingT m a
defaultFiltering = filterLogger (\_ l -> l >= LevelInfo)

#else

globalLogTags :: IORef (Map ThreadId LogTag)
globalLogTags = unsafePerformIO (newIORef mempty)
{-# NOINLINE globalLogTags #-}

runThreadFileLoggingT :: (MonadBaseControl IO m, MonadIO m) => LoggingT m a -> m a
runThreadFileLoggingT l = l `runLoggingT` output
  where
    output loc src lvl msg = do
        mlogFunc <- getLogFunc
        (fromMaybe defaultLogFunc mlogFunc) loc src lvl msg

internalInfo :: MonadBase IO m => LogTag -> Text -> m ()
internalInfo tag = liftBase . logFunc . toLogStr
  where
    logFunc = defaultLogFunc' tag defaultLoc "ThreadFileLogger" LevelInfo

defaultLogFunc :: LogFunc
defaultLogFunc loc src lvl msg = do
    logTag <- getLogTag
    defaultLogFunc' logTag loc src lvl msg

defaultLogFunc' :: LogTag -> LogFunc
defaultLogFunc' tag loc src lvl msg = do
    pid <- getProcessID
    mh <- getLogHandle (fpFromText ("logs/" ++ tshow pid ++ "-" ++ unLogTag tag))
    forM_ mh $ \h -> do
        now <- getCurrentTime
        hPut h $ fromLogStr $
            "<<" ++ toLogStr (show now) ++ ">> " ++
            defaultLogStr loc src lvl msg
        hFlush h

defaultLoc :: Loc
defaultLoc = Loc "<unknown>" "<unknown>" "<unknown>" (0,0) (0,0)

logNest :: MonadBaseControl IO m => LogTag -> m a -> m a
logNest (LogTag tag) f = do
    -- TODO: this is a bit inefficient - redundant lookups.
    mold <- lookupRefMap globalLogTags =<< myThreadId
    let new = LogTag $ maybe tag (\(LogTag old) -> old <> "-" <> tag) mold
    old <- maybe defaultLogTag return mold
    internalInfo old $ "Switching log to " <> unLogTag new
    result <- logExceptions' "logNest" new f
    internalInfo old $ "Returned from " <> unLogTag new
    return result

logExceptions :: MonadBaseControl IO m => Text -> m a -> m a
logExceptions src f = do
    tag <- getLogTag
    logExceptions' ("logExceptions " ++ src) tag f

logExceptions' :: MonadBaseControl IO m => Text -> LogTag -> m a -> m a
logExceptions' prefix tag f = control $ \restore ->
    (restore $ setLogTag tag >> f) `catch` \(ex :: SomeException) -> do
         let msg = prefix ++ " " ++ unLogTag tag ++ " caught exception: " <> tshow ex
         internalInfo tag msg `onException` throwIO ex
         throwIO ex

getLogTag :: MonadBase IO m => m LogTag
getLogTag =
    myThreadId >>=
    lookupRefMap globalLogTags >>=
    maybe defaultLogTag return

defaultLogTag :: MonadBase IO m => m LogTag
defaultLogTag = LogTag . tshow <$> myThreadId

setLogTag :: MonadBase IO m => LogTag -> m ()
setLogTag tag = do
    tid <- myThreadId
    insertRefMap globalLogTags tid tag

withLogTag :: MonadBaseControl IO m => LogTag -> m a -> m a
withLogTag tag f =
    bracket
        (do old <- getLogTag
            setLogTag tag
            return old)
        setLogTag
        (\_ -> f)

filterThreadLogger
    :: MonadBaseControl IO m
    => (LogSource -> LogLevel -> Bool) -> m a -> m a
filterThreadLogger p = modifyLogFunc (applyFiltering . fromMaybe defaultLogFunc)
  where
    applyFiltering f loc src lvl msg =
        when (p src lvl) $ f loc src lvl msg

logIO :: MonadBase IO m => LoggingT IO a -> m a
logIO f = liftBase . runLoggingT f . fromMaybe defaultLogFunc =<< getLogFunc

-- Global map of ThreadId to logging functions.  These functions will
-- be invoked when 'runThreadFileLoggingT' is used.

globalLogFuncs :: IORef (Map ThreadId LogFunc)
globalLogFuncs = unsafePerformIO (newIORef mempty)
{-# NOINLINE globalLogFuncs #-}

withLogFunc
    :: MonadBaseControl IO m
    => LogFunc -> m a -> m a
withLogFunc x = modifyLogFunc (\_ -> x)

modifyLogFunc
    :: MonadBaseControl IO m
    => (Maybe LogFunc -> LogFunc) -> m a -> m a
modifyLogFunc f m = do
    tid <- myThreadId
    let update = (\_ old _ -> f (Just old))
    mold <- insertLookupRefMap globalLogFuncs update tid (f Nothing)
    let rollback =
            case mold of
                Nothing -> deleteRefMap globalLogFuncs tid
                Just old -> insertRefMap globalLogFuncs tid old
    -- TODO: catch and log exceptions
    m `finally` rollback

getLogFunc :: MonadBase IO m => m (Maybe LogFunc)
getLogFunc = lookupRefMap globalLogFuncs =<< myThreadId

-- Global map of file handles, kept open to the end of execution.

-- TODO: Have them time out after a while?  Alternatively, keep them
-- on a bounded priority queue where removal closes them.

globalFileMap :: IORef (Map FilePath Handle)
globalFileMap = unsafePerformIO (newIORef mempty)
{-# NOINLINE globalFileMap #-}

getLogHandle :: FilePath -> IO (Maybe Handle)
getLogHandle fp = returnHandleOr $ do
    liftBase $ createDirectoryIfMissing True (takeDirectory fp)
    eres <- tryAny $ openFile (fpToString fp) AppendMode
    case eres of
        -- Since the file open succeeded, the handle really
        -- shouldn't be in the map.
        Right h -> do
            insertRefMap globalFileMap fp h
            return (Just h)
        -- File may have been opened by a different thread,
        -- concurrently.
        Left err -> returnHandleOr $ do
            _ <- tryAny $ appendFile "logs/tfl-errors" ('\n' : show err)
            return Nothing
  where
    returnHandleOr :: IO (Maybe Handle) -> IO (Maybe Handle)
    returnHandleOr f =
        maybe f (return . Just) =<< lookupRefMap globalFileMap fp

-- Ref map utils

insertRefMap :: (MonadBase IO m, Ord k) => IORef (Map k a) -> k -> a -> m ()
insertRefMap ref k a =
    liftBase $ atomicModifyIORef' ref $ \mp -> (insertMap k a mp, ())

deleteRefMap :: (MonadBase IO m, Ord k) => IORef (Map k a) -> k -> m ()
deleteRefMap ref k =
    liftBase $ atomicModifyIORef' ref $ \mp -> (deleteMap k mp, ())

lookupRefMap :: (MonadBase IO m, Ord k) => IORef (Map k a) -> k -> m (Maybe a)
lookupRefMap ref k =
    liftBase $ lookup k <$> readIORef ref

insertLookupRefMap :: (MonadBase IO m, Ord k) => IORef (Map k a) -> (k -> a -> a -> a) -> k -> a -> m (Maybe a)
insertLookupRefMap ref f k x = liftBase
    atomicModifyIORef' ref $ \mp ->
        let (mx, mp') = insertLookupWithKey f k x mp
         in (mp', mx)

modifyRefMap :: (MonadBase IO m, Ord k) => IORef (Map k a) -> (k -> a -> Maybe a) -> k -> m (Maybe a)
modifyRefMap ref f k =
    atomicModifyIORef' ref $ \mp ->
        let (mx, mp') = updateLookupWithKey f k mp
         in (mp', mx)

#endif

-- Utilities for debugging without explicit MonadLogger

logIODebugS :: Q Exp
logIODebugS = [|\a b -> logIO ($(logDebugS) a b) |]

logIOInfoS :: Q Exp
logIOInfoS = [|\a b -> logIO ($(logInfoS) a b) |]

logIOWarnS :: Q Exp
logIOWarnS = [|\a b -> logIO ($(logWarnS) a b) |]

logIOErrorS :: Q Exp
logIOErrorS = [|\a b -> logIO ($(logErrorS) a b) |]

logIOOtherS :: Q Exp
logIOOtherS = [|\a b c -> logIO ($(logOtherS) a b c) |]
