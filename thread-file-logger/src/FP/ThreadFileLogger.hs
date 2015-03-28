{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE CPP #-}

module FP.ThreadFileLogger
    -- The other definitions in this file could be useful, and would
    -- likely be exposed.  For now I'm just keeping it as a minimal
    -- list of things that are actually used.
    ( LogTag(..)
    , runThreadFileLoggingT
    , logNest
    , getLogTag
    , setLogTag
    , withLogTag
    , filterThreadLogger
    ) where

import ClassyPrelude hiding (catch)
import Control.Concurrent.Lifted (ThreadId, myThreadId)
import Control.Exception.Lifted (catch)
import Control.Monad.Base (MonadBase(liftBase))
import Control.Monad.Logger (LogSource, LogLevel(LevelInfo), LoggingT, runLoggingT, defaultLogStr, runStdoutLoggingT)
import Control.Monad.Trans.Control (MonadBaseControl)
import Language.Haskell.TH (Loc(..))
import System.Directory (createDirectoryIfMissing)
import System.IO (IOMode(AppendMode), openFile, hFlush)
import System.IO.Unsafe (unsafePerformIO)
import System.Log.FastLogger (LogStr, toLogStr, fromLogStr)
import System.Posix.Process (getProcessID)

-- TODO: Use weak pointers for thread IDs, as otherwise threads can't
-- be GCed.

type LogFunc = Loc -> LogSource -> LogLevel -> LogStr -> IO ()

newtype LogTag = LogTag { unLogTag :: Text }
    deriving (Eq, Ord, Show, IsString)

-- Default to using standard stdout logging and omitting the effect of
-- this library.
#if 1

runThreadFileLoggingT :: (MonadBaseControl IO m, MonadIO m) => LoggingT m a -> m a
runThreadFileLoggingT = runStdoutLoggingT

logNest :: (MonadBaseControl IO m, MonadIO m) => LogTag -> m a -> m a
logNest _ = id

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

internalInfo :: MonadBase IO m => Bool -> LogTag -> Text -> m ()
internalInfo omitIfInitial tag = liftBase . logFunc . toLogStr
  where
    logFunc = defaultLogFunc' omitIfInitial tag defaultLoc "ThreadFileLogger" LevelInfo

defaultLogFunc :: LogFunc
defaultLogFunc loc src lvl msg = do
    logTag <- getLogTag
    defaultLogFunc' False logTag loc src lvl msg

defaultLogFunc' :: Bool -> LogTag -> LogFunc
defaultLogFunc' omitIfInitial tag loc src lvl msg = do
    pid <- getProcessID
    mh <- getLogHandle omitIfInitial (fpFromText ("logs/" ++ tshow pid ++ "-" ++ unLogTag tag))
    forM_ mh $ \h -> do
        now <- getCurrentTime
        hPut h $ fromLogStr $
            "<<" ++ toLogStr (show now) ++ ">> " ++
            defaultLogStr loc src lvl msg
        hFlush h

defaultLoc :: Loc
defaultLoc = Loc "<unknown>" "<unknown>" "<unknown>" (0,0) (0,0)

logNest :: (MonadBaseControl IO m, MonadIO m) => LogTag -> m a -> m a
logNest (LogTag tag) f = do
    -- TODO: this is a bit inefficient - redundant lookups.
    mold <- lookupRefMap globalLogTags =<< myThreadId
    let new = LogTag $ maybe tag (\(LogTag old) -> old <> "-" <> tag) mold
    old <- maybe defaultLogTag return mold
    internalInfo True old $ "Switching log to " <> unLogTag new
    result <- withLogTag new $ f `catch` \(ex :: SomeException) -> do
        internalInfo False new ("logNest caught exception: " <> tshow ex)
            `onException` liftBase (throwIO ex)
        liftBase (throwIO ex)
    internalInfo True old $ "Returned from " <> unLogTag new
    return result

getLogTag :: MonadBase IO m => m LogTag
getLogTag =
    myThreadId >>=
    lookupRefMap globalLogTags >>=
    maybe defaultLogTag return

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

defaultLogTag :: MonadBase IO m => m LogTag
defaultLogTag = LogTag . tshow <$> myThreadId

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

-- When the 'Bool' is 'True', this indicates that the file shouldn't
-- be newly opened.  When this is 'True' and no the file hasn't been
-- newly opened, then 'Nothing' is returned.
--
-- NOTE: If, in the future, we hClose handles, we shouldn't clear the
-- record of the fact that we already wrote to them.
getLogHandle :: Bool -> FilePath -> IO (Maybe Handle)
getLogHandle omitIfInitial fp = returnHandleOr $ do
    liftBase $ createDirectoryIfMissing True (fpToString (directory fp))
    if omitIfInitial
        then return Nothing
        else do
            eres <- tryAny $ openFile (fpToString fp) AppendMode
            case eres of
                -- Since the file open succeeded, the handle really
                -- shouldn't be in the map.
                Right h -> do
                    insertRefMap globalFileMap fp h
                    return (Just h)
                -- File may have been opened by a different thread,
                -- concurrently.
                Left err -> returnHandleOr $ liftBase $ throwIO err
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
