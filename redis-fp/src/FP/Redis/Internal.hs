{-# LANGUAGE NoImplicitPrelude, OverloadedStrings, ScopedTypeVariables, TypeFamilies,
             DeriveDataTypeable, FlexibleContexts, FlexibleInstances, RankNTypes, GADTs,
             ConstraintKinds, NamedFieldPuns #-}

-- | Redis internal utilities.

module FP.Redis.Internal where

import Blaze.ByteString.Builder (Builder)
import qualified Blaze.ByteString.Builder as Builder
import qualified Blaze.ByteString.Builder.Char.Utf8 as Builder
import ClassyPrelude.Conduit hiding (Builder)
import Control.Concurrent.STM (retry)
import Control.Exception.Lifted (BlockedIndefinitelyOnMVar(..), BlockedIndefinitelyOnSTM(..))
import Data.DList (DList)
import qualified Data.DList as DList

import FP.Redis.Types.Internal
import Control.Concurrent.STM.TSQueue

-- | Like 'takeMVar', but convert wrap 'BlockedIndefinitelyOnMVar' exception in other exception.
takeMVarE :: (MonadBaseControl IO m) => (SomeException -> RedisException) -> MVar a -> m a
takeMVarE exception mvar =
    catch (takeMVar mvar)
          (\e@BlockedIndefinitelyOnMVar -> throwIO (exception (toException e)))

-- | Make a command request
makeCommand :: (Result a) => ByteString -> [ByteString] -> CommandRequest a
makeCommand cmd args =
    CommandRequest (Command (renderRequest (encodeArg cmd:args)))

-- | Add a request to the requests queue.  Blocks if waiting for too many responses.
sendRequest :: (MonadCommand m) => Connection -> Request -> m ()
sendRequest Connection{connectionInfo_, connectionRequestQueue, connectionPendingResponseQueue}
            request =
    catch (atomically addRequest)
          (\BlockedIndefinitelyOnSTM -> throwIO DisconnectedException)
  where
    addRequest :: STM ()
    addRequest = do
        lr <- lengthTSQueue connectionRequestQueue
        when (lr >= connectRequestsPerBatch connectionInfo_ * 2) retry
        lp <- lengthTSQueue connectionPendingResponseQueue
        when (lp >= connectMaxPendingResponses connectionInfo_) retry
        writeTSQueue connectionRequestQueue request

-- | Convert 'CommandRequest' to a list of 'Request's and an action to get the response.
commandToRequestPair :: (MonadIO m, MonadIO n)
                     => CommandRequest a
                     -> m (DList Request, n a)
commandToRequestPair (CommandRequest command) = do
    respMVar <- liftIO newEmptyMVar
    let getResponse = do
            resp <- takeMVarE (const DisconnectedException) respMVar
            case resp of
                Error msg -> liftIO (throwM (CommandException msg))
                _ -> case decodeResponse resp of
                    Just result -> return result
                    Nothing -> throwM $ DecodeResponseException
                        (Builder.toByteString $ requestBuilder request)
                        resp
        request = command (const (putMVar respMVar))
    return (DList.singleton request, liftIO getResponse)
commandToRequestPair (CommandPure val) =
    return (DList.empty, return val)
commandToRequestPair (CommandAp a b) = do
    (aReqs,aAction) <- commandToRequestPair a
    (bReqs,bAction) <- commandToRequestPair b
    let resultAction = do
            aResult <- aAction
            bResult <- bAction
            return (aResult bResult)
    return (aReqs ++ bReqs, resultAction)

-- | Render list to Redis request protocol (adapted from hedis)
renderRequest :: [ByteString] -> Builder
renderRequest req = concat (argCnt:args)
  where
    argCnt = star
             ++ Builder.fromShow (length req)
             ++ crlf
    args   = map renderArg req
    renderArg arg = dollar
                    ++ argLen arg
                    ++ crlf
                    ++ Builder.fromByteString arg
                    ++ crlf
    argLen = Builder.fromShow . length
    crlf = Builder.copyByteString "\r\n"
    star = Builder.copyByteString "*"
    dollar = Builder.copyByteString "$"
