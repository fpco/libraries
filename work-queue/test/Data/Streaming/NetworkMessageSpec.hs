{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DataKinds #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Data.Streaming.NetworkMessageSpec (spec) where

import           ClassyPrelude
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import           Data.Streaming.Network
import qualified Data.Conduit.Network as CN
import           Data.Streaming.NetworkMessage
import           Test.Hspec hiding (shouldBe)
import qualified Test.Hspec
import qualified Control.Concurrent.Mesosync.Lifted.Safe as Async
import           FP.Redis (MonadConnect)
import           Control.Monad.Trans.Control (control)
import           Control.Concurrent (threadDelay)
import           Data.Void (absurd)
import           Data.Store.TypeHash (mkManyHasTypeHash)
import           Data.Store.TypeHash.Orphans ()

import           TestUtils

shouldBe :: (Eq a, Show a, MonadIO m) => a -> a -> m ()
shouldBe x y = liftIO (Test.Hspec.shouldBe x y)

mkManyHasTypeHash
    [[t|Bool|], [t|ByteString|], [t|Maybe Bool|], [t|Int|], [t|LBS.ByteString|]]

spec :: Spec
spec = do
    loggingIt "sends messages both ways" $ do
        let client app = do
                res <- nmRead app
                res `shouldBe` (1 :: Int)
                nmWrite app True
            server app = do
                nmWrite app (1 :: Int)
                res <- nmRead app
                res `shouldBe` True
        finished :: Either () () <- Async.race
            (liftIO (threadDelay (5 * 1000 * 1000)))
            (runClientAndServer client server)
        when (finished == Left ()) $ fail "Client / server needed to be killed"
    loggingIt "can yield a value from the client" $ do
        let client app = do
                nmWrite app True -- Just to make the type unambiguous
                nmRead app
            server app = do
                nmWrite app (1 :: Int)
        finished :: Either () Int <- Async.race
            (liftIO (threadDelay (1000 * 1000 * 2)))
            (runClientAndServer client server)
        finished `shouldBe` Right 1
    loggingIt "successfully transfers a 10MB bytestring both ways" $
        largeSendTest
    loggingIt "throws MismatchedHandshakes when client -> server types mismatch" $ do
        expectMismatchedHandshakes () True (Just True) ()
    loggingIt "throws MismatchedHandshakes when server -> client types mismatch" $ do
        expectMismatchedHandshakes True () () (Just True)
    loggingIt "throws MismatchedHandshakes when types mismatch, even when unqualified names match" $ do
        expectMismatchedHandshakes () LBS.empty BS.empty ()
    loggingIt "throws NMDecodeFailure when fed bogus data" $ do
        let client :: (MonadConnect m) => NMApp Bool Int m ()
            client app = void $ nmRead app
            bogusData = "bogus data that is longer than message magic"
            server app = liftIO (appWrite (nmAppData app) $ bogusData)
        mb <- try (runClientAndServer client server)
        mb `shouldBe` Left (NMDecodeFailure "nmRead PeekException {peekExBytesFromEnd = 0, peekExMessage = \"Wrong message magic, 7017769799613116258\"}")
    loggingIt "throws NMDecodeFailure when fed too little data" $ do
        let client :: (MonadConnect m) => NMApp Bool Int m ()
            client app = void $ nmRead app
            bogusData = "short data"
            server app = liftIO (appWrite (nmAppData app) $ bogusData)
        mb <- try (runClientAndServer client server)
        mb `shouldBe` Left (NMDecodeFailure "nmRead Couldn't decode: no data")
    loggingIt "one side can terminate" $ do
        let client :: (MonadConnect m) => NMApp () () m ()
            client _ = return ()
        let server :: (MonadConnect m) => NMApp () () m ()
            server _ = return ()
        runClientAndServer client server

largeSendTest :: (MonadConnect m) => m ()
largeSendTest = do
     let xs = BS.replicate (10 * 1024 * 1024) 42
         client app = do
             res <- nmRead app
             res `shouldBe` xs
             nmWrite app xs
         server app = do
             nmWrite app xs
             res <- nmRead app
             res `shouldBe` xs
     finished :: Either () () <- Async.race
        (liftIO (threadDelay (1000 * 1000 * 10)))
        (runClientAndServer client server)
     when (finished == Left ()) $ fail "Client / server needed to be killed"

expectMismatchedHandshakes :: forall a b c d m. (MonadConnect m, Sendable a, Sendable b, Sendable c, Sendable d)
                           => a -> b -> c -> d -> m ()
expectMismatchedHandshakes _ _ _ _ = do
    exitedLateRef <- newIORef False
    let client (_ :: NMAppData a b) = writeIORef exitedLateRef True
        server (_ :: NMAppData c d) = writeIORef exitedLateRef True
        -- Ignore mismatched handshakes exceptions in the server handler
        -- so that they don't get displayed
        wrapServer m = do
            mbExc :: Either NetworkMessageException () <- try m
            case mbExc of
                Left NMMismatchedHandshakes{} -> return ()
                Left err -> throwIO err
                Right () -> return ()
    res <- try $ runClientAndServer_ client wrapServer server
    case res of
        Left NMMismatchedHandshakes {} -> return ()
        _ -> fail $ "Expected MismatchedHandshakes, got " ++ show res
    exitedLate <- readIORef exitedLateRef
    exitedLate `shouldBe` False

runClientAndServer :: forall m clientSends serverSends a.
       (MonadConnect m, Sendable clientSends, Sendable serverSends)
    => NMApp clientSends serverSends m a
    -> NMApp serverSends clientSends m ()
    -> m a
runClientAndServer client = runClientAndServer_ client id

runClientAndServer_ :: forall m a b c d e.
       (MonadConnect m, Sendable a, Sendable b, Sendable c, Sendable d)
    => NMApp a b m e
    -> (m () -> m ()) -- ^ Wrapper for the server handler, useful to hide exception
    -> NMApp c d m ()
    -> m e
runClientAndServer_ client serverWrapper server = do
    settings <- defaultNMSettings
    (ss, getServerPort) <- liftIO (getPortAfterBind (CN.serverSettings 0 "127.0.0.1"))
    fmap (either absurd id) $ Async.race
        (CN.runGeneralTCPServer ss (serverWrapper . runNMApp settings server))
        (do port <- liftIO getServerPort
            let cs = CN.clientSettings port "127.0.0.1"
            control $ \invert -> do
                runTCPClient cs (invert . runNMApp settings client))
