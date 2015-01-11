{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable #-}
module Data.Streaming.NetworkMessageSpec (spec) where

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Exception
import           Control.Exception.Enclosed (tryAny)
import           Control.Monad
import           Data.Binary
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import           Data.IORef
import           Data.Maybe (isNothing)
import           Data.Streaming.Network
import           Data.Streaming.NetworkMessage
import           Data.Typeable
import qualified Network.Socket as NS
import           System.Timeout (timeout)
import           Test.Hspec

spec :: Spec
spec = do
    it "sends messages both ways" $ do
        let client app = do
                res <- nmRead app
                res `shouldBe` (1 :: Int)
                nmWrite app True
            server app = do
                nmWrite app (1 :: Int)
                res <- nmRead app
                res `shouldBe` True
        finished <- timeout (1000 * 1000) $
            runClientAndServer client server
        when (isNothing finished) $ fail "Client / server needed to be killed"
    it "successfully transfers a 10MB bytestring both ways" $ do
        let xs = BS.replicate (10 * 1024 * 1024) 42
            client app = do
                res <- nmRead app
                res `shouldBe` xs
                nmWrite app xs
            server app = do
                nmWrite app xs
                res <- nmRead app
                res `shouldBe` xs
        finished <- timeout (10 * 1000 * 1000) $
            runClientAndServer client server
        when (isNothing finished) $ fail "Client / server needed to be killed"
    it "throws MismatchedHandshakes when client -> server types mismatch" $ do
        expectMismatchedHandshakes () True (Just True) ()
    it "throws MismatchedHandshakes when server -> client types mismatch" $ do
        expectMismatchedHandshakes True () () (Just True)
    it "throws MismatchedHandshakes when types mismatch, even when unqualified names match" $ do
        expectMismatchedHandshakes () LBS.empty BS.empty ()
    it "throws NMConnectionClosed when server completes while client is waiting" $ do
        res <- try $ runClientAndServer
            (void . nmRead :: NMApp Int Int IO ())
            (\_ -> return ())
        res `shouldBe` Left NMConnectionClosed
    it "throws DecodeFailed when fed bogus data" $ do
        let client :: NMApp Bool Int IO ()
            client app = void $ nmRead app
            server app = appWrite (nmAppData app) "bogus data"
        res <- try $ runClientAndServer client server
        res `shouldBe` Left (DecodeFailure "Unknown encoding for constructor")
    it "throws HeartbeatFailure when heartbeat intervals are too small" $ do
        exitedLateRef <- newIORef False
        let settings = setNMHeartbeat 10 defaultNMSettings
            both :: NMApp () () IO ()
            both _ = do
                threadDelay (1000 * 200)
                writeIORef exitedLateRef True
        res <- try $ runClientAndServer' settings both both
        res `shouldBe` Left HeartbeatFailure
        exitedLate <- readIORef exitedLateRef
        exitedLate `shouldBe` False
    --TODO: add test for (DecodeFailure "demandInput: not enough bytes")

expectMismatchedHandshakes :: forall a b c d. (Binary a, Binary b, Binary c, Binary d, Typeable a, Typeable b, Typeable c, Typeable d)
                           => a -> b -> c -> d -> IO ()
expectMismatchedHandshakes _ _ _ _ = do
    exitedLateRef <- newIORef False
    let client (_ :: NMAppData a b) = writeIORef exitedLateRef True
        server (_ :: NMAppData c d) = writeIORef exitedLateRef True
    res <- try $ runClientAndServer' defaultNMSettings client server
    case res of
        Left MismatchedHandshakes {} -> return ()
        _ -> fail $ "Expected MismatchedHandshakes, got " ++ show res
    exitedLate <- readIORef exitedLateRef
    exitedLate `shouldBe` False

runClientAndServer :: forall a b. (Binary a, Binary b, Typeable a, Typeable b)
                   => NMApp a b IO () -> NMApp b a IO () -> IO ()
runClientAndServer = runClientAndServer' defaultNMSettings

runClientAndServer' :: forall a b c d. (Binary a, Binary b, Binary c, Binary d, Typeable a, Typeable b, Typeable c, Typeable d)
                    => NMSettings -> NMApp a b IO () -> NMApp c d IO () -> IO ()
runClientAndServer' settings client server = void $
    (waitForSocket >> runTCPClient clientSettings (runNMApp settings client)) `race`
    runTCPServer serverSettings (runNMApp settings server)

-- Repeatedly attempts to connect to the test socket, and returns once
-- a connection succeeds.
waitForSocket :: IO ()
waitForSocket = loop (10 :: Int)
  where
    loop 0 = fail "Ran out of waitForSocket retries, indicating that the server from the prior test likely didn't exit."
    loop n = do
        eres <- tryAny $ getSocketFamilyTCP host port NS.AF_UNSPEC
        case eres of
            Left _ -> do
                threadDelay (20 * 1000)
                loop (n - 1)
            Right (socket, _) ->
                NS.close socket

serverSettings :: ServerSettings
serverSettings = serverSettingsTCP port "*"

clientSettings :: ClientSettings
clientSettings = clientSettingsTCP port host

port :: Int
port = 2015

host :: BS.ByteString
host = "localhost"
