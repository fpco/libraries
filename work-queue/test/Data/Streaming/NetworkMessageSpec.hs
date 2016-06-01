{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ConstraintKinds #-}
module Data.Streaming.NetworkMessageSpec (spec) where

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Exception
import           Control.Monad
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import           Data.IORef
import           Data.Maybe (isNothing)
import           Data.Store (encode)
import           Data.Streaming.Network
import           Data.Streaming.NetworkMessage
import           Data.TypeHashSpec ()
import           System.IO.Unsafe (unsafePerformIO)
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
    it "can yield a value from the client" $ do
        let client app = do
                nmWrite app True -- Just to make the type unambiguous
                nmRead app
            server app = do
                nmWrite app (1 :: Int)
        finished <- timeout (1000 * 1000 * 2) $
            runClientAndServer client server
        finished `shouldBe` Just 1
    it "doesn't fail when a lazy value takes more than the heartbeat time" $ do
        settings <- defaultNMSettings
        -- Using an MVar instead of directly threadDelaying is to avoid
        -- a "Control.Concurrent.STM.atomically was nested" message.
        valueReady <- newEmptyMVar
        let heartbeatMicros = getNMHeartbeat settings
            client app = do
                nmWrite app True
                nmRead app
            triggerValueReady = do
                threadDelay (heartbeatMicros * 3)
                putMVar valueReady ()
            server app =
                withAsync triggerValueReady $ \_ -> nmWrite app $ unsafePerformIO $ do
                    takeMVar valueReady
                    return (1 :: Int)
        finished <- timeout (heartbeatMicros * 6) $
            runClientAndServer' settings client server
        finished `shouldBe` Just 1
    it "successfully transfers a 10MB bytestring both ways" $
        largeSendTest =<< defaultNMSettings
    it "throws MismatchedHandshakes when client -> server types mismatch" $ do
        expectMismatchedHandshakes () True (Just True) ()
    it "throws MismatchedHandshakes when server -> client types mismatch" $ do
        expectMismatchedHandshakes True () () (Just True)
    it "throws MismatchedHandshakes when types mismatch, even when unqualified names match" $ do
        expectMismatchedHandshakes () LBS.empty BS.empty ()
    it "throws NMConnectionClosed for every nmRead, when server completes while client is waiting" $ do
        let client :: NMApp Int Int IO Bool
            client app = do
                nmRead app `shouldThrow` (== NMConnectionClosed)
                nmRead app `shouldThrow` (== NMConnectionClosed)
                return True
        res <- timeout (1000 * 1000) $ runClientAndServer client (\_ -> return ())
        res `shouldBe` Just True
    -- I'm not perfectly happy with this behaviour.  When fed bogus
    -- data, the first bytes will be interpreted as the message
    -- size. This will likely be a large number, so we'll wait for
    -- more data indefinitely. We should come up with something
    -- better. Maybe a magic number?
    it "throws HeartbeatFailure when fed bogus data" $ do
        let client :: NMApp Bool Int IO ()
            client app = void $ nmRead app
            server app = appWrite (nmAppData app) "bogus data"
        res <- try $ runClientAndServer client server
        res `shouldSatisfy` (`elem` [Left NMHeartbeatFailure, Left NMConnectionDropped])
    it "throws DecodeFailed when fed bogus data of correct length" $ do
        let client :: NMApp Bool Int IO ()
            client app = void $ nmRead app
            bogusData = "bogus data"
            l = BS.length bogusData
            server app = appWrite (nmAppData app) $ encode l `BS.append` "bogus data"
        res <- try $ runClientAndServer client server
        res `shouldSatisfy` isDecodeFailure
    it "throws HeartbeatFailure when heartbeat intervals are too small" $ do
        exitedLateRef <- newIORef False
        settings <- setNMHeartbeat 10 <$> defaultNMSettings
        let both :: NMApp () () IO ()
            both _ = do
                threadDelay (1000 * 200)
                writeIORef exitedLateRef True
        res <- try $ runClientAndServer' settings both both
        -- One of the ends of the connection will throw heartbeat
        -- failure, and the other will see that the connection
        -- dropped.
        res `shouldSatisfy` (`elem` [Left NMHeartbeatFailure, Left NMConnectionDropped])
        exitedLate <- readIORef exitedLateRef
        exitedLate `shouldBe` False

isDecodeFailure :: Either NetworkMessageException a -> Bool
isDecodeFailure (Left (NMDecodeFailure _)) = True
isDecodeFailure _ = False

largeSendTest :: NMSettings -> IO ()
largeSendTest settings = do
     let xs = BS.replicate (10 * 1024 * 1024) 42
         client app = do
             res <- nmRead app
             res `shouldBe` xs
             nmWrite app xs
         server app = do
             nmWrite app xs
             res <- nmRead app
             res `shouldBe` xs
     finished <- timeout (1000 * 1000 * 10) $
         runClientAndServer' settings client server
     when (isNothing finished) $ fail "Client / server needed to be killed"

expectMismatchedHandshakes :: forall a b c d. (Sendable a, Sendable b, Sendable c, Sendable d)
                           => a -> b -> c -> d -> IO ()
expectMismatchedHandshakes _ _ _ _ = do
    exitedLateRef <- newIORef False
    let client (_ :: NMAppData a b) = writeIORef exitedLateRef True
        server (_ :: NMAppData c d) = writeIORef exitedLateRef True
    nmSettings <- defaultNMSettings
    res <- try $ runClientAndServer'' nmSettings client server
    case res of
        Left NMMismatchedHandshakes {} -> return ()
        _ -> fail $ "Expected MismatchedHandshakes, got " ++ show res
    exitedLate <- readIORef exitedLateRef
    exitedLate `shouldBe` False

runClientAndServer :: forall a b r. (Sendable a, Sendable b)
                   => NMApp a b IO r -> NMApp b a IO () -> IO r
runClientAndServer client server = do
    nmSettings <- defaultNMSettings
    runClientAndServer' nmSettings client server

runClientAndServer' :: forall a b r. (Sendable a, Sendable b)
                   => NMSettings -> NMApp a b IO r -> NMApp b a IO () -> IO r
runClientAndServer' = runClientAndServer''

runClientAndServer'' :: forall a b c d r. (Sendable a, Sendable b, Sendable c, Sendable d)
                      => NMSettings -> NMApp a b IO r -> NMApp c d IO () -> IO r
runClientAndServer'' settings client server = do
    serverReady <- newEmptyMVar
    let serverSettings' = setAfterBind (\_ -> putMVar serverReady ()) serverSettings
    result <-
        (takeMVar serverReady >> runTCPClient clientSettings (runNMApp settings client)) `race`
        runTCPServer serverSettings' (runNMApp settings server)
    case result of
        Left x -> return x
        Right () -> fail "Expected client to return a value."

serverSettings :: ServerSettings
serverSettings = serverSettingsTCP port "*"

clientSettings :: ClientSettings
clientSettings = clientSettingsTCP port "localhost"

port :: Int
port = 2015
