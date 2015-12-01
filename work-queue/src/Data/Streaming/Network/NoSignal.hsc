{-# LANGUAGE CPP #-}
-- | Standard behavior when sending data to a closed socket is to signal the
-- process with SIGPIPE. We'd rather that not happen (e.g., using foreign C
-- libraries that will die on a SIGPIPE). Therefore, when possible on Linux, we
-- use the MSG_NOSIGNAL flag for the send system call.
module Data.Streaming.Network.NoSignal (appWrite') where

#if LINUX

import Data.Streaming.Network
import Network.Socket (Socket(..))
import qualified Data.ByteString as B
import Control.Monad (liftM, when)
import Data.ByteString (ByteString)
import Foreign.C.Types (CInt(..))
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import Foreign.Ptr (Ptr)
import Foreign.C.Types (CSize(..))

#include <sys/socket.h>

foreign import ccall unsafe "send"
  c_send :: CInt -> Ptr a -> CSize -> CInt -> IO CInt

msgNoSignal :: CInt
msgNoSignal = #const MSG_NOSIGNAL

-- | Send data to the socket.  The socket must be connected to a
-- remote socket.  Returns the number of bytes sent. Applications are
-- responsible for ensuring that all data has been sent.
send :: Socket      -- ^ Connected socket
     -> ByteString  -- ^ Data to send
     -> IO Int      -- ^ Number of bytes sent
send (MkSocket s _ _ _ _) xs =
    unsafeUseAsCStringLen xs $ \(str, len) ->
    liftM fromIntegral $
        -- FIXME throwSocketErrorWaitWrite sock "send" $
        c_send s str (fromIntegral len) msgNoSignal

-- | Send data to the socket.  The socket must be connected to a
-- remote socket.  Unlike 'send', this function continues to send data
-- until either all data has been sent or an error occurs.  On error,
-- an exception is raised, and there is no way to determine how much
-- data, if any, was successfully sent.
sendAll :: Socket      -- ^ Connected socket
        -> ByteString  -- ^ Data to send
        -> IO ()
sendAll sock bs = do
    sent <- send sock bs
    when (sent < B.length bs) $ sendAll sock (B.drop sent bs)

appWrite' :: AppData -> ByteString -> IO ()
appWrite' ad bs =
    case appRawSocket ad of
        Nothing -> appWrite ad bs
        Just socket -> sendAll socket bs

#else
import Data.Streaming.Network (appWrite)

appWrite' :: AppData -> ByteString -> IO ()
appWrite' = appWrite
#endif
