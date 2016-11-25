module TcpKeepalive ( setTcpKeepalive ) where

import           System.Posix.Types (Fd)
import           Foreign.C.Types
import           Foreign.Marshal.Alloc
import           Foreign.Storable
import           Foreign.Ptr
import           Network.Socket.Internal (throwSocketErrorIfMinus1Retry_)
import           System.Posix.Types (Fd(..))
import           Control.Monad

#include "sys/socket.h"
#include "sys/types.h"
#include "netinet/tcp.h"
#include "arpa/inet.h"

foreign import ccall unsafe "sys/socket.h setsockopt"
    c_setsockopt :: CInt -> CInt -> CInt -> Ptr CInt -> CInt -> IO CInt

foreign import ccall unsafe "sys/socket.h getsockopt"
    c_getsockopt :: CInt -> CInt -> CInt -> Ptr CInt -> Ptr CInt -> IO CInt

setTcpKeepalive :: Fd -> IO ()
setTcpKeepalive (Fd fd) = do
  alloca $ \keepalive -> do
    poke keepalive 1
    setOpt "SO_KEEPALIVE" (#const SOL_SOCKET) (#const SO_KEEPALIVE) keepalive
  alloca $ \tcp_keepcnt -> do
    poke tcp_keepcnt 2
    setOpt "TCP_KEEPCNT" (#const SOL_TCP) (#const TCP_KEEPCNT) tcp_keepcnt
  alloca $ \tcp_keepidle -> do
    poke tcp_keepidle 2
    setOpt "TCP_KEEPIDLE" (#const SOL_TCP) (#const TCP_KEEPIDLE) tcp_keepidle
  alloca $ \tcp_keepintvl -> do
    poke tcp_keepintvl 4
    setOpt "TCP_KEEPINTVL" (#const SOL_TCP) (#const TCP_KEEPINTVL) tcp_keepintvl

  where
    setOpt s level opt ptr = do
      val0 <- peek ptr
      throwSocketErrorIfMinus1Retry_ ("setTcpKeepalive.setsockopt." ++ s) $
        c_setsockopt fd level opt ptr (fromIntegral (sizeOf (undefined :: CInt)))
      alloca $ \optlen -> do
        poke optlen (fromIntegral (sizeOf (undefined :: CInt)))
        throwSocketErrorIfMinus1Retry_ ("setTcpKeepalive.getsockopt." ++ s) $
          c_getsockopt fd level opt ptr optlen
      val1 <- peek ptr
      when (val0 /= val1) $ do
        fail ("Got different values: " ++ show val0 ++ " and then " ++ show val1)