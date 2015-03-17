{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}
{-# LANGUAGE DeriveDataTypeable #-}

-- | More flexible versions of "Control.Exception" masking, including
-- support for masking / unmasking in STM.
--
-- This module should be moved out of the work-queue package.
module Control.Exception.Mask
    ( RestoreAction(..)
    -- * Exception masking in IO
    , mask, uninterruptibleMask, restore
    -- * Exception masking in STM
    , maskSTM, uninterruptibleMaskSTM, restoreSTM
    , getMaskingStateSTM
    )
    where

import Control.Exception hiding (mask, uninterruptibleMask)
import Data.Typeable (Typeable)
import GHC.Base (IO(IO), maskAsyncExceptions#, unmaskAsyncExceptions#,
                 maskUninterruptible#, getMaskingState#)
import GHC.Conc (STM(STM))

data RestoreAction
    = NoAction
    | Unblock
    | Block
    deriving (Eq, Show, Typeable)

mask :: (RestoreAction -> IO a) -> IO a
mask io = do
    b <- getMaskingState
    case b of
        Unmasked              -> block (io Unblock)
        MaskedInterruptible   -> io NoAction
        MaskedUninterruptible -> io NoAction

uninterruptibleMask :: (RestoreAction -> IO a) -> IO a
uninterruptibleMask io = do
    b <- getMaskingState
    case b of
        Unmasked              -> blockUninterruptible (io Unblock)
        MaskedInterruptible   -> blockUninterruptible (io Block)
        MaskedUninterruptible -> io NoAction

restore :: RestoreAction -> IO a -> IO a
restore NoAction f = f
restore Unblock f = unblock f
restore Block f = block f

-- | Since STM is transactional, I'm having a hard time thinking of a
-- usecase for this.
maskSTM :: (RestoreAction -> STM a) -> STM a
maskSTM stm = do
    b <- getMaskingStateSTM
    case b of
        Unmasked              -> blockSTM (stm Unblock)
        MaskedInterruptible   -> stm NoAction
        MaskedUninterruptible -> stm NoAction

uninterruptibleMaskSTM :: (RestoreAction -> STM a) -> STM a
uninterruptibleMaskSTM stm = do
    b <- getMaskingStateSTM
    case b of
        Unmasked              -> blockUninterruptibleSTM (stm Unblock)
        MaskedInterruptible   -> blockUninterruptibleSTM (stm Block)
        MaskedUninterruptible -> stm NoAction

restoreSTM :: RestoreAction -> STM a -> STM a
restoreSTM NoAction f = f
restoreSTM Unblock f = unblockSTM f
restoreSTM Block f = blockSTM f

getMaskingStateSTM :: STM MaskingState
getMaskingStateSTM = STM $ \s ->
    case getMaskingState# s of
        (# s', i #) -> (# s', case i of
                                  0# -> Unmasked
                                  1# -> MaskedUninterruptible
                                  _  -> MaskedInterruptible #)

blockSTM :: STM a -> STM a
blockSTM (STM io) = STM $ maskAsyncExceptions# io

unblockSTM :: STM a -> STM a
unblockSTM = unsafeUnmaskSTM

unsafeUnmaskSTM :: STM a -> STM a
unsafeUnmaskSTM (STM io) = STM $ unmaskAsyncExceptions# io

blockUninterruptibleSTM :: STM a -> STM a
blockUninterruptibleSTM (STM io) = STM $ maskUninterruptible# io

-- These were copied verbatim from Control.Exception

block :: IO a -> IO a
block (IO io) = IO $ maskAsyncExceptions# io

unblock :: IO a -> IO a
unblock = unsafeUnmask

unsafeUnmask :: IO a -> IO a
unsafeUnmask (IO io) = IO $ unmaskAsyncExceptions# io

blockUninterruptible :: IO a -> IO a
blockUninterruptible (IO io) = IO $ maskUninterruptible# io
