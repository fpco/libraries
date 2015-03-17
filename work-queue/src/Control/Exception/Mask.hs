{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}
{-# LANGUAGE DeriveDataTypeable #-}

-- | More flexible versions of "Control.Exception" masking, including
-- support for masking / unmasking in STM.
--
-- This module should be moved out of the work-queue package.
module Control.Exception.Mask
    ( OldMaskingState(..), OldUninterruptibleMaskingState(..)
    -- * Exception masking in IO
    , mask, unmask, uninterruptibleMask, uninterruptibleUnmask
    -- * Exception masking in STM
    , maskSTM, unmaskSTM, uninterruptibleMaskSTM, uninterruptibleUnmaskSTM
    , getMaskingStateSTM
    )
    where

import Control.Exception hiding (mask, uninterruptibleMask)
import Data.Typeable (Typeable)
import GHC.Base (IO(IO), maskAsyncExceptions#, unmaskAsyncExceptions#,
                 maskUninterruptible#, getMaskingState#)
import GHC.Conc (STM(STM))

newtype OldMaskingState = OldMaskingState
    { unOldMaskingState :: MaskingState }
    deriving (Eq, Show, Typeable)

mask :: (OldMaskingState -> IO a) -> IO a
mask io = do
    b <- getMaskingState
    let f = io (OldMaskingState b)
    case b of
        Unmasked              -> block f
        MaskedInterruptible   -> f
        MaskedUninterruptible -> f

unmask :: OldMaskingState -> IO a -> IO a
unmask b f =
    case unOldMaskingState b of
        Unmasked              -> unblock f
        MaskedInterruptible   -> f
        MaskedUninterruptible -> f

-- | Since STM is transactional, I'm having a hard time thinking of a
-- usecase for this.
maskSTM :: (OldMaskingState -> STM a) -> STM a
maskSTM stm = do
    b <- getMaskingStateSTM
    let f = stm (OldMaskingState b)
    case b of
        Unmasked              -> blockSTM f
        MaskedInterruptible   -> f
        MaskedUninterruptible -> f

unmaskSTM :: OldMaskingState -> STM a -> STM a
unmaskSTM b f =
    case unOldMaskingState b of
        Unmasked              -> unblockSTM f
        MaskedInterruptible   -> f
        MaskedUninterruptible -> f

newtype OldUninterruptibleMaskingState = OldUninterruptibleMaskingState
    { unOldUninterruptibleMaskingState :: MaskingState }
    deriving (Eq, Show, Typeable)

uninterruptibleMask :: (OldUninterruptibleMaskingState -> IO a) -> IO a
uninterruptibleMask io = do
    b <- getMaskingState
    let f = io (OldUninterruptibleMaskingState b)
    case b of
        Unmasked              -> blockUninterruptible f
        MaskedInterruptible   -> blockUninterruptible f
        MaskedUninterruptible -> f

uninterruptibleUnmask :: OldUninterruptibleMaskingState -> IO a -> IO a
uninterruptibleUnmask b f =
    case unOldUninterruptibleMaskingState b of
        Unmasked              -> unblock f
        MaskedInterruptible   -> block f
        MaskedUninterruptible -> f

uninterruptibleMaskSTM :: (OldUninterruptibleMaskingState -> STM a) -> STM a
uninterruptibleMaskSTM stm = do
    b <- getMaskingStateSTM
    let f = stm (OldUninterruptibleMaskingState b)
    case b of
        Unmasked              -> blockUninterruptibleSTM f
        MaskedInterruptible   -> blockUninterruptibleSTM f
        MaskedUninterruptible -> f

uninterruptibleUnmaskSTM :: OldUninterruptibleMaskingState -> STM a -> STM a
uninterruptibleUnmaskSTM b f =
    case unOldUninterruptibleMaskingState b of
        Unmasked              -> unblockSTM f
        MaskedInterruptible   -> blockSTM f
        MaskedUninterruptible -> f

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
