{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
-- | The War card game. For more information:
--
-- http://en.wikipedia.org/wiki/War_%28card_game%29
--
-- Our tweak is that we require each player start off with the same
-- distribution of cards for fairness. However, choice of ordering is
-- available, which in fact determines the outcome of the game.
module Data.War
    ( Deck
    , Winner (..)
    , simpleDeck
    , randomDeck
    , validDeck
    , play
    , mutateDeck
    , score
    ) where

import           Control.Monad.Primitive          (PrimMonad, PrimState)
import           Control.Monad.ST
import           Data.Data                        (Data, Typeable)
import           Data.Monoid                      ((<>))
import           Data.Mutable.Deque
import           Data.Vector                      (Vector)
import           Data.Vector.Algorithms.Insertion (sort)
import qualified Data.Vector.Generic              as V
import qualified Data.Vector.Unboxed              as U
import qualified Data.Vector.Unboxed.Mutable      as VM
import           Data.Word                        (Word8)
import           GHC.Generics                     (Generic)
import           System.Random.MWC                (Gen, uniformR)
import           System.Random.MWC.Distributions  (uniformShuffle)

-- | A single card. Must be a value from 2 to 14, with 14 repesenting Ace, 13
-- King, and so on.
type Card = Word8

-- | A starting deck, consisting of 26 cards, two of each rank.
type Deck = U.Vector Card

-- | The simplest starting deck, with 2-Ace followed by 2-Ace.
simpleDeck :: Deck
simpleDeck =
    x <> x
  where
    x = V.enumFromTo 2 14

-- | Generating a random starting deck.
randomDeck :: PrimMonad m => Gen (PrimState m) -> m Deck
randomDeck = uniformShuffle simpleDeck

-- | Is a deck valid
validDeck :: Deck -> Bool
validDeck v =
    V.modify sort v == sortedSimpleDeck
  where
    sortedSimpleDeck = V.modify sort simpleDeck

-- | The winner of a game.
data Winner = Player1 | Player2 | Draw
    deriving (Show, Read, Eq, Ord, Enum, Bounded, Typeable, Data, Generic)

-- | Play a game of war between two decks.
--
-- If we go through over 10000 cards draw, declare the game a tie.
play :: Deck -- ^ player 1
     -> Deck -- ^ player 2
     -> Winner
play deck1 deck2 = runST $ do
    hand1 <- fmap asUDeque newColl
    V.forM_ deck1 $ pushBack hand1
    hand2 <- fmap asUDeque newColl
    V.forM_ deck2 $ pushBack hand2
    draws <- VM.new 1
    VM.unsafeWrite draws 0 (0 :: Int)
    let draw f = do
            draws' <- VM.unsafeRead draws 0
            if draws' >= 10000
                then return Draw
                else do
                    VM.unsafeWrite draws 0 $! draws' + 1

                    mx <- popFront hand1
                    my <- popFront hand2
                    case (mx, my) of
                        (Nothing, Nothing) -> return Draw
                        (Just _, Nothing) -> return Player1
                        (Nothing, Just _) -> return Player2
                        (Just x, Just y) -> f x y

        start = start' id id

        start' pile1 pile2 = draw $ \x y ->
            case compare x y of
                LT -> do
                    mapM_ (pushBack hand2) (pile2 $ y : pile1 [x])
                    start
                GT -> do
                    mapM_ (pushBack hand1) (pile1 $ x : pile2 [y])
                    start
                EQ -> war (3 :: Int) pile1 pile2

        war 0 pile1 pile2 = start' pile1 pile2
        war i pile1 pile2 = draw $ \x y ->
            war (i - 1) (pile1 . (x:)) (pile2 . (y:))
    start

-- | Perform a random swap of two cards in the deck.
mutateDeck :: PrimMonad m => Gen (PrimState m) -> Deck -> m Deck
mutateDeck gen deck = do
    x <- uniformR (0, len - 1) gen
    y <- uniformR (0, len - 1) gen
    return $! V.modify (\mv -> VM.unsafeSwap mv x y) deck
  where
    len = V.length deck

-- | Find the score of a player against a set of competitors.
score :: Vector Deck -- ^ competitors
      -> Deck -- ^ player
      -> Int
score competitors deck =
    V.foldl' go 0 competitors
  where
    go total x = total +
        (case play deck x of
            Player1 -> 1
            Player2 -> -1
            Draw    -> 0)
