{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
-- | Generate some random competitor decks, and use random mutation to find
-- players that get the highest scores against them.
import           Control.Concurrent.STM (atomically)
import           Data.Vector (Vector)
import           Data.Vector.Algorithms.Merge (sortBy)
import qualified Data.Vector.Generic as V
import qualified Data.Vector.Generic.Mutable as VM
import qualified Data.Vector.Hybrid as VH
import qualified Data.Vector.Unboxed as U
import           Data.War
import           Data.WorkQueue
import           Prelude hiding (round)
import           System.Random.MWC
import           Distributed.WorkQueue
import           Data.TypeFingerprint (mkManyHasTypeFingerprint)

$(mkManyHasTypeFingerprint
    [ [t| Int |]
    , [t| Deck |]
    , [t| Vector Deck |]
    ])

competitorCount :: Int
competitorCount = 80

playerCount :: Int
playerCount = 1000

stages :: Int
stages = 10

getInitialData :: GenIO -> IO (Vector Deck)
getInitialData = V.replicateM competitorCount . randomDeck

calc :: Vector Deck -> Deck -> IO Int
calc competitors player = do
    print player
    return $ score competitors player

main :: IO ()
main = do
    gen <- createSystemRandom
    runArgs (getInitialData gen) calc $ \competitors queue -> do
        players0 <- V.replicateM playerCount (randomDeck gen)
        result <- runRounds queue gen stages players0
        V.forM_ result $ \player -> print (player, score competitors player)

-- | Run through the given number of rounds.
runRounds :: WorkQueue Deck Int
          -> GenIO
          -> Int -- ^ total rounds
          -> Vector Deck -- ^ initial players
          -> IO (Vector Deck)
runRounds _ _ 0 players = return $ V.take 10 players
runRounds queue gen i players = do
    putStrLn $ "Rounds remaining: " ++ show i
    players' <- runRound queue gen players
    runRounds queue gen (i - 1) players'

-- | Perform a single round of scoring, elimination, and mutation.
runRound :: WorkQueue Deck Int
         -> GenIO
         -> Vector Deck -- ^ players
         -> IO (Vector Deck) -- ^ new players
runRound queue gen players = do
    -- Create a new mutable hybrid vector to hold the players and their
    -- scores
    scores <- VM.new (V.length players)

    let items = flip V.imap players $ \j player ->
            (player, \s -> VM.unsafeWrite scores j (s, player))
    atomically $ queueItems queue items
    atomically $ checkEmptyWorkQueue queue

    -- Sort the vector by score
    sortBy (\(x, _) (y, _) -> compare y x) scores

    scores' <- V.unsafeFreeze scores
    let sortedPlayers :: Vector Deck
        sortedPlayers = VH.projectSnd
            -- Explicit type signature to force specify vector
            -- types to be used by the hybrid vector
            (scores' :: VH.Vector U.Vector Vector (Int, Deck))

        -- Top 25% of scores
        winners :: Vector Deck
        winners = V.take (playerCount `div` 4) sortedPlayers

    -- Create a new vector of players by mutating the winners
    winners1 <- V.forM winners $ mutateDeck gen
    winners2 <- V.forM winners $ mutateDeck gen
    winners3 <- V.forM winners $ mutateDeck gen

    -- NOTE: would ideally remove duplicates here, not required
    return $! V.concat
        [ winners
        , winners1
        , winners2
        , winners3
        ]
