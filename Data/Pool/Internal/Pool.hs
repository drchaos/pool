{-# LANGUAGE RecordWildCards #-}
-- | Internal functions related to the pool structure.
module Data.Pool.Internal.Pool
  ( LocalPool(..)
  , Entry(..)
  , allocateInLocalPool
  , tryAllocateInLocalPool
  ) where

import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.IORef
import Data.Typeable
import System.Clock

-- | A single resource pool entry.
data Entry a = Entry {
      entry :: a
    , lastUse :: TimeSpec
    -- ^ Time of last return.
    }

-- | A single striped pool.
data LocalPool a = LocalPool {
      inUse :: TVar Int
    -- ^ Count of open entries (both idle and in use).
    , entries :: TVar [Entry a]
    -- ^ Idle entries.
    , lfin :: IORef ()
    -- ^ empty value used to attach a finalizer to (internal)
    } deriving (Typeable)

-- | Safely allocate a resource in the 'LocalPool'.
-- Blocks if max capacity is reached.
allocateInLocalPool :: Int -> IO a -> LocalPool a -> IO a
allocateInLocalPool maxResources create LocalPool{..} = join $ atomically $ do
  ents <- readTVar entries
  case ents of
    (Entry{..}:es) -> writeTVar entries es >> return (return entry)
    [] -> do
      used <- readTVar inUse
      when (used == maxResources) retry
      writeTVar inUse $! used + 1
      return $
        create `onException` atomically (modifyTVar' inUse (subtract 1))

-- | Safely allocate a resource in the 'LocalPool'
-- Returns @Nothing@ if max capacity is reached
tryAllocateInLocalPool :: Int -> IO a -> LocalPool a -> IO (Maybe a)
tryAllocateInLocalPool maxResources create LocalPool{..} = join $ atomically $ do
    ents <- readTVar entries
    case ents of
      (Entry{..}:es) -> writeTVar entries es >> return (return . Just $ entry)
      [] -> do
        used <- readTVar inUse
        if used == maxResources
          then return (return Nothing)
          else do
            writeTVar inUse $! used + 1
            return $ Just <$>
              create `onException` atomically (modifyTVar' inUse (subtract 1))
