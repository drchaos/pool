{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeApplications #-}
-- | Implements resource reaper.
-- This is a library specific resource reaper. It cleans
-- resources that should be removed as they were not used
-- longer than a timeout.
module Data.Pool.Internal.Reaper
  ( reaper
  ) where

import Control.Concurrent.STM
import Control.Exception (SomeException)
import Control.Monad (unless)
import qualified Control.Exception as E
import Data.Foldable (for_)
import Data.Function
import Data.List (partition)
import qualified Data.List.NonEmpty as NE
import Data.Time.Clock
import qualified Data.Vector as V

import Data.Pool.Internal.Pool

-- | Runs a thread that kills unused resources. This method has the following
-- properties:
--   1. It creates only a single timer no matter how many resources
--      were allocated.
--   2. It waits for the first resource to be freed
--   3. After freeing if there are more resources to be freed they are freed immediately,
--      otherwise the thread waits for the first one to be freed
--   4. If no resources are left the thread waits until the new resurce will appear
--      without running any work.
--   
reaper :: (a -> IO ()) -- ^ Destroy action.
       -> Int -- ^ Resource life-time in microseconds.
       -> TMVar () -- ^ Internal lock that should be set whenever a new resource is allocated.
       -> V.Vector (LocalPool a) -- ^ Local pools
       -> IO ()
reaper destroy idleTime inLock pools = fix $ \next -> do
  atomically $ takeTMVar inLock
  flip fix idleTime $ \loop c -> do
    delay <- registerDelay c
    atomically $ readTVar delay >>= check
    fix $ \again -> do
      minTimes
       <- V.forM pools $ \LocalPool{..} -> do
            now <- getCurrentTime
            let isStale Entry{..} = now `diffUTCTime` lastUse > fromIntegral idleTime
            (resources, minTime) <- atomically $ do
              (stale, fresh) <- partition isStale <$> readTVar entries
              let minTime = fmap minimum $ NE.nonEmpty $ map lastUse fresh
              unless (null stale) $ do
                writeTVar entries fresh
                modifyTVar_ inUse (subtract (length stale))
              pure (stale, minTime)
            for_ resources $ \resource -> do
               destroy (entry resource) `E.catch` \(_::SomeException) -> return () -- XXX: unsafe(!)
            pure minTime
      case V.mapMaybe id minTimes of
        xs | V.null xs -> next
           | otherwise -> do
         let minTime = minimum xs
         now1 <- getCurrentTime
         let nextDelay = max 0 $ round $ 1000000*realToFrac @_ @Double (minTime `diffUTCTime` now1)
         if nextDelay <= 0
           then loop nextDelay
           else again

modifyTVar_ :: TVar a -> (a -> a) -> STM ()
modifyTVar_ v f = readTVar v >>= \a -> writeTVar v $! f a
