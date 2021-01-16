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
import Data.Function
import Data.Int
import Data.List (partition)
import qualified Data.List.NonEmpty as NE
import qualified Data.Vector as V
import System.Clock

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
            now <- getTime Monotonic
            let isStale Entry{..} = toMicroseconds (now `diffTimeSpec` lastUse) > fromIntegral idleTime
            (resources, minTime) <- atomically $ do
              (stale, fresh) <- partition isStale <$> readTVar entries
              let minTime = fmap minimum $ NE.nonEmpty $ map lastUse fresh
              unless (null stale) $ do
                writeTVar entries fresh
                modifyTVar_ inUse (subtract (length stale))
              pure (stale, minTime)
            E.mask_ $ foldr E.finally (pure ()) $
              map (\x -> destroy (entry x) `E.catch` \(_ :: SomeException) -> return ())
                  resources
            pure minTime
      case V.mapMaybe id minTimes of
        xs | V.null xs -> next
           | otherwise -> do
         let minTime = minimum xs
         now1 <- getTime Monotonic
         if minTime < now1
           then again
           else loop (fromIntegral $ toMicroseconds (minTime `diffTimeSpec` now1))

modifyTVar_ :: TVar a -> (a -> a) -> STM ()
modifyTVar_ v f = readTVar v >>= \a -> writeTVar v $! f a

toMicroseconds :: TimeSpec -> Int64
toMicroseconds (TimeSpec secs nsecs) = secs*1000000 + nsecs `div` 1000
