{-# LANGUAGE NamedFieldPuns, RecordWildCards, ScopedTypeVariables, RankNTypes, DeriveDataTypeable, TypeApplications #-}
{-# LANGUAGE RankNTypes #-}

-- |
-- Module:      Data.Pool
-- Copyright:   (c) 2011 MailRank, Inc.
-- License:     BSD3
-- Maintainer:  Bryan O'Sullivan <bos@serpentine.com>,
--              Bas van Dijk <v.dijk.bas@gmail.com>
-- Stability:   experimental
-- Portability: portable
--
-- A high-performance striped pooling abstraction for managing
-- flexibly-sized collections of resources such as database
-- connections.
--
-- \"Striped\" means that a single 'Pool' consists of several
-- sub-pools, each managed independently.  A single stripe is fine for
-- many applications, and probably what you should choose by default.
-- More stripes will lead to reduced contention in high-performance
-- multicore applications, at a trade-off of causing the maximum
-- number of simultaneous resources in use to grow.
module Data.Pool.Internal
    (
      Pool(..)
    , LocalPool(..)
    , Entry(..)
    , createPool
    , createPoolEx
    , withResource
    , takeResource
    , tryWithResource
    , tryTakeResource
    , destroyResource
    , putResource
    , putResourceAndSignal
    , destroyAllResources
    , purgeLocalPool
    ) where

import Control.Concurrent (killThread, myThreadId)
import Control.Concurrent.STM
import Control.Exception (SomeException, onException, mask, mask_)
import qualified Control.Exception as E
import Control.Monad (liftM3, when, void, replicateM)
import Data.Hashable (hash)
import Data.IORef (IORef, newIORef, mkWeakIORef)
import Data.Typeable (Typeable)
import qualified Data.Vector as V
import System.Clock

import Data.Pool.Internal.Pool
import Data.Pool.Internal.Reaper
import Data.Pool.Internal.Missing


data Pool a = Pool {
      create :: IO a
    -- ^ Action for creating a new entry to add to the pool.
    , destroy :: a -> IO ()
    -- ^ Action for destroying an entry that is now done with.
    , numStripes :: Int
    -- ^ The number of stripes (distinct sub-pools) to maintain.
    -- The smallest acceptable value is 1.
    , idleTime :: Double
    -- ^ Amount of time for which an unused resource is kept alive.
    -- The smallest acceptable value is 0.5 seconds.
    --
    -- The elapsed time before closing may be a little longer than
    -- requested, as the reaper thread wakes at 1-second intervals.
    , maxResources :: Int
    -- ^ Maximum number of resources to maintain per stripe.  The
    -- smallest acceptable value is 1.
    --
    -- Requests for resources will block if this limit is reached on a
    -- single stripe, even if other stripes have idle resources
    -- available.
    , localPools :: V.Vector (LocalPool a)
    -- ^ Per-capability resource pools.
    , fin :: IORef ()
    -- ^ empty value used to attach a finalizer to (internal)
    , signal :: STM ()
    -- ^ signal that a new resource was allocated
    } deriving (Typeable)

instance Show (Pool a) where
    show Pool{..} = "Pool {numStripes = " ++ show numStripes ++ ", " ++
                    "idleTime = " ++ show idleTime ++ ", " ++
                    "maxResources = " ++ show maxResources ++ "}"

-- | Create a striped resource pool.
--
-- Although the garbage collector will destroy all idle resources when
-- the pool is garbage collected it's recommended to manually
-- 'destroyAllResources' when you're done with the pool so that the
-- resources are freed up as soon as possible.
-- 
-- _NOTE: Function is kept for compatibility reasons. Use 'createPoolEx`._
createPool
    :: (Real t)
    => IO a
    -- ^ Action that creates a new resource.
    -> (a -> IO ())
    -- ^ Action that destroys an existing resource.
    -> Int
    -- ^ The number of stripes (distinct sub-pools) to maintain.
    -- The smallest acceptable value is 1.
    -> t
    -- ^ Amount of time for which an unused resource is kept open.
    -- The smallest acceptable value is 0.5 seconds.
    --
    -- The elapsed time before destroying a resource may be a little
    -- longer than requested, as the reaper thread wakes at 1-second
    -- intervals.
    -> Int
    -- ^ Maximum number of resources to keep open per stripe.  The
    -- smallest acceptable value is 1.
    --
    -- Requests for resources will block if this limit is reached on a
    -- single stripe, even if other stripes have idle resources
    -- available.
     -> IO (Pool a)
createPool create destroy numStripes idleTime maxResources =
  createPoolEx create destroy numStripes (realToFrac idleTime) 0 maxResources

-- | Create a striped resource pool.
--
-- Although the garbage collector will destroy all idle resources when
-- the pool is garbage collected it's recommended to manually
-- 'destroyAllResources' when you're done with the pool so that the
-- resources are freed up as soon as possible.
createPoolEx
    :: IO a
    -- ^ Action that creates a new resource.
    -> (a -> IO ())
    -- ^ Action that destroys an existing resource.
    -> Int
    -- ^ The number of stripes (distinct sub-pools) to maintain.
    -- The smallest acceptable value is 1.
    -> Double
    -- ^ Amount of time for which an unused resource is kept open.
    -- The smallest acceptable value is 0.5 seconds.
    --
    -- The elapsed time before destroying a resource may be a little
    -- longer than requested, as the reaper thread wakes at 1-second
    -- intervals.
    -> Int
    -- ^ Minimum number of resources to keep open per stripe. The smallest
    -- acceptable value is 0.
    --
    -- If resources will be closed on timeout they will be automatically
    -- reallocated if total number of values will be below this value.
    -> Int
    -- ^ Maximum number of resources to keep open per stripe.  The
    -- smallest acceptable value is 1.
    --
    -- Requests for resources will block if this limit is reached on a
    -- single stripe, even if other stripes have idle resources
    -- available.
     -> IO (Pool a)
createPoolEx create destroy numStripes idleTime minResources maxResources = do
  when (numStripes < 1) $
    modError "pool " $ "invalid stripe count " ++ show numStripes
  when (idleTime < 0.5) $
    modError "pool " $ "invalid idle time " ++ show idleTime
  when (maxResources < 1) $
    modError "pool " $ "invalid maximum resource count " ++ show maxResources
  localPools <- V.replicateM numStripes $
                liftM3 LocalPool (newTVarIO 0) (newTVarIO []) (newIORef ())
  lock <- newEmptyTMVarIO
  reaperId <- forkIOLabeledWithUnmask "resource-pool: reaper" $ \unmask ->
                unmask $ reaper destroy
                  (void . createInLocalPool maxResources create)
                  (minResources)
                  (round $ 1000000 * idleTime)
                  lock
                  localPools
  fin <- newIORef ()
  let signal = tryPutTMVar lock () >> pure ()
  let p = Pool {
            create
          , destroy
          , numStripes
          , idleTime
          , maxResources
          , localPools
          , fin
          , signal
          }
  mkWeakIORef fin (killThread reaperId) >>
    V.mapM_ (\lp -> mkWeakIORef (lfin lp) (purgeLocalPool destroy lp)) localPools
  when (minResources > 0) $ do
    V.forM_ localPools $ replicateM minResources . createInLocalPool maxResources create
    atomically signal
  return p

-- | Destroy all idle resources of the given 'LocalPool' and remove them from
-- the pool.
purgeLocalPool :: (a -> IO ()) -> LocalPool a -> IO ()
purgeLocalPool destroy LocalPool{..} = do
  resources <- atomically $ do
    idle <- swapTVar entries []
    modifyTVar' inUse (subtract (length idle))
    return (map entry idle)
  mask_ $
    foldr E.finally (pure ()) 
      (map (\resource -> destroy resource `E.catch` \(_::SomeException) -> return ())
           resources)

-- | Temporarily take a resource from a 'Pool', perform an action with
-- it, and return it to the pool afterwards.
--
-- * If the pool has an idle resource available, it is used
--   immediately.
--
-- * Otherwise, if the maximum number of resources has not yet been
--   reached, a new resource is created and used.
--
-- * If the maximum number of resources has been reached, this
--   function blocks until a resource becomes available.
--
-- If the action throws an exception of any type, the resource is
-- destroyed, and not returned to the pool.
--
-- It probably goes without saying that you should never manually
-- destroy a pooled resource, as doing so will almost certainly cause
-- a subsequent user (who expects the resource to be valid) to throw
-- an exception.
withResource :: Pool a -> (a -> IO b) -> IO b
withResource pool act = mask $ \restore -> do
  (resource, local) <- takeResource pool
  ret <- restore (act resource) `onException`
            destroyResource pool local resource
  putResourceAndSignal pool local resource
  return ret
{-# INLINABLE withResource #-}

-- | Take a resource from the pool, following the same results as
-- 'withResource'. Note that this function should be used with caution, as
-- improper exception handling can lead to leaked resources.
--
-- This function returns both a resource and the @LocalPool@ it came from so
-- that it may either be destroyed (via 'destroyResource') or returned to the
-- pool (via 'putResource').
takeResource :: Pool a -> IO (a, LocalPool a)
takeResource pool@Pool{..} = do
  local <- getLocalPool pool
  resource <- allocateInLocalPool maxResources create local
  return (resource, local)
{-# INLINABLE takeResource #-}

-- | Similar to 'withResource', but only performs the action if a resource could
-- be taken from the pool /without blocking/. Otherwise, 'tryWithResource'
-- returns immediately with 'Nothing' (ie. the action function is /not/ called).
-- Conversely, if a resource can be borrowed from the pool without blocking, the
-- action is performed and it's result is returned, wrapped in a 'Just'.
tryWithResource :: Pool a -> (a -> IO b) -> IO (Maybe b)
tryWithResource pool act = mask $ \restore -> do
  res <- tryTakeResource pool
  case res of
    Just (resource, local) -> do
      ret <- restore (Just <$> act resource) `onException`
                destroyResource pool local resource
      putResourceAndSignal pool local resource
      return ret
    Nothing -> restore $ return (Nothing :: Maybe b)
{-# INLINABLE tryWithResource #-}

-- | A non-blocking version of 'takeResource'. The 'tryTakeResource' function
-- returns immediately, with 'Nothing' if the pool is exhausted, or @'Just' (a,
-- 'LocalPool' a)@ if a resource could be borrowed from the pool successfully.
tryTakeResource :: Pool a -> IO (Maybe (a, LocalPool a))
tryTakeResource pool@Pool{..} = do
  local <- getLocalPool pool
  resource <- tryAllocateInLocalPool maxResources create local
  return $ (flip (,) local) <$> resource
{-# INLINABLE tryTakeResource #-}

-- | Get a (Thread-)'LocalPool'
--
-- Internal, just to not repeat code for 'takeResource' and 'tryTakeResource'
getLocalPool :: Pool a -> IO (LocalPool a)
getLocalPool Pool{..} = do
  i <- ((`mod` numStripes) . hash) <$> myThreadId
  return $ localPools V.! i
{-# INLINABLE getLocalPool #-}

-- | Destroy a resource. Note that this will ignore any exceptions in the
-- destroy function.
destroyResource :: Pool a -> LocalPool a -> a -> IO ()
destroyResource Pool{..} LocalPool{..} resource = do
   mask_ $ (destroy resource `E.catch` \(_::SomeException) -> return ())
     `E.finally` atomically (modifyTVar' inUse (subtract 1))
{-# INLINABLE destroyResource #-}

-- | Return a resource to the given 'LocalPool'.
putResourceAndSignal :: Pool a -> LocalPool a -> a -> IO ()
putResourceAndSignal Pool{..} LocalPool{..} resource = do
    now <- getTime Monotonic
    atomically $ do
      modifyTVar' entries (Entry resource now:)
      signal 

{-# INLINABLE putResourceAndSignal #-}

-- | Return a resource to the given 'LocalPool'.
--
-- _NOTE: Function is kept for compatibility reasons. Use putResourceAndSignal`._
putResource :: LocalPool a -> a -> IO ()
putResource LocalPool{..} resource = do
    now <- getTime Monotonic
    atomically $ do
      modifyTVar' entries (Entry resource now:)
{-# INLINABLE putResource #-}

-- | Destroy all resources in all stripes in the pool. Note that this
-- will ignore any exceptions in the destroy function.
--
-- This function is useful when you detect that all resources in the
-- pool are broken. For example after a database has been restarted
-- all connections opened before the restart will be broken. In that
-- case it's better to close those connections so that 'takeResource'
-- won't take a broken connection from the pool but will open a new
-- connection instead.
--
-- Another use-case for this function is that when you know you are
-- done with the pool you can destroy all idle resources immediately
-- instead of waiting on the garbage collector to destroy them, thus
-- freeing up those resources sooner.
destroyAllResources :: Pool a -> IO ()
destroyAllResources Pool{..} = 
  foldr E.finally (pure ()) (V.map (purgeLocalPool destroy) localPools)

modError :: String -> String -> a
modError func msg =
    error $ "Data.Pool.Internal" ++ func ++ ": " ++ msg
