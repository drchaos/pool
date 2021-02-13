{-# LANGUAGE RankNTypes #-}
-- | 
-- List of funcitons missing from the other packages,
-- theoretically these functions should not be part of
-- the module.
module Data.Pool.Internal.Missing
  ( forkIOLabeledWithUnmask
  ) where

import Control.Concurrent
import Control.Exception

import GHC.Conc.Sync (labelThread)

-- | Sparks off a new thread using 'forkIOWithUnmask' to run the given
-- IO computation, but first labels the thread with the given label
-- (using 'labelThread').
--
-- The implementation makes sure that asynchronous exceptions are
-- masked until the given computation is executed. This ensures the
-- thread will always be labeled which guarantees you can always
-- easily find it in the GHC event log.
--
-- Like 'forkIOWithUnmask', the given computation is given a function
-- to unmask asynchronous exceptions. See the documentation of that
-- function for the motivation of this.
--
-- Returns the 'ThreadId' of the newly created thread.
forkIOLabeledWithUnmask :: String
                        -> ((forall a. IO a -> IO a) -> IO ())
                        -> IO ThreadId
forkIOLabeledWithUnmask label m = mask_ $ forkIOWithUnmask $ \unmask -> do
  tid <- myThreadId
  labelThread tid label
  m unmask
