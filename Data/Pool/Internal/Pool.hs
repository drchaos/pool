module Data.Pool.Internal.Pool
  ( LocalPool(..)
  , Entry(..)
  ) where

import Control.Concurrent.STM
import Data.Time.Clock
import Data.IORef
import Data.Typeable

-- | A single resource pool entry.
data Entry a = Entry {
      entry :: a
    , lastUse :: UTCTime
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
