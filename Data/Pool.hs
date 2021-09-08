module Data.Pool
    (
      Pool(idleTime, maxResources, numStripes)
    , LocalPool
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
    ) where

import Data.Pool.Internal
