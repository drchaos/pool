module Data.Pool
    (
      Pool(idleTime, maxResources, numStripes)
    , LocalPool
    , createPool
    , withResource
    , takeResource
    , tryWithResource
    , tryTakeResource
    , destroyResource
    , putResource
    , destroyAllResources
    ) where

import Data.Pool.Internal
