module Common ( bSize
              , bSizeOff
              , log
              ) where

import Foreign.C.Types
import Prelude hiding (log)
import System.IO
import System.Posix.Types

bSize :: CSize
bSize = 4096

bSizeOff :: COff
bSizeOff = fromIntegral bSize

log :: String -> IO ()
log = hPutStrLn stderr
