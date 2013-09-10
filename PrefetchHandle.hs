{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PackageImports #-}
{-# LANGUAGE ScopedTypeVariables #-}

module PrefetchHandle ( PrefetchHandle
                      , readBlockFromPrefetchHandle
                      , writeToPrefetchHandle
                      , releasePrefetchHandle
                      , prefetchThread
                      , prefetchHandle
                      ) where

import Control.Concurrent
import Control.Concurrent.MSampleVar
import Control.Exception
import Control.Monad (when, unless, void)
import qualified Data.ByteString as BS
import Data.ByteString.Internal
import Data.Foldable
import GHC.IO.Exception
import Prelude hiding (log)
import System.IO
import System.Posix.IO (waitToSetLock, closeFd, LockRequest(..))
import "unix-bytestring" System.Posix.IO.ByteString (fdPread, fdPwrite)
import System.Posix.Types

import Common

data PrefetchHandle =
  PrefetchHandle { sourceFd :: Fd
                 , destFd :: Fd
                 , metaFd :: Fd
                 , blocks :: FileOffset
                 , prefetchSVar :: MSampleVar FileOffset
                 , prefetchThreadId ::  Maybe ThreadId }

prefetchHandle :: Fd -> Fd -> Fd -> FileOffset -> MSampleVar FileOffset -> Maybe ThreadId -> PrefetchHandle
prefetchHandle = PrefetchHandle

readBlockFromPrefetchHandle :: Bool -> PrefetchHandle -> COff -> IO ByteString
readBlockFromPrefetchHandle prefetch (PrefetchHandle s d m _ svNextBlock _) blockn
  | m == -1 = fdPread s bSize (blockn * bSizeOff)
  | otherwise =
    bracket_
      (waitToSetLock m (WriteLock, AbsoluteSeek, blockn, 1))
      (waitToSetLock m (Unlock, AbsoluteSeek, blockn, 1)) $ do
        isCached <- fdPread m 1 blockn
        if isCached == oneBS
          then do log $ "block " ++ show blockn ++ " is already cached"
                  fdPread d bSize (blockn * bSizeOff)
          else do log $ "block " ++ show blockn ++ " is being cached"
                  unless prefetch $ writeSV svNextBlock (blockn + 1)
                  dat <- fdPread s bSize (blockn * bSizeOff)
                  -- try hard to work even when the caching disk is full
                  (do writtenBytes <- fdPwrite d dat $ blockn * bSizeOff
                      when (fromIntegral writtenBytes /= BS.length dat) $ throw $ userError "bad pwrite"
                      void $ fdPwrite m oneBS blockn
                      log $ "block " ++ show blockn ++ " caching done")
                    `catch` (\(_ :: IOException) -> do
                                log $ "block " ++ show blockn ++ " couldn't be cached")
                  return dat
          where
            oneBS = BS.singleton 1

writeToPrefetchHandle :: PrefetchHandle -> ByteString -> FileOffset -> IO ByteCount
writeToPrefetchHandle (PrefetchHandle { sourceFd = fd }) bs off = fdPwrite fd bs off

releasePrefetchHandle :: PrefetchHandle -> IO ()
releasePrefetchHandle (PrefetchHandle { sourceFd = s, destFd = d, metaFd = m, prefetchThreadId = t }) = do
  traverse_ killThread t
  when (d >= 0) $ closeFd d
  when (m >= 0) $ closeFd m
  closeFd s

prefetchThread :: PrefetchHandle -> IO ()
prefetchThread state =
  overrideFetch `finally` log "prefetchThread exit"
  where
    svNextBlock = prefetchSVar state
    overrideFetch = do
      log "Getting next block to prefetch from the main thread"
      readSV svNextBlock >>= prefetch
    prefetch blck | blck >= blocks state || blck < 0 = overrideFetch
                  | otherwise = do
      log $ "prefetching block " ++ show blck
      void $ readBlockFromPrefetchHandle True state blck
      isEmptySV svNextBlock >>= \case
        True -> prefetch (blck+1)
        False -> overrideFetch
