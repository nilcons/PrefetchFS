{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PackageImports #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.MSampleVar
import Control.Exception
import Control.Exception.Lens
import Control.Lens
import Control.Monad (when, unless, void)
import qualified Data.ByteString as BS
import Data.ByteString.Internal
import Data.Foldable (forM_)
import Foreign.C.Types
import GHC.IO.Exception
import Prelude hiding (log)
import System.Directory (createDirectoryIfMissing, getDirectoryContents, canonicalizePath)
import System.Environment
import System.FilePath.Posix (dropFileName)
import System.Fuse
import System.IO
import System.IO.Error.Lens
import System.Posix.Directory
import System.Posix.Files
import System.Posix.IO (openFd, defaultFileFlags, waitToSetLock, closeFd, LockRequest(..))
import "unix-bytestring" System.Posix.IO.ByteString (fdPread, fdPwrite)
import System.Posix.IO (fdSeek)
import System.Posix.Types

bSize :: CSize
bSize = 4096

bSizeOff :: COff
bSizeOff = fromIntegral bSize

-- The standard </> shipped in System.FilePath.Posix drops `a' if `b'
-- is absolute, that is not the behavior we want.
(</>) :: String -> String -> String
a </> b = a ++ "/" ++ b

-- How many b sized blocks is needed to store a bytes.
(//) :: COff -> COff -> COff
a // b = a `div` b + signum (a `mod` b)

log :: String -> IO ()
log = hPutStrLn stderr

-- General HFuse helper to return (Either Errno)s instead of exceptions.
runIO :: IO a -> IO (Either Errno a)
runIO io = handling _IOException errnoHandler (Right <$> io)
  where
    errnoHandler e = do
      case e ^. errno of
        Nothing -> throw e
        Just en -> return $ Left $ Errno en

data PrefetchHandle =
  PrefetchHandle { sourceFd :: Fd
                 , destFd :: Fd
                 , metaFd :: Fd
                 , blocks :: FileOffset
                 , prefetchSVar :: MSampleVar FileOffset
                 , prefetchThreadId ::  Maybe ThreadId }

fuseStat :: FilePath -> IO FileStat
fuseStat path = statusToStat <$> getSymbolicLinkStatus path
  where
    statusToStat :: FileStatus ->  FileStat
    statusToStat status = FileStat { statEntryType = entryType
                                   , statFileMode = fileMode status
                                   , statLinkCount = linkCount status
                                   , statFileOwner = fileOwner status
                                   , statFileGroup = fileGroup status
                                   , statSpecialDeviceID = specialDeviceID status
                                   , statFileSize = fileSize status
                                   , statBlocks = fromIntegral $ fileSize status // 512
                                   , statAccessTime = accessTime status
                                   -- TODO(errge): check precision
                                   , statModificationTime = modificationTime status
                                   , statStatusChangeTime = statusChangeTime status }
      where
        entryType :: EntryType
        entryType | isRegularFile status = RegularFile
                  | isDirectory status = Directory
                  | isSymbolicLink status = SymbolicLink
                  | isNamedPipe status = NamedPipe
                  | isSocket status = Socket
                  | isCharacterDevice status = CharacterSpecial
                  | isBlockDevice status = BlockSpecial
                  | otherwise  = Unknown

prefetchStat :: FilePath -> FilePath -> IO (Either Errno FileStat)
prefetchStat sourceDir path = runIO $ fuseStat $ sourceDir </> path

prefetchReadDir :: FilePath -> FilePath -> IO (Either Errno [(FilePath, FileStat)])
prefetchReadDir sourceDir path = runIO $ do
  files <- getDirectoryContents $ sourceDir </> path
  zip files <$> mapM (fuseStat . (sourceDir </> path </>)) files

prefetchOpen :: FilePath -> FilePath -> FilePath -> OpenMode -> OpenFileFlags -> IO (Either Errno PrefetchHandle)
prefetchOpen sourceDir cacheDir path mode flags = runIO $ do
  source <- openFd (sourceDir </> path) mode Nothing flags
  size <- fdSeek source SeekFromEnd 0
  log $ "successful open of " ++ path ++ " with size: " ++ show size
  case (size::COff) > 8388608 of
    True -> do -- TODO(errge): option for specifying large files
      -- caching only for big files, subtitle hack
      createDirectoryIfMissing True (dropFileName $ cacheDir </> path)
      dest <- openFd (cacheDir </> path) ReadWrite (Just stdFileMode) defaultFileFlags
      meta <- openFd (cacheDir </> path ++ ".meta") ReadWrite (Just stdFileMode) defaultFileFlags
      let blockCount = size // bSizeOff
      setFdSize meta blockCount
      svar <- newSV 0
      tid <- forkIO $ prefetchThread (PrefetchHandle source dest meta blockCount svar Nothing)
      return $ PrefetchHandle source dest meta blockCount svar $ Just tid
    False -> return $ PrefetchHandle source (-1) (-1) (-1) undefined Nothing

rangeToBlocks :: COff -> COff -> [COff]
rangeToBlocks offset len = [offset `div` bSizeOff .. (offset + len - 1) `div` bSizeOff]

readBlock :: Bool -> PrefetchHandle -> COff -> IO ByteString
readBlock prefetch (PrefetchHandle s d m _ svNextBlock _) blockn
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

prefetchRead :: FilePath -> PrefetchHandle -> ByteCount -> FileOffset -> IO (Either Errno BS.ByteString)
prefetchRead _ state len offset = runIO $ do
  bss <- mapM (readBlock False state) (rangeToBlocks offset (fromIntegral len))
  return $ BS.take (fromIntegral len) $ BS.drop (fromIntegral $ offset `mod` bSizeOff) $ BS.concat bss

prefetchRelease :: FilePath -> PrefetchHandle -> IO ()
prefetchRelease _ (PrefetchHandle { sourceFd = s, destFd = d, metaFd = m, prefetchThreadId = t }) = do
  forM_ t killThread
  when (d >= 0) $ closeFd d
  when (m >= 0) $ closeFd m
  closeFd s

prefetchFS :: FilePath -> FilePath -> FuseOperations PrefetchHandle
prefetchFS sourceDir cacheDir = FuseOperations
  { fuseGetFileStat = prefetchStat sourceDir
    , fuseReadSymbolicLink = \p -> fmap Right $ readSymbolicLink (sourceDir </> p)
    , fuseCreateDevice =
      \p t m _ -> case t of
        RegularFile -> openFd (sourceDir </> p) WriteOnly (Just m) defaultFileFlags >>= closeFd >> return eOK
        _ -> return eNOSYS
    , fuseCreateDirectory = \p m -> createDirectory (sourceDir </> p) m >> return eOK
    , fuseRemoveLink = \p -> removeLink (sourceDir </> p) >> return eOK
    , fuseRemoveDirectory = \d -> removeDirectory (sourceDir </> d) >> return eOK
    , fuseCreateSymbolicLink = \_ _ -> return eNOSYS
    , fuseRename = \p1 p2 -> rename (sourceDir </> p1) (sourceDir </> p2) >> return eOK
    , fuseCreateLink = \_ _ -> return eNOSYS
    , fuseSetFileMode = \p m -> setFileMode (sourceDir </> p) m >> return eOK
    , fuseSetOwnerAndGroup = \_ _ _ -> return eNOSYS
    , fuseSetFileSize = \p o -> setFileSize (sourceDir </> p) o >> return eOK
    , fuseSetFileTimes = \p t1 t2 -> setFileTimes (sourceDir </> p) t1 t2 >> return eOK
    , fuseOpen = prefetchOpen sourceDir cacheDir
    , fuseRead = prefetchRead
    , fuseWrite =  \_ (PrefetchHandle { sourceFd = fd }) bs off -> fmap Right $ fdPwrite fd bs off
    , fuseGetFileSystemStats = \_ -> return (Left eNOSYS)
    , fuseFlush = \_ _ -> return eOK
    , fuseRelease = prefetchRelease
    , fuseSynchronizeFile = \_ _ -> return eNOSYS
    , fuseOpenDirectory = const $ return eOK
    , fuseReadDirectory = prefetchReadDir sourceDir
    , fuseReleaseDirectory = const $ return eOK
    , fuseSynchronizeDirectory = \_ _ -> return eNOSYS
    , fuseAccess = \_ _ -> return eNOSYS
    , fuseInit = return ()
    , fuseDestroy = return ()
  }

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
      void $ readBlock True state blck
      isEmptySV svNextBlock >>= \case
        True -> prefetch (blck+1)
        False -> overrideFetch

main :: IO ()
main =
  do
    sourceDir:cacheDir:rest <- getArgs
    [sourceDirAbs, cacheDirAbs] <- mapM canonicalizePath [sourceDir, cacheDir]
    withArgs rest $
      fuseMain (prefetchFS sourceDirAbs cacheDirAbs) defaultExceptionHandler
