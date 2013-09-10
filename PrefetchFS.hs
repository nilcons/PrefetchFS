{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE PackageImports #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.MSampleVar
import Control.Exception
import Control.Exception.Lens
import Control.Lens
import Data.Bits ((.&.))
import qualified Data.ByteString as BS
import Prelude hiding (log)
import System.Directory (createDirectoryIfMissing, getDirectoryContents, canonicalizePath)
import System.Environment
import System.FilePath.Posix (dropFileName)
import System.Fuse
import System.IO
import System.IO.Error.Lens
import System.Posix.Directory
import System.Posix.Files
import System.Posix.IO (openFd, defaultFileFlags, closeFd)
import System.Posix.IO (fdSeek)
import System.Posix.Types
import System.Posix.StatVFS

import Common
import PrefetchHandle

-- The standard </> shipped in System.FilePath.Posix drops `a' if `b'
-- is absolute, that is not the behavior we want.
(</>) :: String -> String -> String
a </> b = a ++ "/" ++ b

-- How many b sized blocks is needed to store a bytes.
(//) :: COff -> COff -> COff
a // b = a `div` b + signum (a `mod` b)

-- General HFuse helper to return (Either Errno)s instead of exceptions.
runIOEither :: IO a -> IO (Either Errno a)
runIOEither io = handling _IOException errnoHandler (Right <$> io)
  where
    errnoHandler e = do
      case e ^. errno of
        Nothing -> throw e
        Just en -> return $ Left $ Errno en

-- General HFuse helper to return (Errno)s instead of exceptions.
runIO :: IO () -> IO Errno
runIO io = handling _IOException errnoHandler (const eOK <$> io)
  where
    errnoHandler e = do
      case e ^. errno of
        Nothing -> throw e
        Just en -> return $ Errno en

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
prefetchStat sourceDir path = runIOEither $ fuseStat $ sourceDir </> path

prefetchFsStat :: FilePath -> FilePath -> IO (Either Errno FileSystemStats)
prefetchFsStat sourceDir path = runIOEither $ fuseFsStat $ sourceDir </> path
  where
    fuseFsStat :: FilePath -> IO FileSystemStats
    fuseFsStat p = statVFSToFuseFsStats <$> statVFS p
    statVFSToFuseFsStats :: StatVFS -> FileSystemStats
    statVFSToFuseFsStats s = FileSystemStats { fsStatBlockSize = toInteger $ statVFS_bsize s
                                   , fsStatBlockCount = toInteger $ statVFS_blocks s
                                   , fsStatBlocksFree = toInteger $ statVFS_bfree s
                                   , fsStatBlocksAvailable = toInteger $ statVFS_bavail s
                                   , fsStatFileCount = toInteger $ statVFS_files s
                                   , fsStatFilesFree = toInteger $ statVFS_ffree s
                                   , fsStatMaxNameLength = toInteger $ statVFS_namemax s
                                   }

prefetchReadDirectory :: FilePath -> FilePath -> IO (Either Errno [(FilePath, FileStat)])
prefetchReadDirectory sourceDir path = runIOEither $ do
  files <- getDirectoryContents $ sourceDir </> path
  zip files <$> mapM (fuseStat . (sourceDir </> path </>)) files

prefetchOpen :: FilePath -> FilePath -> FilePath -> OpenMode -> OpenFileFlags -> IO (Either Errno PrefetchHandle)
prefetchOpen sourceDir cacheDir path mode flags = runIOEither $ do
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
      tid <- forkIO $ prefetchThread (prefetchHandle source dest meta blockCount svar Nothing)
      return $ prefetchHandle source dest meta blockCount svar $ Just tid
    False -> return $ prefetchHandle source (-1) (-1) (-1) undefined Nothing

rangeToBlocks :: COff -> COff -> [COff]
rangeToBlocks offset len = [offset `div` bSizeOff .. (offset + len - 1) `div` bSizeOff]

prefetchRead :: FilePath -> PrefetchHandle -> ByteCount -> FileOffset -> IO (Either Errno BS.ByteString)
prefetchRead _ state len offset = runIOEither $ do
  bss <- mapM (readBlockFromPrefetchHandle False state) (rangeToBlocks offset (fromIntegral len))
  return $ BS.take (fromIntegral len) $ BS.drop (fromIntegral $ offset `mod` bSizeOff) $ BS.concat bss

prefetchRelease :: FilePath -> PrefetchHandle -> IO ()
prefetchRelease _ = releasePrefetchHandle

prefetchAccess :: FilePath -> FilePath -> Int -> IO Errno
prefetchAccess sourceDir p bits = runIO_ $ fileAccess (sourceDir </> p) (bits .&. 4 == 1) (bits .&. 2 == 1) (bits .&. 1 == 1)
  where
    runIO_ io = do
      ret <- runIOEither io
      case ret of
        Right True -> return eOK
        Right False -> return eACCES
        Left en -> return en

prefetchFS :: FilePath -> FilePath -> FuseOperations PrefetchHandle
prefetchFS sourceDir cacheDir = FuseOperations
  { fuseGetFileStat = prefetchStat sourceDir
    , fuseReadSymbolicLink = \p -> fmap Right $ readSymbolicLink (sourceDir </> p)
    , fuseCreateDevice =
      \p t m _ -> case t of
        RegularFile -> runIO $ openFd (sourceDir </> p) WriteOnly (Just m) defaultFileFlags >>= closeFd
        _ -> return eNOSYS
    , fuseCreateDirectory = \p m -> runIO $ createDirectory (sourceDir </> p) m
    , fuseRemoveLink = \p -> runIO $ removeLink (sourceDir </> p)
    , fuseRemoveDirectory = \d -> runIO $ removeDirectory (sourceDir </> d)
    , fuseCreateSymbolicLink = \old new -> runIO $ createSymbolicLink old $ sourceDir </> new
    , fuseRename = \p1 p2 -> runIO $ rename (sourceDir </> p1) (sourceDir </> p2)
    , fuseCreateLink = \old new -> runIO $ createLink (sourceDir </> old) (sourceDir </> new)
    , fuseSetFileMode = \p m -> runIO $ setFileMode (sourceDir </> p) m
    , fuseSetOwnerAndGroup = \p o g -> runIO $ setOwnerAndGroup (sourceDir </> p) o g
    , fuseSetFileSize = \p o -> runIO $ setFileSize (sourceDir </> p) o
    , fuseSetFileTimes = \p t1 t2 -> runIO $ setFileTimes (sourceDir </> p) t1 t2
    , fuseOpen = prefetchOpen sourceDir cacheDir
    , fuseRead = prefetchRead
    , fuseWrite = \_filePath pHandle bs offset -> runIOEither $ writeToPrefetchHandle pHandle bs offset
    , fuseGetFileSystemStats = prefetchFsStat sourceDir
    , fuseFlush = \_ _ -> return eOK
    , fuseRelease = prefetchRelease
    , fuseSynchronizeFile = \_ _ -> return eOK
    , fuseOpenDirectory = \p -> runIO $ do
      log $ "opendir start" ++ p
      bracket (openDirStream $ sourceDir </> p) closeDirStream $ const $ return ()
      log $ "opendir end"
    , fuseReadDirectory = prefetchReadDirectory sourceDir
    , fuseReleaseDirectory = const $ return eOK
    , fuseSynchronizeDirectory = \_ _ -> return eOK
    , fuseAccess = prefetchAccess sourceDir
    , fuseInit = return ()
    , fuseDestroy = return ()
  }

main :: IO ()
main =
  do
    sourceDir:cacheDir:rest <- getArgs
    [sourceDirAbs, cacheDirAbs] <- mapM canonicalizePath [sourceDir, cacheDir]
    withArgs rest $
      fuseMain (prefetchFS sourceDirAbs cacheDirAbs) defaultExceptionHandler
