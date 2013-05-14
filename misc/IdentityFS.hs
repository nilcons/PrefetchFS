{-# LANGUAGE ScopedTypeVariables #-}

import Control.Concurrent
import Data.ByteString hiding (pack)
import Data.ByteString.Char8
import Data.ByteString.Internal
import Debug.Trace
import Foreign.C.Error
import System.Directory
import System.Environment
import System.Fuse
import System.IO
import System.Posix.Files
import System.Posix.IO
import System.Posix.IO.Extra
import System.Posix.Types
import System.Posix.Unistd

a </> b = a ++ "/" ++ b
           
statusToEntryType :: FileStatus -> EntryType
statusToEntryType status | isRegularFile status = RegularFile
                         | isDirectory status = Directory
                         | isSymbolicLink status = SymbolicLink
                         | isNamedPipe status = NamedPipe
                         | isSocket status = Socket
                         | isCharacterDevice status = CharacterSpecial
                         | isBlockDevice status = BlockSpecial
                         | otherwise  = Unknown

statusToStat :: FileStatus ->  FileStat
statusToStat status =
  FileStat {
    statEntryType = statusToEntryType status,
    statFileMode = fileMode status,
    statLinkCount = linkCount status,
    statFileOwner = fileOwner status,
    statFileGroup = fileGroup status,
    statSpecialDeviceID = specialDeviceID status,
    statFileSize = fileSize status,
    statBlocks = fromIntegral $ ((fileSize status - 1) `div` 512) + 1,
    statAccessTime = accessTime status,
    statModificationTime = modificationTime status,
    statStatusChangeTime = statusChangeTime status
    }

fuseStat path = fmap statusToStat $ getSymbolicLinkStatus path

logging str = System.IO.hPutStrLn stderr str >> System.IO.hFlush stderr

cacheStat sourceDir path =
  do -- logging $ "statting " ++ show (sourceDir, path)
     -- fib 35 `seq` return ()
     res <- fuseStat $ sourceDir </> path
     return $ Right res

cacheReadDir sourceDir path =
  do files <- getDirectoryContents $ sourceDir </> path
     fmap Right $ mapM (\f -> do stat <- fuseStat (sourceDir </> path </> f)
                                 return (f, stat)) files

cacheOpen :: FilePath -> FilePath -> OpenMode -> OpenFileFlags -> IO (Either Errno Fd)
cacheOpen sourceDir path mode flags = fmap Right $ openFd (sourceDir </> path) mode Nothing flags

fib 0 = 1
fib 1 = 1
fib n = fib (n - 1) + fib (n - 2)

cacheRead :: FilePath -> FilePath -> Fd -> ByteCount -> FileOffset -> IO (Either Errno ByteString)
cacheRead sourceDir path fd length offset = do
  -- logging $ "reading " ++ show (sourceDir, path) ++ " " ++ show (fib 35)
  fib 35 `seq` return ()
  -- sleep 6
  fmap Right $ createAndTrim
    (fromIntegral length)
    (\buf -> fmap fromIntegral $ pread fd buf (fromIntegral length) offset)
  -- return $ Right $ pack "Hello world\n"
  
cacheRelease _ fd = closeFd fd

identityFS sourceDir = defaultFuseOps
  {
    fuseGetFileStat = cacheStat sourceDir,
    fuseOpenDirectory = const $ return eOK,
    fuseReleaseDirectory = const $ return eOK,
    fuseReadDirectory = cacheReadDir sourceDir,
    fuseOpen = cacheOpen sourceDir,
    fuseRead = cacheRead sourceDir,
    fuseRelease = cacheRelease
  }

main =
  do
    sourceDir:rest <- getArgs
    withArgs rest $ do
      fuseMain (identityFS sourceDir) defaultExceptionHandler
