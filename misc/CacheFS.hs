{-# LANGUAGE ScopedTypeVariables #-}

import Control.Exception
import qualified Data.ByteString as BS
import Data.ByteString.Internal
import Foreign.C.Error
import Foreign.C.Types
import Foreign.ForeignPtr.Safe
import System.Directory
import System.Environment
import System.FilePath.Posix hiding ((</>))
import System.Fuse
import System.IO
import System.Posix.Files
import System.Posix.IO
import System.Posix.IO.Extra
import System.Posix.Types

foreign import ccall safe ftruncate :: Fd -> COff -> IO CInt
ftrunc :: Fd -> Integer -> IO ()
ftrunc fd offset = throwErrnoIfMinus1Retry_ "ftruncate" $ ftruncate fd $ fromIntegral offset

a </> b = a ++ "/" ++ b

bSize :: Num a => a
bSize = fromIntegral 4096

data State = State { sourceFd :: Fd,
                     destFd :: Fd,
                     metaFd :: Fd }

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
    statBlocks = fromIntegral $ fileSize status `fitDiv` 512,
    statAccessTime = accessTime status,
    statModificationTime = modificationTime status,
    statStatusChangeTime = statusChangeTime status
    }

fuseStat path = fmap statusToStat $ getSymbolicLinkStatus path

logging str = return () -- System.IO.hPutStrLn stderr str

cacheStat sourceDir path =
  do logging $ "statting " ++ show (sourceDir, path)
     res <- fuseStat $ sourceDir </> path
     return $ Right res

cacheReadDir sourceDir path =
  do files <- getDirectoryContents $ sourceDir </> path
     fmap Right $ mapM (\f -> do stat <- fuseStat (sourceDir </> path </> f)
                                 return (f, stat)) files

a `fitDiv` b | a `mod` b == 0 = a `div` b
             | otherwise = a `div` b + 1

cacheOpen :: FilePath -> FilePath -> FilePath -> OpenMode -> OpenFileFlags -> IO (Either Errno State)
cacheOpen sourceDir destDir path mode flags = do
  source <- openFd (sourceDir </> path) mode Nothing flags
  createDirectoryIfMissing True (dropFileName $ destDir </> path)
  dest <- openFd (destDir </> path) ReadWrite (Just 438) defaultFileFlags
  meta <- openFd (destDir </> path ++ ".meta") ReadWrite (Just 438) defaultFileFlags
  size <- fdSeek source SeekFromEnd 0
  logging $ "successful open of " ++ path ++ " with size: " ++ show size
  ftrunc meta (fromIntegral $ size `fitDiv` bSize)
  return $ Right $ State source dest meta

rangeToBlocks offset len = [offset `div` bSize .. (offset + len - 1) `div` bSize]

preadBS :: Fd -> COff -> Int -> IO ByteString
preadBS f offset len = createAndTrim
                       len
                       (\buf -> fmap fromIntegral $ pread f buf len offset)

pwriteBS :: Fd -> ByteString -> COff -> Int -> IO ()
pwriteBS f dat offset len = do
  let (fp, start, size) = toForeignPtr dat
  assert (start == 0) $ return ()
  assert (size == len) $ return ()
  withForeignPtr fp (\p -> pwrite f p len offset >> return ())

oneBS = BS.singleton 1

readBlock (State s d m) blockn = do
  bracket_
    (waitToSetLock m (WriteLock, AbsoluteSeek, blockn, 1))
    (waitToSetLock m (Unlock, AbsoluteSeek, blockn, 1)) $ do
      [isCached] <- fmap BS.unpack $ preadBS m blockn 1
      case isCached of
        1 -> do logging $ "block " ++ show blockn ++ " is already cached"
                preadBS d (blockn * bSize) bSize
        _ -> do logging $ "block " ++ show blockn ++ " is being cached"
                dat <- preadBS s (blockn * bSize) bSize
                pwriteBS d dat (blockn * bSize) (BS.length dat)
                pwriteBS m oneBS blockn 1
                logging $ "block " ++ show blockn ++ " caching done"
                return dat

cacheRead :: FilePath -> State -> ByteCount -> FileOffset -> IO (Either Errno BS.ByteString)
cacheRead _ state len offset = do
  bss <- mapM (readBlock state) (rangeToBlocks offset (fromIntegral len))
  return $ Right $ BS.take (fromIntegral len) $ BS.drop (fromIntegral $ offset `mod` bSize) $ BS.concat bss

cacheRelease _ (State s d m) = do
  closeFd s
  closeFd d
  closeFd m

cacheFS sourceDir destDir = defaultFuseOps
  {
    fuseGetFileStat = cacheStat sourceDir,
    fuseOpenDirectory = const $ return eOK,
    fuseReleaseDirectory = const $ return eOK,
    fuseReadDirectory = cacheReadDir sourceDir,
    fuseOpen = cacheOpen sourceDir destDir,
    fuseRead = cacheRead,
    fuseRelease = cacheRelease
  }

main =
  do
    sourceDir:destDir:rest <- getArgs
    withArgs rest $ do
      fuseMain (cacheFS sourceDir destDir) defaultExceptionHandler
