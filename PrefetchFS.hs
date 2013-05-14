import Control.Concurrent
import Control.Concurrent.MSampleVar
import Control.Exception
import Control.Monad (when, unless, void)
import qualified Data.ByteString as BS
import Data.ByteString.Internal
import Data.Maybe
import Data.Foldable (forM_)
import Foreign.ForeignPtr.Safe
import GHC.IO.Exception
import System.Directory (createDirectoryIfMissing, getDirectoryContents)
import System.Environment
import System.FilePath.Posix (dropFileName)
import System.Fuse
import System.IO
import System.IO.Error
import System.Posix.Directory
import System.Posix.Files
import System.Posix.IO
import System.Posix.IO.Extra
import System.Posix.Types

(</>) :: String -> String -> String
a </> b = a ++ "/" ++ b

bSize :: Int
bSize = 4096

bSizeOff :: COff
bSizeOff = fromIntegral bSize

data State = State { sourceFd :: Fd,
                     destFd :: Fd,
                     metaFd :: Fd,
                     blocks :: FileOffset,
                     prefetchSVar :: MSampleVar FileOffset,
                     prefetchThreadId :: Maybe ThreadId }

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

fuseStat :: FilePath -> IO FileStat
fuseStat path = fmap statusToStat $ getSymbolicLinkStatus path

logging :: String -> IO ()
logging _ = return () -- void (System.IO.hPutStrLn stderr str)

prefetchStat :: FilePath -> FilePath -> IO (Either Errno FileStat)
prefetchStat sourceDir path =
  do logging $ "statting " ++ show (sourceDir, path)
     fmap Right (fuseStat $ sourceDir </> path)
       `catch`
       (\e -> case ioeGetErrorType e of
           GHC.IO.Exception.NoSuchThing -> return $ Left eNOENT
           _ -> throw e)

prefetchReadDir :: FilePath -> FilePath -> IO (Either Errno [(FilePath, FileStat)])
prefetchReadDir sourceDir path =
  do files <- getDirectoryContents $ sourceDir </> path
     fmap Right $ mapM (\f -> do stat <- fuseStat (sourceDir </> path </> f)
                                 return (f, stat)) files

fitDiv :: COff -> COff -> COff
a `fitDiv` b | a `mod` b == 0 = a `div` b
             | otherwise = a `div` b + 1

prefetchOpen :: FilePath -> FilePath -> FilePath -> OpenMode -> OpenFileFlags -> IO (Either Errno State)
prefetchOpen sourceDir destDir path mode flags = do
  source <- openFd (sourceDir </> path) mode Nothing flags
  size <- fdSeek source SeekFromEnd 0
  logging $ "successful open of " ++ path ++ " with size: " ++ show size
  if (size::COff) > 8388608 -- TODO(errge): option for specifying large files
   then do -- caching only for big files, subtitle hack
    createDirectoryIfMissing True (dropFileName $ destDir </> path)
    dest <- openFd (destDir </> path) ReadWrite (Just 438) defaultFileFlags
    meta <- openFd (destDir </> path ++ ".meta") ReadWrite (Just 438) defaultFileFlags
    let blockCount = size `fitDiv` bSizeOff
    setFdSize meta $ fromIntegral blockCount
    svar <- newSV 0
    tid <- forkIO $ prefetchThread (State source dest meta blockCount svar Nothing)
    return $ Right $ State source dest meta blockCount svar $ Just tid
   else
    return $ Right $ State source (-1) (-1) (-1) undefined Nothing

rangeToBlocks :: COff -> COff -> [COff]
rangeToBlocks offset len = [offset `div` bSizeOff .. (offset + len - 1) `div` bSizeOff]

preadBS :: Fd -> COff -> Int -> IO ByteString
preadBS f offset len = createAndTrim
                       len
                       (\buf -> fmap fromIntegral $ pread f buf len offset)

pwriteBS :: Fd -> ByteString -> COff -> Int -> IO ByteCount
pwriteBS f dat offset len = do
  let (fp, start, size) = toForeignPtr dat
  assert (start == 0) $ return ()
  assert (size == len) $ return ()
  withForeignPtr fp (\p -> pwrite f p len offset)

oneBS :: ByteString
oneBS = BS.singleton 1

assertEqM :: (Monad m, Eq a) => a -> m a -> m ()
assertEqM etalon a = do res <- a
                        assert (res == etalon) $ return ()

readBlock :: Bool -> State -> COff -> IO ByteString
readBlock prefetch (State s d m _ svNextBlock _) blockn | m == -1 = preadBS s (blockn * bSizeOff) bSize
                                                        | otherwise =
  bracket_
    (lock m (WriteLock, AbsoluteSeek, blockn, 1))
    (lock m (Unlock, AbsoluteSeek, blockn, 1)) $ do
      [isCached] <- fmap BS.unpack $ preadBS m blockn 1
      case isCached of
        1 -> do logging $ "block " ++ show blockn ++ " is already cached"
                preadBS d (blockn * bSizeOff) bSize
        _ -> do logging $ "block " ++ show blockn ++ " is being cached"
                unless prefetch $ writeSV svNextBlock (blockn + 1)
                dat <- preadBS s (blockn * bSizeOff) bSize
                assertEqM (fromIntegral $ BS.length dat) $ pwriteBS d dat (blockn * bSizeOff) (BS.length dat)
                assertEqM 1 $ pwriteBS m oneBS blockn 1
                logging $ "block " ++ show blockn ++ " caching done"
                return dat
  where
    lock = if prefetch then setLock else waitToSetLock

prefetchRead :: FilePath -> State -> ByteCount -> FileOffset -> IO (Either Errno BS.ByteString)
prefetchRead _ state len offset = do
  bss <- mapM (readBlock False state) (rangeToBlocks offset (fromIntegral len))
  return $ Right $ BS.take (fromIntegral len) $ BS.drop (fromIntegral $ offset `mod` bSizeOff) $ BS.concat bss

prefetchRelease :: FilePath -> State -> IO ()
prefetchRelease _ (State { sourceFd = s, destFd = d, metaFd = m, prefetchThreadId = t }) = do
  forM_ t killThread
  when (d >= 0) $ closeFd d
  when (m >= 0) $ closeFd m
  closeFd s

prefetchFS :: FilePath -> FilePath -> FuseOperations State
prefetchFS sourceDir destDir = FuseOperations
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
    , fuseOpen = prefetchOpen sourceDir destDir
    , fuseRead = prefetchRead
    , fuseWrite =  \_ (State { sourceFd = fd }) bs off -> fmap Right $ pwriteBS fd bs off (BS.length bs)
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

prefetchThread :: State -> IO ()
prefetchThread state =
  overrideFetch `finally` (logging $ "prefetchThread exit")
  where
    svNextBlock = prefetchSVar state
    overrideFetch = do
      logging $ "Getting next block to prefetch from the main thread"
      readSV svNextBlock >>= prefetch
    prefetch blck | blck >= blocks state || blck < 0 = overrideFetch
    prefetch blck | otherwise = do
      logging $ "prefetching block " ++ show blck
      empty <- isEmptySV svNextBlock
      case empty of
        False -> overrideFetch
        True -> do
          (void $ readBlock True state blck) `catch` (\e -> when (ioeGetErrorType e /= GHC.IO.Exception.ResourceBusy) (throw e))
          prefetch (blck+1)

main :: IO ()
main =
  do
    sourceDir:destDir:rest <- getArgs
    withArgs rest $ do
      fuseMain (prefetchFS sourceDir destDir) defaultExceptionHandler
