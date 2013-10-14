{-# LANGUAGE RankNTypes #-}
{-|
    @pipes-tar@ is a library for the @pipes@-ecosystem that provides the ability
    to read from tar files in constant memory, and write tar files using as much
    memory as the largest file.
-}
module Pipes.Tar
    ( -- * Reading
      -- $reading
      parseTarEntries
    , iterTarArchive

      -- * Writing
      -- $writing
    , writeTarArchive

      -- * 'TarEntry'
    , TarHeader(..)
    , TarEntry, tarHeader, tarContent
    , EntryType(..)
    ) where

--------------------------------------------------------------------------------
import Control.Applicative
import Control.Monad ((>=>), guard, join, unless, void)
import Control.Monad.Trans.Free (FreeT(..), FreeF(..), iterT, transFreeT)
import Control.Monad.Writer.Class (tell)
import Data.Char (digitToInt, intToDigit, isDigit, ord)
import Data.Digits (digitsRev, unDigits)
import Data.Monoid ((<>), mconcat, Sum(..))
import Data.Time (UTCTime)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime, utcTimeToPOSIXSeconds)
import Data.Tuple (swap)
import Pipes ((>->))
import System.Posix.Types (CMode(..), FileMode)

--------------------------------------------------------------------------------
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import qualified Data.Serialize.Get as Get
import qualified Pipes
import qualified Pipes.Lift as Pipes
import qualified Pipes.Prelude as Pipes
import qualified Pipes.ByteString as PBS
import qualified Pipes.Parse as Parse

--------------------------------------------------------------------------------
-- | A 'TarEntry' contains all the metadata about a single entry in a tar file.
data TarHeader
    = TarHeader
        { entryPath :: !FilePath
        , entryMode :: !FileMode
        , entryUID :: !Int
        , entryGID :: !Int
        , entrySize :: !Int
        , entryLastModified :: !UTCTime
        , entryType :: !EntryType
        , entryLinkName :: !String
        }
    deriving (Eq, Show)


--------------------------------------------------------------------------------
data EntryType = File | Directory
    deriving (Eq, Show)


--------------------------------------------------------------------------------
data TarEntry m r = TarEntry { tarHeader :: TarHeader
                             , tarContent :: Pipes.Producer BS.ByteString m r
                             }

instance Monad m => Functor (TarEntry m) where
  fmap f (TarEntry header r) = TarEntry header (fmap f r)

----------------------------------------------------------------------------------
newtype TarArchive m = TarArchive (FreeT (TarEntry m) m
                                   (Pipes.Producer BS.ByteString m ()))

iterTarArchive
  :: Monad m
  => (forall a. TarHeader -> Pipes.Consumer BS.ByteString m a)
  -> TarArchive m -> m ()
iterTarArchive mkConsumer (TarArchive free) = iterT iterator (void free)
  where
    iterator (TarEntry h content) = join $ Pipes.runEffect $
      content Pipes.>-> mkConsumer h

----------------------------------------------------------------------------------
--{- $reading
--    tar files are read using 'parseTarEntries'.  In short, 'parseTarEntries' is
--    used to read the format of a tar file, turning a stream of bytes into a
--    stream of 'TarEntry's. The stream is contained under a free monad, which
--    forces you to consume each tar entry in order (as streams can only be read
--    in the forward direction).
--
--    Here is an example of how to read a tar file, outputting just the names of
--    all entries in the archive:
--
--    > import Control.Monad (void)
--    > import Control.Monad.IO.Class (liftIO)
--    > import Pipes
--    > import Pipes.ByteString (fromHandle)
--    > import Pipes.Parse (wrap)
--    > import Pipes.Prelude (discard)
--    > import Pipes.Tar
--    > import System.Environment (getArgs)
--    > import System.IO (withFile, IOMode(ReadMode))
--    >
--    > main :: IO ()
--    > main = do
--    >   (tarPath:_) <- getArgs
--    >   withFile tarPath ReadMode $ \h ->
--    >     void $ iterT readEntry (parseTarEntry (fromHandle h))
--    >
--    >  where
--    >
--    >   readEntry (TarEntry header k) = do
--    >     putStrLn . entryPath $ header
--    >     join (run (for k (const $ return ()))))
--
--    We use 'System.IO.withFile' to open a handle to the tar archive we wish to
--    read, and then 'Pipes.ByteString.fromHandle' to stream the contents of this
--    handle into something that can work with @pipes@.
--
--    As mentioned before, 'parseTarEntries' yields a free monad, and we can
--    destruct this using 'iterT'. We pass 'iterT' a function to operate on each
--    element, and the whole tar entry will be consumed from the start. The
--    important thing to note here is that each element contains the next
--    element in the stream as the return type of its content producer. It is
--    this property that forces us to consume every entry in order.
--
--    Our function to 'iterT' simply prints the 'entryPath' of a 'TarHeader',
--    and then runs the 'Pipes.Producer' for the content - substituting each
--    'Pipes.yield' with an empty action using 'Pipes.for'. Running this will
--    return us the 'IO ()' action that streams the /next/ 'TarEntry', so we
--    just 'join' that directly to ourselves.
--
--    Discarding content is a little boring though - here's an example of how we
--    can extend @readEntry@ to show all files ending with @.txt@:
--
--    > import Data.List (isSuffixOf)
--    > ...
--    >  where
--    >   readEntry (TarEntry header k) =
--    >     let reader = if ".txt" `isSuffixOf` entryPath tarEntry &&
--    >                       entryType tarEntry == File =
--    >                    then lift . putStrLn
--    >                    else (const (return ()))
--    >     in join (run (for k reader))
--
--    We now have two branches in @readEntry@ - and we choose a branch depending
--    on whether or not we are looking at a file with a file name ending in
--    ".txt". If so, we read the body by substitutting each 'Pipes.yield' with
--    'putStr'. If not, then we discard the contents as before. Either way, we
--    run the entire producer, and make sure to 'join' the next entry.
---}
parseTarEntries
  :: (Functor m, Monad m) => Pipes.Producer BS.ByteString m () -> TarArchive m
parseTarEntries = TarArchive . loop

 where

  loop upstream = FreeT $ do
    (headerBytes, rest) <- drawBytesUpTo 512 upstream
    go headerBytes rest

  go headerBytes remainder
    | BS.length headerBytes < 512 =
        return $ Pure (Pipes.yield headerBytes >> remainder)

    | BS.all (== 0) headerBytes = do
        (eofMarker, rest) <- drawBytesUpTo 512 remainder

        return $ Pure $
          if BS.all (== 0) eofMarker
            then rest
            else Pipes.yield eofMarker >> rest

    | otherwise =
        case decodeTar headerBytes of
          Left _ -> return (Pure (Pipes.yield headerBytes >> remainder))
          Right header ->
            return $ Free $ TarEntry header $ parseBody header remainder

  parseBody header = fmap loop . produceBody

   where

    produceBody = PBS.splitAt (entrySize header) >=> consumePadding

    consumePadding p
      | entrySize header == 0 = return p
      | otherwise =
          let padding = 512 - entrySize header `mod` 512
          in Pipes.for (PBS.splitAt padding p) (const $ return ())

--------------------------------------------------------------------------------
{-serializeTarEntries-}
  {-:: Monad m-}
  {-=> FreeT (TarEntry m) m (Pipes.Producer BS.ByteString m ())-}
  {--> Pipes.Producer BS.ByteString m ()-}
{-serializeTarEntries freed = join . Pipes.lift . iterT iterator-}

 {-where-}

  {-iterator (TarEntry header body) = do-}
    {-b <- body-}
    {-return body-}
    {-return $ do-}
      {-Pipes.yield (encodeTar header)-}
      {-b-}
    --Pipes.run body

--------------------------------------------------------------------------------
decodeTar :: BS.ByteString -> Either String TarHeader
decodeTar header = flip Get.runGet header $
    TarHeader <$> parseASCII 100
              <*> fmap fromIntegral (readOctal 8)
              <*> readOctal 8
              <*> readOctal 8
              <*> readOctal 12
              <*> (posixSecondsToUTCTime . fromIntegral <$> readOctal 12)
              <*  (do checksum <- Char8.takeWhile isDigit .
                                    Char8.dropWhile (== ' ') <$> Get.getBytes 8
                      guard (parseOctal checksum == expectedChecksum))
              <*> (Get.getWord8 >>= parseType . toEnum . fromIntegral)
              <*> parseASCII 100
              <*  Get.getBytes 255

  where

    readOctal :: Int -> Get.Get Int
    readOctal n = parseOctal <$> Get.getBytes n

    parseOctal :: BS.ByteString -> Int
    parseOctal x
        | BS.head x == 128 =
            foldl (\acc y -> acc * 256 + fromIntegral y) 0 .
                BS.unpack . BS.drop 1 $ x
        | otherwise =
            unDigits 8 . map digitToInt . Char8.unpack .
                Char8.dropWhile (== ' ') .
                BS.takeWhile (not . (`elem` [ fromIntegral $ ord ' ', 0 ])) $ x

    parseType '\0' = return File
    parseType '0' = return File
    parseType '5' = return Directory
    parseType x = error . show $ x

    parseASCII n = Char8.unpack . BS.takeWhile (/= 0) <$> Get.getBytes n

    expectedChecksum =
        let (left, rest) = BS.splitAt 148 header
            right = BS.drop 8 rest
        in (sum :: [Int] -> Int) $ map fromIntegral $ concatMap BS.unpack
            [ left, Char8.replicate 8 ' ', right ]


--------------------------------------------------------------------------------
encodeTar :: TarHeader -> BS.ByteString
encodeTar e =
  let truncated = take 100 (entryPath e)
      filePath = Char8.pack truncated <>
                 BS.replicate (100 - length truncated) 0
      mode = toOctal 7 $ case entryMode e of CMode m -> m
      uid = toOctal 7 $ entryUID e
      gid = toOctal 7 $ entryGID e
      size = toOctal 11 (entrySize e)
      modified = toOctal 11 . toInteger . round . utcTimeToPOSIXSeconds $
                     entryLastModified e
      eType = Char8.singleton $ case entryType e of
          File -> '0'
          Directory -> '5'
      linkName = BS.replicate 100 0
      checksum = toOctal 6 $ (sum :: [Int] -> Int) $ map fromIntegral $
          concatMap BS.unpack
              [ filePath, mode, uid, gid, size, modified
              , Char8.replicate 8 ' '
              , eType, linkName
              ]

  in mconcat [ filePath, mode, uid, gid, size, modified
             , checksum <> Char8.singleton ' '
             , eType
             , linkName
             , BS.replicate 255 0
             ]

 where

  toOctal n = Char8.pack . zeroPad .  reverse . ('\000' :) .
    map (intToDigit . fromIntegral) . digitsRev 8

   where

    zeroPad l = replicate (max 0 $ n - length l + 1) '0' ++ l


----------------------------------------------------------------------------------
{- $writing
    Like reading, writing tar files is done using @writeEntry@ type functions for
    the individual files inside a tar archive, and a final 'writeTar' 'P.Pipe' to
    produce a correctly formatted stream of 'BS.ByteString's.

    However, unlike reading, writing tar files can be done entirely with *pull
    composition*. Here's an example of producing a tar archive with one file in a
    directory:

    > import Data.Text
    > import Data.Text.Encoding (encodeUtf8)
    > import Pipes
    > import Pipes.ByteString (writeHandle)
    > import Pipes.Parse (wrap)
    > import Pipes.Tar
    > import Data.Time (getCurrentTime)
    > import System.IO (withFile, IOMode(WriteMode))
    >
    > main :: IO ()
    > main = withFile "hello.tar" WriteMode $ \h ->
    >   runEffect $
    >     (wrap . tarEntries >-> writeTar >-> writeHandle h) ()
    >
    >  where
    >
    >   tarEntries () = do
    >     now <- lift getCurrentTime
    >     writeDirectoryEntry "text" 0 0 0 now
    >     writeFileEntry "text/hello" 0 0 0 now <-<
    >       (wrap . const (respond (encodeUtf8 "Hello!"))) $ ()

    First, lets try and understand @tarEntries@, as this is the bulk of the work.
    @tarEntries@ is a 'Pipes.Producer', which is responsible for producing
    'CompleteEntry's for 'writeTar' to consume. A 'CompleteEntry' is a
    combination of a 'TarEntry' along with any associated data. This bundled is
    necessary to ensure that when writing the tar, we don't produce a header
    where the size header itself doesn't match the actual amount of data that
    follows. It's for this reason 'CompleteEntry''s constructor is private - you
    can only respond with 'CompleteEntry's using primitives provided by
    @pipes-tar@.

    The first primitive we use is 'writeDirectory', which takes no input and
    responds with a directory entry. The next response is a little bit more
    complicated. Here, I'm writing a text file to the path \"text/hello\" with the
    contents \"Hello!\". 'writeFileEntry' is a 'Pipes.Pipe' that consumes a
    'Nothing' terminated stream of 'BS.ByteStrings', and responds with a
    corresponding 'CompleteEntry'. I've used flipped pull combination ('Pipes.<-<') to
    avoid more parenthesis, and 'Pipes.Parse.wrap' a single 'respond' call which
    produces the actual file contents.
-}
writeTarArchive
  :: Monad m
  => TarArchive m -> Pipes.Producer BS.ByteString m ()
writeTarArchive (TarArchive archive) = Parse.concat (transFreeT f (void archive))
  where
    f (TarEntry h content) = do
      Pipes.yield (encodeTar h)
      (r, Sum fileSize) <- Pipes.runWriterP $
        Pipes.hoist Pipes.lift content >-> Pipes.chain (tell . Sum . BS.length)
      unless (fileSize `mod` 512 == 0) $
          Pipes.yield (BS.replicate (512 - fileSize `mod` 512) 0)
      return r


----------------------------------------------------------------------------------
---- | Fold all 'Just BS.ByteString's into a single 'CompleteEntry' with file
---- related metadata.
--writeFileEntry
--    :: Monad m
--    => FilePath -> FileMode -> Int -> Int -> UTCTime
--    -> () -> Pipes.Pipe (Maybe BS.ByteString) CompleteEntry m ()
--writeFileEntry path mode uid gid modified () = do
--    content <- mconcat <$> requestAll
--    let header =
--            TarEntry { entryPath = path
--                     , entrySize = BS.length content
--                     , entryMode = mode
--                     , entryUID = uid
--                     , entryGID = gid
--                     , entryLastModified = modified
--                     , entryType = File
--                     , entryLinkName = ""
--                     }
--
--    Pipes.respond (CompleteEntry header content)
--
--  where
--
--    requestAll = go id
--      where
--        go diffAs = do
--            ma <- Pipes.request ()
--            case ma of
--                Nothing -> return (diffAs [])
--                Just a -> go (diffAs . (a:))
--
--
----------------------------------------------------------------------------------
---- | Produce a single 'CompleteEntry' with correct metadata for a directory.
--writeDirectoryEntry
--    :: Monad m
--    => FilePath -> FileMode -> Int -> Int -> UTCTime
--    -> Pipes.Producer CompleteEntry m ()
--writeDirectoryEntry path mode uid gid modified = do
--    let header =
--            TarEntry { entryPath = path
--                     , entrySize = 0
--                     , entryMode = mode
--                     , entryUID = uid
--                     , entryGID = gid
--                     , entryLastModified = modified
--                     , entryType = Directory
--                     , entryLinkName = ""
--                     }
--
--    Pipes.respond (CompleteEntry header mempty)
--
--
----------------------------------------------------------------------------------
---- | Convert a stream of 'Nothing' terminated 'CompleteEntry's into a
---- 'BS.ByteString' stream. Terminates after writing the EOF marker when the
---- first 'Nothing' value is consumed.
--writeTar :: Monad m => () -> Pipes.Pipe (Maybe CompleteEntry) BS.ByteString m ()
--writeTar () = do
--    entry <- Pipes.request ()
--    case entry of
--        Nothing -> Pipes.respond (BS.replicate 1024 0)
--        Just (CompleteEntry header content) -> do
--            let fileSize = entrySize header
--            Pipes.respond (encodeTar header)
--            Pipes.respond content
--            unless (fileSize `mod` 512 == 0) $
--                Pipes.respond (BS.replicate (512 - fileSize `mod` 512) 0)
--            writeTar ()

--------------------------------------------------------------------------------
drawBytesUpTo
  :: (Monad m, Functor m)
  => Int
  -> Pipes.Producer PBS.ByteString m r
  -> m (PBS.ByteString, Pipes.Producer PBS.ByteString m r)
drawBytesUpTo n p = fmap swap $ Pipes.runEffect $ Pipes.runWriterP $
  Pipes.for (Pipes.hoist Pipes.lift (PBS.splitAt n p)) tell
