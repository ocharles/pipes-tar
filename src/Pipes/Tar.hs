{-# LANGUAGE RecordWildCards #-}
{-|
    @pipes-tar@ is a library for the @pipes@-ecosystem that provides the ability
    to read from tar files in constant memory, and write tar files using as much
    memory as the largest file.
-}
module Pipes.Tar
    ( -- * Reading
      -- $reading
      parseTarEntries

      -- * Writing
      -- $writing

      -- * API Reference
      -- ** Parsing Isomorphisms
    ) where

--------------------------------------------------------------------------------
import Control.Applicative
import Control.Monad (guard, join, unless, when)
import Control.Monad.Trans.Free (FreeT(..), FreeF(..), iterT)
import Data.Char (digitToInt, intToDigit, isDigit, ord)
import Data.Digits (digitsRev, unDigits)
import Data.Maybe (fromMaybe)
import Data.Monoid ((<>), mconcat, mempty)
import Data.Time (UTCTime)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime, utcTimeToPOSIXSeconds)
import Pipes ((>->))
import System.Posix.Types (CMode(..), FileMode)

--------------------------------------------------------------------------------
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import qualified Data.Serialize.Get as Get
import qualified Pipes
import qualified Pipes.ByteString as Pipes
import qualified Pipes.Parse as Parse
import qualified Pipes.Prelude as Pipes

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


data EntryType = File | Directory
    deriving (Eq, Show)


data TarEntry m r = TarEntry TarHeader (Pipes.Producer BS.ByteString m r)

instance Monad m => Functor (TarEntry m) where
  fmap f (TarEntry header r) = TarEntry header (fmap f r)

--------------------------------------------------------------------------------
drawBytesUpTo
  :: (Functor m, Monad m)
  => Int
  -> Parse.StateT (Pipes.Producer BS.ByteString m r) m BS.ByteString
drawBytesUpTo = loop mempty

 where

  loop acc remaining
    | remaining <= 0 = return acc
    | otherwise = do
        (bytes, suffix) <-
          fromMaybe (mempty, mempty) . fmap (BS.splitAt remaining) <$>
            Parse.draw
        let acc' = acc <> bytes
        if BS.null suffix
            then loop acc' (remaining - BS.length bytes)
            else Parse.unDraw suffix >> return acc'


--------------------------------------------------------------------------------
yieldBytesUpTo
  :: (Functor m, Monad m)
  => Int
  -> Pipes.Producer BS.ByteString m r
  -> Pipes.Producer BS.ByteString m (Pipes.Producer BS.ByteString m r)
yieldBytesUpTo maxBytes = loop maxBytes

 where

  loop n p
    | n == 0 = return p
    | otherwise = do
        stepped <- Pipes.lift (Pipes.next p)
        case stepped of
          Left r -> return (return r)
          Right (produced, k) -> do
            let (bytes, suffix) = BS.splitAt n produced
            Pipes.yield (bytes :: BS.ByteString)
            if BS.null suffix
                then loop (n - BS.length bytes) k
                else return (Pipes.yield suffix >> k)

--------------------------------------------------------------------------------
parseTarEntries
  :: (Functor m, Monad m)
  => Pipes.Producer BS.ByteString m ()
  -> FreeT (TarEntry m) m (Pipes.Producer BS.ByteString m ())
parseTarEntries upstream = FreeT $ do
  (headerBytes, rest) <- Parse.runStateT (drawBytesUpTo 512) upstream
  go headerBytes rest

 where

  go headerBytes remainder
    | BS.length headerBytes < 512 =
        return $ Pure (Pipes.yield headerBytes >> remainder)

    | BS.all (== 0) headerBytes = do
        (eofMarker, rest) <- Parse.runStateT (drawBytesUpTo 512) remainder

        return $
          if BS.all (== 0) eofMarker
            then Pure rest
            else Pure (Pipes.yield eofMarker >> rest)

    | otherwise =
        case decodeTar headerBytes of
          Left _ -> return (Pure (Pipes.yield headerBytes))
          Right header ->
            return $ Free $ TarEntry header $ parseBody remainder header

  parseBody prod header = parseTarEntries <$> do
    produceBody prod >>= consumePadding

   where

    produceBody = yieldBytesUpTo (entrySize header)

    consumePadding p
      | entrySize header == 0 = return p
      | otherwise =
          let padding = 512 - entrySize header `mod` 512
          in Pipes.for (yieldBytesUpTo padding p) (const $ return ())

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
-- | Utility function to fold the entire ByteString producer into
-- a ByteString
drawAll
  :: Monad m => Parse.StateT (Pipes.Producer BS.ByteString m r) m BS.ByteString
drawAll = Pipes.fold BS.append BS.empty id Parse.input

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
            foldl (\acc x -> acc * 256 + fromIntegral x) 0 .
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
--{- $reading
--    tar files are read using two functions provided by @pipes-tar@ - 'readTar'
--    and 'readCurrentEntry'. In short, 'readTar' is used to read the format of a
--    tar file, turning a stream of bytes into a stream of 'TarEntry's, while
--    'readCurrentEntry' is used to stream the contents of the current file.
--
--    Here is an example of how to read a tar file, outputting just the names of
--    all entries in the archive:
--
--    > import Control.Monad (void)
--    > import Control.Monad.IO.Class (liftIO)
--    > import Pipes
--    > import Pipes.ByteString (readHandle)
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
--    >     void $ runEffect $ runTarP $
--    >       (wrap . hoist liftIO . readHandle h >-> readTar />/ readEntry) ()
--    >
--    >  where
--    >
--    >   readEntry tarEntry = do
--    >     liftIO $ print . entryPath $ tarEntry
--    >     (readCurrentEntry >-> discard) ()
--
--    We use 'System.IO.withFile' to open a handle to the tar archive we wish to
--    read, and then 'Pipes.ByteString.readHandle' to stream the contents of this
--    handle into something that can work with @pipes@. 'readTar' requires the
--    stream to be wrapped so it knows when the end of the file is encountered,
--    which we can achieve composing with 'Pipes.Parse.wrap'. As
--    'Pipes.ByteString.readHandle' has 'IO' as its base monad, we use
--    'Control.Monad.Morph.hoist' and 'Control.Monad.IO.Class.liftIO' to allow
--    'Pipes.ByteString.readHandle' to be composed with 'readTar'.
--
--    To connect 'readTar', we use the 'Pipes./>/' combinator which is
--    /respond composition/. Thus @\/>\/ readEntry@ can be understood as
--    substituing all 'Pipe.respond's from 'readTar' (that is, whenever it responds
--    with a 'TarEntry') with a call directly to @readEntry@.
--
--    @readEntry@ is a simple monadic action that uses a combination of
--    'Control.Monad.IO.Class.liftIO' and 'print' to output the 'entryPath'
--    attribute of each 'TarEntry' that 'readTar' reads.
--
--    We have to complete this action by reading all bytes of that tar entry. As we
--    dont plan to do anything with them, we simply 'Pipes.discard' all bytes. If
--    you forget to consume all bytes of a 'TarEntry', you will be informed by a
--    type error:
--
--    This is because the type of 'readEntry' is @Monad m => TarEntry -> Pipes.Proxy ()
--    (Maybe ByteString) b' b m TarReader@ -- notice the return type.
--
--    'TarReader' is a special value, that can be thought of as a token that
--    "grants" a proxy the ability to read from a tar file. @pipes-tar@ uses this
--    to ensure that downstream consumers of a tar file never under- or
--    over-consume the contents of individual entries. The only way you can
--    generate a 'TarReader' is with 'readCurrentEntry' -- notice that '>->'
--    preserves the return type of the leftmost proxy.
--
--    'discard'ing is a little boring though - here's an example of how we can
--    extend @readEntry@ to show all files ending with @.txt@:
--
--    > import Data.List (isSuffixOf)
--    > ...
--    >  where
--    >   readEntry tarEntry
--    >     | ".txt" `isSuffixOf` entryPath tarEntry &&
--    >       entryType tarEntry == File =
--    >         (readCurrentEntry >-> hoist liftIO . Pipes.Prelude.print) ()
--    >     | otherwise =
--    >         (readCurrentEntry >-> discard) ()
---}
--
--
----------------------------------------------------------------------------------
--{- $writing
--    Like reading, writing tar files is done using @writeEntry@ type functions for
--    the individual files inside a tar archive, and a final 'writeTar' 'P.Pipe' to
--    produce a correctly formatted stream of 'BS.ByteString's.
--
--    However, unlike reading, writing tar files can be done entirely with *pull
--    composition*. Here's an example of producing a tar archive with one file in a
--    directory:
--
--    > import Data.Text
--    > import Data.Text.Encoding (encodeUtf8)
--    > import Pipes
--    > import Pipes.ByteString (writeHandle)
--    > import Pipes.Parse (wrap)
--    > import Pipes.Tar
--    > import Data.Time (getCurrentTime)
--    > import System.IO (withFile, IOMode(WriteMode))
--    >
--    > main :: IO ()
--    > main = withFile "hello.tar" WriteMode $ \h ->
--    >   runEffect $
--    >     (wrap . tarEntries >-> writeTar >-> writeHandle h) ()
--    >
--    >  where
--    >
--    >   tarEntries () = do
--    >     now <- lift getCurrentTime
--    >     writeDirectoryEntry "text" 0 0 0 now
--    >     writeFileEntry "text/hello" 0 0 0 now <-<
--    >       (wrap . const (respond (encodeUtf8 "Hello!"))) $ ()
--
--    First, lets try and understand @tarEntries@, as this is the bulk of the work.
--    @tarEntries@ is a 'Pipes.Producer', which is responsible for producing
--    'CompleteEntry's for 'writeTar' to consume. A 'CompleteEntry' is a
--    combination of a 'TarEntry' along with any associated data. This bundled is
--    necessary to ensure that when writing the tar, we don't produce a header
--    where the size header itself doesn't match the actual amount of data that
--    follows. It's for this reason 'CompleteEntry''s constructor is private - you
--    can only respond with 'CompleteEntry's using primitives provided by
--    @pipes-tar@.
--
--    The first primitive we use is 'writeDirectory', which takes no input and
--    responds with a directory entry. The next response is a little bit more
--    complicated. Here, I'm writing a text file to the path \"text/hello\" with the
--    contents \"Hello!\". 'writeFileEntry' is a 'Pipes.Pipe' that consumes a
--    'Nothing' terminated stream of 'BS.ByteStrings', and responds with a
--    corresponding 'CompleteEntry'. I've used flipped pull combination ('Pipes.<-<') to
--    avoid more parenthesis, and 'Pipes.Parse.wrap' a single 'respond' call which
--    produces the actual file contents.
---}
--
--
----------------------------------------------------------------------------------
---- | A 'CompleteEntry' is a 'TarEntry' along with any corresponding data. These
---- values are constructed using the @writeEntry@ family of functions (e.g.,
---- 'writeFileEntry').
--data CompleteEntry = CompleteEntry TarEntry BS.ByteString
--  deriving (Eq, Show)
--
--
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
--
--
----------------------------------------------------------------------------------
---- | The 'TarT' monad transformer is  'State.StateT' with
---- 'TarParseState' as state, and the possibility of errors via 'SomeException'
--type TarT m = State.StateT TarParseState (Either.EitherT SomeException m)
--
--
----------------------------------------------------------------------------------
---- | Run a 'TarP' 'Pipes.Proxy'.
--runTarP
--    :: Monad m
--    => Pipes.Proxy a' a b' b (TarT m) r
--    -> Pipes.Proxy a' a b' b m (Either SomeException r)
--runTarP = runEitherP . Pipes.evalStateP startingState
--  where
--    startingState = TarParseState [] Nothing
--    runEitherP p = case p of
--        PI.Request a' fa -> PI.Request a' (\a -> runEitherP (fa a ))
--        PI.Respond b fb' -> PI.Respond b (\b' -> runEitherP (fb' b'))
--        PI.Pure r -> PI.Pure (Right r)
--        PI.M m -> PI.M (do
--            x <- Either.runEitherT m
--            return (case x of
--                Left e -> PI.Pure (Left e)
--                Right p' -> runEitherP p' ) )
