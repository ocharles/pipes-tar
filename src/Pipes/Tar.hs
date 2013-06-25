{-# LANGUAGE DeriveDataTypeable #-}
module Pipes.Tar
    ( -- * Reading archives
      readTar
    , readCurrentEntry
    , TarEntry(..)
    , TarReader

      -- * Writing archives
    , writeTar
    , writeFileEntry
    , writeDirectoryEntry

      -- * TarT transformer
    , TarParseState
    , TarT
    , runTarP

    , TarException(..)
    ) where


--------------------------------------------------------------------------------
import Control.Applicative
import Control.Exception
import Control.Monad (guard, msum, mzero, unless, when)
import Control.Monad.Trans.Class (lift)
import Data.Char (intToDigit, isDigit, ord)
import Data.Digits (digitsRev)
import Data.Foldable (forM_)
import Data.Function (fix)
import Data.Monoid (Monoid(..), (<>))
import Data.Time.Clock.POSIX (posixSecondsToUTCTime, utcTimeToPOSIXSeconds)
import Data.Time (UTCTime)
import Data.Typeable (Typeable)
import Data.Word ()
import Pipes.ByteString (drawBytesUpTo, skipBytesUpTo)
import System.Posix.Types (CMode(..), FileMode)


--------------------------------------------------------------------------------
import qualified Control.Monad.Trans.Either as Either
import qualified Control.Monad.Trans.State.Strict as State
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lex.Integral as Lexing
import qualified Data.Serialize.Get as Get
import qualified Pipes
import qualified Pipes.Internal as PI
import qualified Pipes.Lift as Pipes
import qualified Pipes.Parse as Parse


--------------------------------------------------------------------------------
-- | 'TarParseState' is internal state that keeps track of how much of a file
-- has been read, along with other parts of book keeping.
data TarParseState = TarParseState [BS.ByteString] (Maybe TarEntry)
  deriving (Eq, Show)

-- Lens into push-back buffer
pushBack
    :: Functor f
    => ([BS.ByteString] -> f [BS.ByteString]) -> TarParseState -> f TarParseState
pushBack f (TarParseState pb e) = (\x -> TarParseState x e) <$> (f pb)

-- Lens into the current 'TarEntry' being processed
currentTarEntry
    :: Functor f
    => (Maybe TarEntry -> f (Maybe TarEntry)) -> TarParseState -> f TarParseState
currentTarEntry f (TarParseState pb e) = (\x -> TarParseState pb x) <$> (f e)


--------------------------------------------------------------------------------
-- | The 'TarReader' type is a token that allows a 'Pipes.Proxy' to be composed
-- with 'readTar'. The only thing producing these tokens is 'readCurrentEntry',
-- thus the only thing that can be immediately composed with 'readTar' is
-- 'readCurrentEntry'.
newtype TarReader = TarR ()


--------------------------------------------------------------------------------
-- | Possible errors that can occur when reading tar files
data TarException
  = -- | The header for a tar entry could not be parsed
    InvalidHeader BS.ByteString
  | -- | The EOF marker in the archive could not be parsed
    InvalidEOF
  deriving (Show, Typeable)

instance Exception TarException


--------------------------------------------------------------------------------
-- | A 'TarEntry' contains all the metadata about a single entry in a tar file.
data TarEntry = TarEntry { entryPath :: !FilePath
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


--------------------------------------------------------------------------------
decodeTar :: BS.ByteString -> Either String TarEntry
decodeTar header = flip Get.runGet header $
    TarEntry <$> parseASCII 100
             <*> fmap fromIntegral (readOctal 8)
             <*> readOctal 8
             <*> readOctal 8
             <*> readOctal 12
             <*> (posixSecondsToUTCTime . fromIntegral <$> readOctal 12)
             <*  (do checksum <- Char8.takeWhile isDigit .
                                   Char8.dropWhile (== ' ') <$> Get.getBytes 8
                     parseOctal checksum >>= guard . (== expectedChecksum))
             <*> (Get.getWord8 >>= parseType . toEnum . fromIntegral)
             <*> parseASCII 100
             <*  Get.getBytes 255

  where

    readOctal n = Get.getBytes n >>= parseOctal

    parseOctal x =
        msum [ maybe mzero (return . fst) . Lexing.readOctal .
                   Char8.dropWhile (== ' ') .
                   BS.takeWhile (not . (`elem` [ fromIntegral $ ord ' ', 0 ])) $
                     x
             , return (readBase256 x)
             ]

    readBase256 :: BS.ByteString -> Int
    readBase256 = foldl (\acc x -> acc * 256 + fromIntegral x) 0 .
        BS.unpack . BS.drop 1

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
encodeTar :: TarEntry -> BS.ByteString
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

        toOctal n =
            Char8.pack . zeroPad .
                reverse . ('\000' :) .  map (intToDigit . fromIntegral) . digitsRev 8

          where

            zeroPad l = replicate (max 0 $ n - length l + 1) '0' ++ l


--------------------------------------------------------------------------------
-- | Transform a 'BS.ByteString' into a stream of 'TarEntry's. Each 'TarEntry'
-- can be expanded into its respective 'BS.ByteString' using 'readTar'. Parsing
-- tar archives can fail, so you may wish to use the @pipes-safe@ library, which
-- is compatible with 'Pipes.Proxy'.
--
-- Users should note that when 'readTar' is combined with 'readTar' using
-- 'Pipes./>/' (for example, 'readTar' 'Pipes./>/' 'flip' 'readTar' @()),
-- then 'readTar' will have control of terminating the entire 'Proxy' and
-- *not* the down-stream handler. This means that the entire archive will always
-- be streamed, whether or not you consume all files.
readTar :: Monad m
    => () -> Pipes.Proxy () (Maybe BS.ByteString) TarReader TarEntry (TarT m) ()
readTar () = fix $ \loop -> do
    header <- Parse.zoom pushBack $ drawBytesUpTo 512

    if BS.all (== 0) header
        then parseEOF
        else parseHeader header loop

  where
    parseHeader header loop =
        case decodeTar header of
            Left _ -> lift . lift . Either.left . toException $
                InvalidHeader header
            Right e -> do
                Parse.zoom currentTarEntry $ lift $ State.put (Just e)
                Pipes.respond e
                loop

    parseEOF = do
        part2 <- Parse.zoom pushBack $ drawBytesUpTo 512
        when (BS.all (/= 0) part2) $
            lift . lift . Either.left . toException $ InvalidEOF


--------------------------------------------------------------------------------
-- | Expand the current 'TarEntry' into a 'BS.ByteString'. The intended usage
-- here is you nest this call inside the 'Pipes.Proxy' downstream of 'readTar'.
--
-- For example:
--
-- > readTar />/ (\e -> (readCurrentEntry >-> writeFile (entryName e)) ())
--
-- This example uses respond composition ('Pipes./>/') to join in the tar
-- entry handler, introduced with @\e@. This allows you to perform some logic on
-- each 'TarEntry' - deciding how/whether you want to process each entry.
readCurrentEntry
    :: Monad m
    => () -> Pipes.Proxy () (Maybe BS.ByteString) () BS.ByteString
                 (TarT m) TarReader
readCurrentEntry () = do
    e <- Parse.zoom currentTarEntry $ lift State.get
    forM_ e $ \entry ->
        case entryType entry of
            File -> do
                loop entry (entrySize entry)
                Parse.zoom pushBack $
                    skipBytesUpTo (512 - entrySize entry `mod` 512)
            _ -> return ()

    return $ TarR ()
  where
    loop entry remainder = when (remainder > 0) $ do
        mbs <- Parse.zoom pushBack Parse.draw
        forM_ mbs $ \bs -> do
            let len = BS.length bs
            if len <= remainder
                then do
                    Pipes.respond bs
                    loop entry (remainder - len)
                else do
                    let (prefix, suffix) = BS.splitAt remainder bs
                    Parse.zoom pushBack $ Parse.unDraw suffix
                    Pipes.respond prefix


--------------------------------------------------------------------------------
data CompleteEntry = CompleteEntry TarEntry BS.ByteString
  deriving (Eq, Show)


--------------------------------------------------------------------------------
writeFileEntry
    :: Monad m
    => FilePath -> FileMode -> Int -> Int -> UTCTime ->
    () -> Pipes.Pipe (Maybe BS.ByteString) CompleteEntry m ()
writeFileEntry path mode uid gid modified () = do
    content <- mconcat <$> requestAll
    let header =
            TarEntry { entryPath = path
                     , entrySize = BS.length content
                     , entryMode = mode
                     , entryUID = uid
                     , entryGID = gid
                     , entryLastModified = modified
                     , entryType = File
                     , entryLinkName = ""
                     }

    Pipes.respond (CompleteEntry header content)

  where

    requestAll = go id
      where
        go diffAs = do
            ma <- Pipes.request ()
            case ma of
                Nothing -> return (diffAs [])
                Just a -> go (diffAs . (a:))


--------------------------------------------------------------------------------
writeDirectoryEntry
    :: Monad m
    => FilePath -> FileMode -> Int -> Int -> UTCTime
    -> Pipes.Producer CompleteEntry m ()
writeDirectoryEntry path mode uid gid modified = do
    let header =
            TarEntry { entryPath = path
                     , entrySize = 0
                     , entryMode = mode
                     , entryUID = uid
                     , entryGID = gid
                     , entryLastModified = modified
                     , entryType = Directory
                     , entryLinkName = ""
                     }

    Pipes.respond (CompleteEntry header mempty)


--------------------------------------------------------------------------------
writeTar :: Monad m => () -> Pipes.Pipe (Maybe CompleteEntry) BS.ByteString m ()
writeTar () = fix $ \next -> do
    entry <- Pipes.request ()
    case entry of
        Nothing -> Pipes.respond (BS.replicate 1024 0)
        Just (CompleteEntry header content) -> do
            let fileSize = entrySize header
            Pipes.respond (encodeTar header)
            Pipes.respond content
            unless (fileSize `mod` 512 == 0) $
                Pipes.respond (BS.replicate (512 - fileSize `mod` 512) 0)
            next


--------------------------------------------------------------------------------
-- | The 'TarT' monad transformer is  'State.StateT' with
-- 'TarParseState' as state, and the possibility of errors via 'SomeException'
type TarT m = State.StateT TarParseState (Either.EitherT SomeException m)


-- | Run a 'TarP' 'Pipes.Proxy'.
runTarP :: Monad m =>
    Pipes.Proxy a' a b' b (TarT m) r ->
    Pipes.Proxy a' a b' b m (Either SomeException r)
runTarP = runEitherP . Pipes.evalStateP startingState
  where
    startingState = TarParseState [] Nothing
    runEitherP p = case p of
        PI.Request a' fa -> PI.Request a' (\a -> runEitherP (fa a ))
        PI.Respond b fb' -> PI.Respond b (\b' -> runEitherP (fb' b'))
        PI.Pure r -> PI.Pure (Right r)
        PI.M m -> PI.M (do
            x <- Either.runEitherT m
            return (case x of
                Left e -> PI.Pure (Left e)
                Right p' -> runEitherP p' ) )
