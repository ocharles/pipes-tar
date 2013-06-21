{-# LANGUAGE DeriveDataTypeable #-}
module Pipes.Tar
    ( -- * Reading archives
      readTar
    , readCurrentEntry
    , TarEntry(..)

      -- * Writing archives
    , writeTar
    , writeTarEntry

      -- * TarT transformer
    , TarParseState
    , TarT
    , runTarP

    , TarException(..)
    ) where

--------------------------------------------------------------------------------
import Control.Applicative
import Control.Exception
import Control.Monad (forever, msum, mzero, when)
import Control.Monad.Morph (hoist)
import Control.Monad.Trans.Class (lift)
import Data.Digits (digits, digitsRev, unDigits)
import Data.Function (fix)
import Data.Foldable (forM_)
import Data.Monoid ((<>), Monoid(..), Sum(..))
import Data.Serialize (Serialize(..), decode, encode)
import Data.Serialize.Get ()
import Data.Time (UTCTime)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import Data.Typeable (Typeable)
import Data.Word ()
import Pipes ((>->))
import Pipes.ByteString (drawBytesUpTo, skipBytesUpTo)
import System.Posix.Types (CMode(..), FileMode)


--------------------------------------------------------------------------------
import qualified Data.Serialize.Put as Put
import qualified Pipes
import qualified Pipes.Prelude as Pipes
import qualified Pipes.Lift as Pipes
import qualified Pipes.Internal as PI
import qualified Control.Monad.Trans.Either as Either
import qualified Control.Monad.Trans.State.Strict as State
import qualified Pipes.Parse as Parse
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lex.Integral as Lexing
import qualified Data.Serialize.Get as Get


--------------------------------------------------------------------------------
-- | 'TarParseState' is internal state that keeps track of how much of a file
-- has been read, along with other parts of book keeping.
data TarParseState = TarParseState [BS.ByteString] (Sum Int) (Maybe TarEntry)
  deriving (Eq, Show)

-- Lens into push-back buffer
pushBack
    :: Functor f
    => ([BS.ByteString] -> f [BS.ByteString]) -> TarParseState -> f TarParseState
pushBack f (TarParseState pb s e) = (\x -> TarParseState x s e) <$> (f pb)

-- Lens into the amount of bytes read
bytesRead
    :: Functor f
    => (Sum Int -> f (Sum Int)) -> TarParseState -> f TarParseState
bytesRead f (TarParseState pb s e) = (\x -> TarParseState pb x e) <$> (f s)

-- Lens into the current 'TarEntry' being processed
currentTarEntry
    :: Functor f
    => (Maybe TarEntry -> f (Maybe TarEntry)) -> TarParseState -> f TarParseState
currentTarEntry f (TarParseState pb s e) = (\x -> TarParseState pb s x) <$> (f e)

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
instance Serialize TarEntry where
    get = TarEntry <$> parseASCII 100
                   <*> fmap (CMode . fromIntegral) (readOctal 7) <* Get.skip 1
                   <*> readOctal 7 <* Get.skip 1
                   <*> readOctal 7 <* Get.skip 1
                   <*> readOctal 12
                   <*> (posixSecondsToUTCTime . fromIntegral <$> readOctal 11)
                   <* Get.skip 9
                   <*> (Get.getWord8 >>= parseType . toEnum . fromIntegral)
                   <*> parseASCII 100
                   <*  Get.getBytes 255
      where
        readOctal n =
            Get.getBytes n >>= \x ->
                msum [ maybe mzero (return . fst) . Lexing.readOctal $ BS.take 11 x
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

    -----------------------------------------------------------------------------
    put e = do
        let truncated = take 100 (entryPath e)
        Put.putByteString (Char8.pack truncated)
        Put.putByteString (BS.replicate (100 - length truncated) 0)
        writeOctal 7 0
        writeOctal 7 0
        writeOctal 7 0
        writeOctal 11 (entrySize e)
        writeOctal 11 0
        Put.putByteString (BS.replicate 8 0)
        Put.putWord8 0
        Put.putByteString (BS.replicate 100 0)
        Put.putByteString (BS.replicate 255 0)

      where

        writeOctal n =
            Put.putByteString . Char8.pack . zeroPad .
                reverse . ('\000' :) .  map toEnum . digitsRev 8

          where

            zeroPad l = (replicate (max 0 $ n - length l) '0' ++ l)


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
    => () -> Pipes.Pipe (Maybe BS.ByteString) TarEntry (TarT m) ()
readTar () = fix $ \loop -> do
    header <- Parse.zoom pushBack $ drawBytesUpTo 512

    if BS.all (== 0) header
        then parseEOF
        else parseHeader header loop

  where
    parseHeader header loop =
        case decode header of
            Left _ -> lift . lift . Either.left . toException $
                InvalidHeader header
            Right e -> do
                Parse.zoom bytesRead $ lift $ State.put (Sum 0)
                Parse.zoom currentTarEntry $ lift $ State.put (Just e)
                Pipes.respond e
                Sum consumed <- lift $ State.gets (getConst . bytesRead Const)
                Parse.zoom pushBack $ skipBytesUpTo (tarBlocks e - consumed)
                loop

    parseEOF = do
        part2 <- Parse.zoom pushBack $ drawBytesUpTo 512
        when (BS.all (/= 0) part2) $
            lift . lift . Either.left . toException $ InvalidEOF

    tarBlocks entry = (((entrySize entry - 1) `div` 512) + 1) * 512



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
    => () -> Pipes.Pipe (Maybe BS.ByteString) BS.ByteString (TarT m) ()
readCurrentEntry () = do
    e <- Parse.zoom currentTarEntry $ lift $ State.get
    forM_ e $ \entry -> do
        case entryType entry of
            File -> loop entry (entrySize entry)
            _ -> return ()
  where
    loop entry remainder = when (remainder > 0) $ do
        mbs <- Parse.zoom pushBack Parse.draw
        forM_ mbs $ \bs -> do
            let len = BS.length bs
            if len <= remainder
                then do
                    Parse.zoom bytesRead $ do
                        Sum n <- lift State.get
                        lift . State.put $! Sum (n + len)
                    Pipes.respond bs
                    loop entry (remainder - len)
                else do
                    let (prefix, suffix) = BS.splitAt remainder bs
                    Parse.zoom pushBack $ Parse.unDraw suffix
                    Parse.zoom bytesRead $
                      lift $ State.put (Sum (entrySize entry))
                    Pipes.respond prefix


--------------------------------------------------------------------------------
data CompleteEntry = CompleteEntry (Int -> TarEntry) BS.ByteString


--------------------------------------------------------------------------------
writeTarEntry
    :: Monad m
    => UTCTime -> () -> Pipes.Pipe (Maybe BS.ByteString) CompleteEntry m ()
writeTarEntry t () = do
    content <- mconcat <$> requestAll
    let headerBuilder = \size ->
            TarEntry { entryPath = "My little test path"
                     , entrySize = size
                     , entryMode = 0
                     , entryUID = 0
                     , entryGID = 0
                     , entryLastModified = t
                     , entryType = File
                     , entryLinkName = ""
                     }

    Pipes.respond (CompleteEntry headerBuilder content)

  where

    requestAll = go id
      where
        go diffAs = do
            ma <- Pipes.request ()
            case ma of
                Nothing -> return (diffAs [])
                Just a -> go (diffAs . (a:))


--------------------------------------------------------------------------------
writeTar :: Monad m => () -> Pipes.Pipe (Maybe CompleteEntry) BS.ByteString m ()
writeTar () = fix $ \next -> do
    entry <- Pipes.request ()
    case entry of
        Nothing -> Pipes.respond (BS.replicate 1024 0)
        Just (CompleteEntry headerBuilder content) -> do
            let fileSize = BS.length content
            Pipes.respond (encode $ headerBuilder fileSize)
            Pipes.respond content
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
    startingState = TarParseState [] (Sum 0) Nothing
    runEitherP p = case p of
        PI.Request a' fa -> PI.Request a' (\a -> runEitherP (fa a ))
        PI.Respond b fb' -> PI.Respond b (\b' -> runEitherP (fb' b'))
        PI.Pure r -> PI.Pure (Right r)
        PI.M m -> PI.M (do
            x <- Either.runEitherT m
            return (case x of
                Left e -> PI.Pure (Left e)
                Right p' -> runEitherP p' ) )
