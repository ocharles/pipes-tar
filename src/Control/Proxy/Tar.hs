{-# LANGUAGE DeriveDataTypeable #-}
module Control.Proxy.Tar
    ( tarArchive
    , tarEntry
    , TarEntry(..)
    , TarParseState

    , TarP
    , runTarK
    ) where

--------------------------------------------------------------------------------
import Control.Applicative
import Control.Exception
import Control.Monad (msum, mzero, when)
import Control.Proxy ((>->))
import Control.Proxy.ByteString (passBytesUpTo)
import Data.Function (fix)
import Data.Foldable (forM_)
import Data.Monoid ((<>), Monoid(..), Sum(..))
import Data.Serialize (Serialize(..), decode)
import Data.Serialize.Get ()
import Data.Time (UTCTime)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import Data.Typeable (Typeable)
import Data.Word ()
import System.Posix.Types (CMode(..), FileMode)


--------------------------------------------------------------------------------
import qualified Control.Proxy as Pipes
import qualified Control.Proxy.Parse as Parse
import qualified Control.Proxy.Trans.Either as Either
import qualified Control.Proxy.Trans.State as State
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lex.Integral as Lexing
import qualified Data.Serialize.Get as Get


--------------------------------------------------------------------------------
-- | 'TarParseState' is internal state that keeps track of how much of a file
-- has been read.
data TarParseState = TarParseState [BS.ByteString] (Sum Int)
  deriving (Eq, Show)

instance Monoid TarParseState where
    mappend (TarParseState pbA brA) (TarParseState pbB brB) =
        TarParseState (pbA <> pbB) (brA <> brB)
    mempty = TarParseState mempty mempty

-- Lens into push-back buffer
pushBack :: Functor f => ([BS.ByteString] -> f [BS.ByteString])
                      -> TarParseState -> f TarParseState
pushBack f (TarParseState pb s) = fmap (\x -> TarParseState x s) (f pb)

-- Lens into the amount of bytes read
bytesRead :: Functor f => (Sum Int -> f (Sum Int))
                       -> TarParseState -> f TarParseState
bytesRead f (TarParseState pb s) = fmap (\x -> TarParseState pb x) (f s)


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

    put = error "TarEntry serialization is not implemented"


--------------------------------------------------------------------------------
-- | Transform a 'BS.ByteString' into a stream of 'TarEntry's. Each 'TarEntry'
-- can be expanded into its respective 'BS.ByteString' using 'tarEntry'. Parsing
-- tar archives can fail, so you may wish to use the @pipes-safe@ library, which
-- is compatible with 'Pipes.Proxy'.
--
-- Users should note that when 'tarArchive' is combined with 'tarEntry' using
-- 'Pipes./>/' (for example, 'tarArchive' 'Pipes./>/' 'flip' 'tarEntry' @()),
-- then 'tarArchive' will have control of terminating the entire 'Proxy' and
-- *not* the down-stream handler. This means that the entire archive will always
-- be streamed, whether or not you consume all files.
tarArchive :: (Monad m, Pipes.Proxy p)
    => () -> Pipes.Pipe (TarP p) (Maybe BS.ByteString) TarEntry m ()
tarArchive () = fix $ \loop -> do
    header <- Parse.zoom pushBack $ drawBytes 512

    if BS.all (== 0) header
        then parseEOF
        else parseHeader header loop

  where
    parseHeader header loop =
        case decode header of
            Left _ -> Pipes.liftP . Either.left . toException $
                InvalidHeader header
            Right e -> do
                Parse.zoom bytesRead $ State.put (Sum 0)
                Pipes.respond e
                Sum consumed <- State.gets (getConst . bytesRead Const)
                Parse.zoom pushBack $ skipBytes (tarBlocks e - consumed)
                loop

    parseEOF = do
        part2 <- Parse.zoom pushBack $ drawBytes 512
        when (BS.all (/= 0) part2) $
            Pipes.liftP . Either.left . toException $ InvalidEOF

    tarBlocks entry = (((entrySize entry - 1) `div` 512) + 1) * 512



--------------------------------------------------------------------------------
-- | Expand a 'TarEntry' into a 'BS.ByteString'. The intended usage here is
-- you nest this call inside the 'Pipes.Proxy' downstream of 'tarArchive'.
-- For example:
--
-- > tarArchive />/ (\e -> tarEntry e >-> writeFile (entryName e))
--
-- This example uses respond composition ('Pipes./>/') to join in the tar
-- entry handler, introduced with @\e@. This allows you to perform some logic on
-- each 'TarEntry' - deciding how/whether you want to process each entry.
--
-- There is one caveat with 'tarEntry', which is: you can only stream the last
-- value produced by 'tarArchive'. So if you were to use pull composition and
-- 'Pipes.request'ed multiple 'TarEntry's - you can only call 'tarEntry' on the
-- latest 'TarEntry' that upstream responds with. Use of other 'tarEntry's will
-- type check, but the contents streamed will be the latest tar entry - not the
-- one passed in! If you use respond composition ('Pipes./>/') you should never
-- encounter this problem.
tarEntry :: (Monad m, Pipes.Proxy p)
    => TarEntry
    -> () -> Pipes.Pipe (TarP p) (Maybe BS.ByteString) BS.ByteString m ()
tarEntry entry () = case entryType entry of
    File -> loop (entrySize entry)
    _ -> return ()
  where
    loop remainder = when (remainder > 0) $ do
        mbs <- Parse.zoom pushBack Parse.draw
        forM_ mbs $ \bs -> do
            let len = BS.length bs
            if len <= remainder
                then do
                    Parse.zoom bytesRead $ do
                        Sum n <- State.get
                        State.put $! Sum (n + len)
                    Pipes.respond bs
                    loop (remainder - len)
                else do
                    let (prefix, suffix) = BS.splitAt remainder bs
                    Parse.zoom pushBack $ Parse.unDraw suffix
                    Parse.zoom bytesRead $ State.put (Sum (entrySize entry))
                    Pipes.respond prefix


--------------------------------------------------------------------------------
-- | The 'TarP' 'Pipes.Proxy' transformer is simply 'State.StateP' with
-- 'TarParseState' as state.
type TarP p = State.StateP TarParseState (Either.EitherP SomeException p)


-- | Run a 'TarP' 'Pipes.Proxy'.
runTarK :: (Monad m, Pipes.Proxy p) =>
    (b' -> TarP p a' a b' b m r) -> b' -> p a' a b' b m (Either SomeException r)
runTarK = Either.runEitherK . State.evalStateK mempty


--------------------------------------------------------------------------------
drawBytes :: (Monad m, Pipes.Proxy p) =>
    Int -> State.StateP [BS.ByteString] p
            () (Maybe BS.ByteString) y' y m BS.ByteString
drawBytes n = (passBytesUpTo n >-> const go) ()
  where
    go = Pipes.request () >>= maybe (return mempty) (\x -> BS.append x <$> go)


--------------------------------------------------------------------------------
skipBytes :: (Monad m, Pipes.Proxy p) =>
    Int -> State.StateP [BS.ByteString] p () (Maybe BS.ByteString) y' y m ()
skipBytes n = (passBytesUpTo n >-> const go) ()
  where go = Pipes.request () >>= maybe (return ()) (const go)
