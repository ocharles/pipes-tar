{-# LANGUAGE DeriveDataTypeable #-}
module Control.Proxy.Tar
    ( tarArchive
    , TarParseState
    , tarEntry
    , TarEntry(..)
    ) where

--------------------------------------------------------------------------------
import Control.Applicative
import Control.Exception
import Control.Monad (forever, mzero)
import Data.ByteString (ByteString)
import Data.Function (fix)
import Data.Monoid ((<>), Monoid(..), Sum(..))
import Data.Serialize (Serialize(..), decode)
import Data.Serialize.Get ()
import Data.Typeable (Typeable)
import Data.Word ()


--------------------------------------------------------------------------------
import qualified Control.Proxy as Pipes
import qualified Control.Proxy.Handle as Handle
import qualified Control.Proxy.Trans.Either as Either
import qualified Control.Proxy.Trans.State as State
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lex.Integral as Lexing
import qualified Data.Serialize.Get as Get


--------------------------------------------------------------------------------
-- | 'TarParseState' is internal state that keeps track of how much of a file
-- has been read.
data TarParseState = TarParseState [Maybe BS.ByteString] (Sum Int)

pushBack :: Functor f => ([Maybe BS.ByteString] -> f [Maybe BS.ByteString])
                      -> TarParseState -> f TarParseState
pushBack f (TarParseState pb s) = fmap (\x -> TarParseState x s) (f pb)

bytesRead :: Functor f => (Sum Int -> f (Sum Int))
                       -> TarParseState -> f TarParseState
bytesRead f (TarParseState pb s) = fmap (\x -> TarParseState pb x) (f s)

instance Monoid TarParseState where
    mappend (TarParseState pbA brA) (TarParseState pbB brB) =
        TarParseState (pbA <> pbB) (brA <> brB)
    mempty = TarParseState mempty mempty


--------------------------------------------------------------------------------
-- | Possible errors that can occur when reading tar files
data TarException
  = -- | The header for a tar entry could not be parsed
    InvalidHeader
  deriving (Show, Typeable)

instance Exception TarException

--------------------------------------------------------------------------------
-- | A 'TarEntry' contains all the metadata about a single entry in a tar file.
data TarEntry = TarEntry { entryName :: !String
                         , entryMode :: !ByteString
                         , entryUID :: !ByteString
                         , entryGID :: !ByteString
                         , entrySize :: !Int
                         , entryLastModified :: !ByteString
                         , entryType :: !ByteString
                         , entryLinkName :: !ByteString
                         }
    deriving (Show)


--------------------------------------------------------------------------------
instance Serialize TarEntry where
    get = TarEntry <$> (Char8.unpack . BS.takeWhile (/= 0) <$> Get.getBytes 100)
                   <*> Get.getBytes 7 <* Get.skip 1
                   <*> Get.getBytes 7 <* Get.skip 1
                   <*> Get.getBytes 7 <* Get.skip 1
                   <*> (Get.getBytes 11 >>= readOctal) <* Get.skip 1
                   <*> Get.getBytes 10 <* Get.skip 2
                   <*> Get.getBytes 6 <* Get.skip 2
                   <*> Get.getBytes 1
                   <*  Get.getBytes 99 <* Get.skip 1
                   <*  Get.getBytes 255
      where readOctal = maybe mzero (return . fst) . Lexing.readOctal

    put = error "TarEntry serialization is not implemented"


--------------------------------------------------------------------------------
-- | Transform a 'BS.ByteString' into a stream of 'TarEntry's. Each 'TarEntry'
-- can be expanded into its respective 'BS.ByteString' using 'tarEntry'. Parsing
-- tar archives can fail, so you may wish to use the @pipes-safe@ library, which
-- is compatible with 'Pipes.Proxy'.
tarArchive :: (Monad m, Pipes.Proxy p)
    => () -> Pipes.Pipe
                (State.StateP TarParseState (Either.EitherP SomeException p))
                (Maybe BS.ByteString) TarEntry m ()
tarArchive () = fix $ \loop -> do
    header <- Handle.zoom pushBack $ drawBytes 512
    case decode header of
        Left _ -> Pipes.liftP . Either.left . toException $ InvalidHeader
        Right e -> do
            Handle.zoom bytesRead $ State.put (Sum 0)
            Pipes.respond e
            Sum consumed <- State.gets (getConst . bytesRead Const)
            Handle.zoom pushBack $ skipBytes (tarBlocks e - consumed)
            loop

  where

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
tarEntry :: (Monad m, Pipes.Proxy p)
    => TarEntry
    -> () -> Pipes.Pipe (State.StateP TarParseState p)
        (Maybe BS.ByteString) (Maybe BS.ByteString) m r
tarEntry entry () = loop (entrySize entry)
  where
    loop remainder =
        if (remainder > 0)
        then do
            mbs <- Handle.zoom pushBack Handle.draw
            case mbs of
                Nothing -> forever $ Pipes.respond Nothing
                Just bs -> do
                    let len = BS.length bs
                    if (len <= remainder)
                        then do
                            Handle.zoom bytesRead $ do
                                Sum n <- State.get
                                State.put $! Sum (n + len)
                            Pipes.respond mbs
                            loop (remainder - len)
                        else do
                            let (prefix, suffix) = BS.splitAt remainder bs
                            Handle.zoom pushBack $ Handle.unDraw (Just suffix)
                            Handle.zoom bytesRead $ State.put (Sum (entrySize entry))
                            Pipes.respond (Just prefix)
                            forever $ Pipes.respond Nothing
        else forever $ Pipes.respond Nothing


--------------------------------------------------------------------------------
drawBytes :: (Monad m, Pipes.Proxy p)
    => Int
    -> State.StateP [Maybe BS.ByteString] p () (Maybe BS.ByteString) b' b m BS.ByteString
drawBytes = loop id
  where
    loop diffBs remainder
        | remainder <= 0 = return $ BS.concat (diffBs [])
        | otherwise = do
            mbs <- Handle.draw
            case mbs of
                Nothing -> return $ BS.concat (diffBs [])
                Just bs -> do
                    let len = BS.length bs
                    if (len <= remainder)
                        then loop (diffBs . (bs:)) (remainder - len)
                        else do
                            let (prefix, suffix) = BS.splitAt remainder bs
                            Handle.unDraw (Just suffix)
                            return $ BS.concat (diffBs [prefix])


--------------------------------------------------------------------------------
skipBytes :: (Monad m, Pipes.Proxy p)
    => Int -> State.StateP [Maybe BS.ByteString] p () (Maybe BS.ByteString) b' b m ()
skipBytes = loop
  where
    loop remainder =
        if (remainder > 0)
        then do
            mbs <- Handle.draw
            case mbs of
                Nothing -> return ()
                Just bs -> do
                    let len = BS.length bs
                    if (len <= remainder)
                        then loop (remainder - len)
                        else do
                            let (_, suffix) = BS.splitAt remainder bs
                            Handle.unDraw (Just suffix)
                            return ()
        else return ()
