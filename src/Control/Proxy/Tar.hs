module Control.Proxy.Tar
    ( tarArchive
    , TarParseState
    , tarEntry
    , TarEntry(..)
    ) where

--------------------------------------------------------------------------------
import Control.Applicative
import Control.Monad (forever, mzero)
import Data.ByteString (ByteString)
import Data.Function (fix)
import Data.Monoid ((<>), Monoid(..), Sum(..))
import Data.Serialize (Serialize(..), decode)
import Data.Serialize.Get ()
import Data.Word ()


--------------------------------------------------------------------------------
import qualified Control.Proxy as Pipes
import qualified Control.Proxy.Handle as Handle
import qualified Control.Proxy.Trans.Maybe as Maybe
import qualified Control.Proxy.Trans.State as State
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString.Lex.Integral as Lexing
import qualified Data.Serialize.Get as Get


--------------------------------------------------------------------------------
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

    put = undefined


--------------------------------------------------------------------------------
tarArchive :: (Monad m, Pipes.Proxy p)
    => () -> Pipes.Pipe (State.StateP TarParseState (Maybe.MaybeP p))
                (Maybe BS.ByteString) TarEntry m ()
tarArchive () = fix $ \loop -> do
    header <- Handle.zoom pushBack $ drawBytes 512
    case decode header of
        Left _ -> mzero
        Right e -> do
            Handle.zoom bytesRead $ State.put (Sum 0)
            Pipes.respond e
            Sum consumed <- State.gets (getConst . bytesRead Const)
            Handle.zoom pushBack $ skipBytes (tarBlocks e - consumed)
            loop

  where

    tarBlocks entry = (((entrySize entry - 1) `div` 512) + 1) * 512



--------------------------------------------------------------------------------
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
