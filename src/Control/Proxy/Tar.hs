{-# LANGUAGE NoMonomorphismRestriction #-}
module Control.Proxy.Tar where

--------------------------------------------------------------------------------
import Control.Applicative
import Control.Exception (SomeException)
import Control.Proxy.Core
import Control.Proxy.Parse (ParseP, parse, parseFail, unDraw, drawMay)
import Control.Proxy.Core.Correct (runProxy)
import Control.Proxy.ByteString (fromHandleS)
import Control.Proxy.Trans.Either (EitherP, runEitherK)
import Control.Proxy.Trans.Writer (execWriterK)
import Data.ByteString (ByteString)
import Data.Function (fix)
import Data.Char (ord)
import Data.Serialize (Serialize(..), decode)
import Data.Serialize.Get ()
import Data.Word ()
import System.IO (openFile, IOMode(ReadMode), Handle)


--------------------------------------------------------------------------------
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as Char8
import qualified Data.Serialize.Get as Get


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
                   <*> (readOctal <$> Get.getBytes 11) <* Get.skip 1
                   <*> Get.getBytes 10 <* Get.skip 2
                   <*> Get.getBytes 6 <* Get.skip 2
                   <*> Get.getBytes 1
                   <*  Get.getBytes 99 <* Get.skip 1
                   <*  Get.getBytes 255

    put = undefined


--------------------------------------------------------------------------------
readOctal :: ByteString -> Int
readOctal = sum .
    map (\(pow, n) -> n * (8 ^ (pow :: Int))) . zip [0..] .
    map (\x -> fromIntegral x - (ord '0')) . reverse . BS.unpack


--------------------------------------------------------------------------------
-- | Parse entries out of a tar file.
tarEntry :: Proxy p => () -> ParseP
                               ByteString (EitherP SomeException p)
                               () (Maybe ByteString)
                               () (Either TarEntry ByteString)
                               IO
                               ()
tarEntry () = go
  where
    go = do
        header <- takeBytes 512

        if (not (BS.null header) && any (/= 0) (BS.unpack header))
            then case decode header of
                Right tarEntry ->
                    let blocks = blockSize tarEntry
                    in do
                        respond $ Left tarEntry
                        respondBytes blocks
                        go
                Left e -> error (show e)
            else return ()

    blockSize e = 512 * ceiling (fromIntegral (entrySize e) / 512)

    respondBytes x = go x
      where
        go n = do
            got <- drawMay
            case got of
                Nothing ->
                  parseFail $ "Stream did not produce " ++ show x ++ " bytes"
                Just g ->
                  let l = BS.length g
                  in if l > n
                      then let (need, rest) = BS.splitAt n g
                           in unDraw rest >> respond (Right need)
                      else respond (Right g) >> go (n - l)

--------------------------------------------------------------------------------
takeBytes :: (Monad m, Proxy p) =>
    Int -> ParseP ByteString (EitherP SomeException p)
            () (Maybe ByteString)
            () b
            m ByteString
takeBytes x = go x
  where
    go n = do
        got <- drawMay
        case got of
            Nothing ->
              parseFail $ "Stream did not produce " ++ show x ++ " bytes"
            Just g ->
              let l = BS.length g
              in if l > n
                  then let (need, rest) = BS.splitAt n g
                       in unDraw rest >> return need
                  else (g `BS.append`) <$> go (n - l)


--------------------------------------------------------------------------------
-- | Find all the entries in a tar file.
tarEntries :: Handle -> IO (Either SomeException [TarEntry])
tarEntries h = runProxy $ runEitherK $ execWriterK $
    liftP . parse (fromHandleS h) tarEntry >-> takeL >-> toListD
  where
    takeL = foreverK $ \() -> request () >>= either respond (const $ takeL ())
