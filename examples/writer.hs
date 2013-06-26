{-# LANGUAGE OverloadedStrings #-}
import Data.Text
import Data.Text.Encoding (encodeUtf8)
import Pipes
import Pipes.Prelude (discard)
import Pipes.ByteString (writeHandle)
import Pipes.Parse (unwrap, wrap)
import Pipes.Tar
import Data.Time (getCurrentTime)
import System.IO (withFile, IOMode(WriteMode))

main :: IO ()
main = withFile "hello.tar" WriteMode $ \h ->
  runEffect $
    (wrap . tarEntries >-> writeTar >-> forever . writeHandle h >-> discard) ()

 where

  tarEntries () = do
    now <- lift getCurrentTime
    writeDirectoryEntry "text" 0 0 0 now
    writeFileEntry "text/hello" 0 0 0 now <-<
      (wrap . const (respond (encodeUtf8 "Hello!"))) $ ()
