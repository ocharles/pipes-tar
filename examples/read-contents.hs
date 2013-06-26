import Control.Monad.IO.Class (liftIO)
import Control.Monad (void)
import Data.List (isSuffixOf)
import Pipes
import Pipes.ByteString (readHandle)
import Pipes.Parse (wrap)
import Pipes.Prelude (discard, print)
import Pipes.Tar
import System.Environment (getArgs)
import System.IO (withFile, IOMode(ReadMode))

main :: IO ()
main = do
  (tarPath:_) <- getArgs
  withFile tarPath ReadMode $ \h ->
    void $ runEffect $ runTarP $
      (wrap . hoist liftIO . readHandle h >-> readTar />/ readEntry) ()

 where

  readEntry tarEntry
    | ".txt" `isSuffixOf` entryPath tarEntry &&
      entryType tarEntry == File =
        (readCurrentEntry >-> hoist liftIO . Pipes.Prelude.print) ()
    | otherwise =
        (readCurrentEntry >-> discard) ()
