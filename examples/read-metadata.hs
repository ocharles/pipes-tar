import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Pipes
import Pipes.ByteString (readHandle)
import Pipes.Parse (wrap)
import Pipes.Prelude (discard)
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

  readEntry tarEntry = do
    liftIO $ print . entryPath $ tarEntry
    (readCurrentEntry >-> discard) ()
