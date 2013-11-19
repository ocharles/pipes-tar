{-# LANGUAGE OverloadedStrings #-}
module Main where

import Data.Function (on)
import Data.List (sortBy)

import Test.Tasty
import Test.Tasty.HUnit

import Control.Monad.Trans.Free (hoistFreeT, iterT)
import qualified Control.Monad.Trans.Writer.Strict as Writer

import qualified Codec.Archive.Tar as Tar

import qualified Pipes
import qualified Pipes.Prelude
import qualified Pipes.Lift as Pipes
import qualified Pipes.ByteString as PipesBS
import qualified Pipes.Tar as PipesTar

import System.IO
import System.IO.Temp
import System.Directory (getModificationTime)

import Control.Concurrent

import Data.Time (getCurrentTime)

import Data.Text (Text, pack)
import Data.Text.Encoding (encodeUtf8)

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests = testGroup "Reference tests"
  [ testCase "Reading" $
      withSystemTempDirectory "read" $ \tmpDir -> do
        let aContent = "Hello"
        
        writeFile (tmpDir ++ "/a") aContent
        writeFile (tmpDir ++ "/b") (concat $ replicate 4095 "Good bye")

        aModifiedAt <- getModificationTime (tmpDir ++ "/a")

        let tarFilePath = tmpDir ++ "/readme.tar"

        Tar.create tarFilePath tmpDir ["a" ] --, "b"]

        wrote <- withFile tarFilePath ReadMode $ \h ->
          let t = PipesTar.parseTarEntries (PipesBS.fromHandle h)
          in Writer.execWriterT $ PipesTar.iterTarArchive
            (\e -> do
                (k, r) <- Pipes.runEffect $ Pipes.runWriterP $
                            Pipes.for (Pipes.hoist Pipes.lift $ PipesTar.tarContent e) $
                            Pipes.lift . Writer.tell

                Writer.tell [(PipesTar.tarHeader e, r)]

                k)
            t

        sortBy (compare `on` (PipesTar.entryPath . fst)) wrote @?=
          [ (PipesTar.TarHeader { PipesTar.entryPath = "a"
                                , PipesTar.entryMode = 420
                                , PipesTar.entryUID = 0
                                , PipesTar.entryGID = 0
                                , PipesTar.entrySize = length aContent
                                , PipesTar.entryLastModified = aModifiedAt
                                , PipesTar.entryType = PipesTar.File
                                , PipesTar.entryLinkName = ""
                                }, encodeUtf8 (pack aContent))
          ]

  , testCase "Writing" $ do
      assertFailure "Do this"
  ]
