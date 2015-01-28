import Control.Monad (when)
import Distribution.Simple
import System.Directory (doesFileExist)
import System.Executable.Hash.Internal (maybeInjectExecutableHash)
import System.FilePath ((</>))

main :: IO ()
main = defaultMainWithHooks $ simpleUserHooks
    { postBuild = \_ _ _ _ ->
        mapM_ (\n -> maybeInjectExecutableHash ("dist/build" </> n </> n))
            [ "hpc-example-war"
            , "hpc-example-redis"
            , "test"
            , "bench"
            ]
    }
