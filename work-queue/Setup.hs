import Distribution.Simple (defaultMainWithHooks, simpleUserHooks, postBuild)
import Distribution.Simple.LocalBuildInfo (buildDir)
import System.Executable.Hash.Internal (maybeInjectExecutableHash)
import System.FilePath ((</>))

main :: IO ()
main = defaultMainWithHooks $ simpleUserHooks
    { postBuild = \_ _ _ buildInfo -> do
        let dir = buildDir buildInfo
        mapM_ (\n -> maybeInjectExecutableHash (dir </> n </> n))
            [ "hpc-example-war"
            -- , "hpc-example-redis"
            , "test"
            , "bench"
            ]
    }
