import Control.Monad (when)
import Distribution.Simple
import System.Directory (doesFileExist)
import System.Executable.Hash.Internal (injectExecutableHash)
import System.FilePath ((</>))

main :: IO ()
main = defaultMainWithHooks $ simpleUserHooks
    { postBuild = \_ _ _ _ -> do
        let injectIfExists n = do
                let fp = "dist/build" </> n </> n
                exists <- doesFileExist fp
                when exists $ do
                    putStrLn $ "Injecting executable hash into " ++ fp
                    injectExecutableHash fp
        mapM_ injectIfExists
            [ "hpc-example-war"
            , "hpc-example-redis"
            , "test"
            , "bench"
            ]
    }
