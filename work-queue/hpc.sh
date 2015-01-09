#!/bin/bash -ex

cabal configure --ghc-option=-fhpc --enable-tests
rm -f test.tix test.mix
cabal build
# || true is used here so that coverage is still generated when tests fail.
./dist/build/test/test || true
hpc markup test
hpc report test
