#!/bin/bash -ex

cabal configure --ghc-option=-fhpc --enable-tests
rm -f test.tix test.mix
cabal build
./dist/build/test/test
hpc markup test
hpc report test
