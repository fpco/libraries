This is an attempt at a repro for
https://github.com/fpco/libraries/issues/74, where using 'mapQueue'
with a large request seems to cause a memory to leak in the master.

I'm building it with

`ghc large-payloads.hs -Wall -threaded -O2 -eventlog -rtsopts`

and then running it with

`./large-payloads master 0 9000 +RTS -ls`

and

`./large-payloads slave localhost 9000`

Then, analyzing with threadscope via

`threadscope large-payloads.eventlog`
