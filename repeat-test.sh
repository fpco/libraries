#!/bin/sh
# Usage: ./repeat-test.sh PROJECT_NAME TEST_MATCH
# Ex: ./repeat-test work-queue "NetworkMessage"
DIST=$(stack path --dist-dir)
ITER=0
rm -r logs # Clear out thread-file-logger logs
killall -9 test # Kill old work-queue test processes
stack test $1 --no-run-tests
if [ $? -ne 0 ]
  then exit
fi
$1/$DIST/build/test/test -m "$2"
while [ $? -eq 0 ]; do
  echo "**** Iteration #$ITER ****"
  ITER=$((ITER+1))
  rm -r logs # Clear out thread-file-logger logs
  killall -9 test # Kill old work-queue test processes
  $1/$DIST/build/test/test -m "$2"
done
