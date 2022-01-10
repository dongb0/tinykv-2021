#! /bin/bash

num=5
if [ $# -eq 1 ]; then
    num=$1
fi
echo Total run $num epoch

for (( i=0; i<$num; i++ ))
do
  for (( j=1; j<=11; j++ ))
  do
    fileName=tmp-t${j}-r${i}.log
    echo "running test $j round $i"
    make project2b${j} > $fileName
    tail -n 10 $fileName | grep PASS
    if [ $? != 0 ]; then
        echo fail
        rm /tmp/test-raftstore* -rf
    else
        rm $fileName
    fi
  done
done

rm /tmp/test-raftstore* -rf

