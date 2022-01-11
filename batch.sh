#! /bin/bash
start=`date +%s`
num=5
failCount=0
if [ $# -eq 1 ]; then
    num=$1
fi
echo Total run $num epoch

for (( i=0; i<$num; i++ ))
do
  for (( j=6; j<=11; j++ ))
  do
    fileName=tmp-t${j}-r${i}.log
    echo "running test $j round $i"
    make project2b${j} > $fileName
    tail -n 10 $fileName | grep PASS
    if [ $? != 0 ]; then
        echo fail
        failCount=$((${failCount}+1))
    else
        rm $fileName
    fi
    rm /tmp/test-raftstore* -rf
  done
done
end=`date +%s`
time=$(($end-$start))
echo Time: $time seconds, $failCount tests failed

# failed output: failure|Append result|wrong value|logs were not trimmed|region is not split|unexpected truncated state

