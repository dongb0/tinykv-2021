#! /bin/bash
test=2c
begin=5
end=5

num=5
failCount=0
if [ $# -eq 1 ]; then
    num=$1
fi
echo Total run $num epoch

start=`date +%s`
for (( i=0; i<$num; i++ ))
do
  for (( j=$begin; j<=$end; j++ ))
  do
    fileName=tmp-t${j}-r${i}.log
    if [ -f $fileName ]; then
      fileName=$fileName.log
    fi
    echo "running test $j round $i"
    make project${test}${j} > $fileName
    sleep 1
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

