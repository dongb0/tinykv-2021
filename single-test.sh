#! /bin/bash

test=2c
begin=1
end=6

num=20
failCount=0
if [ $# -eq 1 ]; then
    num=$1
fi

echo Run test-$test $begin to $end, $num epoch
start=`date +%s`

for (( j=$begin; j<=$end; j++ ))
do
    if [ -f $fileName ]; then
        fileName=$fileName.log
    fi
    for (( i=0; i<$num; i++))
    do
        fileName=tmp-t${j}-r${i}.log
        echo "Running test $j round $i"
        make project${test}${j} > $fileName
        if [ $? != 0 ]; then
            echo fail
            failCount=$((${failCount}+1))
        else
            rm $fileName
        fi
    done
done


end=`date +%s`
time=$(($end-$start))
echo Time: $time seconds, $failCount tests failed