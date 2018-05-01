#!/bin/bash
echo 'Starting test parallel indexer'
echo "First param: $1"
for i in $(seq 1 $1)
do
   echo "TEST: $i of $1"
   java -jar /Users/dvsmedeiros/dev/projects/workspace/default/parallel-indexer/target/parallel-indexer-0.0.1-SNAPSHOT.jar
done
echo 'End of test parallel indexer'
