#!/bin/bash
input="./hbaseCommands.txt"
while IFS= read -r line
do
  echo "$line"
  echo "$line" | hbase shell -n
done < "$input"
