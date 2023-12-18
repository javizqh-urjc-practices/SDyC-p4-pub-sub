#!/usr/bin/bash

mkdir -p test_sec_50/
mkdir -p test_sec_500/
mkdir -p test_sec_900/

for i in $(seq 1 1); do
    ./subscriber --ip 0.0.0.0 --port 6000 --topic "test" > test_sec_50/$i.txt &
done;
