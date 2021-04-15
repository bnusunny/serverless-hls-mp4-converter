#!/bin/bash

for i in {1..1000}
do 
    aws lambda invoke --invocation-type Event  --function-name hls-mp4-converter-StreamConvertFunction-J3ORABJA3Y36 --payload fileb://../events/streamconverter.json /dev/null
done 