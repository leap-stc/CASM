#!/bin/bash

# Script to transfer files from Zenodo to OSN
for year in {2002..2020}
do
  url="https://zenodo.org/records/7072512/files/CASM_SM_${year}.nc"

  # Run the rclone command for each URL
  rclone copyurl "$url" osnmanual:leap-pangeo-manual/CASM \
    --auto-filename -vv --progress --fast-list --max-backlog 500000 \
    --s3-chunk-size 200M --s3-upload-concurrency 128 \
    --transfers 128 --checkers 128
done
