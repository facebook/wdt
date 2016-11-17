#! /bin/bash


if [ "$#" -eq 0 ]; then
  binary=_bin/wdt/wdt_url_test
else
  binary=$1
fi

set -o pipefail
set -e

searchStr='Generated a transfer id'
transferId1=$($binary -v=1 2>&1 | grep "$searchStr" | head -1 | \
  awk '{print $10}')
transferId2=$($binary -v=1 2>&1 | grep "$searchStr" | head -1 | \
  awk '{print $10}')

if [ "$transferId1" == "$transferId2" ]; then
  echo "Failed to get different transfer-id $transferId1"
  exit 1
fi
echo "Test successful $transferId1 $transferId2"
exit 0
