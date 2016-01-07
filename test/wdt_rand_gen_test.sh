#! /bin/sh

transferId1=$(_bin/wdt/wdt_url_test -v=1 2>&1 | grep Generated | head -1 | \
  awk '{print $9}')
transferId2=$(_bin/wdt/wdt_url_test -v=1 2>&1 | grep Generated | head -1 | \
  awk '{print $9}')

if [ "$transferId1" == "$transferId2" ]; then
  echo "Failed to get different transfer-id $transferId1"
  exit 1
fi
echo "Test successful $transferId1 $transferId2"
exit 0
