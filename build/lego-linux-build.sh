#! /bin/bash
# If using variables directly from the spec, the variables are replaced by the
# wrong value (from initial step)
echo "BOX_DIR=$BOX_DIR"
set -x
pwd
uname -a
mkdir wdt_build
cd wdt_build || exit 1
ls -l "$BOX_DIR"
cmake ../wdt -DFOLLY_SOURCE_DIR="$BOX_DIR" -DBUILD_TESTING=on && make -j
