#! /bin/sh
# Mac version of wdt_cont_build_setup.sh
set -x
set -e
CDIR=/data/users/${USER}_wdt_contbuild
sudo sh -c "mkdir -p $CDIR || true"
sudo chown $USER $CDIR
( echo p | svn list https://googlemock.googlecode.com ) || true
cd $CDIR
hg-clone-fbsource --sparse fbcode/wdt/.hgsparse --simple
mkdir cmake_wdt_build
cd cmake_wdt_build
echo "might need to get a new enough cmake if this fails:"
cmake ../fbsource/fbcode/wdt -DBUILD_TESTING=1 \
    -DFOLLY_SOURCE_DIR=$(cd ../fbsource/fbcode;pwd)
make -j 4
CTEST_OUTPUT_ON_FAILURE=1 make test
