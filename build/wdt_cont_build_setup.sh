#! /bin/bash
# A lot of this is facebook specific but can give an idea of how
# to setup custom tests for WDT
# run this once and then _run will run a loop and email results
set -x
while sh -c "g++ --version | fgrep 4.4." ; do
  smcc add-services -e hhvm.oss $HOSTNAME:22
  date
  echo "you have an old g++ let see if chefctl can upgrade it"
  sleep 1
  sudo chefctl -i
done
echo congrats on
set -e
g++ --version
CDIR=/data/users/${USER}_wdt_contbuild
BINDIR=/data/users/${USER}_wdt_contbuild/bin
LIBDIR=/data/users/${USER}_wdt_contbuild/lib
sudo sh -c "mkdir -p $BINDIR || true"
sudo sh -c "mkdir -p $LIBDIR || true"
sudo chown -R $USER $CDIR
export PATH=$BINDIR:$PATH
export LD_LIBRARY_PATH=$LIBDIR:$LD_LIBRARY_PATH

cd $CDIR
hg-clone-fbsource --sparse .hgsparse-fbcode --simple

# open source part
export https_proxy=http://fwdproxy.any:8080
git clone https://cmake.org/cmake.git
cd cmake
./bootstrap --prefix=$CDIR --parallel=16
make -j 16
make install
cd ..

# should be same:
which cmake
ls -l $BINDIR/cmake

# similar as travis_linux.sh
git clone https://github.com/floitsch/double-conversion.git
(cd double-conversion; cmake -DCMAKE_INSTALL_PREFIX=$CDIR .; make -j 16 && make install)
git clone https://github.com/schuhschuh/gflags.git
(mkdir gflags/build; cd gflags/build; cmake -DCMAKE_INSTALL_PREFIX=$CDIR -D GFLAGS_NAMESPACE=google -D BUILD_SHARED_LIBS=on .. && make -j 16 && make install)
svn checkout http://google-glog.googlecode.com/svn/trunk/ glog
( cd glog && ./configure --with-gflags=$CDIR --prefix=$CDIR && make -j 16 && make install )
OPENSSL_VERSION=openssl-1.0.1q
wget https://www.openssl.org/source/$OPENSSL_VERSION.tar.gz
tar xfz $OPENSSL_VERSION.tar.gz
( cd $OPENSSL_VERSION ; ./config --prefix=$CDIR threads shared; make ; make install )
$CDIR/bin/openssl version
ldd $CDIR/bin/openssl
export OPENSSL_ROOT_DIR=$CDIR

mkdir cmake_wdt_build
cd cmake_wdt_build
cmake ../fbsource/fbcode/wdt -DFOLLY_SOURCE_DIR=$CDIR/fbsource/fbcode\
        -DBUILD_TESTING=1

time make -j 16

CTEST_OUTPUT_ON_FAILURE=1 time make test
