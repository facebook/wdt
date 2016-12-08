#! /bin/bash

# Remove the double-conversion part when Task#10922468 is done

set -x
pwd
uname -a
mkdir wdt_build
cd wdt_build || exit 1
cmake --version
export https_proxy=fwdproxy:8080
git clone https://github.com/floitsch/double-conversion.git
mkdir double-conversion-build
(cd double-conversion-build && \
 cmake -DBUILD_SHARED_LIBS=on \
       -DCMAKE_INSTALL_PREFIX="$HOME" \
       ../double-conversion; \
 make -j 4 && make install)

cmake ../wdt -DFOLLY_SOURCE_DIR="$(cd ..;pwd)" -DBUILD_TESTING=on \
      -DDOUBLECONV_INCLUDE_DIR="$HOME/include" \
      -DDOUBLECONV_LIBRARY="$HOME/lib/libdouble-conversion.dylib" && make -j
