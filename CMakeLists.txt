# CMake build file - use CMake 3.1 or later to build WDT and its dependencies.
#
# mkdir build; cd build; cmake ..; make -j 24
#
# (at fb:
# mkdir wdt_build; cd wdt_build
# cmake31 ~/fbcode/wdt -DFOLLY_SOURCE_DIR=$HOME/fbcode
# make -j 24
# )
#
#  Copywright (c) 2014, Facebook, Inc.
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree. An additional grant
#  of patent rights can be found in the PATENTS file in the same directory.
#

cmake_minimum_required(VERSION 3.1)

project("WDT" C CXX)

# We need C++ 11
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED on)

# somehow 'option' for this doesn't seeem to work/I don't know how to make it
set(BUILD_SHARED_LIBS on CACHE Bool "build shared libs")

# Optimized by default
# TODO: This doesn't seem to work / sets default to "" instead of Release...
# set(CMAKE_BUILD_TYPE Release CACHE String "build type")
# So hardcoding for now:
set(CMAKE_BUILD_TYPE Release)


# Folly source tree
set(FOLLY_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/deps/folly CACHE path
    "Folly source tree (folly/ThreadLocal.h should be reachable from there")
# Check for folly - TODO: this doesn't work well for relative paths
# (because of relative to build dir vs relative to source tree for -I)
if(NOT EXISTS "${FOLLY_SOURCE_DIR}/folly/ThreadLocal.h")
  MESSAGE(FATAL_ERROR "${FOLLY_SOURCE_DIR}/folly/ThreadLocal.h not found
Fix using:
mkdir deps; cd deps; git clone https://github.com/facebook/folly.git
or change FOLLY_SOURCE_DIR (use ccmake or -DFOLLY_SOURCE_DIR=...)
")
endif()


# The part of folly that isn't pure .h and we use:
add_library(folly4wdt
${FOLLY_SOURCE_DIR}/folly/Conv.cpp
${FOLLY_SOURCE_DIR}/folly/FileUtil.cpp # used by Random used by tests
${FOLLY_SOURCE_DIR}/folly/Demangle.cpp
${FOLLY_SOURCE_DIR}/folly/Malloc.cpp
${FOLLY_SOURCE_DIR}/folly/Random.cpp # used indirectly by tests
)

# WDT's library proper - comes from: ls -1 *.cpp | grep -iv test
add_library(wdtlib
ClientSocket.cpp
DirectorySourceQueue.cpp
FileByteSource.cpp
FileCreator.cpp
Protocol.cpp
Receiver.cpp
Reporting.cpp
Sender.cpp
ServerSocket.cpp
SocketUtils.cpp
Throttler.cpp
WdtOptions.cpp
)

# Folly uses boost system
#set(Boost_USE_STATIC_LIBS on)
find_package(Boost COMPONENTS system REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

# We use std:: threads
find_package(Threads) # will set  ${CMAKE_THREAD_LIBS_INIT} (ie pthreads)

# Glog
#find_path(GLog_INCLUDE_DIRS glog/glog.h
#find_package(GLog)

# You can also add jemalloc to the list if you have it/want it
target_link_libraries(wdtlib
  folly4wdt
  glog
  gflags
  ${CMAKE_THREAD_LIBS_INIT}
  ${Boost_LIBRARIES}
)

# What we need to build the part of folly we use:
add_definitions("-DFOLLY_NO_CONFIG=1")
add_definitions("-DFOLLY_HAVE_CLOCK_GETTIME=1")
add_definitions("-DFOLLY_HAVE_MALLOC_H=1")
add_definitions("-DFOLLY_HAVE_WEAK_SYMBOLS=1")
add_definitions("-DFOLLY_HAVE_FEATURES_H=1")
include_directories(${FOLLY_SOURCE_DIR})

add_executable(wdt wdtCmdLine.cpp)

target_link_libraries(wdt wdtlib)

### Install rules
# probably need folly4wdt in there too ?
install(TARGETS wdt wdtlib CONFIGURATIONS Release
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
  )
# wcp script
install(PROGRAMS wcp.sh DESTINATION bin RENAME wcp)


### Testing

# With sub modules, message would be :
# Try updating your submodule with:
# git submodule update --init --recursive


include(ExternalProject)

#add_subdirectory(external/gflags)

#find_package(GLog)
#ExternalProject_Add(
#  glog
#  DEPENDS gflags
#  PREFIX ${CMAKE_CURRENT_BINARY_DIR}/glog
#  SVN_REPOSITORY http://google-glog.googlecode.com/svn/trunk
#  CONFIGURE_COMMAND configure
#  LOG_DOWNLOAD ON
#  LOG_CONFIGURE ON
#  LOG_BUILD ON
# )


# GTest
set(GTEST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gtest")
externalproject_add(
  gtest
  SVN_REPOSITORY https://googletest.googlecode.com/svn/trunk/
  INSTALL_COMMAND "" # Disable install step
  UPDATE_COMMAND "" # Doesn't change often
  PREFIX "${GTEST_PREFIX}"
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON
)

# Specify include dir
externalproject_get_property(gtest SOURCE_DIR)
include_directories(${SOURCE_DIR}/include)

enable_testing()

add_executable(protocol_test ProtocolTest.cpp)
externalproject_get_property(gtest BINARY_DIR)
target_link_libraries(protocol_test wdtlib ${BINARY_DIR}/libgtest.a)
add_test(AllTestsInProtocolTest protocol_test)

# Needs gmock for this... to be fixed
#add_executable(sender_test SenderTest.cpp)
#target_link_libraries(sender_test wdtlib ${BINARY_DIR}/libgtest.a)
#add_test(AllTestsInSenderTest sender_test)
