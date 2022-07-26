# ubuntu 16.04 has cmake 3.5
cmake_minimum_required(VERSION 3.5)

#include(${CMAKE_CURRENT_LIST_DIR}/eRPC/CMakeLists.txt)
add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/eRPC/)
message(STATUS "foo include dir-1: ${CMAKE_CURRENT_SOURCE_DIR}")
# use hunter instead of conan
#include("HunterGate.cmake")

#if(UNIX AND NOT APPLE)
#    # Linux
#    HunterGate(
##            URL "https://github.com/cpp-pm/hunter/archive/v0.23.242.tar.gz"
##            SHA1 "503b149d0ebdbc598e498f1669ae828a2136b838"
#            URL "https://github.com/cpp-pm/hunter/archive/v0.23.254.tar.gz"
#            SHA1 "017e688b9848c1f695303e8cfe9f35d40e154112"
#            #        "${CMAKE_CURRENT_LIST_DIR}/cxx11.cmake"
#            #        FILEPATH "/home/shuai/git/janus/config.cmake"
#            LOCAL
#    )
#elseif(APPLE)
#    HunterGate(
#            URL "https://github.com/cpp-pm/hunter/archive/v0.23.254.tar.gz"
#            SHA1 "017e688b9848c1f695303e8cfe9f35d40e154112"
#    )
#endif()



#set(
#        CMAKE_TOOLCHAIN_FILE
#        "${CMAKE_CURRENT_LIST_DIR}/cmake/cxx14.cmake"
#        CACHE
#        FILEPATH
#        "Default toolchain"
#)

project(janus)

# Hunter disallows using (cmake_)cxx_standard, use toolchain instead.
set (CMAKE_CXX_STANDARD 14)
SET(CMAKE_INCLUDE_PATH ${CMAKE_INCLUDE_PATH} "/.../boost")
SET(CMAKE_LIBRARY_PATH ${CMAKE_LIBRARY_PATH} "/.../boost/lib")
set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
set(Boost_NO_BOOST_CMAKE ON)
find_package(Boost COMPONENTS system filesystem context thread coroutine REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
#if(Boost_FOUND)
#    add_executable(progname file1.cxx file2.cxx)
#    target_link_libraries(progname ${Boost_LIBRARIES})
#endif()
#



find_package(Threads)
find_package(PkgConfig REQUIRED)
pkg_check_modules(PYTHON REQUIRED python3)
#pkg_check_modules(YAML_CPP REQUIRED yaml-cpp)


#hunter_add_package(Boost COMPONENTS system filesystem coroutine context thread)
#hunter_add_package(GTest)
#hunter_add_package(GSL)

#find_package(Boost CONFIG REQUIRED
#        system filesystem coroutine context thread)
#find_package(GTest CONFIG REQUIRED)
#find_package(GSL CONFIG REQUIRED)

include_directories(
        ${Boost_INCLUDE_DIRS}
#        ${YAML_CPP_INCLUDE_DIRS}
)
link_directories(
        ${Boost_LIBRARY_DIRS}
#        ${YAML_CPP_LIBRARY_DIRS}
        ${PYTHON_LIBRARY_DIRS}
)


# deprecated conan; using hunter now.
#
# do the following after git clone:
#   1. install conan package manage tool
#   2. create build_cmake directory:
#   3. run `conan remote add conan-community https://api.bintray.com/conan/conan-community/conan`
#   4. in build_cmake, run `conan install --build=missing ..`
# after a compiler upgrade in the system, re-execute above.
#

if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/build_cmake/conanbuildinfo.cmake)
    #Clion, using conan, with conanbuildinfo.cmake in build folder
    include(${CMAKE_CURRENT_SOURCE_DIR}/build_cmake/conanbuildinfo.cmake)
    conan_basic_setup()
    MESSAGE(${CONAN_LIBS})
elseif(EXISTS ${CMAKE_BINARY_DIR}/conanbuildinfo.cmake )
    # Not CLion, but using conan
    include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
    conan_basic_setup()
    MESSAGE(${CONAN_LIBS})
else()
    # not using conan
    # use hunter
endif()

if(APPLE)
    set(RT "")
else()
    set(RT "rt")
endif()

# enable sanitizers
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -O1 -fno-omit-frame-pointer -g -fstandalone-debug")

# for c build

add_library(
        EXTERN_C
        extern_interface/scheduler.c
        extern_interface/scheduler.h
)

# yaml
file(GLOB YAML_SRC
    "dependencies/yaml-cpp/src/*.h"
    "dependencies/yaml-cpp/src/*/*.h"
    "dependencies/yaml-cpp/src/*.cpp"
    "dependencies/yaml-cpp/src/*/*.cpp"
)

add_library(YAMLCPP
    ${YAML_SRC}
)

# for cxx build
set(CMAKE_VERBOSE_MAKEFILE on)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-register -Wno-error")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lnuma -ldl -lgflags -ldl -libverbs -lmlx4 -lmlx5")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I /home/users/vyip/depfast-ae/eRPC/src -DERPC_INFINIBAND=true -march=native -I /usr/include/dpdk -DERPC_LOG_LEVEL=6 -DERPC_TESTING=false -DGFLAGS_IS_A_DLL=0")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_IPC")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Weffc++")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCHECK_ISO")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDB_CHECKSUM")
# this makes gcc>=5 support conan.io libraries, e.g. yaml-cpp
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")

#pkg_check_modules(APR REQUIRED apr-1)
#pkg_check_modules(APRUTIL REQUIRED apr-util-1)
#pkg_check_modules(PROFILER REQUIRED libprofiler)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEBUG_CODE")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEBUG_CHECK")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DEVENT_TIMEOUT_CHECK")
set(CMAKE_CXX_FLAGS_RELEASE  "${CMAKE_CXX_FLAGS_RELEASE} -DLOG_LEVEL_AS_INFO")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DLOG_LEVEL_AS_DEBUG")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCPU_PROFILE")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSIMULATE_WAN")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DREUSE_CORO")

#FIND_LIBRARY (DMALLOC_LIBRARY dmalloc /usr/lib /sw/lib)
#FIND_PATH (DMALLOC_INCLUDE_DIR dmalloc.h PATHS /usr/include /sw/include)
#IF(DMALLOC_LIBRARY)
#  ADD_DEFINITIONS(-D_USE_DMALLOC)
#  SET(LINK_TO_DMALLOC 1)
#ENDIF(DMALLOC_LIBRARY)

#add_custom_command(
#    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/deptran/mdcc/mdcc_rpc.h
#    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/bin/rpcgen --cpp deptran/mdcc/mdcc_rpc.rpc
#    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/deptran/mdcc/mdcc_rpc.rpc
#    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/deptran/rcc_rpc.h
           ${CMAKE_CURRENT_SOURCE_DIR}/src/deptran/rcc_rpc.py
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/bin/rpcgen --cpp --python
        src/deptran/rcc_rpc.rpc
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/deptran/rcc_rpc.rpc
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_custom_target (gen-rpc
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/deptran/rcc_rpc.h
            ${CMAKE_CURRENT_SOURCE_DIR}/src/deptran/rcc_rpc.py)



include_directories(
    ${PYTHON_INCLUDE_DIRS}
    src
    src/rrr
    src/memdb
    src/deptran
    eRPC/src
    eRPC/third_party/asio/include
    /usr/include/dpdk
    dependencies/yaml-cpp/include
)

link_directories(
    ${PYTHON_LIBRARY_DIRS}
#    ${APR_LIBRARY_DIRS}
#    ${PROFILER_LIBRARY_DIRS}
)


#set (CMAKE_SHARED_LINKER_FLAGS "-Wl, -lyaml-cpp")


file(GLOB RRR_SRC
    "src/rrr/base/*.hpp"
    "src/rrr/base/*.cpp"
    "src/rrr/misc/*.hpp"
    "src/rrr/misc/*.cpp"
    "src/rrr/reactor/*.h"
    "src/rrr/reactor/*.cc"
#    "rrr/mq/*.cpp"
#    "rrr/mq/*.h"
    "src/rrr/pylib/simplerpc/_pyrpc.cpp"
    "src/rrr/rpc/*.cpp"
    "src/rrr/rpc/*.hpp"
    "src/rrr/utils/*.h"
    "src/rrr/rrr.h"
    "src/rrr/rrr.hpp"
)

file(GLOB MEMDB_SRC
    "src/memdb/*.h"
    "src/memdb/*.cc")

file(GLOB BENCH_SRC
    "src/bench/*/*.h"
    "src/bench/*/*.cc"
)

file(GLOB DEPTRAN_SRC
    "src/deptran/*.h"
    "src/deptran/*.cc"
    "src/deptran/*/*.cc"
    "src/deptran/*/*.h"
)

#file(GLOB ERPC_SRC
#  eRPC/src/nexus_impl/nexus.cc
#  eRPC/src/nexus_impl/nexus_bg_thread.cc
#  eRPC/src/nexus_impl/nexus_sm_thread.cc
#  eRPC/src/rpc_impl/rpc.cc
#  eRPC/src/rpc_impl/rpc_queues.cc
#  eRPC/src/rpc_impl/rpc_rfr.cc
#  eRPC/src/rpc_impl/rpc_cr.cc
#  eRPC/src/rpc_impl/rpc_kick.cc
#  eRPC/src/rpc_impl/rpc_req.cc
#  eRPC/src/rpc_impl/rpc_resp.cc
#  eRPC/src/rpc_impl/rpc_ev_loop.cc
#  eRPC/src/rpc_impl/rpc_fault_inject.cc
#  eRPC/src/rpc_impl/rpc_pkt_loss.cc
#  eRPC/src/rpc_impl/rpc_rx.cc
#  eRPC/src/rpc_impl/rpc_connect_handlers.cc
#  eRPC/src/rpc_impl/rpc_disconnect_handlers.cc
#  eRPC/src/rpc_impl/rpc_reset_handlers.cc
 # eRPC/src/rpc_impl/rpc_sm_api.cc
#  eRPC/src/rpc_impl/rpc_sm_helpers.cc
#  eRPC/src/transport_impl/transport.cc
#  eRPC/src/transport_impl/dpdk/dpdk_transport.cc
#  eRPC/src/transport_impl/dpdk/dpdk_init.cc
#  eRPC/src/transport_impl/dpdk/dpdk_externs.cc
#  eRPC/src/transport_impl/dpdk/dpdk_transport_datapath.cc
#  eRPC/src/transport_impl/infiniband/ib_transport.cc
#  eRPC/src/transport_impl/infiniband/ib_transport_datapath.cc
#  eRPC/src/transport_impl/raw/raw_transport.cc
#  eRPC/src/transport_impl/raw/raw_transport_datapath.cc
#  eRPC/src/transport_impl/fake/fake_transport.cc
#  eRPC/src/util/huge_alloc.cc
#  eRPC/src/util/numautils.cc
#  eRPC/src/util/tls_registry.cc
#)
list(FILTER DEPTRAN_SRC EXCLUDE REGEX
        "src/deptran/s_main.cc"
)

list(FILTER DEPTRAN_SRC EXCLUDE REGEX
        "src/deptran/paxos_main_helper.cc"
)

add_library(RRR
    ${RRR_SRC})

add_library(MEMDB
    ${MEMDB_SRC})

add_library(JANUS
        ${DEPTRAN_SRC}
        ${BENCH_SRC})


#dir name: ERPC, lib name: erpc
#Attempt Two
#add_library(ERPC
        #${ERPC_SRC})

#Attempt One
#set(ERPC_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/eRPC)
#add_subdirectory(eRPC)
#
#include_directories(${ERPC_INCLUDE_DIR})
#add_executable(tester testing.cc)
#
#target_link_libraries(tester eRPC)
#ADD_DEPENDENCIES(tester erpc)


add_executable(deptran_server
        "src/deptran/s_main.cc"
        ${DEPTRAN_SRC}
        ${BENCH_SRC})

add_executable(hello_server
        "src/HelloServer.cc"
        "src/deptran/paxos_main_helper.cc"
        ${DEPTRAN_SRC}
        ${BENCH_SRC})

add_executable(hello_client
	"src/HelloClient.cc"
        "src/deptran/paxos_main_helper.cc"
        ${DEPTRAN_SRC}
        ${BENCH_SRC})


#add_executable(paxosbench
#        "src/run.cc"
#        "src/deptran/paxos_main_helper.cc"
#        ${DEPTRAN_SRC}
#        ${BENCH_SRC})


add_dependencies(deptran_server gen-rpc)

#add_executable(JANUS_PROCESS
#        "deptran/empty.cc")

#add_executable(TEST
#        "test/coroutine.cc")

#target_link_libraries(
#        MEMDB
#        RRR
##        ${CONAN_LIBS}
#        ${Boost_LIBRARIES}
##        ${YAML_CPP_LIBRARIES}
#        ${RT})

target_link_libraries(
        hello_server 
        RRR
        MEMDB
        EXTERN_C
        YAMLCPP
        ${RT}
	${JANUS}
	${CMAKE_CURRENT_SOURCE_DIR}/eRPC/build/liberpc.a
        ${Boost_LIBRARIES}
        ${CMAKE_THREAD_LIBS_INIT}
        ${PYTHON_LIBRARIES}
	"-Wl,--whole-archive -ldpdk -Wl,--no-whole-archive -lpthread -lnuma -ldl -lgflags -ldl -libverbs -lmlx4 -lmlx5"
)

target_link_libraries(
        hello_client
        RRR
        MEMDB
        EXTERN_C
        YAMLCPP
        ${RT}
	${JANUS}
	${CMAKE_CURRENT_SOURCE_DIR}/eRPC/build/liberpc.a
        ${Boost_LIBRARIES}
        ${CMAKE_THREAD_LIBS_INIT}
        ${PYTHON_LIBRARIES}
	"-Wl,--whole-archive -ldpdk -Wl,--no-whole-archive -lpthread -lnuma -ldl -lgflags -ldl -libverbs -lmlx4 -lmlx5"
)

target_link_libraries(
        deptran_server
        RRR
        MEMDB
        EXTERN_C
        YAMLCPP
        ${RT}
	${JANUS}
	${CMAKE_CURRENT_SOURCE_DIR}/eRPC/build/liberpc.a
        ${Boost_LIBRARIES}
        ${CMAKE_THREAD_LIBS_INIT}
        ${PYTHON_LIBRARIES}
	"-Wl,--whole-archive -ldpdk -Wl,--no-whole-archive -lpthread -lnuma -ldl -lgflags -ldl -libverbs -lmlx4 -lmlx5"
)

#target_link_libraries(
#        paxosbench
#        RRR
#        MEMDB
#        EXTERN_C
#        YAMLCPP
#        ${RT}
#        #        ${CONAN_LIBS}
#        ${Boost_LIBRARIES}
#        ${CMAKE_THREAD_LIBS_INIT}
#        ${PYTHON_LIBRARIES}
#        #    ${APR_LIBRARIES}
#        #    ${APRUTIL_LIBRARIES}
#        #    ${PROFILER_LIBRARIES}
#)

#target_link_libraries(
#        JANUS_PROCESS
#        JANUS
#)

#target_link_libraries(
#        TEST
#        RRR
##        GTest::main
##        GTest::gtest
##        GSL:gsl
#        ${Boost_LIBRARIES}
##        ${CONAN_LIBS}
#        ${CMAKE_THREAD_LIBS_INIT}
#)
