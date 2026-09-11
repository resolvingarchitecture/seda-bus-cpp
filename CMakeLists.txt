cmake_minimum_required(VERSION 3.16)
project(seda_bus LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# The envelope carried on the bus is ra::common::Envelope, so seda-bus-cpp
# depends on its ra-common-cpp sibling (same monorepo layout dependency as
# seda-bus-ts -> "@resolvingarchitecture/ra-common": "file:../../common/ra-common-ts").
# Skip its own test/example subtargets when pulled in this way.
set(RA_COMMON_BUILD_TESTS OFF CACHE BOOL "" FORCE)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../../common/ra-common-cpp ${CMAKE_CURRENT_BINARY_DIR}/ra-common-cpp)

# Header-only library.
add_library(seda_bus INTERFACE)
target_include_directories(seda_bus INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(seda_bus INTERFACE ra_common)
target_compile_features(seda_bus INTERFACE cxx_std_20)

option(SEDA_BUS_BUILD_TESTS "Build seda-bus-cpp tests" ON)
if(SEDA_BUS_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

option(SEDA_BUS_BUILD_EXAMPLES "Build seda-bus-cpp examples" ON)
if(SEDA_BUS_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()
