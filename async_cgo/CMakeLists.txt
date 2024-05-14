cmake_minimum_required(VERSION 3.22)
project(concurrency VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)  
include(/Users/zilliz/repo/github/microsoft/vcpkg/scripts/buildsystems/vcpkg.cmake)

find_package(folly CONFIG REQUIRED)

add_subdirectory(futures)
