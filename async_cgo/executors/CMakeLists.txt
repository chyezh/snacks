
include(/home/chyezh/repository/microsoft/vcpkg/scripts/buildsystems/vcpkg.cmake)

find_package(folly CONFIG REQUIRED)

include_directories(${FOLLY_INCLUDE_DIRS})

add_library(EXECUTORS Executors.cpp)
