
include(/Users/zilliz/repo/github/microsoft/vcpkg/scripts/buildsystems/vcpkg.cmake)

find_package(folly CONFIG REQUIRED)

include_directories(${FOLLY_INCLUDE_DIRS})

add_executable(FUTURES Futures.cpp)

target_link_libraries(FUTURES PRIVATE Folly::folly)
