cmake_minimum_required(VERSION 3.5)
project(wtc)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED True)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_executable(wtc src/main.c)
target_include_directories(wtc PUBLIC /usr/local/include)
target_link_libraries(wtc PUBLIC wtl wiredtiger)
target_compile_options(wtc PRIVATE -Wall -Wextra -Wpedantic -Werror)

add_library(wtl STATIC src/wtl.c)
target_include_directories(wtl PUBLIC /usr/local/include)
target_link_libraries(wtl PUBLIC bson wiredtiger)
target_compile_options(wtl PRIVATE -Wall -Wextra -Wpedantic -Werror)

add_library(bson STATIC IMPORTED)
set_target_properties(bson PROPERTIES
    IMPORTED_LOCATION /usr/local/lib/libbson-static-1.0.a)

add_library(wiredtiger STATIC IMPORTED)
target_link_libraries(wtl PUBLIC snappy zlib zstd lz4)
set_target_properties(wiredtiger PROPERTIES
    IMPORTED_LOCATION /usr/local/lib/libwiredtiger.a)

add_library(snappy STATIC IMPORTED)
set_target_properties(snappy PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LOCATION /opt/homebrew/lib/libsnappy.a)

add_library(zlib STATIC IMPORTED)
set_target_properties(zlib PROPERTIES
    IMPORTED_LOCATION /opt/homebrew/Cellar/zlib/1.2.13/lib/libz.a)

add_library(zstd STATIC IMPORTED)
set_target_properties(zstd PROPERTIES
    IMPORTED_LOCATION /opt/homebrew/lib/libzstd.a)

add_library(lz4 STATIC IMPORTED)
set_target_properties(lz4 PROPERTIES
    IMPORTED_LOCATION /opt/homebrew/lib/liblz4.a)
