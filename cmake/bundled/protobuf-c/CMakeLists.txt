# Builds only the protobuf-c runtime (one C file) from an extracted protobuf-c release. Upstream's
# build-cmake always requires the C++ protobuf package for protoc-c, which the runtime does not use.
cmake_minimum_required(VERSION 3.20)
project(protobuf-c-runtime LANGUAGES C)

if(NOT PROTOBUF_C_SOURCE_DIR OR NOT EXISTS "${PROTOBUF_C_SOURCE_DIR}/protobuf-c/protobuf-c.c")
  message(FATAL_ERROR "PROTOBUF_C_SOURCE_DIR must point at an extracted protobuf-c release")
endif()

include(GNUInstallDirs)
add_library(protobuf-c STATIC "${PROTOBUF_C_SOURCE_DIR}/protobuf-c/protobuf-c.c")
target_include_directories(protobuf-c PUBLIC "${PROTOBUF_C_SOURCE_DIR}")
if(MSVC)
  target_compile_options(protobuf-c PRIVATE /wd4267 /wd4244)
endif()
install(TARGETS protobuf-c ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")
install(FILES "${PROTOBUF_C_SOURCE_DIR}/protobuf-c/protobuf-c.h"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/protobuf-c")
