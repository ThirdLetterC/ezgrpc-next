# Builds OpenSSL's libssl and libcrypto as static, position-independent libraries from an extracted
# release. OpenSSL is not a CMake project: its Perl Configure script writes a Makefile for make
# (nmake with MSVC), so this project runs Configure when it is configured, make when it is built
# and `make install_dev` (headers, libraries, pkg-config files) when it is installed.
#
# Needs Perl (on Windows, Strawberry Perl; Git's MSYS Perl does not work with MSVC). Assembly is
# used where OpenSSL needs no extra tool, and on Windows only when NASM is found.
#
#   -DOPENSSL_SOURCE_DIR=<extracted release>    (required)
#   -DOPENSSL_OPENSSLDIR=<dir>                  default certificate store and openssl.cnf location
cmake_minimum_required(VERSION 3.20)
project(openssl-static LANGUAGES C)

if(NOT OPENSSL_SOURCE_DIR OR NOT EXISTS "${OPENSSL_SOURCE_DIR}/Configure")
  message(FATAL_ERROR "OPENSSL_SOURCE_DIR must point at an extracted OpenSSL release")
endif()

find_package(Perl)
if(NOT PERL_FOUND)
  message(FATAL_ERROR "building OpenSSL needs Perl (perl was not found)")
endif()

include(ProcessorCount)
ProcessorCount(jobs)
if(NOT jobs OR jobs LESS 1)
  set(jobs 2)
endif()

set(options no-shared no-module no-tests no-apps no-docs "--prefix=${CMAKE_INSTALL_PREFIX}"
  --libdir=lib)
if(OPENSSL_OPENSSLDIR)
  list(APPEND options "--openssldir=${OPENSSL_OPENSSLDIR}")
endif()
string(TOUPPER "${CMAKE_BUILD_TYPE}" build_type)
if(build_type STREQUAL "DEBUG")
  list(APPEND options --debug)
else()
  list(APPEND options --release)
endif()

# The Configure target. Left empty, Configure picks the host's (and CC below, the compiler).
set(target "")
if(MSVC)
  if(CMAKE_C_COMPILER_ARCHITECTURE_ID MATCHES "ARM64")
    set(target VC-WIN64-ARM)
  elseif(CMAKE_SIZEOF_VOID_P EQUAL 8)
    set(target VC-WIN64A)
  else()
    set(target VC-WIN32)
  endif()
  find_program(NASM_EXECUTABLE nasm)
  if(NOT NASM_EXECUTABLE)
    list(APPEND options no-asm)
  endif()
  # nmake sits next to cl.exe
  get_filename_component(compiler_dir "${CMAKE_C_COMPILER}" DIRECTORY)
  find_program(MAKE_EXECUTABLE nmake HINTS "${compiler_dir}" REQUIRED)
  set(make "${MAKE_EXECUTABLE}")
else()
  if(MINGW)
    if(CMAKE_SIZEOF_VOID_P EQUAL 8)
      set(target mingw64)
    else()
      set(target mingw)
    endif()
  elseif(APPLE)
    # one architecture per build: the one asked for, else the compiler's
    set(arch "${CMAKE_OSX_ARCHITECTURES}")
    if(NOT arch)
      set(arch "${CMAKE_SYSTEM_PROCESSOR}")
    endif()
    list(LENGTH arch count)
    if(count GREATER 1)
      message(FATAL_ERROR "OpenSSL cannot be built for several architectures at once (${arch})")
    endif()
    if(arch MATCHES "arm64|aarch64")
      set(target darwin64-arm64-cc)
    elseif(arch MATCHES "x86_64")
      set(target darwin64-x86_64-cc)
    endif()
    if(CMAKE_OSX_DEPLOYMENT_TARGET)
      list(APPEND options "-mmacosx-version-min=${CMAKE_OSX_DEPLOYMENT_TARGET}")
    endif()
    if(CMAKE_OSX_SYSROOT)
      list(APPEND options "-isysroot${CMAKE_OSX_SYSROOT}")
    endif()
  endif()
  find_program(MAKE_EXECUTABLE NAMES gmake make mingw32-make REQUIRED)
  set(make "${MAKE_EXECUTABLE}" -j${jobs})
  list(APPEND options "CC=${CMAKE_C_COMPILER}")
  if(CMAKE_AR)
    list(APPEND options "AR=${CMAKE_AR}")
  endif()
  if(CMAKE_RANLIB)
    list(APPEND options "RANLIB=${CMAKE_RANLIB}")
  endif()
endif()

set(build "${CMAKE_CURRENT_BINARY_DIR}/openssl")
file(MAKE_DIRECTORY "${build}")
execute_process(
  COMMAND "${PERL_EXECUTABLE}" "${OPENSSL_SOURCE_DIR}/Configure" ${target} ${options}
  WORKING_DIRECTORY "${build}"
  RESULT_VARIABLE result)
if(NOT result EQUAL 0)
  message(FATAL_ERROR "OpenSSL's Configure failed (${result})")
endif()

add_custom_target(openssl ALL
  COMMAND ${make} build_libs
  WORKING_DIRECTORY "${build}"
  USES_TERMINAL
  VERBATIM)
set(make_quoted "")
foreach(arg IN LISTS make)
  string(APPEND make_quoted " [==[${arg}]==]")
endforeach()
install(CODE "
  execute_process(COMMAND ${make_quoted} install_dev WORKING_DIRECTORY [==[${build}]==]
    RESULT_VARIABLE result)
  if(NOT result EQUAL 0)
    message(FATAL_ERROR \"make install_dev failed (\${result})\")
  endif()
")
