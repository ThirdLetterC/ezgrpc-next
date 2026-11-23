# Bundled third-party libraries.
#
# With GRPC_DEPENDENCIES=BUNDLED (the default) each library the build needs, unless it is named in
# GRPC_SYSTEM_DEPENDENCIES, is downloaded as a pinned release archive, checked against its
# SHA-256 and built once at configure time. It is built as a static, position-independent library
# into ${GRPC_BUNDLED_DIR}/install. Later configures reuse that build until the pin or the
# toolchain changes.
#
# Archives are cached in GRPC_DEPS_CACHE_DIR, which every build directory shares, so deleting a
# build directory does not download them again. A machine without network access works once the
# archives are copied there (file names below).
#
# GRPC_DEPS_MIRRORS lists base URLs that are tried before the upstream ones. Each mirror serves the
# archives under the file names below, e.g. https://mirror.example/ezgrpc/nghttp2-1.71.0.tar.gz.

include(ProcessorCount)
include(CheckLanguage)

set(GRPC_BUNDLED_DIR "${FETCHCONTENT_BASE_DIR}/bundled")
set(GRPC_BUNDLED_NAMES)

# grpc_bundled_declare(<name> VERSION <v> FILE <archive file name> URLS <url>=<sha256>...
#   HEADER <header> LIBRARY_NAMES <name>... [SOURCE_SUBDIR <dir>] [OVERLAY <dir>]
#   [CMAKE_ARGS <arg>...] [DEFINITIONS <def>...] [LINK <item>...] [CXX])
# Records a pinned dependency. Every URL is tried in order. Each URL has its own hash, because a
# release asset and a GitHub tag archive of the same release differ byte-for-byte. A mirror
# serves FILE, so the file may match any of the listed hashes. OVERLAY builds the extracted
# sources with a CMake project of ours (passed -D<NAME>_SOURCE_DIR=<sources>). CXX means the
# library is C++, so its C++ runtime is linked as well.
macro(grpc_bundled_declare name)
  cmake_parse_arguments(_bd "CXX" "VERSION;FILE;HEADER;SOURCE_SUBDIR;OVERLAY"
    "URLS;LIBRARY_NAMES;CMAKE_ARGS;DEFINITIONS;LINK" ${ARGN})
  string(MAKE_C_IDENTIFIER "${name}" _bd_id)
  list(APPEND GRPC_BUNDLED_NAMES "${name}")
  set(GRPC_BUNDLED_${_bd_id}_NAME "${name}")
  foreach(_bd_field VERSION FILE HEADER SOURCE_SUBDIR OVERLAY URLS LIBRARY_NAMES CMAKE_ARGS
                    DEFINITIONS LINK CXX)
    set(GRPC_BUNDLED_${_bd_id}_${_bd_field} "${_bd_${_bd_field}}")
  endforeach()
endmacro()

# ---- pinned versions ------------------------------------------------------------------------
# Update a pin by changing VERSION, FILE and every URL=hash pair (sha256sum of the archive).

grpc_bundled_declare(nghttp2
  VERSION 1.71.0 FILE nghttp2-1.71.0.tar.gz
  URLS
    "https://github.com/nghttp2/nghttp2/releases/download/v1.71.0/nghttp2-1.71.0.tar.gz=0cdd35ecec77919b4b7b245f87fe8d2216e3d62da854eb0838c594ff058042fc"
  HEADER nghttp2/nghttp2.h LIBRARY_NAMES nghttp2 nghttp2_static
  DEFINITIONS NGHTTP2_STATICLIB
  CMAKE_ARGS -DENABLE_LIB_ONLY=ON -DBUILD_SHARED_LIBS=OFF -DBUILD_STATIC_LIBS=ON -DENABLE_DOC=OFF
    -DENABLE_HTTP3=OFF -DENABLE_FAILMALLOC=OFF -DENABLE_STATIC_CRT=OFF -DWITH_LIBXML2=OFF
    -DWITH_JEMALLOC=OFF)

if(WIN32)
  set(_grpc_uv_link psapi user32 advapi32 iphlpapi userenv ws2_32 dbghelp ole32 shell32)
elseif(APPLE)
  set(_grpc_uv_link Threads::Threads)
else()
  set(_grpc_uv_link Threads::Threads ${CMAKE_DL_LIBS})
  if(CMAKE_SYSTEM_NAME MATCHES "Linux|Android")
    list(APPEND _grpc_uv_link rt)
  elseif(CMAKE_SYSTEM_NAME MATCHES "BSD|DragonFly")
    list(APPEND _grpc_uv_link kvm)
  endif()
endif()
grpc_bundled_declare(libuv
  VERSION 1.53.0 FILE libuv-v1.53.0.tar.gz
  URLS
    "https://dist.libuv.org/dist/v1.53.0/libuv-v1.53.0.tar.gz=cb0d6dd2128d5a95bd242c6cc982a24fe608fa93da57b6b4ec763b0018c53e64"
    "https://github.com/libuv/libuv/archive/refs/tags/v1.53.0.tar.gz=279f3f67a24bb9921fe999ca6cd5e332fade8d515873ef9ba054b70e70a31d9e"
  HEADER uv.h LIBRARY_NAMES uv libuv uv_a
  LINK ${_grpc_uv_link}
  CMAKE_ARGS -DLIBUV_BUILD_SHARED=OFF -DLIBUV_BUILD_TESTS=OFF -DLIBUV_BUILD_BENCH=OFF)

grpc_bundled_declare(zlib
  VERSION 1.4 FILE zlib-1.4.tar.gz
  URLS
    "https://github.com/madler/zlib/releases/download/v1.4/zlib-1.4.tar.gz=02dd159783677a749c04adbdd02b55b8a2bcc7e054f51b76b549199a916ad584"
    "https://zlib.net/fossils/zlib-1.4.tar.gz=02dd159783677a749c04adbdd02b55b8a2bcc7e054f51b76b549199a916ad584"
  HEADER zlib.h LIBRARY_NAMES z zs libz libzs zlibstatic
  CMAKE_ARGS -DZLIB_BUILD_SHARED=OFF -DZLIB_BUILD_STATIC=ON -DZLIB_BUILD_TESTING=OFF)

grpc_bundled_declare(zstd
  VERSION 1.5.7 FILE zstd-1.5.7.tar.gz SOURCE_SUBDIR build/cmake
  URLS
    "https://github.com/facebook/zstd/releases/download/v1.5.7/zstd-1.5.7.tar.gz=eb33e51f49a15e023950cd7825ca74a4a2b43db8354825ac24fc1b7ee09e6fa3"
  HEADER zstd.h LIBRARY_NAMES zstd zstd_static
  CMAKE_ARGS -DZSTD_BUILD_SHARED=OFF -DZSTD_BUILD_STATIC=ON -DZSTD_BUILD_PROGRAMS=OFF
    -DZSTD_BUILD_TESTS=OFF -DZSTD_BUILD_CONTRIB=OFF -DZSTD_LEGACY_SUPPORT=OFF
    -DZSTD_MULTITHREAD_SUPPORT=OFF)

grpc_bundled_declare(snappy CXX
  VERSION 1.3.1 FILE snappy-1.3.1.tar.gz
  URLS
    "https://github.com/google/snappy/archive/refs/tags/1.3.1.tar.gz=893f708a0bf4b5529d555ffcee390e940e932fcf90261f682604475a76cd0247"
  HEADER snappy-c.h LIBRARY_NAMES snappy
  CMAKE_ARGS -DBUILD_SHARED_LIBS=OFF -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF
    -DSNAPPY_INSTALL=ON)

grpc_bundled_declare(protobuf-c
  VERSION 1.5.2 FILE protobuf-c-1.5.2.tar.gz
  URLS
    "https://github.com/protobuf-c/protobuf-c/releases/download/v1.5.2/protobuf-c-1.5.2.tar.gz=e2c86271873a79c92b58fef7ebf8de1aa0df4738347a8bd5d4e65a80a16d0d24"
  HEADER protobuf-c/protobuf-c.h LIBRARY_NAMES protobuf-c
  OVERLAY "${CMAKE_CURRENT_LIST_DIR}/bundled/protobuf-c")

# OpenSSL's directory (OPENSSLDIR) holds the default certificate store that clients verify servers
# against when no CA is configured. The bundled build uses the system's, as the host openssl
# command reports it, or the usual location. SSL_CERT_FILE / SSL_CERT_DIR still override it.
set(GRPC_OPENSSL_DIR "" CACHE STRING
  "OPENSSLDIR of the bundled OpenSSL (certificate store, openssl.cnf); empty: the system's")
set(_grpc_openssldir "${GRPC_OPENSSL_DIR}")
if(NOT _grpc_openssldir AND NOT WIN32 AND NOT CMAKE_CROSSCOMPILING)
  find_program(GRPC_HOST_OPENSSL_EXECUTABLE NAMES openssl NO_CMAKE_PATH)
  if(GRPC_HOST_OPENSSL_EXECUTABLE)
    execute_process(COMMAND "${GRPC_HOST_OPENSSL_EXECUTABLE}" version -d
      OUTPUT_VARIABLE _grpc_openssldir_out ERROR_QUIET RESULT_VARIABLE _grpc_openssldir_rc)
    if(_grpc_openssldir_rc EQUAL 0 AND _grpc_openssldir_out MATCHES "OPENSSLDIR: \"([^\"]+)\"")
      set(_grpc_openssldir "${CMAKE_MATCH_1}")
    endif()
  endif()
  if(NOT _grpc_openssldir)
    foreach(_grpc_dir IN ITEMS /etc/pki/tls /etc/ssl /usr/local/etc/ssl)
      if(NOT _grpc_openssldir AND (EXISTS "${_grpc_dir}/cert.pem" OR EXISTS "${_grpc_dir}/certs"))
        set(_grpc_openssldir "${_grpc_dir}")
      endif()
    endforeach()
  endif()
endif()
set(_grpc_openssl_args)
if(_grpc_openssldir)
  set(_grpc_openssl_args "-DOPENSSL_OPENSSLDIR=${_grpc_openssldir}")
endif()
if(WIN32)
  set(_grpc_openssl_link ws2_32 crypt32 user32 advapi32)
elseif(APPLE)
  set(_grpc_openssl_link Threads::Threads)
else()
  set(_grpc_openssl_link Threads::Threads ${CMAKE_DL_LIBS})
endif()
# libcrypto; GrpcDependencies.cmake adds libssl, installed next to it (grpc_bundled_openssl)
grpc_bundled_declare(openssl
  VERSION 3.5.9 FILE openssl-3.5.9.tar.gz
  URLS
    "https://github.com/openssl/openssl/releases/download/openssl-3.5.9/openssl-3.5.9.tar.gz=603f5602e2eef00d77fbd429d34dcd5822bb301757a1bc9cdb24c670f1eb859a"
    "https://www.openssl.org/source/openssl-3.5.9.tar.gz=603f5602e2eef00d77fbd429d34dcd5822bb301757a1bc9cdb24c670f1eb859a"
  HEADER openssl/crypto.h LIBRARY_NAMES crypto libcrypto
  LINK ${_grpc_openssl_link}
  CMAKE_ARGS ${_grpc_openssl_args}
  OVERLAY "${CMAKE_CURRENT_LIST_DIR}/bundled/openssl")

# ---- machinery ------------------------------------------------------------------------------

# The configurations to build. GCC/Clang code links into any configuration, so one Release build
# serves every configuration. MSVC debug and release code use different C runtimes (and C++
# iterator debugging), so a Debug build is added for debug configurations.
set(GRPC_BUNDLED_CONFIGS Release)
if(MSVC)
  if(CMAKE_CONFIGURATION_TYPES)
    set(GRPC_BUNDLED_CONFIGS Release Debug)
  elseif(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(GRPC_BUNDLED_CONFIGS Debug)
  endif()
endif()

ProcessorCount(_grpc_bundled_jobs)
if(NOT _grpc_bundled_jobs OR _grpc_bundled_jobs LESS 1)
  set(_grpc_bundled_jobs 2)
endif()

# Sets <out> to the cached archive for dependency <id>, downloading it when absent. On failure
# <out> is empty and <out>_ERROR says why.
function(_grpc_bundled_fetch id out)
  set(file "${GRPC_BUNDLED_${id}_FILE}")
  set(dest "${GRPC_DEPS_CACHE_DIR}/${file}")
  set(hashes)
  set(candidates)
  foreach(mirror IN LISTS GRPC_DEPS_MIRRORS)
    string(REGEX REPLACE "/+$" "" mirror "${mirror}")
    list(APPEND candidates "${mirror}/${file}")
  endforeach()
  foreach(entry IN LISTS GRPC_BUNDLED_${id}_URLS)
    string(REGEX MATCH "^(.*)=([0-9a-f]+)$" _ "${entry}")
    list(APPEND candidates "${CMAKE_MATCH_1}")
    list(APPEND hashes "${CMAKE_MATCH_2}")
  endforeach()
  set(${out} "" PARENT_SCOPE)

  if(EXISTS "${dest}")
    file(SHA256 "${dest}" actual)
    if(actual IN_LIST hashes)
      set(${out} "${dest}" PARENT_SCOPE)
      return()
    endif()
    message(WARNING "ezGRPC-next: ${dest} does not match its pinned SHA-256; downloading it again")
    file(REMOVE "${dest}")
  endif()

  file(MAKE_DIRECTORY "${GRPC_DEPS_CACHE_DIR}")
  string(RANDOM LENGTH 8 suffix)
  set(partial "${dest}.${suffix}.part")
  set(errors)
  foreach(url IN LISTS candidates)
    message(STATUS "ezGRPC-next: downloading ${url}")
    file(DOWNLOAD "${url}" "${partial}" STATUS status TLS_VERIFY ON
      INACTIVITY_TIMEOUT 60 TIMEOUT 900)
    list(GET status 0 code)
    if(code EQUAL 0)
      file(SHA256 "${partial}" actual)
      if(actual IN_LIST hashes)
        file(RENAME "${partial}" "${dest}")
        set(${out} "${dest}" PARENT_SCOPE)
        return()
      endif()
      list(APPEND errors "  ${url}: SHA-256 ${actual} is not a pinned hash")
    else()
      list(GET status 1 reason)
      list(APPEND errors "  ${url}: ${reason}")
    endif()
    file(REMOVE "${partial}")
  endforeach()
  list(JOIN errors "\n" errors)
  set(${out}_ERROR "could not download ${file}:\n${errors}\nCopy the archive into ${GRPC_DEPS_CACHE_DIR} to build offline, or add a mirror to GRPC_DEPS_MIRRORS." PARENT_SCOPE)
endfunction()

# Runs one step of a dependency build, logging to <log>. Sets <ok> (and <ok>_ERROR on failure).
function(_grpc_bundled_run ok log)
  execute_process(COMMAND ${ARGN} RESULT_VARIABLE result OUTPUT_FILE "${log}" ERROR_FILE "${log}")
  if(result EQUAL 0)
    set(${ok} TRUE PARENT_SCOPE)
    return()
  endif()
  set(${ok} FALSE PARENT_SCOPE)
  set(tail "")
  if(EXISTS "${log}")
    file(STRINGS "${log}" lines)
    list(LENGTH lines count)
    if(count GREATER 25)
      math(EXPR start "${count} - 25")
      list(SUBLIST lines ${start} 25 lines)
    endif()
    list(JOIN lines "\n    " tail)
  endif()
  set(${ok}_ERROR "step failed (${result}); full log: ${log}\n    ${tail}" PARENT_SCOPE)
endfunction()

# Builds and installs dependency <id> from <source> for configuration <config> into <prefix>,
# unless the stamp from an identical earlier build is present. Sets <ok> and <ok>_ERROR.
function(_grpc_bundled_build id source config prefix ok)
  set(${ok} FALSE PARENT_SCOPE)
  set(name "${GRPC_BUNDLED_${id}_NAME}")
  string(TOLOWER "${config}" config_lower)
  set(build "${GRPC_BUNDLED_DIR}/build/${id}-${config_lower}")
  set(logs "${GRPC_BUNDLED_DIR}/logs")
  file(MAKE_DIRECTORY "${logs}")

  set(args -G "${CMAKE_GENERATOR}" -Wno-dev --no-warn-unused-cli)
  if(CMAKE_GENERATOR_PLATFORM)
    list(APPEND args -A "${CMAKE_GENERATOR_PLATFORM}")
  endif()
  if(CMAKE_GENERATOR_TOOLSET)
    list(APPEND args -T "${CMAKE_GENERATOR_TOOLSET}")
  endif()
  if(CMAKE_MAKE_PROGRAM)
    list(APPEND args "-DCMAKE_MAKE_PROGRAM=${CMAKE_MAKE_PROGRAM}")
  endif()
  if(CMAKE_TOOLCHAIN_FILE)
    list(APPEND args "-DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}")
  endif()
  if(NOT CMAKE_GENERATOR MATCHES "Visual Studio|Xcode")
    list(APPEND args "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}")
    if(GRPC_BUNDLED_${id}_CXX)
      list(APPEND args "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}")
    endif()
  endif()
  if(CMAKE_C_COMPILER_LAUNCHER)
    list(APPEND args "-DCMAKE_C_COMPILER_LAUNCHER=${CMAKE_C_COMPILER_LAUNCHER}")
  endif()
  foreach(var IN ITEMS CMAKE_OSX_ARCHITECTURES CMAKE_OSX_SYSROOT CMAKE_OSX_DEPLOYMENT_TARGET
                       CMAKE_SYSTEM_NAME CMAKE_SYSTEM_PROCESSOR)
    if(DEFINED CACHE{${var}} AND NOT "${${var}}" STREQUAL "")
      list(APPEND args "-D${var}=${${var}}")
    endif()
  endforeach()
  if(NOT CMAKE_CONFIGURATION_TYPES)
    list(APPEND args "-DCMAKE_BUILD_TYPE=${config}")
  endif()
  list(APPEND args
    "-DCMAKE_INSTALL_PREFIX=${prefix}" -DCMAKE_INSTALL_LIBDIR=lib -DCMAKE_INSTALL_INCLUDEDIR=include
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DBUILD_TESTING=OFF
    "-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded$<$<CONFIG:Debug>:Debug>DLL"
    -DCMAKE_POLICY_DEFAULT_CMP0091=NEW
    ${GRPC_BUNDLED_${id}_CMAKE_ARGS})
  if(GRPC_BUNDLED_${id}_OVERLAY)
    string(TOUPPER "${id}" upper)
    list(APPEND args "-D${upper}_SOURCE_DIR=${source}")
    set(project_dir "${GRPC_BUNDLED_${id}_OVERLAY}")
    file(SHA256 "${project_dir}/CMakeLists.txt" overlay_hash)
  else()
    set(project_dir "${source}")
    if(GRPC_BUNDLED_${id}_SOURCE_SUBDIR)
      string(APPEND project_dir "/${GRPC_BUNDLED_${id}_SOURCE_SUBDIR}")
    endif()
    set(overlay_hash "")
  endif()

  # rebuild when the pin, the toolchain or the arguments change
  string(SHA256 stamp_content "${GRPC_BUNDLED_${id}_VERSION}|${CMAKE_VERSION}|${CMAKE_C_COMPILER}|${CMAKE_C_COMPILER_VERSION}|${overlay_hash}|${args}")
  set(stamp "${prefix}/.stamps/${id}")
  if(EXISTS "${stamp}")
    file(READ "${stamp}" previous)
    if(previous STREQUAL stamp_content)
      set(${ok} TRUE PARENT_SCOPE)
      return()
    endif()
  endif()

  message(STATUS "ezGRPC-next: building bundled ${name} ${GRPC_BUNDLED_${id}_VERSION} (${config}); logs in ${logs}")
  file(REMOVE_RECURSE "${build}")
  _grpc_bundled_run(step "${logs}/${id}-${config_lower}-configure.log"
    "${CMAKE_COMMAND}" -S "${project_dir}" -B "${build}" ${args})
  if(step)
    _grpc_bundled_run(step "${logs}/${id}-${config_lower}-build.log"
      "${CMAKE_COMMAND}" --build "${build}" --config "${config}" --parallel ${_grpc_bundled_jobs})
  endif()
  if(step)
    _grpc_bundled_run(step "${logs}/${id}-${config_lower}-install.log"
      "${CMAKE_COMMAND}" --install "${build}" --config "${config}")
  endif()
  if(NOT step)
    set(${ok}_ERROR "${step_ERROR}" PARENT_SCOPE)
    return()
  endif()
  file(WRITE "${stamp}" "${stamp_content}")
  set(${ok} TRUE PARENT_SCOPE)
endfunction()

# Libraries the C++ runtime needs beyond the C compiler's, as the dependency's own build found
# them (e.g. stdc++ and m for GCC, c++ for Apple Clang; nothing for MSVC, which links it implicitly).
function(_grpc_bundled_cxx_runtime id config out)
  string(TOLOWER "${config}" config_lower)
  set(${out} "" PARENT_SCOPE)
  set(file "${GRPC_BUNDLED_DIR}/build/${id}-${config_lower}/CMakeFiles/${CMAKE_VERSION}/CMakeCXXCompiler.cmake")
  if(NOT EXISTS "${file}")
    return()
  endif()
  set(c_libs ${CMAKE_C_IMPLICIT_LINK_LIBRARIES})
  include("${file}")
  set(libs ${CMAKE_CXX_IMPLICIT_LINK_LIBRARIES})
  list(REMOVE_DUPLICATES libs)
  if(c_libs)
    list(REMOVE_ITEM libs ${c_libs})
  endif()
  set(${out} "${libs}" PARENT_SCOPE)
endfunction()

# grpc_bundled_dependency(<out> <name>)
# Provides the pinned dependency <name> and sets <out> to an imported target for it. On failure
# <out> is empty and GRPC_BUNDLED_<id>_ERROR explains why.
function(grpc_bundled_dependency out name)
  string(MAKE_C_IDENTIFIER "${name}" id)
  set(${out} "" PARENT_SCOPE)
  set(target grpc_bundled_${id})
  if(TARGET ${target})
    set(${out} ${target} PARENT_SCOPE)
    return()
  endif()

  if(GRPC_BUNDLED_${id}_CXX)
    if(NOT CMAKE_CXX_COMPILER)
      check_language(CXX)
    endif()
    if(NOT CMAKE_CXX_COMPILER)
      set(GRPC_BUNDLED_${id}_ERROR "${name} is C++ and no C++ compiler was found" PARENT_SCOPE)
      return()
    endif()
  endif()

  _grpc_bundled_fetch(${id} archive)
  if(NOT archive)
    set(GRPC_BUNDLED_${id}_ERROR "${archive_ERROR}" PARENT_SCOPE)
    return()
  endif()

  # extracted once per archive (the file name carries the version)
  set(source "${GRPC_BUNDLED_DIR}/src/${id}-${GRPC_BUNDLED_${id}_VERSION}")
  if(NOT EXISTS "${source}/.grpc-extracted")
    file(REMOVE_RECURSE "${source}" "${source}.tmp")
    file(ARCHIVE_EXTRACT INPUT "${archive}" DESTINATION "${source}.tmp")
    file(GLOB entries LIST_DIRECTORIES true "${source}.tmp/*")
    list(LENGTH entries count)
    if(count EQUAL 1 AND IS_DIRECTORY "${entries}")
      file(RENAME "${entries}" "${source}")
      file(REMOVE_RECURSE "${source}.tmp")
    else()
      file(RENAME "${source}.tmp" "${source}")
    endif()
    file(TOUCH "${source}/.grpc-extracted")
  endif()

  set(locations)
  foreach(config IN LISTS GRPC_BUNDLED_CONFIGS)
    string(TOLOWER "${config}" config_lower)
    set(prefix "${GRPC_BUNDLED_DIR}/install/${config_lower}")
    _grpc_bundled_build(${id} "${source}" ${config} "${prefix}" built)
    if(NOT built)
      set(GRPC_BUNDLED_${id}_ERROR "building ${name} failed: ${built_ERROR}" PARENT_SCOPE)
      return()
    endif()
    set(library "")
    set(lib_names ${GRPC_BUNDLED_${id}_LIBRARY_NAMES})
    if(config STREQUAL "Debug")
      # zlib and libuv force CMAKE_DEBUG_POSTFIX "d" on MSVC
      list(TRANSFORM lib_names APPEND d OUTPUT_VARIABLE debug_names)
      list(PREPEND lib_names ${debug_names})
    endif()
    foreach(lib_name IN LISTS lib_names)
      foreach(candidate IN ITEMS
          "${CMAKE_STATIC_LIBRARY_PREFIX}${lib_name}${CMAKE_STATIC_LIBRARY_SUFFIX}"
          "lib${lib_name}.a" "${lib_name}.lib")
        if(NOT library AND EXISTS "${prefix}/lib/${candidate}")
          set(library "${prefix}/lib/${candidate}")
        endif()
      endforeach()
    endforeach()
    if(NOT library OR NOT EXISTS "${prefix}/include/${GRPC_BUNDLED_${id}_HEADER}")
      set(GRPC_BUNDLED_${id}_ERROR "the ${name} build did not install ${GRPC_BUNDLED_${id}_HEADER} and a library in ${prefix}" PARENT_SCOPE)
      return()
    endif()
    list(APPEND locations ${config} "${library}")
    set(include_dir "${prefix}/include")
  endforeach()

  add_library(${target} UNKNOWN IMPORTED)
  list(GET locations 1 release_location)
  set_target_properties(${target} PROPERTIES
    IMPORTED_LOCATION "${release_location}"
    INTERFACE_INCLUDE_DIRECTORIES "${include_dir}")
  if("Debug" IN_LIST GRPC_BUNDLED_CONFIGS)
    list(FIND locations Debug index)
    math(EXPR index "${index} + 1")
    list(GET locations ${index} debug_location)
    set_target_properties(${target} PROPERTIES IMPORTED_LOCATION_DEBUG "${debug_location}")
  endif()
  if(GRPC_BUNDLED_${id}_DEFINITIONS)
    set_target_properties(${target} PROPERTIES
      INTERFACE_COMPILE_DEFINITIONS "${GRPC_BUNDLED_${id}_DEFINITIONS}")
  endif()
  set(link ${GRPC_BUNDLED_${id}_LINK})
  if(GRPC_BUNDLED_${id}_CXX)
    list(GET GRPC_BUNDLED_CONFIGS 0 first_config)
    _grpc_bundled_cxx_runtime(${id} ${first_config} cxx_runtime)
    list(APPEND link ${cxx_runtime})
  endif()
  if(link)
    set_target_properties(${target} PROPERTIES INTERFACE_LINK_LIBRARIES "${link}")
  endif()
  set(${out} ${target} PARENT_SCOPE)
endfunction()

# Whether dependency <name> should come from the bundled build: GRPC_DEPENDENCIES is BUNDLED and
# <name> is not listed in GRPC_SYSTEM_DEPENDENCIES.
function(grpc_dependency_is_bundled out name)
  if(GRPC_DEPENDENCIES STREQUAL "BUNDLED" AND NOT name IN_LIST GRPC_SYSTEM_DEPENDENCIES)
    set(${out} TRUE PARENT_SCOPE)
  else()
    set(${out} FALSE PARENT_SCOPE)
  endif()
endfunction()
