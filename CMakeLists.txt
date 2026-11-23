cmake_minimum_required(VERSION 3.20...3.31)
project(ezGRPC-next VERSION 1.0.0 LANGUAGES C)

if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
  set(GRPC_IS_TOP_LEVEL ON)
else()
  set(GRPC_IS_TOP_LEVEL OFF)
endif()

if(GRPC_IS_TOP_LEVEL AND NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()
if(GRPC_IS_TOP_LEVEL AND "${CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "")
  # compile_commands.json for clangd and other tools unless the caller chose (CMake pre-creates
  # an empty cache entry; an explicit OFF is kept). Generators without support ignore it.
  set(CMAKE_EXPORT_COMPILE_COMMANDS ON CACHE BOOL "Write compile_commands.json" FORCE)
endif()

if(GRPC_IS_TOP_LEVEL)
  include(CTest)
  set(_grpc_default_tests "${BUILD_TESTING}")
else()
  set(_grpc_default_tests OFF)
endif()
include(FetchContent)
include(GNUInstallDirs)
find_package(PkgConfig QUIET)
# Keep discovery local to this project without replacing FindPkgConfig's macro.
macro(grpc_pkg_check_modules prefix)
  if(PkgConfig_FOUND)
    pkg_check_modules(${prefix} ${ARGN})
  else()
    set(_grpc_pkg_args ${ARGN})
    if("REQUIRED" IN_LIST _grpc_pkg_args)
      message(FATAL_ERROR "pkg-config is required for ${prefix}; install it or choose another backend")
    endif()
    set(${prefix}_FOUND FALSE)
  endif()
endmacro()
find_package(Threads REQUIRED)

option(GRPC_BUILD_TESTS "Build unit tests" ${_grpc_default_tests})
option(GRPC_BUILD_EXAMPLES "Build example servers" ${GRPC_IS_TOP_LEVEL})
option(GRPC_BUILD_SHARED "Build the shared library alongside the static library" ON)
option(GRPC_BUILD_INTEGRATION_TESTS "Register Python gRPC integration tests" ${_grpc_default_tests})
option(GRPC_BUILD_DEVELOPER_TARGETS "Add source formatting targets" ${GRPC_IS_TOP_LEVEL})
option(GRPC_INSTALL "Generate CMake install and pkg-config targets" ${GRPC_IS_TOP_LEVEL})
option(GRPC_IPO "Enable interprocedural optimization in optimized configurations" OFF)
option(GRPC_WERROR "Treat warnings in the library's sources as errors" OFF)
option(GRPC_HARDENING
  "Build the library with stack protection, _FORTIFY_SOURCE=3, CET and RELRO (GCC/Clang)" ON)
option(GRPC_ASAN "Build with AddressSanitizer (same as GRPC_SANITIZE=address)" OFF)
set(GRPC_SANITIZE "" CACHE STRING
  "Sanitizers for the library, tests and examples (GCC/Clang), e.g. address;undefined or thread")
option(GRPC_DEBUG_LOGS "Compile debug logging support" OFF)
option(GRPC_TRACE "Compile per-frame trace logging" OFF)
set(GRPC_ALLOCATOR "default" CACHE STRING "Allocator: default, mimalloc, or jemalloc")
set_property(CACHE GRPC_ALLOCATOR PROPERTY STRINGS default mimalloc jemalloc)
set(GRPC_TLS_BACKEND "AUTO" CACHE STRING "TLS backend: AUTO, openssl, libressl, boringssl, awslc, wolfssl, gnutls, rustls, or none")
set_property(CACHE GRPC_TLS_BACKEND PROPERTY STRINGS AUTO openssl libressl boringssl awslc wolfssl gnutls rustls none)
set(GRPC_TLS_PREFIX "" CACHE PATH "Prefix containing a non-system TLS backend")
set(GRPC_FLATCC_PREFIX "" CACHE PATH "Prefix containing flatcc, headers, and runtime library")

# Where third-party libraries come from (cmake/GrpcBundledDeps.cmake). BUNDLED downloads pinned,
# checksummed releases and builds them; SYSTEM uses installed packages. A vcpkg toolchain means the
# caller already manages packages, so SYSTEM is the default there.
if(CMAKE_TOOLCHAIN_FILE MATCHES "vcpkg\\.cmake$" OR VCPKG_TOOLCHAIN)
  set(_grpc_default_dependencies SYSTEM)
else()
  set(_grpc_default_dependencies BUNDLED)
endif()
set(GRPC_DEPENDENCIES "${_grpc_default_dependencies}" CACHE STRING
  "Third-party libraries: BUNDLED (download pinned releases and build them) or SYSTEM (installed packages)")
set_property(CACHE GRPC_DEPENDENCIES PROPERTY STRINGS BUNDLED SYSTEM)
string(TOUPPER "${GRPC_DEPENDENCIES}" GRPC_DEPENDENCIES)
if(NOT GRPC_DEPENDENCIES MATCHES "^(BUNDLED|SYSTEM)$")
  message(FATAL_ERROR "GRPC_DEPENDENCIES must be BUNDLED or SYSTEM")
endif()
set(GRPC_SYSTEM_DEPENDENCIES "" CACHE STRING
  "With GRPC_DEPENDENCIES=BUNDLED, dependencies to take from the system anyway: any of nghttp2;libuv;zlib;zstd;snappy;protobuf-c;python")
set(GRPC_DEPS_MIRRORS "$ENV{GRPC_DEPS_MIRRORS}" CACHE STRING
  "Base URLs tried before upstream when downloading bundled dependencies (each serves the archive file names)")
if(DEFINED ENV{GRPC_DEPS_CACHE_DIR})
  set(_grpc_default_cache "$ENV{GRPC_DEPS_CACHE_DIR}")
elseif(WIN32 AND DEFINED ENV{LOCALAPPDATA})
  set(_grpc_default_cache "$ENV{LOCALAPPDATA}/ezgrpc-next/cache")
elseif(DEFINED ENV{XDG_CACHE_HOME})
  set(_grpc_default_cache "$ENV{XDG_CACHE_HOME}/ezgrpc-next")
elseif(DEFINED ENV{HOME})
  set(_grpc_default_cache "$ENV{HOME}/.cache/ezgrpc-next")
else()
  set(_grpc_default_cache "${FETCHCONTENT_BASE_DIR}/downloads")
endif()
file(TO_CMAKE_PATH "${_grpc_default_cache}" _grpc_default_cache)
set(GRPC_DEPS_CACHE_DIR "${_grpc_default_cache}" CACHE PATH
  "Download cache for bundled dependency archives, shared by build directories")

foreach(feature IN ITEMS PROTOBUF_C FLATCC ZLIB ZSTD SNAPPY HTTP3)
  set(GRPC_${feature} "AUTO" CACHE STRING "${feature} support: AUTO, ON, or OFF")
  set_property(CACHE GRPC_${feature} PROPERTY STRINGS AUTO ON OFF)
  string(TOUPPER "${GRPC_${feature}}" feature_value)
  if(NOT feature_value MATCHES "^(AUTO|ON|OFF)$")
    message(FATAL_ERROR "GRPC_${feature} must be AUTO, ON, or OFF")
  endif()
  set(GRPC_${feature} "${feature_value}")
endforeach()

string(TOLOWER "${GRPC_ALLOCATOR}" GRPC_ALLOCATOR)
if(NOT GRPC_ALLOCATOR MATCHES "^(default|mimalloc|jemalloc)$")
  message(FATAL_ERROR "GRPC_ALLOCATOR must be default, mimalloc, or jemalloc")
endif()
string(TOLOWER "${GRPC_TLS_BACKEND}" _tls_backend)
set(_tls_backends auto openssl libressl boringssl awslc wolfssl gnutls rustls none)
if(NOT _tls_backend IN_LIST _tls_backends)
  message(FATAL_ERROR "Unknown GRPC_TLS_BACKEND '${GRPC_TLS_BACKEND}'")
endif()

if(GRPC_BUILD_TESTS OR (GRPC_BUILD_EXAMPLES AND GRPC_BUILD_INTEGRATION_TESTS))
  enable_testing()
endif()

set(_grpc_preferred_prefixes)
if(GRPC_TLS_PREFIX)
  list(APPEND _grpc_preferred_prefixes "${GRPC_TLS_PREFIX}")
endif()
if(GRPC_FLATCC_PREFIX)
  list(APPEND _grpc_preferred_prefixes "${GRPC_FLATCC_PREFIX}")
endif()
if(DEFINED ENV{HOME})
  list(APPEND _grpc_preferred_prefixes "$ENV{HOME}/.local" "$ENV{HOME}/.local/usr")
endif()
if(_grpc_preferred_prefixes)
  list(PREPEND CMAKE_PREFIX_PATH ${_grpc_preferred_prefixes})
endif()

include(cmake/GrpcCompiler.cmake)

include(cmake/GrpcUlog.cmake)

include(cmake/GrpcBundledDeps.cmake)

include(cmake/GrpcDependencies.cmake)

include(cmake/GrpcPython.cmake)

set(GRPC_GENERATED_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated/include")
file(MAKE_DIRECTORY "${GRPC_GENERATED_INCLUDE_DIR}")
configure_file(include/grpc_version_config.h.in
  "${GRPC_GENERATED_INCLUDE_DIR}/grpc_version_config.h" @ONLY)

set(GRPC_SOURCES
  src/grpc_call.c
  src/grpc_call_access.c
  src/grpc_call_async.c
  src/grpc_call_dispatch.c
  src/grpc_call_response.c
  src/grpc_channelz.c
  src/grpc_client.c
  src/grpc_client_connect.c
  src/grpc_client_io.c
  src/grpc_client_response.c
  src/grpc_compression.c
  src/grpc_health.c
  src/grpc_http3.c
  src/grpc_interceptor.c
  src/grpc_mem.c
  src/grpc_message.c
  src/grpc_metadata.c
  src/grpc_reflection.c
  src/grpc_server.c
  src/grpc_server_listen.c
  src/grpc_server_log.c
  src/grpc_server_routes.c
  src/grpc_server_run.c
  src/grpc_status.c
  src/grpc_status_details.c
  src/grpc_stream_core.c
  src/grpc_timeout.c
  src/grpc_tls.c
  src/grpc_version.c
  src/internal_helpers.c
  src/internal_session_io.c
  src/internal_nghttp2_callbacks.c
)
if(GRPC_TLS_SOURCE)
  list(APPEND GRPC_SOURCES "${GRPC_TLS_SOURCE}")
endif()
if(GRPC_PROTOBUF_C_ENABLED)
  list(APPEND GRPC_SOURCES src/grpc_protobuf_c.c)
endif()
if(GRPC_HTTP3_ENABLED)
  list(APPEND GRPC_SOURCES src/grpc_http3_server.c src/grpc_http3_server_callbacks.c
    src/grpc_http3_server_conn.c src/grpc_http3_client.c src/grpc_http3_client_callbacks.c "${GRPC_QUIC_TLS_SOURCE}")
endif()
if(GRPC_FLATCC_ENABLED)
  list(APPEND GRPC_SOURCES src/grpc_flatcc.c)
endif()

add_library(grpc_objects OBJECT ${GRPC_SOURCES})
set_target_properties(grpc_objects PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(grpc_objects PUBLIC include "${GRPC_GENERATED_INCLUDE_DIR}")
# bundled code: its headers are not held to the library's warning flags
target_include_directories(grpc_objects SYSTEM PRIVATE "${ulog_SOURCE_DIR}/include")
if(MSVC)
  target_include_directories(grpc_objects BEFORE PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/generated/ulog")
endif()
target_include_directories(grpc_objects PRIVATE include/internal)
target_compile_options(grpc_objects PRIVATE ${GRPC_WARNING_FLAGS} ${GRPC_STRICT_WARNING_FLAGS}
  ${GRPC_HARDENING_COMPILE_FLAGS})
if(GRPC_WERROR)
  target_compile_options(grpc_objects PRIVATE $<IF:$<C_COMPILER_ID:MSVC>,/WX,-Werror>)
endif()
target_link_libraries(grpc_objects PRIVATE grpc_build_options)
if(NOT MSVC)
  target_compile_options(grpc_objects PRIVATE -fvisibility=hidden)
endif()
if(NOT GRPC_HAS_C23_CONSTEXPR)
  # Clang 18 accepts -std=c23 but does not yet implement C23 constexpr.
  target_compile_definitions(grpc_objects PRIVATE constexpr=const)
endif()
# _FORTIFY_SOURCE is not imposed on users; the library sets it for itself (GRPC_HARDENING)
set(GRPC_PUBLIC_COMPILE_DEFINITIONS _GNU_SOURCE)
if(NOT GRPC_HAS_NULLPTR)
  list(APPEND GRPC_PUBLIC_COMPILE_DEFINITIONS "$<$<COMPILE_LANGUAGE:C>:nullptr=((void*)0)>")
endif()
if(NOT GRPC_HAS_ALIGNOF)
  list(APPEND GRPC_PUBLIC_COMPILE_DEFINITIONS "$<$<COMPILE_LANGUAGE:C>:alignof=_Alignof>")
endif()
if(GRPC_ALLOCATOR_DEFINE)
  list(APPEND GRPC_PUBLIC_COMPILE_DEFINITIONS "${GRPC_ALLOCATOR_DEFINE}")
endif()
foreach(feature IN ITEMS ZLIB ZSTD SNAPPY PROTOBUF_C FLATCC HTTP3)
  if(GRPC_${feature}_ENABLED)
    list(APPEND GRPC_PUBLIC_COMPILE_DEFINITIONS "GRPC_ENABLE_${feature}")
  endif()
endforeach()
if(GRPC_TLS_ENABLED)
  string(TOUPPER "${_tls_backend}" _tls_backend_macro)
  list(APPEND GRPC_PUBLIC_COMPILE_DEFINITIONS GRPC_ENABLE_TLS "GRPC_TLS_${_tls_backend_macro}")
endif()
if(GRPC_DEBUG_LOGS)
  list(APPEND GRPC_PUBLIC_COMPILE_DEFINITIONS GRPC_DEBUG)
  if(MSVC)
    target_compile_options(grpc_objects PRIVATE /Zi /Od)
  else()
    target_compile_options(grpc_objects PRIVATE -ggdb3 -O0)
  endif()
endif()
if(GRPC_TRACE)
  list(APPEND GRPC_PUBLIC_COMPILE_DEFINITIONS GRPC_TRACE)
endif()
target_compile_definitions(grpc_objects PUBLIC ${GRPC_PUBLIC_COMPILE_DEFINITIONS})
target_compile_definitions(grpc_objects PRIVATE
  ULOG_BUILD_EXTRA_OUTPUTS=1 ULOG_BUILD_TOPICS_MODE=2 ULOG_BUILD_COLOR=0
  ULOG_BUILD_SOURCE_LOCATION=0 ULOG_BUILD_TIME=0 ULOG_BUILD_WARN_NOT_ENABLED=0)
if(GRPC_FLATCC_ENABLED)
  target_include_directories(grpc_objects PUBLIC "${FLATCC_INCLUDE_DIR}")
endif()
if(_tls_backend STREQUAL "rustls")
  target_include_directories(grpc_objects PRIVATE "${RUSTLS_INCLUDE_DIR}")
endif()

  add_library(grpc_static STATIC $<TARGET_OBJECTS:grpc_objects>)
  set_target_properties(grpc_static PROPERTIES OUTPUT_NAME grpc EXPORT_NAME static)
  set(GRPC_LIBRARY_TARGETS grpc_static)
  if(GRPC_BUILD_SHARED)
    add_library(grpc_shared SHARED $<TARGET_OBJECTS:grpc_objects>)
    set_target_properties(grpc_shared PROPERTIES
      OUTPUT_NAME grpc
      EXPORT_NAME shared
      VERSION "${PROJECT_VERSION}"
      SOVERSION "${PROJECT_VERSION_MAJOR}")
    # dependencies from a custom prefix (GRPC_TLS_PREFIX: OpenSSL 3.5, ngtcp2, ...) stay findable
    # after install; system library directories are never added
    set_target_properties(grpc_shared PROPERTIES INSTALL_RPATH_USE_LINK_PATH ON)
    target_link_options(grpc_shared PRIVATE ${GRPC_HARDENING_LINK_FLAGS})
    if(NOT WIN32 AND NOT APPLE)
      # the static archives linked in (bundled nghttp2, libuv, ...) stay private to libgrpc.so,
      # so they cannot interpose on another copy an application loads
      include(CheckLinkerFlag)
      check_linker_flag(C "LINKER:--exclude-libs,ALL" GRPC_HAS_LINK_EXCLUDE_LIBS)
      if(GRPC_HAS_LINK_EXCLUDE_LIBS)
        target_link_options(grpc_shared PRIVATE "LINKER:--exclude-libs,ALL")
      endif()
    endif()
  if(WIN32)
    # The DLL import library and the static archive must not both be grpc.lib.
    set_target_properties(grpc_shared PROPERTIES ARCHIVE_OUTPUT_NAME grpc_dll
      WINDOWS_EXPORT_ALL_SYMBOLS ON)
  endif()
  add_library(grpc::shared ALIAS grpc_shared)
  list(APPEND GRPC_LIBRARY_TARGETS grpc_shared)
endif()
add_library(grpc::grpc ALIAS grpc_static)
add_library(grpc::static ALIAS grpc_static)

set(GRPC_LINK_LIBRARIES ulog::ulog "${GRPC_NGHTTP2_TARGET}" ${GRPC_LIBUV_TARGET} Threads::Threads
  ${GRPC_TLS_LIBRARIES} ${GRPC_ALLOCATOR_LIBRARIES})
if(WIN32)
  list(APPEND GRPC_LINK_LIBRARIES ws2_32)
endif()
if(GRPC_ZLIB_ENABLED)
  list(APPEND GRPC_LINK_LIBRARIES "${GRPC_ZLIB_TARGET}")
endif()
if(GRPC_ZSTD_ENABLED)
  list(APPEND GRPC_LINK_LIBRARIES "${GRPC_ZSTD_TARGET}")
endif()
if(GRPC_SNAPPY_ENABLED)
  list(APPEND GRPC_LINK_LIBRARIES "${GRPC_SNAPPY_TARGET}")
endif()
if(GRPC_PROTOBUF_C_ENABLED)
  list(APPEND GRPC_LINK_LIBRARIES "${GRPC_PROTOBUF_C_TARGET}")
endif()
if(GRPC_FLATCC_ENABLED)
  list(APPEND GRPC_LINK_LIBRARIES "${FLATCC_LIBRARY}")
endif()
if(GRPC_HTTP3_ENABLED)
  # the TLS libraries again after the crypto helper, which may be a static archive using them
  list(APPEND GRPC_LINK_LIBRARIES ${GRPC_HTTP3_TARGETS} ${GRPC_TLS_LIBRARIES})
endif()
target_link_libraries(grpc_objects PRIVATE ${GRPC_LINK_LIBRARIES})
# Installed consumers of the static library also need ulog and every dependency; the shared
# library records its dependencies itself.
grpc_resolve_link_files(GRPC_INSTALL_LINK_FILES ${GRPC_LINK_LIBRARIES})
# Bundled dependency archives are installed next to the library, in <libdir>/ezgrpc-next, so
# their install-interface paths point there instead of into this build tree.
set(GRPC_BUNDLED_INSTALL_FILES)
set(GRPC_INSTALL_LINK_ITEMS)
foreach(item IN LISTS GRPC_INSTALL_LINK_FILES)
  string(FIND "${item}" "${GRPC_BUNDLED_DIR}/install/" _grpc_bundled_pos)
  if(_grpc_bundled_pos EQUAL 0)
    set(_grpc_item_in_bundled TRUE)
  else()
    set(_grpc_item_in_bundled FALSE)
  endif()
  if(item STREQUAL "Threads::Threads")
    list(APPEND GRPC_INSTALL_LINK_ITEMS "${item}")
  elseif(IS_ABSOLUTE "${item}" AND _grpc_item_in_bundled)
    get_filename_component(name "${item}" NAME)
    list(APPEND GRPC_BUNDLED_INSTALL_FILES "${item}")
    list(APPEND GRPC_INSTALL_LINK_ITEMS "@bundled@/${name}")
    target_link_libraries(grpc_static PUBLIC
      "$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/${CMAKE_INSTALL_LIBDIR}/ezgrpc-next/${name}>")
  else()
    list(APPEND GRPC_INSTALL_LINK_ITEMS "${item}")
    target_link_libraries(grpc_static PUBLIC "$<INSTALL_INTERFACE:${item}>")
  endif()
endforeach()
list(REMOVE_DUPLICATES GRPC_BUNDLED_INSTALL_FILES)
# the public grpc_protobuf_c.h includes <protobuf-c/protobuf-c.h>, so a bundled copy is installed
set(GRPC_BUNDLED_PUBLIC_HEADERS FALSE)
if(GRPC_PROTOBUF_C_ENABLED AND GRPC_PROTOBUF_C_TARGET MATCHES "^grpc_bundled_")
  set(GRPC_BUNDLED_PUBLIC_HEADERS TRUE)
  target_include_directories(grpc_static PUBLIC
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/ezgrpc-next>")
endif()
target_link_libraries(grpc_static PUBLIC $<INSTALL_INTERFACE:ulog>)
foreach(library_target IN LISTS GRPC_LIBRARY_TARGETS)
  target_link_libraries(${library_target} PUBLIC $<BUILD_INTERFACE:${GRPC_LINK_LIBRARIES}>)
  target_link_libraries(${library_target} PUBLIC $<INSTALL_INTERFACE:Threads::Threads>)
  target_include_directories(${library_target} PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${GRPC_GENERATED_INCLUDE_DIR}>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
  if(GRPC_FLATCC_ENABLED)
    target_include_directories(${library_target} PUBLIC $<BUILD_INTERFACE:${FLATCC_INCLUDE_DIR}>)
  endif()
  target_link_libraries(${library_target} PUBLIC grpc_build_options)
  target_compile_definitions(${library_target} PUBLIC ${GRPC_PUBLIC_COMPILE_DEFINITIONS})
endforeach()

set(_grpc_sanitizers ${GRPC_SANITIZE})
if(GRPC_ASAN)
  list(APPEND _grpc_sanitizers address)
endif()
list(REMOVE_DUPLICATES _grpc_sanitizers)
if(_grpc_sanitizers)
  if(MSVC)
    message(FATAL_ERROR "GRPC_SANITIZE / GRPC_ASAN are supported only with GCC/Clang")
  endif()
  if("thread" IN_LIST _grpc_sanitizers AND ("address" IN_LIST _grpc_sanitizers OR
                                            "memory" IN_LIST _grpc_sanitizers))
    message(FATAL_ERROR "the thread sanitizer cannot be combined with address or memory")
  endif()
  list(JOIN _grpc_sanitizers "," _grpc_sanitize_flag)
  add_library(grpc_sanitizers INTERFACE)
  target_compile_options(grpc_sanitizers INTERFACE
    -fsanitize=${_grpc_sanitize_flag} -fno-omit-frame-pointer)
  target_link_options(grpc_sanitizers INTERFACE -fsanitize=${_grpc_sanitize_flag})
  foreach(target IN LISTS GRPC_LIBRARY_TARGETS)
    target_link_libraries(${target} PUBLIC grpc_sanitizers)
  endforeach()
  target_link_libraries(grpc_objects PRIVATE grpc_sanitizers)
  target_link_libraries(ulog PRIVATE grpc_sanitizers)
endif()

set(GRPC_TEST_TARGETS)
set(GRPC_EXAMPLE_TARGETS)

# Generates a self-signed certificate for CN=localhost into <dir>/<cert> and <dir>/<key> with the
# host openssl command, behind a custom target <target>. The selected TLS prefix is not searched:
# AWS-LC's installed CLI can resolve against an incompatible system libssl at run time.
function(grpc_add_certificate target dir cert key)
  find_program(GRPC_HOST_OPENSSL_EXECUTABLE NAMES openssl REQUIRED NO_CMAKE_PATH)
  file(MAKE_DIRECTORY "${dir}")
  add_custom_command(OUTPUT "${dir}/${cert}" "${dir}/${key}"
    COMMAND "${GRPC_HOST_OPENSSL_EXECUTABLE}" req -batch -x509 -newkey rsa:2048 -nodes
      -keyout "${dir}/${key}" -out "${dir}/${cert}" -days 365 -subj /CN=localhost
    VERBATIM)
  add_custom_target(${target} DEPENDS "${dir}/${cert}" "${dir}/${key}")
endfunction()

if(GRPC_BUILD_TESTS)
  enable_testing()
  # grpc_add_unit_test(<name> <source> [LIBRARY grpc_static|grpc_shared] [SOURCES ...]
  #                    [INCLUDES ...] [DEFINITIONS ...] [WORKING_DIRECTORY dir] [DEPENDS target...])
  # Builds test_<name> from <source> (with the src/tests/test.h harness) and registers it as test <name>.
  function(grpc_add_unit_test name source)
    cmake_parse_arguments(arg "" "LIBRARY;WORKING_DIRECTORY" "SOURCES;INCLUDES;DEFINITIONS;DEPENDS"
                          ${ARGN})
    if(NOT arg_LIBRARY)
      set(arg_LIBRARY grpc_static)
    endif()
    add_executable(test_${name} "${source}" ${arg_SOURCES})
    target_include_directories(test_${name} PRIVATE src/tests include/internal ${arg_INCLUDES})
    target_link_libraries(test_${name} PRIVATE ${arg_LIBRARY})
    if(arg_DEFINITIONS)
      target_compile_definitions(test_${name} PRIVATE ${arg_DEFINITIONS})
    endif()
    if(arg_DEPENDS)
      add_dependencies(test_${name} ${arg_DEPENDS})
    endif()
    add_test(NAME ${name} COMMAND test_${name})
    if(arg_WORKING_DIRECTORY)
      set_tests_properties(${name} PROPERTIES WORKING_DIRECTORY "${arg_WORKING_DIRECTORY}")
    endif()
    set(GRPC_TEST_TARGETS ${GRPC_TEST_TARGETS} test_${name} PARENT_SCOPE)
  endfunction()

  set(grpc_unit_tests api msg_queue mem metadata timeout compression health interceptor status_details
                      reflection version client)
  # integration and oom exercise gzip throughout (as do the Python greeter tests below)
  if(GRPC_ZLIB_ENABLED)
    list(APPEND grpc_unit_tests integration oom)
  else()
    message(STATUS "ezGRPC-next: zlib is disabled; skipping the integration, oom and Python greeter tests")
  endif()
  foreach(name IN LISTS grpc_unit_tests)
    grpc_add_unit_test(${name} "src/tests/${name}.c")
  endforeach()
  if(NOT WIN32)
    # the client against a scripted raw HTTP/2 server
    grpc_add_unit_test(client_raw src/tests/client_raw.c)
  endif()
  if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT _grpc_sanitizers)
    # port 0 on IPv4 + IPv6 retries when IPv6 finds the port taken (a preloaded bind() shim
    # simulates that; sanitizer runtimes must come first in the preload order, so not with them)
    add_library(test_bind_v6_busy MODULE src/tests/shims/bind_v6_busy.c)
    target_link_libraries(test_bind_v6_busy PRIVATE ${CMAKE_DL_LIBS})
    grpc_add_unit_test(port_retry src/tests/port_retry.c DEPENDS test_bind_v6_busy)
    set_tests_properties(port_retry PROPERTIES
      ENVIRONMENT "LD_PRELOAD=$<TARGET_FILE:test_bind_v6_busy>")
  endif()
  target_compile_definitions(test_api PRIVATE "GRPC_LICENSE_FILE=\"${CMAKE_CURRENT_SOURCE_DIR}/LICENSE\"")

  if(GRPC_BUILD_SHARED)
    grpc_add_unit_test(version_shared src/tests/version.c LIBRARY grpc_shared)
    if(WIN32)
      # exercise the DLL import library as well as the default static target
      grpc_add_unit_test(mem_shared src/tests/mem.c LIBRARY grpc_shared)
    endif()
  endif()

  if(GRPC_PROTOBUF_C_ENABLED AND PROTOC_C_EXECUTABLE)
    set(proto_test_dir "${CMAKE_CURRENT_BINARY_DIR}/generated/protobuf-tests")
    file(MAKE_DIRECTORY "${proto_test_dir}")
    add_custom_command(OUTPUT "${proto_test_dir}/test_msg.pb-c.c" "${proto_test_dir}/test_msg.pb-c.h"
      COMMAND "${PROTOC_C_EXECUTABLE}" -I "${CMAKE_CURRENT_SOURCE_DIR}/src/tests"
        "--c_out=${proto_test_dir}" "${CMAKE_CURRENT_SOURCE_DIR}/src/tests/test_msg.proto"
      DEPENDS src/tests/test_msg.proto VERBATIM)
    grpc_add_unit_test(pb src/tests/pb.c
      SOURCES "${proto_test_dir}/test_msg.pb-c.c" INCLUDES "${proto_test_dir}")
  endif()

  if(GRPC_FLATCC_ENABLED)
    set(flat_test_dir "${CMAKE_CURRENT_BINARY_DIR}/generated/flatbuffers-tests")
    file(MAKE_DIRECTORY "${flat_test_dir}")
    set(flat_test_outputs)
    foreach(schema IN ITEMS test_msg test_noid)
      foreach(suffix IN ITEMS reader builder verifier)
        list(APPEND flat_test_outputs "${flat_test_dir}/${schema}_${suffix}.h")
      endforeach()
    endforeach()
    add_custom_command(OUTPUT ${flat_test_outputs}
      COMMAND "${FLATCC_EXECUTABLE}" -a "${CMAKE_CURRENT_SOURCE_DIR}/src/tests/test_msg.fbs"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/tests/test_noid.fbs"
      WORKING_DIRECTORY "${flat_test_dir}"
      DEPENDS src/tests/test_msg.fbs src/tests/test_noid.fbs VERBATIM)
    grpc_add_unit_test(fb src/tests/fb.c SOURCES ${flat_test_outputs} INCLUDES "${flat_test_dir}")
  endif()

  if(GRPC_TLS_ENABLED)
    set(tls_test_dir "${CMAKE_CURRENT_BINARY_DIR}/generated/tls-tests")
    grpc_add_certificate(test_tls_certificates "${tls_test_dir}" test_cert.pem test_key.pem)
    # A client certificate (CN=attacker) from a rogue CA that shares the trusted CA's name
    # (CN=localhost) but not its key: mTLS must reject it on every backend and transport.
    add_custom_command(OUTPUT "${tls_test_dir}/forged_cert.pem" "${tls_test_dir}/forged_key.pem"
      COMMAND "${GRPC_HOST_OPENSSL_EXECUTABLE}" req -batch -x509 -newkey rsa:2048 -nodes
        -keyout rogue_ca_key.pem -out rogue_ca_cert.pem -days 365 -subj /CN=localhost
      COMMAND "${GRPC_HOST_OPENSSL_EXECUTABLE}" req -batch -new -newkey rsa:2048 -nodes
        -keyout forged_key.pem -out forged.csr -subj /CN=attacker
      COMMAND "${GRPC_HOST_OPENSSL_EXECUTABLE}" x509 -req -in forged.csr -CA rogue_ca_cert.pem
        -CAkey rogue_ca_key.pem -set_serial 1 -days 365 -out forged_cert.pem
      WORKING_DIRECTORY "${tls_test_dir}" VERBATIM)
    add_custom_target(test_tls_forged_certificates
      DEPENDS "${tls_test_dir}/forged_cert.pem" "${tls_test_dir}/forged_key.pem")
    add_dependencies(test_tls_forged_certificates test_tls_certificates)
    grpc_add_unit_test(tls src/tests/tls.c
      DEFINITIONS "EXPECTED_TLS_BACKEND=\"${_tls_backend}\""
      WORKING_DIRECTORY "${tls_test_dir}" DEPENDS test_tls_certificates test_tls_forged_certificates)
    # backend-specific checks use the TLS library's own API (e.g. a GnuTLS client)
    target_link_libraries(test_tls PRIVATE ${GRPC_TLS_LIBRARIES})
    if(RUSTLS_INCLUDE_DIR)
      target_include_directories(test_tls PRIVATE "${RUSTLS_INCLUDE_DIR}")
    endif()
    if(GRPC_HTTP3_ENABLED)
      grpc_add_unit_test(http3 src/tests/http3.c
        WORKING_DIRECTORY "${tls_test_dir}" DEPENDS test_tls_certificates test_tls_forged_certificates)
      # HTTP/3 client allocation failures (one at a time, on the client's thread)
      grpc_add_unit_test(http3_oom src/tests/http3_oom.c
        WORKING_DIRECTORY "${tls_test_dir}" DEPENDS test_tls_certificates)
      if(NOT WIN32)
        # a raw ngtcp2 client (needs the QUIC libraries' headers) against the server
        grpc_add_unit_test(http3_raw src/tests/http3_raw.c
          WORKING_DIRECTORY "${tls_test_dir}" DEPENDS test_tls_certificates)
        target_link_libraries(test_http3_raw PRIVATE ${GRPC_HTTP3_TARGETS} ${GRPC_TLS_LIBRARIES})
      endif()
      if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT _grpc_sanitizers)
        # full UDP socket buffers, simulated by a preloaded sendmsg() shim (not with sanitizers,
        # whose runtimes must come first in the preload order)
        add_library(test_udp_eagain MODULE src/tests/shims/udp_eagain.c)
        target_link_libraries(test_udp_eagain PRIVATE ${CMAKE_DL_LIBS})
        grpc_add_unit_test(http3_eagain src/tests/http3_eagain.c
          WORKING_DIRECTORY "${tls_test_dir}" DEPENDS test_tls_certificates test_udp_eagain)
        target_link_libraries(test_http3_eagain PRIVATE ${GRPC_HTTP3_TARGETS})
        set_tests_properties(http3_eagain PROPERTIES
          ENVIRONMENT "LD_PRELOAD=$<TARGET_FILE:test_udp_eagain>")
      endif()
    endif()
  endif()
endif()

# grpc_add_integration_test(<name> SERVER <target> CLIENT <script> PORT <n> CWD <dir>
#                           [PYTHON_PATH dir] [ARGS ...])
# Runs scripts/run-integration-test.py: starts the example server, then the Python client.
function(grpc_add_integration_test name)
  cmake_parse_arguments(arg "" "SERVER;CLIENT;PORT;CWD;PYTHON_PATH" "ARGS" ${ARGN})
  set(extra ${arg_ARGS})
  if(arg_PYTHON_PATH)
    list(APPEND extra --python-path "${arg_PYTHON_PATH}")
  endif()
  add_test(NAME integration_${name}
    COMMAND "${GRPC_PYTHON_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/scripts/run-integration-test.py"
      --server "$<TARGET_FILE:${arg_SERVER}>" --client "${arg_CLIENT}"
      --port ${arg_PORT} --cwd "${arg_CWD}" ${extra})
endfunction()

if(GRPC_BUILD_EXAMPLES)
  add_executable(example_single_threaded examples/single_threaded.c)
  set_target_properties(example_single_threaded PROPERTIES OUTPUT_NAME single_threaded.bin)
  target_link_libraries(example_single_threaded PRIVATE grpc_static)
  add_executable(example_multi_threaded examples/multi_threaded.c)
  set_target_properties(example_multi_threaded PROPERTIES OUTPUT_NAME multi_threaded.bin)
  target_link_libraries(example_multi_threaded PRIVATE grpc_static)
  list(APPEND GRPC_EXAMPLE_TARGETS example_single_threaded example_multi_threaded)

  if(GRPC_PROTOBUF_C_ENABLED AND PROTOC_C_EXECUTABLE)
    set(proto_example_dir "${CMAKE_CURRENT_BINARY_DIR}/generated/protobuf-example")
    file(MAKE_DIRECTORY "${proto_example_dir}")
    add_custom_command(OUTPUT "${proto_example_dir}/helloworld.pb-c.c" "${proto_example_dir}/helloworld.pb-c.h"
      COMMAND "${PROTOC_C_EXECUTABLE}" -I "${CMAKE_CURRENT_SOURCE_DIR}/examples/python_client_c_server" "--c_out=${proto_example_dir}" "${CMAKE_CURRENT_SOURCE_DIR}/examples/python_client_c_server/helloworld.proto"
      DEPENDS examples/python_client_c_server/helloworld.proto VERBATIM)
    set(proto_python_dir "${CMAKE_CURRENT_BINARY_DIR}/generated/protobuf-python")
    file(MAKE_DIRECTORY "${proto_python_dir}")
    if(GRPC_BUILD_INTEGRATION_TESTS)
      set(proto_python_outputs "${proto_python_dir}/helloworld_pb2.py" "${proto_python_dir}/helloworld_pb2_grpc.py")
      add_custom_command(OUTPUT ${proto_python_outputs}
        COMMAND "${GRPC_PYTHON_EXECUTABLE}" -m grpc_tools.protoc
          -I "${CMAKE_CURRENT_SOURCE_DIR}/examples/python_client_c_server"
          "--python_out=${proto_python_dir}" "--grpc_python_out=${proto_python_dir}"
          "${CMAKE_CURRENT_SOURCE_DIR}/examples/python_client_c_server/helloworld.proto"
        DEPENDS examples/python_client_c_server/helloworld.proto VERBATIM)
      add_custom_target(example_python_proto DEPENDS ${proto_python_outputs})
    endif()
    add_executable(example_grpc_server examples/python_client_c_server/server.c "${proto_example_dir}/helloworld.pb-c.c")
    set_target_properties(example_grpc_server PROPERTIES OUTPUT_NAME grpc_server.bin)
    target_include_directories(example_grpc_server PRIVATE "${proto_example_dir}")
    target_link_libraries(example_grpc_server PRIVATE grpc_static)
    list(APPEND GRPC_EXAMPLE_TARGETS example_grpc_server)

    if(GRPC_TLS_ENABLED AND GRPC_BUILD_INTEGRATION_TESTS)
      grpc_add_certificate(example_tls_certificates "${proto_example_dir}" server.crt server.key)
      add_dependencies(example_grpc_server example_tls_certificates)
    endif()

    if(GRPC_BUILD_INTEGRATION_TESTS AND GRPC_ZLIB_ENABLED)
      add_dependencies(example_grpc_server example_python_proto)
      set(greeter_client "${CMAKE_CURRENT_SOURCE_DIR}/examples/python_client_c_server/client.py")
      grpc_add_integration_test(grpc_cleartext SERVER example_grpc_server CLIENT "${greeter_client}"
        PYTHON_PATH "${proto_python_dir}" PORT 19009 CWD "${proto_example_dir}")
      if(GRPC_TLS_ENABLED)
        grpc_add_integration_test(grpc_tls SERVER example_grpc_server CLIENT "${greeter_client}"
          PYTHON_PATH "${proto_python_dir}" PORT 19019 CWD "${proto_example_dir}" ARGS --tls)
        set_tests_properties(integration_grpc_tls PROPERTIES DEPENDS integration_grpc_cleartext)
      endif()
    endif()
  endif()

  if(GRPC_FLATCC_ENABLED)
    set(flat_example_dir "${CMAKE_CURRENT_BINARY_DIR}/generated/flatbuffers-example")
    file(MAKE_DIRECTORY "${flat_example_dir}")
    set(flat_example_outputs)
    foreach(suffix IN ITEMS reader builder verifier)
      list(APPEND flat_example_outputs "${flat_example_dir}/greeter_${suffix}.h")
    endforeach()
    add_custom_command(OUTPUT ${flat_example_outputs}
      COMMAND "${FLATCC_EXECUTABLE}" -a "${CMAKE_CURRENT_SOURCE_DIR}/examples/flatbuffers/greeter.fbs"
      WORKING_DIRECTORY "${flat_example_dir}"
      DEPENDS examples/flatbuffers/greeter.fbs VERBATIM)
    add_executable(example_flatbuffers_server examples/flatbuffers/server.c ${flat_example_outputs})
    set_target_properties(example_flatbuffers_server PROPERTIES OUTPUT_NAME flatbuffers_server.bin)
    target_include_directories(example_flatbuffers_server PRIVATE "${flat_example_dir}")
    target_link_libraries(example_flatbuffers_server PRIVATE grpc_static)
    list(APPEND GRPC_EXAMPLE_TARGETS example_flatbuffers_server)
    if(GRPC_BUILD_INTEGRATION_TESTS)
      grpc_add_integration_test(flatbuffers SERVER example_flatbuffers_server
        CLIENT "${CMAKE_CURRENT_SOURCE_DIR}/examples/flatbuffers/client.py"
        PYTHON_PATH "${CMAKE_CURRENT_SOURCE_DIR}/examples/flatbuffers" PORT 19029
        CWD "${flat_example_dir}")
    endif()
  endif()

  if(GRPC_HTTP3_ENABLED)
    # HTTP/3 server and client; the certificate pair for CN=localhost is generated next to them
    set(http3_example_dir "${CMAKE_CURRENT_BINARY_DIR}/generated/http3-example")
    grpc_add_certificate(example_http3_certificates "${http3_example_dir}" server.crt server.key)
    foreach(side IN ITEMS server client)
      add_executable(example_http3_${side} examples/http3/${side}.c)
      set_target_properties(example_http3_${side} PROPERTIES OUTPUT_NAME http3_${side}.bin)
      target_link_libraries(example_http3_${side} PRIVATE grpc_static)
      add_dependencies(example_http3_${side} example_http3_certificates)
      list(APPEND GRPC_EXAMPLE_TARGETS example_http3_${side})
    endforeach()
    # the runner needs only the Python standard library, so this runs wherever tests do
    if((GRPC_BUILD_TESTS OR GRPC_BUILD_INTEGRATION_TESTS) AND GRPC_PYTHON_EXECUTABLE)
      grpc_add_integration_test(http3_example SERVER example_http3_server
        CLIENT "$<TARGET_FILE:example_http3_client>" PORT 19051 CWD "${http3_example_dir}")
    endif()
  endif()

  # The OTLP metrics receiver (examples/otlp_metrics): protobuf-c bindings of the upstream
  # OpenTelemetry protos, generated with protoc-c.
  option(GRPC_EXAMPLE_OTLP_METRICS "Build the OTLP metrics receiver example" ON)
  if(GRPC_EXAMPLE_OTLP_METRICS AND GRPC_PROTOBUF_C_ENABLED AND PROTOC_C_EXECUTABLE)
    add_subdirectory(examples/otlp_metrics)
  elseif(GRPC_EXAMPLE_OTLP_METRICS)
    message(STATUS "ezGRPC-next: the OTLP metrics example needs protobuf-c and protoc-c; skipped")
  endif()

  if(GRPC_BUILD_INTEGRATION_TESTS)
    set(examples_client "${CMAKE_CURRENT_SOURCE_DIR}/examples/test_examples.py")
    grpc_add_integration_test(single_threaded SERVER example_single_threaded
      CLIENT "${examples_client}" PORT 19041 CWD "${CMAKE_CURRENT_BINARY_DIR}" ARGS --client-arg single)
    grpc_add_integration_test(multi_threaded SERVER example_multi_threaded
      CLIENT "${examples_client}" PORT 19042 CWD "${CMAKE_CURRENT_BINARY_DIR}" ARGS --client-arg multi)
  endif()
endif()

if(GRPC_BUILD_TESTS OR (GRPC_BUILD_EXAMPLES AND GRPC_BUILD_INTEGRATION_TESTS))
  get_property(grpc_registered_tests DIRECTORY PROPERTY TESTS)
  foreach(test IN LISTS grpc_registered_tests)
    if(test MATCHES "^integration_")
      set_tests_properties(${test} PROPERTIES LABELS integration TIMEOUT 120)
    else()
      set_tests_properties(${test} PROPERTIES LABELS unit TIMEOUT 30)
    endif()
    if("thread" IN_LIST _grpc_sanitizers)
      # ThreadSanitizer does not intercept io_uring_setup(), which libuv (1.45+) calls for every
      # loop: a descriptor number reused that way looks like a race with the thread that closed
      # it. Plain epoll keeps the reports meaningful.
      set_tests_properties(${test} PROPERTIES ENVIRONMENT "UV_USE_IO_URING=0")
    endif()
  endforeach()
  add_custom_target(grpc_check
    COMMAND "${CMAKE_CTEST_COMMAND}" -C "$<CONFIG>" --output-on-failure
    DEPENDS ${GRPC_TEST_TARGETS} ${GRPC_EXAMPLE_TARGETS}
    WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
    USES_TERMINAL VERBATIM)
  if(GRPC_IS_TOP_LEVEL)
    add_custom_target(check DEPENDS grpc_check)
  endif()
endif()

foreach(target IN ITEMS ulog grpc_objects ${GRPC_LIBRARY_TARGETS} ${GRPC_TEST_TARGETS} ${GRPC_EXAMPLE_TARGETS})
  grpc_enable_ipo(${target})
endforeach()

if(GRPC_BUILD_DEVELOPER_TARGETS)
  file(GLOB_RECURSE GRPC_FORMAT_FILES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/include/*.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/examples/*.c")
  list(FILTER GRPC_FORMAT_FILES EXCLUDE REGEX "_(reader|builder|verifier)\\.h$")
  find_program(CLANG_FORMAT_EXECUTABLE clang-format)
  if(CLANG_FORMAT_EXECUTABLE)
    add_custom_target(format COMMAND "${CLANG_FORMAT_EXECUTABLE}" -i ${GRPC_FORMAT_FILES} VERBATIM)
    add_custom_target(format-check COMMAND "${CLANG_FORMAT_EXECUTABLE}" --dry-run --Werror ${GRPC_FORMAT_FILES} VERBATIM)
  else()
    add_custom_target(format COMMAND "${CMAKE_COMMAND}" -E echo "Install clang-format to use this target" COMMAND "${CMAKE_COMMAND}" -E false)
    add_custom_target(format-check COMMAND "${CMAKE_COMMAND}" -E echo "Install clang-format to use this target" COMMAND "${CMAKE_COMMAND}" -E false)
  endif()
endif()

if(GRPC_INSTALL)
  include(CMakePackageConfigHelpers)
  set(GRPC_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/ezGRPC-next")
  set(GRPC_INSTALL_TARGETS ${GRPC_LIBRARY_TARGETS} grpc_build_options ulog)
  if(TARGET grpc_sanitizers)
    list(APPEND GRPC_INSTALL_TARGETS grpc_sanitizers)
  endif()
  install(TARGETS ${GRPC_INSTALL_TARGETS}
    EXPORT ezGRPC-nextTargets
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
  install(DIRECTORY include/
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
    FILES_MATCHING PATTERN "*.h"
    PATTERN "internal" EXCLUDE)
  install(FILES "${GRPC_GENERATED_INCLUDE_DIR}/grpc_version_config.h"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
  if(GRPC_BUNDLED_INSTALL_FILES)
    install(FILES ${GRPC_BUNDLED_INSTALL_FILES}
      DESTINATION "${CMAKE_INSTALL_LIBDIR}/ezgrpc-next")
  endif()
  if(GRPC_BUNDLED_PUBLIC_HEADERS)
    get_target_property(_pbc_include ${GRPC_PROTOBUF_C_TARGET} INTERFACE_INCLUDE_DIRECTORIES)
    install(DIRECTORY "${_pbc_include}/protobuf-c"
      DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/ezgrpc-next")
  endif()
  install(EXPORT ezGRPC-nextTargets
    FILE ezGRPC-nextTargets.cmake
    NAMESPACE grpc::
    DESTINATION "${GRPC_INSTALL_CMAKEDIR}")
  set(GRPC_ENABLED_FEATURES)
  foreach(feature IN ITEMS ZLIB ZSTD SNAPPY PROTOBUF_C FLATCC HTTP3)
    if(GRPC_${feature}_ENABLED)
      string(TOLOWER "${feature}" feature_name)
      list(APPEND GRPC_ENABLED_FEATURES "${feature_name}")
    endif()
  endforeach()
  if(NOT GRPC_HTTP3_ENABLED)
    set(GRPC_HTTP3_ENABLED OFF)
  endif()
  configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/ezGRPC-nextConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/ezGRPC-nextConfig.cmake"
    INSTALL_DESTINATION "${GRPC_INSTALL_CMAKEDIR}")
  write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/ezGRPC-nextConfigVersion.cmake"
    VERSION "${PROJECT_VERSION}"
    COMPATIBILITY SameMajorVersion)
  install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/ezGRPC-nextConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/ezGRPC-nextConfigVersion.cmake"
    DESTINATION "${GRPC_INSTALL_CMAKEDIR}")
  # pkg-config: the same dependency files for `--static`, and the feature defines in Cflags
  set(GRPC_PC_LIBS_PRIVATE "-lulog")
  foreach(item IN LISTS GRPC_INSTALL_LINK_ITEMS)
    if(item STREQUAL "Threads::Threads")
      string(APPEND GRPC_PC_LIBS_PRIVATE " -pthread")
    elseif(item MATCHES "^@bundled@/(.*)$")
      string(APPEND GRPC_PC_LIBS_PRIVATE " \${libdir}/ezgrpc-next/${CMAKE_MATCH_1}")
    elseif(IS_ABSOLUTE "${item}")
      string(APPEND GRPC_PC_LIBS_PRIVATE " ${item}")
    else()
      string(APPEND GRPC_PC_LIBS_PRIVATE " -l${item}")
    endif()
  endforeach()
  set(GRPC_PC_CFLAGS "")
  if(GRPC_BUNDLED_PUBLIC_HEADERS)
    string(APPEND GRPC_PC_CFLAGS " -I\${includedir}/ezgrpc-next")
  endif()
  foreach(def IN LISTS GRPC_PUBLIC_COMPILE_DEFINITIONS)
    if(NOT def MATCHES "^\\$<")
      string(APPEND GRPC_PC_CFLAGS " -D${def}")
    endif()
  endforeach()
  configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/ezgrpc-next.pc.in"
    "${CMAKE_CURRENT_BINARY_DIR}/ezgrpc-next.pc" @ONLY)
  install(FILES "${CMAKE_CURRENT_BINARY_DIR}/ezgrpc-next.pc"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig")
endif()

# ---- configuration summary ------------------------------------------------------------------
function(grpc_summary_flag out value)
  if(value)
    set(${out} "yes" PARENT_SCOPE)
  else()
    set(${out} "no" PARENT_SCOPE)
  endif()
endfunction()
foreach(feature IN ITEMS ZLIB ZSTD SNAPPY PROTOBUF_C FLATCC HTTP3 BUILD_SHARED BUILD_TESTS
                         BUILD_EXAMPLES BUILD_INTEGRATION_TESTS IPO)
  if(DEFINED GRPC_${feature}_ENABLED)
    grpc_summary_flag(_s_${feature} "${GRPC_${feature}_ENABLED}")
  else()
    grpc_summary_flag(_s_${feature} "${GRPC_${feature}}")
  endif()
endforeach()
set(_s_http3_detail "")
if(GRPC_HTTP3_ENABLED)
  set(_s_http3_detail " (ngtcp2_crypto_${GRPC_QUIC_CRYPTO})")
endif()
set(_s_sanitizers "none")
if(_grpc_sanitizers)
  list(JOIN _grpc_sanitizers "," _s_sanitizers)
endif()
set(_s_dependencies "")
foreach(_entry IN LISTS GRPC_DEPENDENCY_REPORT)
  string(REPLACE "|" ";" _entry "${_entry}")
  list(GET _entry 0 _dep_name)
  list(GET _entry 1 _dep_source)
  string(LENGTH "${_dep_name}" _dep_len)
  math(EXPR _pad "15 - ${_dep_len}")
  if(_pad LESS 1)
    set(_pad 1)
  endif()
  string(REPEAT " " ${_pad} _spaces)
  string(APPEND _s_dependencies "\n       ${_dep_name}${_spaces}${_dep_source}")
endforeach()
message(STATUS "ezGRPC-next ${PROJECT_VERSION} configuration:
     build type     ${CMAKE_BUILD_TYPE}
     TLS backend    ${_tls_backend}
     allocator      ${GRPC_ALLOCATOR}
     codecs         zlib=${_s_ZLIB} zstd=${_s_ZSTD} snappy=${_s_SNAPPY}
     serialization  protobuf-c=${_s_PROTOBUF_C} flatcc=${_s_FLATCC}
     HTTP/3         ${_s_HTTP3}${_s_http3_detail}
     shared library ${_s_BUILD_SHARED}   IPO ${_s_IPO}   sanitizers ${_s_sanitizers}
     tests          unit=${_s_BUILD_TESTS} integration=${_s_BUILD_INTEGRATION_TESTS} examples=${_s_BUILD_EXAMPLES}
     dependencies   ${GRPC_DEPENDENCIES} (GRPC_DEPENDENCIES)${_s_dependencies}")
