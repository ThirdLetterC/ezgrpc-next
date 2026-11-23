# examples/otlp_metrics - an OTLP/gRPC metrics receiver on ezGRPC-next.
#
# Included from the top-level CMakeLists.txt when examples and protobuf-c are enabled. The C
# bindings are generated at build time from the official opentelemetry-proto release pinned below,
# downloaded and checked against its SHA-256 like the bundled dependencies (cached in
# GRPC_DEPS_CACHE_DIR). Set OTLP_PROTO_DIR to an extracted copy of that release to build offline.

set(OTLP_PROTO_VERSION 1.11.1)
set(OTLP_PROTO_COMMIT b3f75588eb23c5fca62264edd05d382de49beb1a)
set(OTLP_PROTO_SHA256 c0bdd2ff083665e0504a6574923ebe53c60c086ea11836c5aa543683f55a6c32)

option(OTLP_METRICS_WAL "Build the write-ahead-log sink of the OTLP metrics example" OFF)
set(OTLP_PROTO_DIR "" CACHE PATH
  "Extracted opentelemetry-proto ${OTLP_PROTO_VERSION} (default: download the pinned release)")

# ---- the protocol definitions ---------------------------------------------------------------
if(OTLP_PROTO_DIR)
  set(otlp_proto_root "${OTLP_PROTO_DIR}")
else()
  grpc_bundled_declare(opentelemetry-proto
    VERSION ${OTLP_PROTO_VERSION} FILE opentelemetry-proto-${OTLP_PROTO_VERSION}.tar.gz
    URLS
      "https://github.com/open-telemetry/opentelemetry-proto/archive/refs/tags/v${OTLP_PROTO_VERSION}.tar.gz=${OTLP_PROTO_SHA256}")
  _grpc_bundled_fetch(opentelemetry_proto otlp_proto_archive)
  if(NOT otlp_proto_archive)
    message(WARNING "otlp_metrics: ${otlp_proto_archive_ERROR}\nThe OTLP metrics example is skipped.")
    return()
  endif()
  set(otlp_proto_root "${CMAKE_CURRENT_BINARY_DIR}/opentelemetry-proto-${OTLP_PROTO_VERSION}")
  if(NOT EXISTS "${otlp_proto_root}/.extracted-${OTLP_PROTO_SHA256}")
    file(REMOVE_RECURSE "${otlp_proto_root}")
    file(ARCHIVE_EXTRACT INPUT "${otlp_proto_archive}" DESTINATION "${CMAKE_CURRENT_BINARY_DIR}")
    file(TOUCH "${otlp_proto_root}/.extracted-${OTLP_PROTO_SHA256}")
  endif()
endif()
set(OTLP_PROTO_FILES
  opentelemetry/proto/common/v1/common.proto
  opentelemetry/proto/resource/v1/resource.proto
  opentelemetry/proto/metrics/v1/metrics.proto
  opentelemetry/proto/collector/metrics/v1/metrics_service.proto)
foreach(proto IN LISTS OTLP_PROTO_FILES)
  if(NOT EXISTS "${otlp_proto_root}/${proto}")
    message(WARNING "otlp_metrics: ${otlp_proto_root}/${proto} is missing; the example is skipped")
    return()
  endif()
endforeach()

# ---- code generation --------------------------------------------------------------------------
# protoc-c runs as a protoc plugin when started under the name protoc-gen-c. The driver is protoc,
# or protoc-c itself (which embeds protoc); the plugin is reached through a shim that adds the
# proto3-optional feature flag protobuf-c does not report (see cmake/protoc_gen_c_shim.c).
get_filename_component(_otlp_pcc_dir "${PROTOC_C_EXECUTABLE}" DIRECTORY)
find_program(OTLP_PROTOC_GEN_C NAMES protoc-gen-c HINTS "${_otlp_pcc_dir}"
  DOC "protobuf-c code generator plugin")
if(NOT OTLP_PROTOC_GEN_C)
  # only the protoc-c front end is installed: give it the plugin name
  set(OTLP_PROTOC_GEN_C "${CMAKE_CURRENT_BINARY_DIR}/protoc-gen-c${CMAKE_EXECUTABLE_SUFFIX}")
  file(CREATE_LINK "${PROTOC_C_EXECUTABLE}" "${OTLP_PROTOC_GEN_C}" SYMBOLIC COPY_ON_ERROR)
endif()
find_program(OTLP_PROTOC NAMES protoc DOC "protoc, driving the protoc-c plugin")
if(NOT OTLP_PROTOC)
  set(OTLP_PROTOC "${PROTOC_C_EXECUTABLE}")
endif()

add_executable(otlp_protoc_gen_c_shim cmake/protoc_gen_c_shim.c)
target_compile_definitions(otlp_protoc_gen_c_shim PRIVATE
  "OTLP_REAL_PLUGIN=\"${OTLP_PROTOC_GEN_C}\"" "$<$<C_COMPILER_ID:MSVC>:_CRT_SECURE_NO_WARNINGS>")
target_link_libraries(otlp_protoc_gen_c_shim PRIVATE grpc_build_options)

set(otlp_gen_dir "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(otlp_gen_sources)
set(otlp_gen_headers)
set(otlp_proto_paths)
foreach(proto IN LISTS OTLP_PROTO_FILES)
  string(REGEX REPLACE "\\.proto$" "" stem "${proto}")
  list(APPEND otlp_gen_sources "${otlp_gen_dir}/${stem}.pb-c.c")
  list(APPEND otlp_gen_headers "${otlp_gen_dir}/${stem}.pb-c.h")
  list(APPEND otlp_proto_paths "${otlp_proto_root}/${proto}")
endforeach()
add_custom_command(OUTPUT ${otlp_gen_sources} ${otlp_gen_headers}
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${otlp_gen_dir}"
  COMMAND "${OTLP_PROTOC}" -I "${otlp_proto_root}"
    "--plugin=protoc-gen-otlpc=$<TARGET_FILE:otlp_protoc_gen_c_shim>"
    "--otlpc_out=${otlp_gen_dir}" ${OTLP_PROTO_FILES}
  DEPENDS ${otlp_proto_paths} otlp_protoc_gen_c_shim
  WORKING_DIRECTORY "${otlp_proto_root}"
  COMMENT "Generating protobuf-c bindings for opentelemetry-proto ${OTLP_PROTO_VERSION}"
  VERBATIM)

# generated code: built without the stricter warnings
add_library(otlp_metrics_proto STATIC ${otlp_gen_sources})
# SYSTEM: the generated headers are not held to the receiver's warning flags
target_include_directories(otlp_metrics_proto SYSTEM PUBLIC "${otlp_gen_dir}")
target_link_libraries(otlp_metrics_proto PUBLIC grpc_static)

# ---- the receiver -----------------------------------------------------------------------------
set(otlp_core_sources
  src/otlp_app.c
  src/otlp_arena.c
  src/otlp_budget.c
  src/otlp_queue.c
  src/otlp_response.c
  src/otlp_service.c
  src/otlp_sink_basic.c
  src/otlp_sink_fault.c
  src/otlp_stats.c
  src/otlp_strbuf.c
  src/otlp_validate.c
  src/otlp_wire.c)
if(OTLP_METRICS_WAL)
  list(APPEND otlp_core_sources src/otlp_sink_wal.c src/otlp_wal_format.c)
endif()
add_library(otlp_metrics_core STATIC ${otlp_core_sources})
target_include_directories(otlp_metrics_core PUBLIC src)
target_link_libraries(otlp_metrics_core PUBLIC otlp_metrics_proto grpc_static)
if(NOT MSVC)
  target_compile_options(otlp_metrics_core PRIVATE ${GRPC_WARNING_FLAGS} ${GRPC_STRICT_WARNING_FLAGS})
  target_link_libraries(otlp_metrics_core PUBLIC m)
endif()
if(OTLP_METRICS_WAL)
  target_compile_definitions(otlp_metrics_core PUBLIC OTLP_METRICS_WAL)
endif()
if(NOT GRPC_HAS_C23_CONSTEXPR)
  target_compile_definitions(otlp_metrics_core PRIVATE constexpr=const)
endif()

add_executable(otlp_metrics_server src/main.c)
target_link_libraries(otlp_metrics_server PRIVATE otlp_metrics_core)
set_target_properties(otlp_metrics_server PROPERTIES OUTPUT_NAME otlp_metrics_server
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")

set(otlp_example_targets otlp_metrics_server)
set(otlp_test_targets)

# ---- tests ------------------------------------------------------------------------------------
if(GRPC_BUILD_TESTS)
  # otlp_add_test(<name> <source> [SOURCES ...]): an executable using src/tests/test.h
  function(otlp_add_test name source)
    cmake_parse_arguments(arg "" "" "SOURCES" ${ARGN})
    add_executable(otlp_${name} "${source}" ${arg_SOURCES})
    target_include_directories(otlp_${name} PRIVATE "${PROJECT_SOURCE_DIR}/src/tests" tests)
    target_link_libraries(otlp_${name} PRIVATE otlp_metrics_core)
    add_test(NAME otlp_${name} COMMAND otlp_${name})
    set_tests_properties(otlp_${name} PROPERTIES LABELS "unit;otlp" TIMEOUT 120)
    set(otlp_test_targets ${otlp_test_targets} otlp_${name} PARENT_SCOPE)
  endfunction()
  otlp_add_test(unit tests/unit_tests.c SOURCES tests/otlp_fixtures.c)
  otlp_add_test(server tests/server_tests.c SOURCES tests/otlp_fixtures.c)
endif()

# ---- integration tests (official OpenTelemetry SDK exporters) --------------------------------
# Python: an interpreter with integration/requirements.txt installed. OTLP_PYTHON_EXECUTABLE names
# one; otherwise the interpreters the build found are tried, and with bundled dependencies a
# virtual environment is created in the build directory (like the repository's Python tests).
set(OTLP_PYTHON_EXECUTABLE "" CACHE FILEPATH
  "Python with examples/otlp_metrics/integration/requirements.txt installed")
set(otlp_python "")
set(otlp_py_modules opentelemetry.sdk.metrics opentelemetry.exporter.otlp.proto.grpc.metric_exporter
  opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 google.rpc.error_details_pb2)
if(GRPC_BUILD_INTEGRATION_TESTS OR GRPC_BUILD_TESTS)
  foreach(py IN ITEMS "${OTLP_PYTHON_EXECUTABLE}" "${GRPC_PYTHON_EXECUTABLE}")
    if(py AND EXISTS "${py}")
      _grpc_python_has(ready "${py}" ${otlp_py_modules})
      if(ready)
        set(otlp_python "${py}")
        break()
      endif()
    endif()
  endforeach()
  grpc_dependency_is_bundled(otlp_py_bundled python)
  if(NOT otlp_python AND otlp_py_bundled AND GRPC_PYTHON_EXECUTABLE)
    set(venv "${FETCHCONTENT_BASE_DIR}/otlp-python-venv")
    set(requirements "${CMAKE_CURRENT_SOURCE_DIR}/integration/requirements.txt")
    if(WIN32)
      set(venv_python "${venv}/Scripts/python.exe")
    else()
      set(venv_python "${venv}/bin/python")
    endif()
    file(SHA256 "${requirements}" req_hash)
    set(stamp "${venv}/.otlp-stamp")
    set(fresh FALSE)
    if(EXISTS "${stamp}")
      file(READ "${stamp}" previous)
      if(previous STREQUAL req_hash)
        set(fresh TRUE)
      endif()
    endif()
    if(NOT fresh)
      message(STATUS "otlp_metrics: installing the OpenTelemetry Python SDK into ${venv}")
      set(log "${CMAKE_CURRENT_BINARY_DIR}/otlp-python-venv.log")
      find_program(GRPC_UV_EXECUTABLE uv)
      # every interpreter the build found, then uv: some Pythons lack ensurepip
      set(makers)
      foreach(py IN LISTS _grpc_py_candidates GRPC_PYTHON_EXECUTABLE)
        list(APPEND makers "${py}|-m|venv")
      endforeach()
      if(GRPC_UV_EXECUTABLE)
        list(APPEND makers "${GRPC_UV_EXECUTABLE}|venv|--seed|--python|${GRPC_PYTHON_EXECUTABLE}")
      endif()
      foreach(maker IN LISTS makers)
        string(REPLACE "|" ";" maker "${maker}")
        file(REMOVE_RECURSE "${venv}")
        execute_process(COMMAND ${maker} "${venv}"
          RESULT_VARIABLE r1 OUTPUT_FILE "${log}" ERROR_FILE "${log}")
        if(NOT r1 EQUAL 0 OR NOT EXISTS "${venv_python}")
          continue()
        endif()
        execute_process(COMMAND "${venv_python}" -m pip install --disable-pip-version-check
          --quiet -r "${requirements}" RESULT_VARIABLE r2 OUTPUT_FILE "${log}" ERROR_FILE "${log}")
        if(r2 EQUAL 0)
          file(WRITE "${stamp}" "${req_hash}")
          break()
        endif()
      endforeach()
    endif()
    if(EXISTS "${venv_python}")
      _grpc_python_has(ready "${venv_python}" ${otlp_py_modules})
      if(ready)
        set(otlp_python "${venv_python}")
      endif()
    endif()
  endif()
  if(otlp_python)
    message(STATUS "otlp_metrics: Python integration tests use ${otlp_python}")
    add_test(NAME otlp_integration_python
      COMMAND "${otlp_python}" "${CMAKE_CURRENT_SOURCE_DIR}/integration/python_sdk_test.py")
    set_tests_properties(otlp_integration_python PROPERTIES LABELS "integration;otlp" TIMEOUT 300
      ENVIRONMENT "OTLP_SERVER=$<TARGET_FILE:otlp_metrics_server>")
  else()
    message(STATUS "otlp_metrics: no Python with the OpenTelemetry SDK; the Python integration "
      "test is skipped (pip install -r ${CMAKE_CURRENT_SOURCE_DIR}/integration/requirements.txt "
      "and set OTLP_PYTHON_EXECUTABLE)")
  endif()
endif()

if("thread" IN_LIST _grpc_sanitizers)
  # see the top-level CMakeLists.txt: ThreadSanitizer does not intercept io_uring
  get_property(otlp_tests DIRECTORY PROPERTY TESTS)
  foreach(test IN LISTS otlp_tests)
    set_property(TEST ${test} APPEND PROPERTY ENVIRONMENT "UV_USE_IO_URING=0")
  endforeach()
endif()

foreach(target IN LISTS otlp_example_targets otlp_test_targets)
  grpc_enable_ipo(${target})
endforeach()
grpc_enable_ipo(otlp_metrics_core)
set(GRPC_EXAMPLE_TARGETS ${GRPC_EXAMPLE_TARGETS} ${otlp_example_targets} PARENT_SCOPE)
set(GRPC_TEST_TARGETS ${GRPC_TEST_TARGETS} ${otlp_test_targets} PARENT_SCOPE)
