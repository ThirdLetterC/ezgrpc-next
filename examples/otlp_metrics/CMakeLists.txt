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
# proto3 `optional` needs libprotoc 3.12 or newer, and until 3.15 an explicit opt-in flag
# (Ubuntu 22.04 ships 3.12.4). protoc-c's own front end reports the libprotoc it embeds.
execute_process(COMMAND "${OTLP_PROTOC}" --version OUTPUT_VARIABLE otlp_protoc_version
  ERROR_VARIABLE otlp_protoc_version RESULT_VARIABLE otlp_protoc_result)
string(REGEX MATCH "libprotoc ([0-9]+\\.[0-9]+(\\.[0-9]+)?)" _ "${otlp_protoc_version}")
set(otlp_protoc_version "${CMAKE_MATCH_1}")
set(otlp_protoc_flags)
if(otlp_protoc_version AND otlp_protoc_version VERSION_LESS 3.12)
  message(WARNING "otlp_metrics: ${OTLP_PROTOC} is libprotoc ${otlp_protoc_version}; proto3 "
    "optional fields need 3.12 or newer. The OTLP metrics example is skipped.")
  return()
elseif(otlp_protoc_version AND otlp_protoc_version VERSION_LESS 3.15)
  list(APPEND otlp_protoc_flags --experimental_allow_proto3_optional)
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
  COMMAND "${OTLP_PROTOC}" ${otlp_protoc_flags} -I "${otlp_proto_root}"
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
  if(OTLP_METRICS_WAL)
    otlp_add_test(wal tests/wal_tests.c)
  endif()
endif()

# deterministic request generator: benchmark workloads and fuzz seeds
add_executable(otlp_gen tools/otlp_gen.c)
target_link_libraries(otlp_gen PRIVATE otlp_metrics_core)
set_target_properties(otlp_gen PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
list(APPEND otlp_example_targets otlp_gen)

# ---- fuzzing ----------------------------------------------------------------------------------
# tests/fuzz_export.c: replayed over a generated seed corpus and tests/fuzz-regressions in every
# build; with -DOTLP_METRICS_FUZZ=ON and Clang also built as a libFuzzer target (otlp_fuzz_export).
option(OTLP_METRICS_FUZZ "Build the libFuzzer target of the OTLP metrics example (Clang)" OFF)
set(otlp_corpus_dir "${CMAKE_CURRENT_BINARY_DIR}/fuzz-corpus")
# "<name>|<otlp_gen arguments separated by |>"
set(otlp_seed_args
  "seed-default"
  "seed-invalid|--invalid-every|3"
  "seed-summary|--kinds|summary|--points|3"
  "seed-wide|--resources|2|--scopes|2|--metrics|3|--points|2"
  "seed-hist|--kinds|histogram,exp|--buckets|3|--points|2|--invalid-every|2")
set(otlp_seeds)
foreach(entry IN LISTS otlp_seed_args)
  string(REPLACE "|" ";" entry "${entry}")
  list(POP_FRONT entry name)
  list(APPEND otlp_seeds "${otlp_corpus_dir}/${name}.bin")
  add_custom_command(OUTPUT "${otlp_corpus_dir}/${name}.bin"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${otlp_corpus_dir}"
    COMMAND otlp_gen --out "${otlp_corpus_dir}/${name}.bin" ${entry}
    DEPENDS otlp_gen VERBATIM)
endforeach()
add_custom_target(otlp_fuzz_corpus DEPENDS ${otlp_seeds})

if(GRPC_BUILD_TESTS)
  add_executable(otlp_fuzz_replay tests/fuzz_export.c)
  target_link_libraries(otlp_fuzz_replay PRIVATE otlp_metrics_core)
  add_dependencies(otlp_fuzz_replay otlp_fuzz_corpus)
  add_test(NAME otlp_fuzz_replay COMMAND otlp_fuzz_replay "${otlp_corpus_dir}"
    "${CMAKE_CURRENT_SOURCE_DIR}/tests/fuzz-regressions")
  set_tests_properties(otlp_fuzz_replay PROPERTIES LABELS "unit;otlp" TIMEOUT 120)
  list(APPEND otlp_test_targets otlp_fuzz_replay)
endif()

if(OTLP_METRICS_FUZZ)
  if(NOT CMAKE_C_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "OTLP_METRICS_FUZZ needs Clang (libFuzzer)")
  endif()
  # protobuf-c's decoder is compiled into the target when its sources are at hand (bundled), so
  # coverage feedback reaches it; otherwise the installed runtime is used uninstrumented
  # coverage feedback from the receiver's code and the generated descriptors too (configure with
  # GRPC_SANITIZE=address;undefined so every target links the sanitizer runtimes)
  target_compile_options(otlp_metrics_core PRIVATE -fsanitize=fuzzer-no-link)
  target_compile_options(otlp_metrics_proto PRIVATE -fsanitize=fuzzer-no-link)
  file(GLOB otlp_pbc_source "${FETCHCONTENT_BASE_DIR}/bundled/src/protobuf_c-*/protobuf-c/protobuf-c.c")
  add_executable(otlp_fuzz_export tests/fuzz_export.c ${otlp_pbc_source})
  target_compile_definitions(otlp_fuzz_export PRIVATE OTLP_LIBFUZZER)
  target_compile_options(otlp_fuzz_export PRIVATE -fsanitize=fuzzer,address,undefined
    -fno-sanitize-recover=undefined)
  if(otlp_pbc_source)
    # Two UBSan checks are off for protobuf-c's own source (both are protobuf-c 1.5.2 patterns,
    # outside this example; every check stays fatal for the receiver's code):
    #  - function: the runtime calls each generated <message>__init(T *) through a generic
    #    void (*)(ProtobufCMessage *) pointer (its descriptor design);
    #  - nonnull-attribute: parse_packed_repeated_member() calls memcpy(NULL, p, 0) for an empty
    #    packed repeated field (found by this fuzzer; see tests/fuzz-regressions/README.txt).
    # Source options come after the library's interface -fsanitize=undefined, so these stick.
    set_source_files_properties(${otlp_pbc_source} PROPERTIES
      COMPILE_OPTIONS "-fno-sanitize=function;-fno-sanitize=nonnull-attribute")
  endif()
  target_link_options(otlp_fuzz_export PRIVATE -fsanitize=fuzzer,address,undefined)
  target_link_libraries(otlp_fuzz_export PRIVATE otlp_metrics_core)
  add_dependencies(otlp_fuzz_export otlp_fuzz_corpus)
  list(APPEND otlp_test_targets otlp_fuzz_export)
endif()

if(OTLP_METRICS_WAL)
  # verifies a log and exports its batches as one ExportMetricsServiceRequest
  add_executable(otlp_wal_dump tools/otlp_wal_dump.c)
  target_link_libraries(otlp_wal_dump PRIVATE otlp_metrics_core)
  set_target_properties(otlp_wal_dump PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
  list(APPEND otlp_example_targets otlp_wal_dump)
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
    if(OTLP_METRICS_WAL)
      add_test(NAME otlp_integration_wal
        COMMAND "${otlp_python}" "${CMAKE_CURRENT_SOURCE_DIR}/integration/wal_test.py")
      set_tests_properties(otlp_integration_wal PROPERTIES LABELS "integration;otlp" TIMEOUT 300
        ENVIRONMENT
          "OTLP_SERVER=$<TARGET_FILE:otlp_metrics_server>;OTLP_WAL_DUMP=$<TARGET_FILE:otlp_wal_dump>")
    endif()
  else()
    message(STATUS "otlp_metrics: no Python with the OpenTelemetry SDK; the Python integration "
      "test is skipped (pip install -r ${CMAKE_CURRENT_SOURCE_DIR}/integration/requirements.txt "
      "and set OTLP_PYTHON_EXECUTABLE)")
  endif()
endif()

# Go: the official OpenTelemetry Go SDK exporter tests, and the grpc-go comparison server used by
# the benchmarks (bench/). Modules are pinned by go.mod/go.sum and downloaded by the Go toolchain.
find_program(OTLP_GO_EXECUTABLE go DOC "Go toolchain for the OTLP Go tests and comparison server")
set(otlp_go_dir "${CMAKE_CURRENT_SOURCE_DIR}/go")
if(OTLP_GO_EXECUTABLE)
  set(otlp_grpcgo_server "${CMAKE_BINARY_DIR}/grpcgo-server${CMAKE_EXECUTABLE_SUFFIX}")
  add_custom_target(otlp_grpcgo_server
    COMMAND "${OTLP_GO_EXECUTABLE}" build -trimpath -o "${otlp_grpcgo_server}" ./cmd/grpcgo-server
    WORKING_DIRECTORY "${otlp_go_dir}"
    BYPRODUCTS "${otlp_grpcgo_server}"
    COMMENT "Building the grpc-go comparison server"
    VERBATIM)
  if(GRPC_BUILD_INTEGRATION_TESTS OR GRPC_BUILD_TESTS)
    add_test(NAME otlp_integration_go
      COMMAND "${OTLP_GO_EXECUTABLE}" test -count=1 -v ./sdktest
      WORKING_DIRECTORY "${otlp_go_dir}")
    set_tests_properties(otlp_integration_go PROPERTIES LABELS "integration;otlp" TIMEOUT 600
      ENVIRONMENT
        "OTLP_SERVER=$<TARGET_FILE:otlp_metrics_server>;GRPCGO_SERVER=${otlp_grpcgo_server}")
    # the comparison server is built by `cmake --build <dir> --target otlp_grpcgo_server`, or by
    # this fixture before the Go tests
    add_test(NAME otlp_build_grpcgo_server
      COMMAND "${CMAKE_COMMAND}" --build "${CMAKE_BINARY_DIR}" --target otlp_grpcgo_server)
    set_tests_properties(otlp_build_grpcgo_server PROPERTIES LABELS "integration;otlp"
      FIXTURES_SETUP otlp_grpcgo TIMEOUT 600)
    set_tests_properties(otlp_integration_go PROPERTIES FIXTURES_REQUIRED otlp_grpcgo)
  endif()
else()
  message(STATUS "otlp_metrics: Go not found; the Go SDK tests and the grpc-go comparison server are skipped")
endif()

# The OpenTelemetry Collector test needs a collector binary (core distribution "otelcol"); see
# integration/fetch-otelcol.sh. Without one it reports itself skipped.
set(OTLP_OTELCOL_EXECUTABLE "" CACHE FILEPATH "otelcol binary for the collector integration test")
if(otlp_python)
  add_test(NAME otlp_integration_collector
    COMMAND "${otlp_python}" "${CMAKE_CURRENT_SOURCE_DIR}/integration/collector_test.py")
  set_tests_properties(otlp_integration_collector PROPERTIES LABELS "integration;otlp"
    TIMEOUT 300 SKIP_RETURN_CODE 77
    ENVIRONMENT
      "OTLP_SERVER=$<TARGET_FILE:otlp_metrics_server>;OTLP_OTELCOL=${OTLP_OTELCOL_EXECUTABLE}")
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
