Inputs found by fuzzing (tests/fuzz_export.c) kept as regression tests: otlp_fuzz_replay runs
every file in this directory (this README too, which is not a valid request and must simply be
refused) through the same invariant checks.

protobuf-c-empty-packed-{1,2}.pb
  Found by the libFuzzer campaign (clang, ASan+UBSan, protobuf-c 1.5.2 compiled into the
  target). A packed repeated field (double/fixed64 list) present with length 0 makes protobuf-c's
  parse_packed_repeated_member() call memcpy(NULL, at, 0): undefined in C17/C23 (well defined
  from C2y), harmless with every libc in practice. It is in protobuf-c (unfixed on its master
  branch at the time of writing), not in this example; the fuzz build turns off UBSan's
  nonnull-attribute check for protobuf-c's source only. The receiver handles these inputs
  correctly: they decode, validate and round-trip.
