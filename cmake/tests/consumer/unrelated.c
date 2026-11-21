/* A target that does not link grpc: none of grpc's compile definitions or compatibility macros
 * may reach it through the parent directory. The build fails if one does. */
#ifdef _CRT_SECURE_NO_WARNINGS
#error grpc compiler definitions leaked to an unrelated target
#endif
#ifdef GRPC_ENABLE_TLS
#error grpc feature definitions leaked to an unrelated target
#endif
#if defined(nullptr) || defined(auto) || defined(alignof)
#error grpc C23 compatibility macros leaked to an unrelated target
#endif

int main(void) {
  return 0;
}
