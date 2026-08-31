set(lmdb_SRCDIR ${CMAKE_CURRENT_SOURCE_DIR}/lmdb/libraries/liblmdb)
add_clang_library(clangdLMDBSchema
  ${lmdb_SRCDIR}/mdb.c
  ${lmdb_SRCDIR}/midl.c)
target_include_directories(clangdLMDBSchema INTERFACE cista/include)
target_include_directories(clangdLMDBSchema SYSTEM BEFORE PUBLIC ${lmdb_SRCDIR})