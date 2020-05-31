include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../)

set(LLVM_LINK_COMPONENTS
  LineEditor
  Support
  )

add_clang_executable(dbindexp
  Dbindexp.cpp
  )

clang_target_link_libraries(dbindexp
  PRIVATE
  clangBasic
  )
target_link_libraries(dbindexp
  PRIVATE
  clangDaemon
  )
