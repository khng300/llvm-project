//===--- Serialization.h - Symbol database ----------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_SERIALIZATION_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_SERIALIZATION_H

#include "IndexDB.h"
#include "LMDBEngine.h"
#include "Token.h"

#include "cista/containers/optional.h"
#include "index/Ref.h"
#include "index/Symbol.h"

#include "Cista.h"
#include <type_traits>

namespace clang {
namespace clangd {
namespace db_index {

using namespace llvm::support;

namespace cl {

namespace data = ::cista::offset;

struct SymbolID : data::array<uint8_t, clangd::SymbolID::RawSize> {};

struct FileDigest : data::array<uint8_t, sizeof(uint64_t)> {};

template <typename T> uint64_t hashObject(T &&Obj) {
  return cista::hashing<std::remove_const_t<std::remove_reference_t<T>>>()(Obj);
}

struct SymbolLocation {
  struct Position {
    uint32_t Line;
    uint32_t Column;
  };
  Position Start;
  Position End;
  data::cstring FileURI;

  explicit operator bool() const { return !FileURI.empty(); }
};

struct Symbol {
  struct IncludeHeaderWithReferences {
    data::cstring IncludeHeader;
    uint32_t References;
    clangd::Symbol::IncludeDirective SupportedDirectives;
  };

  SymbolID ID;
  index::SymbolInfo SymInfo;
  data::cstring Name;
  data::cstring Scope;
  SymbolLocation Definition;
  SymbolLocation CanonicalDeclaration;
  uint32_t References;
  SymbolOrigin Origin = SymbolOrigin::Unknown;
  data::cstring Signature;
  data::cstring TemplateSpecializationArgs;
  data::cstring CompletionSnippetSuffix;
  data::cstring Documentation;
  data::cstring ReturnType;
  data::cstring Type;
  data::vector<IncludeHeaderWithReferences> IncludeHeaders;
  clangd::Symbol::SymbolFlag Flags = clangd::Symbol::SymbolFlag::None;
};

struct Ref {
  SymbolLocation Location;
  RefKind Kind = RefKind::Unknown;
  SymbolID Container;
};

struct Relation {
  SymbolID Subject;
  RelationKind Predicate;
  SymbolID Object;
};

struct IncludeGraphNode {
  clangd::IncludeGraphNode::SourceFlag Flags =
      clangd::IncludeGraphNode::SourceFlag::None;
  data::cstring URI;
  FileDigest Digest{{0}};
  data::vector<data::cstring> DirectIncludes;
};

using IncludeGraph = data::hash_map<data::cstring, IncludeGraphNode>;

struct CompileCommand {
  data::cstring Directory;
  data::cstring Filename;
  data::vector<data::cstring> CommandLine;
  data::cstring Output;
  data::cstring Heuristic;
};

struct IndexFileInfo {
  cista::optional<CompileCommand> Cmd;
  data::cstring ShardPath;
  data::cstring FileUri;
  int64_t ModifiedTime = 0;
  cista::optional<IncludeGraph> Sources;
  bool Valid = false;
};

struct IndexedSymbol {
  Symbol Symbol;
  data::vector<uint64_t> Tokens;
};

} // namespace cl

template <typename ObjectType> struct Serialize<ObjectType> {
  std::string operator()(const ObjectType &Obj) {
    auto Buf = cista::serialize<cista::mode::NONE>(Obj);
    return std::string(reinterpret_cast<const char *>(Buf.data()), Buf.size());
  };
};

} // namespace db_index
} // namespace clangd
} // namespace clang

#endif