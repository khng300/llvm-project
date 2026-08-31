//===--- Serialization.h - Symbol database ----------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_LMDB_INDEX_SERIALIZATION_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_LMDB_INDEX_SERIALIZATION_H

#include "DocumentStore.h"

#include "cista/containers/optional.h"
#include "index/Ref.h"
#include "index/Symbol.h"

#include "Cista.h"
#include <type_traits>

namespace clang {
namespace clangd {
namespace lmdb_index {

using namespace llvm::support;

typedef uint64_t OID;

// NOLINTBEGIN(readability-identifier-naming)
constexpr OID OID_None = 0;
// NOLINTEND(readability-identifier-naming)

namespace cl {

namespace data = ::cista::offset;

template <typename T> uint64_t hashObject(T &&Obj) {
  return cista::hashing<std::remove_const_t<std::remove_reference_t<T>>>()(Obj);
}

struct SymbolID : data::array<uint8_t, clangd::SymbolID::RawSize> {
  bool isNull() const { return *this == SymbolID{}; }
  explicit operator bool() const { return !isNull(); }
};

struct FileDigest : data::array<uint8_t, sizeof(uint64_t)> {};

struct SymbolLocation {
  struct Position {
    static constexpr unsigned ColumnBits =
        clangd::SymbolLocation::Position::ColumnBits;
    static constexpr uint32_t MaxLine =
        clangd::SymbolLocation::Position::MaxLine;
    static constexpr uint32_t MaxColumn =
        clangd::SymbolLocation::Position::MaxColumn;

    uint32_t LineColumnPacked = 0;

    void setLine(uint32_t L) {
      if (L > MaxLine)
        L = MaxLine;
      LineColumnPacked = (L << ColumnBits) | column();
    }
    uint32_t line() const { return LineColumnPacked >> ColumnBits; }

    void setColumn(uint32_t Col) {
      if (Col > MaxColumn)
        Col = MaxColumn;
      LineColumnPacked = (LineColumnPacked & ~MaxColumn) | Col;
    }
    uint32_t column() const { return LineColumnPacked & MaxColumn; }
  };

  Position Start;
  Position End;
  OID FileOid = OID_None;

  explicit operator bool() const { return FileOid != OID_None; }
};

struct Symbol {
  struct IncludeHeaderWithReferences {
    data::cstring IncludeHeader;
    uint32_t References;
    clangd::Symbol::IncludeDirective SupportedDirectives;
  };

  SymbolID ID = {};
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

struct RevRef {
  SymbolLocation Location;
  RefKind Kind = RefKind::Unknown;
  SymbolID Symbol;
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

struct IndexDataFile {
  data::cstring ShardPath;
  data::cstring FileURI;
  uint64_t ShardPathHash;
  uint64_t FileURIHash;
  int64_t ModifiedTime = 0;
  bool Valid = false;
  cista::optional<CompileCommand> Cmd;
  cista::optional<IncludeGraph> Sources;
  data::hash_map<SymbolID, Symbol> Symbols;
  data::hash_map<SymbolID, data::vector<Ref>> Refs;
  data::hash_map<data::pair<SymbolID, RelationKind>, data::vector<Relation>>
      Relations;
  data::hash_map<SymbolID, data::vector<RevRef>> RevRefs;
};

struct IndexedSymbol {
  Symbol Symbol;
  data::vector<uint64_t> Tokens;
};

} // namespace cl

template <typename ObjectType, typename = void> struct Serialize {
  std::string operator()(ObjectType &&Obj) = delete;
};

template <typename ObjectType> std::string serializeObject(ObjectType &&Obj) {
  return Serialize<std::remove_cv_t<std::remove_reference_t<ObjectType>>>()(
      Obj);
}

template <typename ObjectType> struct Serialize<ObjectType> {
  std::string operator()(const ObjectType &Obj) {
    auto Buf = cista::serialize<cista::mode::NONE>(Obj);
    return std::string(reinterpret_cast<const char *>(Buf.data()), Buf.size());
  };
};

} // namespace lmdb_index
} // namespace clangd
} // namespace clang

#endif
