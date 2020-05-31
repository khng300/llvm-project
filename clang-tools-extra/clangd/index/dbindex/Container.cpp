#include "Container.h"
#include "llvm/Support/Endian.h"
#include "llvm/Support/Process.h"
#include "llvm/Support/raw_ostream.h"

namespace clang {
namespace clangd {
namespace dbindex {
namespace container {

static constexpr char CONTAINER_MAGIC[4] = {'M', 'M', 'A', 'P'};

class Reader {
public:
  Reader(const void *Data, size_t N)
      : Begin(static_cast<const char *>(Data)),
        End(static_cast<const char *>(Data) + N) {}

  Reader(llvm::StringRef Data)
      : Begin(Data.data()), End(Data.data() + Data.size()) {}

  uint8_t consume8() {
    if (Begin == End) {
      Err = true;
      return 0;
    }
    return *Begin++;
  }

  uint16_t consume16() {
    if (Begin + 2 > End) {
      Err = true;
      return 0;
    }
    auto Ret = llvm::support::endian::read16le(Begin);
    Begin += 2;
    return Ret;
  }

  uint32_t consume32() {
    if (Begin + 4 > End) {
      Err = true;
      return 0;
    }
    auto Ret = llvm::support::endian::read32le(Begin);
    Begin += 4;
    return Ret;
  }

  uint64_t consume64() {
    if (Begin + 8 > End) {
      Err = true;
      return 0;
    }
    auto Ret = llvm::support::endian::read64le(Begin);
    Begin += 8;
    return Ret;
  }

  template <typename T> T consumeEnum() {
    using UT = typename std::underlying_type<T>::type;
    if (Begin + sizeof(UT) > End) {
      Err = true;
      return static_cast<T>(-1);
    }
    auto Ret =
        llvm::support::endian::read<T, llvm::support::endianness::little>(
            Begin);
    Begin += sizeof(UT);
    return static_cast<T>(Ret);
  }

  llvm::StringRef consumeFixedString(size_t N) {
    if (Begin + N > End) {
      Err = true;
      return llvm::StringRef();
    }
    auto Ret = llvm::StringRef(Begin, N);
    Begin += N;
    return Ret;
  }

  llvm::StringRef consumeCString() {
    const char *Pointer = Begin;
    if (Begin == End) {
      Err = true;
      return llvm::StringRef();
    }
    while (Begin != End && *Begin++)
      ;
    return llvm::StringRef(Pointer, Begin - Pointer).drop_back();
  }

  llvm::StringRef consumeGenericString() { return consumeCString(); }

  bool eof() const { return Begin == End || Err; }
  bool err() const { return Err; }
  size_t remaining() const { return End - Begin; }
  llvm::StringRef rest() const { return llvm::StringRef(Begin, End - Begin); }

private:
  const char *Begin, *End;
  bool Err = false;
};

class Writer {
public:
  Writer(llvm::raw_ostream &OS) : OS(OS) {}

  void write8(uint8_t I) { OS.write(I); }
  void write16(uint16_t I) {
    char Buf[2];
    llvm::support::endian::write16le(Buf, I);
    OS.write(Buf, sizeof(Buf));
  }
  void write32(uint32_t I) {
    char Buf[4];
    llvm::support::endian::write32le(Buf, I);
    OS.write(Buf, sizeof(Buf));
  }
  void write64(uint64_t I) {
    char Buf[8];
    llvm::support::endian::write64le(Buf, I);
    OS.write(Buf, sizeof(Buf));
  }
  template <typename T> void writeEnum(T I) {
    using UT = typename std::underlying_type<T>::type;
    char Buf[sizeof(UT)];
    llvm::support::endian::write<T, llvm::support::endianness::little>(Buf, I);
    OS.write(Buf, sizeof(Buf));
  }
  void writeFixedString(llvm::StringRef Data) {
    OS.write(Data.data(), Data.size());
  }
  void writeFixedString(const char *Data, size_t Size) { OS.write(Data, Size); }
  void writeCString(llvm::StringRef S) {
    OS.write(S.data(), S.size());
    OS.write(0);
  }
  void writeGenericString(llvm::StringRef S) { writeCString(S); }
  void writeZeroes(size_t Size) { OS.write_zeros(Size); }

private:
  llvm::raw_ostream &OS;
};

struct UniqueStringTable {
  struct StringEntry {
    uint32_t Offset;
    uint32_t Size;
  };
  std::string StringStream;
  std::vector<StringEntry> StringIDs;
  llvm::StringMap<uint32_t> IDMap;

  uint32_t intern(llvm::StringRef S) {
    uint32_t StringID = StringIDs.size();
    auto R = IDMap.try_emplace(S, StringID);
    if (!R.second)
      return R.first->second;
    auto Offset = StringStream.size();
    StringIDs.emplace_back(StringEntry{static_cast<uint32_t>(Offset),
                                       static_cast<uint32_t>(S.size())});
    StringStream.append(S);
    StringStream.push_back(0);
    return StringID;
  }

  void writeStringIDsTable(Writer &W) {
    for (auto &I : StringIDs) {
      W.write32(I.Offset);
      W.write32(I.Size);
    }
  }

  void writeStringStream(Writer &W) { W.writeFixedString(StringStream); }
};

UniqueStringTable::StringEntry getStringEntry(llvm::StringRef StringIDsTable,
                                              uint32_t StringID) {
  auto TableAddress =
      StringIDsTable.data() + StringID * sizeof(UniqueStringTable::StringEntry);
  auto Offset = llvm::support::endian::read32le(TableAddress);
  auto Size = llvm::support::endian::read32le(
      TableAddress + sizeof(UniqueStringTable::StringEntry::Size));
  return {Offset, Size};
}

llvm::StringRef getString(llvm::StringRef StringIDsTable,
                          llvm::StringRef StringStream, uint32_t StringID) {
  UniqueStringTable::StringEntry SE = getStringEntry(StringIDsTable, StringID);
  return llvm::StringRef(StringStream.data() + SE.Offset, SE.Size);
}

class Reader2 : public Reader {
public:
  Reader2(llvm::StringRef StringIDsTable, llvm::StringRef StringStream,
          llvm::StringRef Data)
      : Reader(Data), StringIDsTable(StringIDsTable),
        StringStream(StringStream) {}

  llvm::StringRef consumeInternedString() {
    auto StringID = consume32();
    if (err())
      return llvm::StringRef();
    return getString(StringIDsTable, StringStream, StringID);
  }

  llvm::StringRef consumeGenericString() { return consumeInternedString(); }

private:
  llvm::StringRef StringIDsTable;
  llvm::StringRef StringStream;
};

class Writer2 : public Writer {
public:
  Writer2(UniqueStringTable &Strings, llvm::raw_ostream &OS)
      : Writer(OS), Strings(Strings) {}

  void writeInternString(llvm::StringRef S) { write32(Strings.intern(S)); }

  void writeGenericString(llvm::StringRef S) { writeInternString(S); }

private:
  UniqueStringTable &Strings;
};

Container::Container(const void *V, size_t N) : StartAddress(V), Size(N) {
  Reader R(V, N);
  if (R.consumeFixedString(4).compare({CONTAINER_MAGIC, 4}))
    return;
  size_t NumChunks = R.consume32();
  for (size_t I = 0; I < NumChunks; I++) {
    llvm::StringRef ChunkName = R.consumeFixedString(4);
    size_t ChunkSize = R.consume32();
    size_t ChunkOffset = R.consume64();
    if (R.err())
      break;
    llvm::StringRef Data{static_cast<const char *>(StartAddress) + ChunkOffset,
                         ChunkSize};
    ChunksMap.try_emplace(ChunkName, Data);
  }
}

Container::Container(llvm::StringRef Data)
    : Container(Data.data(), Data.size()) {}

inline uint64_t getPageAlignedSize(size_t Size) {
  static unsigned int PageSize = llvm::sys::Process::getPageSizeEstimate();
  return ((Size + PageSize - 1) / PageSize) * PageSize;
}

llvm::raw_ostream &operator<<(llvm::raw_ostream &OS, const File &F) {
  size_t HeaderSize = 4 + 4 + sizeof(ChunkMetadata) * F.Chunks.size();
  size_t PgAlignedHeaderSize =
      getPageAlignedSize(HeaderSize + F.HeaderOffsetInPage);
  uint64_t Offset = PgAlignedHeaderSize - F.HeaderOffsetInPage;
  llvm::StringMap<uint64_t> ChunkOffset;
  for (auto &I : F.Chunks) {
    uint64_t PgAlignedSize = getPageAlignedSize(I.Data.size());
    ChunkOffset.try_emplace(toStringRef(I.Name), Offset);
    Offset += PgAlignedSize;
  }

  Writer W(OS);
  W.writeFixedString(CONTAINER_MAGIC, 4);
  W.write32(F.Chunks.size());
  for (auto &I : F.Chunks) {
    W.writeFixedString(toStringRef(I.Name));
    W.write32(I.Data.size());
    W.write64(ChunkOffset[toStringRef(I.Name)]);
  }
  W.writeZeroes(PgAlignedHeaderSize - HeaderSize - F.HeaderOffsetInPage);
  for (auto &I : F.Chunks) {
    W.writeFixedString(I.Data);
    W.writeZeroes(getPageAlignedSize(I.Data.size()) - I.Data.size());
  }

  return OS;
}

template <typename WriterType>
void writeLocation(const SymbolLocation &Loc, WriterType &&W) {
  W.writeGenericString(Loc.FileURI);
  for (const auto &Endpoint : {Loc.Start, Loc.End}) {
    W.write32(Endpoint.line());
    W.write32(Endpoint.column());
  }
}

template <typename ReaderType> SymbolLocation readLocation(ReaderType &&R) {
  SymbolLocation Loc;
  Loc.FileURI = R.consumeGenericString().data();
  for (auto *Endpoint : {&Loc.Start, &Loc.End}) {
    Endpoint->setLine(R.consume32());
    Endpoint->setColumn(R.consume32());
  }
  return Loc;
}

template <typename ReaderType>
IncludeGraphNode readIncludeGraphNode(ReaderType &&R) {
  IncludeGraphNode IGN;
  IGN.Flags = R.template consumeEnum<IncludeGraphNode::SourceFlag>();
  IGN.URI = R.consumeGenericString();
  llvm::StringRef Digest = R.consumeFixedString(IGN.Digest.size());
  std::copy(Digest.bytes_begin(), Digest.bytes_end(), IGN.Digest.begin());
  IGN.DirectIncludes.resize(R.consume32());
  for (llvm::StringRef &Include : IGN.DirectIncludes)
    Include = R.consumeGenericString();
  return IGN;
}

template <typename WriterType>
void writeIncludeGraphNode(const IncludeGraphNode &IGN, WriterType &&W) {
  W.writeEnum(IGN.Flags);
  W.writeGenericString(IGN.URI);

  llvm::StringRef Hash(reinterpret_cast<const char *>(IGN.Digest.data()),
                       IGN.Digest.size());
  W.writeFixedString(Hash);
  W.write32(IGN.DirectIncludes.size());

  for (llvm::StringRef Include : IGN.DirectIncludes)
    W.writeGenericString(Include);
}

template <typename WriterType>
void writeSymbol(const Symbol &Sym, WriterType &&W) {
  W.writeFixedString(Sym.ID.raw());
  W.writeEnum(Sym.SymInfo.Kind);
  W.writeEnum(Sym.SymInfo.SubKind);
  W.writeEnum(Sym.SymInfo.Lang);
  W.write16(Sym.SymInfo.Properties);
  W.writeGenericString(Sym.Name);
  W.writeGenericString(Sym.Scope);
  W.writeGenericString(Sym.TemplateSpecializationArgs);
  writeLocation(Sym.Definition, W);
  writeLocation(Sym.CanonicalDeclaration, W);
  W.write32(Sym.References);
  W.writeEnum(Sym.Flags);
  W.writeEnum(Sym.Origin);
  W.writeGenericString(Sym.Signature);
  W.writeGenericString(Sym.CompletionSnippetSuffix);
  W.writeGenericString(Sym.Documentation);
  W.writeGenericString(Sym.ReturnType);
  W.writeGenericString(Sym.Type);

  auto WriteInclude = [&](const Symbol::IncludeHeaderWithReferences &Include) {
    W.writeGenericString(Include.IncludeHeader);
    W.write32(Include.References);
  };
  W.write32(Sym.IncludeHeaders.size());
  for (const auto &Include : Sym.IncludeHeaders)
    WriteInclude(Include);
}

std::string writeSymbol(const Symbol &Sym) {
  std::string Data;
  llvm::raw_string_ostream OS(Data);
  writeSymbol(Sym, Writer(OS));
  OS.flush();
  return Data;
}

template <typename ReaderType> Symbol readSymbol(ReaderType &&R) {
  Symbol Sym;
  Sym.ID = SymbolID::fromRaw(R.consumeFixedString(SymbolID::RawSize));
  Sym.SymInfo.Kind = R.template consumeEnum<decltype(Symbol::SymInfo.Kind)>();
  Sym.SymInfo.SubKind =
      R.template consumeEnum<decltype(Symbol::SymInfo.SubKind)>();
  Sym.SymInfo.Lang = R.template consumeEnum<decltype(Symbol::SymInfo.Lang)>();
  Sym.SymInfo.Properties = R.consume16();
  Sym.Name = R.consumeGenericString();
  Sym.Scope = R.consumeGenericString();
  Sym.TemplateSpecializationArgs = R.consumeGenericString();
  Sym.Definition = readLocation(R);
  Sym.CanonicalDeclaration = readLocation(R);
  Sym.References = R.consume32();
  Sym.Flags = R.template consumeEnum<Symbol::SymbolFlag>();
  Sym.Origin = R.template consumeEnum<SymbolOrigin>();
  Sym.Signature = R.consumeGenericString();
  Sym.CompletionSnippetSuffix = R.consumeGenericString();
  Sym.Documentation = R.consumeGenericString();
  Sym.ReturnType = R.consumeGenericString();
  Sym.Type = R.consumeGenericString();
  Sym.IncludeHeaders.resize(R.consume32());
  for (auto &I : Sym.IncludeHeaders) {
    I.IncludeHeader = R.consumeGenericString();
    I.References = R.consume32();
  }
  return Sym;
}

Symbol readSymbol(llvm::StringRef Data) { return readSymbol(Reader(Data)); }

// REFS ENCODING
// A refs section has data grouped by Symbol. Each symbol has:
//  - SymbolID: 8 bytes
//  - NumRefs: uint32
//  - Ref[NumRefs]
// Fields of Ref are encoded in turn, see implementation.

template <typename WriterType> void writeRef(Ref Re, WriterType &&W) {
  W.template writeEnum<RefKind>(Re.Kind);
  writeLocation(Re.Location, W);
}

std::string writeRef(Ref Re) {
  std::string Data;
  llvm::raw_string_ostream OS(Data);
  writeRef(Re, Writer(OS));
  OS.flush();
  return Data;
}

template <typename ReaderType> Ref readRef(ReaderType &&R) {
  auto Kind = R.template consumeEnum<RefKind>();
  auto Location = readLocation(R);
  return {Location, Kind};
}

Ref readRef(llvm::StringRef Data) { return readRef(Reader(Data)); }

template <typename WriterType>
void writeRefs(const SymbolID &ID, llvm::ArrayRef<Ref> Refs, WriterType &&W) {
  W.writeFixedString(ID.raw());
  W.write32(Refs.size());
  for (const auto &Ref : Refs) {
    writeRef(Ref, W);
  }
}

template <typename ReaderType>
std::pair<SymbolID, std::vector<Ref>> readRefs(ReaderType &&R) {
  std::pair<SymbolID, std::vector<Ref>> Result;
  Result.first = SymbolID::fromRaw(R.consumeFixedString(SymbolID::RawSize));
  Result.second.resize(R.consume32());
  for (auto &Ref : Result.second) {
    Ref = readRef(R);
  }
  return Result;
}

// RELATIONS ENCODING
// A relations section is a flat list of relations. Each relation has:
//  - SymbolID (subject): 8 bytes
//  - relation kind (predicate): 1 byte
//  - SymbolID (object): 8 bytes
// In the future, we might prefer a packed representation if the need arises.

template <typename WriterType>
void writeRelation(const Relation &Rel, WriterType &&W) {
  W.writeFixedString(Rel.Subject.raw());
  RelationKind Kind = Rel.Predicate;
  W.writeEnum(Kind);
  W.writeFixedString(Rel.Object.raw());
}

std::string writeRelation(Relation Rel) {
  std::string Data;
  llvm::raw_string_ostream OS(Data);
  writeRelation(Rel, Writer(OS));
  OS.flush();
  return Data;
}

template <typename ReaderType> Relation readRelation(ReaderType &&R) {
  SymbolID Subject = SymbolID::fromRaw(R.consumeFixedString(SymbolID::RawSize));
  RelationKind Predicate = R.template consumeEnum<RelationKind>();
  SymbolID Object = SymbolID::fromRaw(R.consumeFixedString(SymbolID::RawSize));
  return {Subject, Predicate, Object};
}

Relation readRelation(llvm::StringRef Data) { return readRelation(Reader(Data)); }

struct CompileCommand {
  llvm::StringRef Directory;
  std::vector<llvm::StringRef> CommandLine;
};

template <typename WriterType>
void writeCompileCommand(const CompileCommand &Cmd, WriterType &&W) {
  W.writeGenericString(Cmd.Directory);
  W.write32(Cmd.CommandLine.size());
  for (llvm::StringRef C : Cmd.CommandLine)
    W.writeGenericString(C);
}

template <typename ReaderType>
CompileCommand readCompileCommand(ReaderType &&R) {
  CompileCommand Cmd;
  Cmd.Directory = R.consumeGenericString();
  Cmd.CommandLine.resize(R.consume32());
  for (llvm::StringRef &C : Cmd.CommandLine)
    C = R.consumeGenericString();
  return Cmd;
}

constexpr static uint32_t Version = 13;

llvm::Expected<IndexFileIn> readContainer(llvm::StringRef Data) {
  IndexFileIn Ret;
  Container Content(Data);
  llvm::StringRef StringIDsTable, StringStream;
  {
    auto StringStreamEntry = Content.find("Strs");
    auto StringIDsTableEntry = Content.find("StrI");
    StringIDsTable = StringIDsTableEntry->getValue();
    StringStream = StringStreamEntry->getValue();
  }
  {
    auto SymbolsEntry = Content.find("Syms");
    if (SymbolsEntry != Content.end()) {
      auto SymbolsChunk = SymbolsEntry->getValue();
      Reader2 R(StringIDsTable, StringStream, SymbolsChunk);
      SymbolSlab::Builder Builder;
      while (!R.eof())
        Builder.insert(readSymbol(R));
      Ret.Symbols = std::move(Builder).build();
    }
  }
  {
    auto SourcesEntry = Content.find("IGNs");
    if (SourcesEntry != Content.end()) {
      auto SourcesChunk = SourcesEntry->getValue();
      Reader2 R(StringIDsTable, StringStream, SourcesChunk);
      Ret.Sources.emplace();
      while (!R.eof()) {
        auto IGN = readIncludeGraphNode(R);
        auto Entry = Ret.Sources->try_emplace(IGN.URI).first;
        Entry->getValue() = std::move(IGN);
        // We change all the strings inside the structure to point at the keys
        // in the map, since it is the only copy of the string that's going to
        // live.
        Entry->getValue().URI = Entry->getKey();
        for (auto &Include : Entry->getValue().DirectIncludes)
          Include = Ret.Sources->try_emplace(Include).first->getKey();
      }
    }
  }
  {
    auto RefsEntry = Content.find("Refs");
    if (RefsEntry != Content.end()) {
      auto RefsChunk = RefsEntry->getValue();
      Reader2 R(StringIDsTable, StringStream, RefsChunk);
      RefSlab::Builder Builder;
      while (!R.eof()) {
        auto Refs = readRefs(R);
        for (auto &I : Refs.second)
          Builder.insert(Refs.first, I);
      }
      Ret.Refs = std::move(Builder).build();
    }
  }
  {
    auto RelationsEntry = Content.find("Rela");
    if (RelationsEntry != Content.end()) {
      auto RelationsChunk = RelationsEntry->getValue();
      Reader2 R(StringIDsTable, StringStream, RelationsChunk);
      RelationSlab::Builder Builder;
      while (!R.eof())
        Builder.insert(readRelation(R));
      Ret.Relations = std::move(Builder).build();
    }
  }
  {
    auto CmdEntry = Content.find("Cmds");
    if (CmdEntry != Content.end()) {
      auto CmdChunk = CmdEntry->getValue();
      Reader2 R(StringIDsTable, StringStream, CmdChunk);
      CompileCommand Cmd = readCompileCommand(R);
      Ret.Cmd.emplace();
      Ret.Cmd->Directory = Cmd.Directory;
      Ret.Cmd->CommandLine.reserve(Cmd.CommandLine.size());
      for (llvm::StringRef C : Cmd.CommandLine)
        Ret.Cmd->CommandLine.emplace_back(C);
    }
  }
  return Ret;
}

void writeContainer(const IndexFileOut &Data, raw_ostream &OS) {
  assert(Data.Symbols && "An index file without symbols makes no sense!");
  File DataFile;
  UniqueStringTable Strings;

  llvm::SmallString<4> Meta;
  {
    llvm::raw_svector_ostream MetaOS(Meta);
    Writer W(MetaOS);
    W.write32(Version);
  }
  DataFile.Chunks.push_back({fourCC("meta"), Meta});

  std::string SymbolsChunk;
  std::vector<std::pair<SymbolID, uint32_t>> SymbolsOffsetTable;
  if (Data.Symbols) {
    llvm::raw_string_ostream SymbolsChunkOS(SymbolsChunk);
    Writer2 W(Strings, SymbolsChunkOS);
    for (const auto &Sym : *Data.Symbols) {
      auto Offset = SymbolsChunk.size();
      writeSymbol(Sym, W);
      SymbolsChunkOS.flush();
      SymbolsOffsetTable.emplace_back(Sym.ID, Offset);
    }
  }
  std::string SymbolsOffsetTableChunk;
  if (!SymbolsOffsetTable.empty()) {
    llvm::raw_string_ostream SymbolsOffsetTableChunkOS(SymbolsOffsetTableChunk);
    Writer W(SymbolsOffsetTableChunkOS);
    for (const auto &I : SymbolsOffsetTable) {
      W.writeFixedString(I.first.raw());
      W.write32(I.second);
      W.write32(0);
    }
  }

  std::string SourcesChunk;
  if (Data.Sources) {
    llvm::raw_string_ostream SourcesChunkOS(SourcesChunk);
    Writer2 W(Strings, SourcesChunkOS);
    for (const auto &Source : *Data.Sources)
      writeIncludeGraphNode(Source.second, W);
  }

  std::string RefsChunk;
  std::vector<std::pair<SymbolID, uint32_t>> RefsOffsetTable;
  if (Data.Refs) {
    std::vector<RefSlab::value_type> SortedRefs;
    for (const auto &I : *Data.Refs)
      SortedRefs.emplace_back(I);
    llvm::sort(SortedRefs,
               [](const RefSlab::value_type &L, const RefSlab::value_type &R) {
                 return L.first < R.first;
               });
    llvm::raw_string_ostream RefsChunkOS(RefsChunk);
    Writer2 W(Strings, RefsChunkOS);
    for (const auto &I : SortedRefs) {
      auto Offset = RefsChunk.size();
      writeRefs(I.first, I.second, W);
      RefsChunkOS.flush();
      RefsOffsetTable.emplace_back(I.first, Offset);
    }
  }
  std::string RefsOffsetTableChunk;
  if (!RefsOffsetTable.empty()) {
    llvm::raw_string_ostream RefsOffsetTableChunkOS(RefsOffsetTableChunk);
    Writer W(RefsOffsetTableChunkOS);
    for (const auto &I : RefsOffsetTable) {
      W.writeFixedString(I.first.raw());
      W.write32(I.second);
      W.write32(0);
    }
  }

  std::string RelationsChunk;
  if (Data.Relations) {
    llvm::raw_string_ostream RelationsChunkOS(RelationsChunk);
    Writer W(RelationsChunkOS);
    for (const auto &Relation : *Data.Relations) {
      writeRelation(Relation, W);
    }
  }

  std::string CmdChunk;
  if (Data.Cmd) {
    CompileCommand Cmd;
    Cmd.CommandLine.reserve(Data.Cmd->CommandLine.size());
    Cmd.Directory = Data.Cmd->Directory;
    for (llvm::StringRef C : Data.Cmd->CommandLine) {
      Cmd.CommandLine.emplace_back(C);
    }
    llvm::raw_string_ostream CmdChunkOS(CmdChunk);
    Writer2 W(Strings, CmdChunkOS);
    writeCompileCommand(Cmd, W);
  }

  std::string StringsChunk;
  {
    llvm::raw_string_ostream StringsChunkOS(StringsChunk);
    Writer W(StringsChunkOS);
    Strings.writeStringStream(W);
  }
  std::string StringIDsTableChunk;
  {
    llvm::raw_string_ostream StringIDsTableChunkOS(StringIDsTableChunk);
    Writer W(StringIDsTableChunkOS);
    Strings.writeStringIDsTable(W);
  }

  if (!SymbolsOffsetTableChunk.empty())
    DataFile.Chunks.push_back({fourCC("SymO"), SymbolsOffsetTableChunk});
  if (!SymbolsChunk.empty())
    DataFile.Chunks.push_back({fourCC("Syms"), SymbolsChunk});

  if (!SourcesChunk.empty())
    DataFile.Chunks.push_back({fourCC("IGNs"), SourcesChunk});

  if (!RefsOffsetTableChunk.empty())
    DataFile.Chunks.push_back({fourCC("RefO"), RefsOffsetTableChunk});
  if (!RefsChunk.empty())
    DataFile.Chunks.push_back({fourCC("Refs"), RefsChunk});

  if (!RelationsChunk.empty())
    DataFile.Chunks.push_back({fourCC("Rela"), RelationsChunk});

  if (!CmdChunk.empty())
    DataFile.Chunks.push_back({fourCC("Cmds"), CmdChunk});

  DataFile.Chunks.push_back({fourCC("StrI"), StringIDsTableChunk});
  DataFile.Chunks.push_back({fourCC("Strs"), StringsChunk});

  OS << DataFile;
}

llvm::Optional<IncludeGraph> getSourcesInContainer(llvm::StringRef Data,
                                                   SymbolID ID) {
  llvm::Optional<IncludeGraph> Ret;
  Container Content(Data);
  llvm::StringRef StringIDsTable, StringStream;
  {
    auto StringStreamEntry = Content.find("Strs");
    auto StringIDsTableEntry = Content.find("StrI");
    StringIDsTable = StringIDsTableEntry->getValue();
    StringStream = StringStreamEntry->getValue();
  }

  auto SourcesEntry = Content.find("IGNs");
  if (SourcesEntry != Content.end()) {
    auto SourcesChunk = SourcesEntry->getValue();
    Reader2 R(StringIDsTable, StringStream, SourcesChunk);
    Ret.emplace();
    while (!R.eof()) {
      auto IGN = readIncludeGraphNode(R);
      auto Entry = Ret->try_emplace(IGN.URI).first;
      Entry->getValue() = std::move(IGN);
      // We change all the strings inside the structure to point at the keys
      // in the map, since it is the only copy of the string that's going to
      // live.
      Entry->getValue().URI = Entry->getKey();
      for (auto &Include : Entry->getValue().DirectIncludes)
        Include = Ret->try_emplace(Include).first->getKey();
    }
  }
  return Ret;
}

llvm::Optional<Symbol> getSymbolInContainer(llvm::StringRef Data, SymbolID ID) {
  struct SymbolIDEntry {
    char SymbolID[8];
    char Offset[4];
    char Padding[4];
  };
  Container Content(Data);
  llvm::StringRef StringIDsTable, StringStream;
  {
    auto StringStreamEntry = Content.find("Strs");
    auto StringIDsTableEntry = Content.find("StrI");
    StringIDsTable = StringIDsTableEntry->getValue();
    StringStream = StringStreamEntry->getValue();
  }

  auto SymbolOffsetTableEntry = Content.find("SymO");
  if (SymbolOffsetTableEntry == Content.end())
    return llvm::None;

  auto SymbolOffsetTableChunk = SymbolOffsetTableEntry->getValue();
  llvm::ArrayRef<SymbolIDEntry> SOETable(
      reinterpret_cast<const SymbolIDEntry *>(SymbolOffsetTableChunk.data()),
      SymbolOffsetTableChunk.size() / sizeof(SymbolIDEntry));
  auto It = std::partition_point(
      SOETable.begin(), SOETable.end(), [&ID](const SymbolIDEntry &I) {
        return SymbolID::fromRaw({I.SymbolID, SymbolID::RawSize}) < ID;
      });
  if (It == SOETable.end())
    return llvm::None;

  auto Offset = llvm::support::endian::read32le(It->Offset);

  auto SymbolsEntry = Content.find("Syms");
  if (SymbolsEntry == Content.end())
    return llvm::None;
  auto SymbolsChunk = SymbolsEntry->getValue();
  return readSymbol(
      Reader2(StringIDsTable, StringStream,
              {SymbolsChunk.data() + Offset, SymbolsChunk.size() - Offset}));
}

llvm::Optional<std::vector<Ref>> getRefsInContainer(llvm::StringRef Data,
                                                    SymbolID ID) {
  struct SymbolIDEntry {
    char SymbolID[8];
    char Offset[4];
    char Padding[4];
  };
  Container Content(Data);
  llvm::StringRef StringIDsTable, StringStream;
  {
    auto StringStreamEntry = Content.find("Strs");
    auto StringIDsTableEntry = Content.find("StrI");
    StringIDsTable = StringIDsTableEntry->getValue();
    StringStream = StringStreamEntry->getValue();
  }

  auto RefsOffsetTableEntry = Content.find("RefO");
  if (RefsOffsetTableEntry == Content.end())
    return llvm::None;

  auto RefsOffsetTableChunk = RefsOffsetTableEntry->getValue();
  llvm::ArrayRef<SymbolIDEntry> SOETable(
      reinterpret_cast<const SymbolIDEntry *>(RefsOffsetTableChunk.data()),
      RefsOffsetTableChunk.size() / sizeof(SymbolIDEntry));
  auto It = std::partition_point(
      SOETable.begin(), SOETable.end(), [&ID](const SymbolIDEntry &I) {
        return SymbolID::fromRaw({I.SymbolID, SymbolID::RawSize}) < ID;
      });
  if (It == SOETable.end() ||
      !(SymbolID::fromRaw({It->SymbolID, SymbolID::RawSize}) == ID))
    return llvm::None;

  auto Offset = llvm::support::endian::read32le(It->Offset);

  auto RefsEntry = Content.find("Refs");
  if (RefsEntry == Content.end())
    return llvm::None;
  auto RefsChunk = RefsEntry->getValue();
  return readRefs(
             Reader2(StringIDsTable, StringStream,
                     {RefsChunk.data() + Offset, RefsChunk.size() - Offset}))
      .second;
}

void getRelationsInContainer(
    llvm::StringRef Data, SymbolID Subject, RelationKind Predicate,
    llvm::function_ref<bool(const Relation &)> Callback) {
  struct RelationEntry {
    char Subject[8];
    char Predicate[4];
    char Object[8];
  };
  Container Content(Data);
  auto RelationsTableEntry = Content.find("Rela");
  if (RelationsTableEntry == Content.end())
    return;

  auto RelationsTableChunk = RelationsTableEntry->getValue();
  llvm::ArrayRef<RelationEntry> RelationsTable(
      reinterpret_cast<const RelationEntry *>(RelationsTableChunk.data()),
      RelationsTableChunk.size() / sizeof(RelationEntry));
  auto It = std::partition_point(
      RelationsTable.begin(), RelationsTable.end(),
      [&Subject, &Predicate](const RelationEntry &I) {
        Relation Rel = readRelation(Reader(&I, sizeof(RelationEntry)));
        return std::tie(Rel.Subject, Rel.Predicate) <
               std::tie(Subject, Predicate);
      });
  while (It != RelationsTable.end()) {
    Relation Rel = readRelation(Reader(It, sizeof(RelationEntry)));
    if (std::tie(Rel.Subject, Rel.Predicate) != std::tie(Subject, Predicate))
      break;
    if (!Callback(Rel))
      break;
    It++;
  }
}

} // namespace container
} // namespace dbindex
} // namespace clangd
} // namespace clang
