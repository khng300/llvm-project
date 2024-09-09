//===--- TypeTraits.h - Type Traits Helpers ---------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_TYPETRAITS_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_DB_TYPETRAITS_H

#include "llvm/ADT/ArrayRef.h"
#include "llvm/ADT/StringRef.h"

#include <type_traits>

namespace clang {
namespace clangd {
namespace db_index {

template <typename> struct IsString : std::false_type {};
template <> struct IsString<std::string> : std::true_type {};
template <> struct IsString<std::string_view> : std::true_type {};
template <> struct IsString<llvm::StringRef> : std::true_type {};
template <typename _T>
static constexpr bool IsStringV =
    IsString<std::remove_cv_t<std::remove_reference_t<_T>>>::value;

template <typename> struct IsVector : std::false_type {};
template <typename _T> struct IsVector<std::vector<_T>> : std::true_type {};
template <typename _T> struct IsVector<llvm::ArrayRef<_T>> : std::true_type {};
template <typename _T>
static constexpr bool IsVectorV =
    IsVector<std::remove_cv_t<std::remove_reference_t<_T>>>::value;

template <typename _Type> struct MemberPointerTypeHelper;
template <typename _Type, typename _ClassType>
struct MemberPointerTypeHelper<_Type(_ClassType::*)> {
  using Type = _Type;
  using ClassType = _ClassType;
};
template <typename _Type, typename _ClassType>
struct MemberPointerTypeHelper<const _Type(_ClassType::*)> {
  using Type = _Type;
  using ClassType = _ClassType;
};

template <typename Fn> struct FuncSignatureHelper;
template <typename RetType, typename ArgType>
struct FuncSignatureHelper<RetType (*)(ArgType)> {
  using ArgumentType = ArgType;
  using ReturnType = RetType;
};
template <typename RetType, typename ArgType>
struct FuncSignatureHelper<RetType (*)(ArgType) noexcept> {
  using ArgumentType = ArgType;
  using ReturnType = RetType;
};
template <typename RetType, typename _ClassType, typename ArgType>
struct FuncSignatureHelper<RetType (_ClassType::*)(ArgType)> {
  using ArgumentType = ArgType;
  using ClassType = _ClassType;
  using ReturnType = RetType;
};
template <typename RetType, typename _ClassType, typename ArgType>
struct FuncSignatureHelper<RetType (_ClassType::*)(ArgType) noexcept> {
  using ArgumentType = ArgType;
  using ClassType = _ClassType;
  using ReturnType = RetType;
};
template <typename RetType, typename _ClassType, typename ArgType>
struct FuncSignatureHelper<RetType (_ClassType::*)(ArgType) &&> {
  using ArgumentType = ArgType;
  using ClassType = _ClassType;
  using ReturnType = RetType;
};
template <typename RetType, typename _ClassType, typename ArgType>
struct FuncSignatureHelper<RetType (_ClassType::*)(ArgType) && noexcept> {
  using ArgumentType = ArgType;
  using ClassType = _ClassType;
  using ReturnType = RetType;
};
template <typename RetType, typename _ClassType, typename ArgType>
struct FuncSignatureHelper<RetType (_ClassType::*)(ArgType) const> {
  using ArgumentType = ArgType;
  using ClassType = _ClassType;
  using ReturnType = RetType;
};
template <typename RetType, typename _ClassType, typename ArgType>
struct FuncSignatureHelper<RetType (_ClassType::*)(ArgType) const noexcept> {
  using ArgumentType = ArgType;
  using ClassType = _ClassType;
  using ReturnType = RetType;
};
template <typename RetType, typename _ClassType, typename ArgType>
struct FuncSignatureHelper<RetType (_ClassType::*)(ArgType) const volatile> {
  using ArgumentType = ArgType;
  using ClassType = _ClassType;
  using ReturnType = RetType;
};
template <typename RetType, typename _ClassType, typename ArgType>
struct FuncSignatureHelper<RetType (_ClassType::*)(ArgType)
                               const volatile noexcept> {
  using ArgumentType = ArgType;
  using ClassType = _ClassType;
  using ReturnType = RetType;
};
template <typename RetType, typename _ClassType, typename ArgType>
struct FuncSignatureHelper<RetType (_ClassType::*)(ArgType) volatile> {
  using ArgumentType = ArgType;
  using ClassType = _ClassType;
  using ReturnType = RetType;
};
template <typename RetType, typename _ClassType, typename ArgType>
struct FuncSignatureHelper<RetType (_ClassType::*)(ArgType) volatile noexcept> {
  using ArgumentType = ArgType;
  using ClassType = _ClassType;
  using ReturnType = RetType;
};
template <typename RetType, typename _ClassType, typename ArgType>
struct FuncSignatureHelper<RetType (_ClassType::*)(ArgType) volatile &&> {
  using ArgumentType = ArgType;
  using ClassType = _ClassType;
  using ReturnType = RetType;
};
template <typename RetType, typename _ClassType, typename ArgType>
struct FuncSignatureHelper<RetType (_ClassType::*)(ArgType) volatile &&
                           noexcept> {
  using ArgumentType = ArgType;
  using ClassType = _ClassType;
  using ReturnType = RetType;
};

} // namespace db_index
} // namespace clangd
} // namespace clang

#endif