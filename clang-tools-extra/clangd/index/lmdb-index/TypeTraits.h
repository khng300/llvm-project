//===--- TypeTraits.h - Type Traits Helpers ---------------------*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#ifndef LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_LMDB_INDEX_TYPETRAITS_H
#define LLVM_CLANG_TOOLS_EXTRA_CLANGD_INDEX_LMDB_INDEX_TYPETRAITS_H

#include "llvm/ADT/ArrayRef.h"
#include "llvm/ADT/StringRef.h"

#include <type_traits>

namespace clang {
namespace clangd {
namespace lmdb_index {

template <typename T> struct HasDataAndSizeMethods {
  template <typename V>
  static constexpr auto test(int)
      -> decltype(std::declval<V>().data(), std::declval<V>().size(), char());

  template <typename V> static constexpr double test(...);

  static constexpr bool Value = sizeof(decltype(test<T>(0))) == sizeof(char);
};
template <typename T>
static constexpr bool HasDataAndSizeMethodsV = HasDataAndSizeMethods<T>::Value;

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

} // namespace lmdb_index
} // namespace clangd
} // namespace clang

#endif
