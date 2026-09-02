// memfd_compat.hpp —— Android bionic 兼容(rosetta patch)。
// bionic 在 __ANDROID_API__ < 30 不声明 memfd_create;以 raw syscall
// 等价替代(内核 ≥3.17 与 app seccomp 白名单各版本均在)。
#ifndef SHMIPC_MEMFD_COMPAT_HPP
#define SHMIPC_MEMFD_COMPAT_HPP

#include <cerrno>

#if defined(__ANDROID__) && __ANDROID_API__ < 30
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace shmipc {

inline int memfd_create(const char* name, unsigned int flags) noexcept {
#if defined(__ANDROID__) && __ANDROID_API__ < 30
  return static_cast<int>(::syscall(SYS_memfd_create, name, flags));
#else
  return ::memfd_create(name, flags);
#endif
}

}  // namespace shmipc

#endif  // SHMIPC_MEMFD_COMPAT_HPP
