// compat.hpp —— rosetta 平台兼容面(Android bionic / 老 libc++)。
//
// [rosetta patch 0003](自 ab7fec4 的 memfd 兼容扩写):
// 1. memfd_create:bionic __ANDROID_API__ < 30 不声明该函数,以 raw
//    syscall 等价替代(内核 >= 3.17 与 app seccomp 白名单均在)。
// 2. atomic_shared_ptr:std::atomic<std::shared_ptr<T>> 依赖 C++20
//    特化,libc++ 到 LLVM 19 才提供;更老的 libc++(NDK)会选中初级
//    模板,_Atomic 施加在非 trivially-copyable 类型上直接编译失败
//    (SessionManager::SessionSlot 的成员声明在解析期即触发实例化,
//    伞头文件当場炸)。此处的互斥锁版提供与 std::atomic 同形的
//    load/store/exchange/compare_exchange_strong 表面,调用点零改动。
//    仅用于进程内池簿记(SessionSlot),不上线、不进共享内存布局。
#ifndef SHMIPC_COMPAT_HPP
#define SHMIPC_COMPAT_HPP

#include <cerrno>
#include <memory>
#include <mutex>
#include <utility>

#if defined(__ANDROID__) && __ANDROID_API__ < 30
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace shmipc {

// --- 1. memfd_create(bionic API < 30 raw syscall 回退)---

inline int memfd_create(const char* name, unsigned int flags) noexcept {
#if defined(__ANDROID__) && __ANDROID_API__ < 30
  return static_cast<int>(::syscall(SYS_memfd_create, name, flags));
#else
  return ::memfd_create(name, flags);
#endif
}

// --- 2. atomic_shared_ptr(互斥锁版 std::atomic<std::shared_ptr<T>>)---

template <typename T>
class atomic_shared_ptr {
 public:
  atomic_shared_ptr() = default;
  explicit atomic_shared_ptr(std::shared_ptr<T> initial)
      : ptr_(std::move(initial)) {}
  atomic_shared_ptr(const atomic_shared_ptr&) = delete;
  atomic_shared_ptr& operator=(const atomic_shared_ptr&) = delete;

  std::shared_ptr<T> load() const {
    const std::lock_guard<std::mutex> lock(mtx_);
    return ptr_;
  }

  void store(std::shared_ptr<T> desired) {
    const std::lock_guard<std::mutex> lock(mtx_);
    ptr_ = std::move(desired);
  }

  std::shared_ptr<T> exchange(std::shared_ptr<T> desired) {
    const std::lock_guard<std::mutex> lock(mtx_);
    std::shared_ptr<T> old = std::move(ptr_);
    ptr_ = std::move(desired);
    return old;
  }

  bool compare_exchange_strong(std::shared_ptr<T>& expected,
                               std::shared_ptr<T> desired) {
    const std::lock_guard<std::mutex> lock(mtx_);
    if (ptr_ == expected) {
      ptr_ = std::move(desired);
      return true;
    }
    expected = ptr_;
    return false;
  }

 private:
  mutable std::mutex mtx_;
  std::shared_ptr<T> ptr_;
};

}  // namespace shmipc

#endif  // SHMIPC_COMPAT_HPP
