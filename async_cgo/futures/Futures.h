#pragma once

#include <folly/futures/Future.h>
#include <glog/logging.h>

#include <atomic>
#include <functional>
#include <memory>
#include <variant>

/// @brief a virtual class that represents a future that can be run,
/// cancelled, and get.
class IFuture {
 public:
  /// @brief  the wrapped function and sets the result of the future.
  virtual void run() = 0;

  /// @brief cancel the future with the given exception,
  /// and the get method
  /// throws a folly::FutureException .
  virtual void cancel(folly::FutureException&& e) = 0;

  /// @brief block and get the result of the future, the type is determined by
  /// the implementation.
  virtual void* get() = 0;
};

/// @brief  Future is a implementation of IFuture.
/// @tparam R is the return type of the wraped function, which must be a
/// pointer.
/// @tparam Fn is the type of the wrapped function, which must be nothrow and
/// return a R type.
template <class R, class Fn, typename = std::enable_if<std::is_pointer_v<R>>,
          typename = std::enable_if<std::is_invocable_r_v<R, Fn>>>
class Future : public IFuture {
 public:
  Future(Fn&& fn)
      : promise_(), future_(promise_.getSemiFuture()), fn_(std::move(fn)) {
    promise_.setInterruptHandler(
        [this](auto&& e) { promise_.setException(e); });
  }

  /// run the wrapped function and set the result of the future.
  void run() noexcept {
    try {
      promise_.setValue(fn_());
    } catch (const std::exception& e) {
      promise_.setException(e);
    }
  }

  void cancel(folly::FutureException&& e) noexcept { future_.raise(e); }

  void* get() noexcept { return static_cast<void*>(std::move(future_).get()); }

 private:
  Fn fn_;
  folly::Promise<R> promise_;
  folly::SemiFuture<R> future_;
};

template <class R, class Fn>
std::unique_ptr<IFuture> CreateFuture(Fn&& fn) {
  return std::make_unique<Future<R, Fn>>(std::forward<Fn>(fn));
}
