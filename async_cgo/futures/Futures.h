#pragma once

#include <folly/futures/Future.h>
#include <glog/logging.h>

#include <atomic>
#include <functional>
#include <variant>

template <class R, typename Fn>
class Future {
 public:
  Future(Fn&& func)
      : promise_(),
        future_(promise_.getSemiFuture()),
        function_(std::move(func)) {
    promise_.setInterruptHandler(
        [this](auto&& e) { promise_.setException(e); });
  }

  template <typename... Args,
            typename = std::enable_if_t<std::is_invocable_r_v<R, Fn, Args...>>>
  void run(Args&&... args) {
    try {
      promise_.setValue(function_(std::forward<Args>(args)...));
    } catch (const std::exception& e) {
      promise_.setException(e);
    }
  }

  template <typename E,
            typename = std::enable_if_t<std::is_base_of_v<std::exception, E>>>
  void cancel(E&& exception) {
    future_.raise(exception);
  }

  R get() { return std::move(future_).get(); }

 private:
  Fn function_;
  folly::Promise<R> promise_;
  folly::SemiFuture<R> future_;
};
