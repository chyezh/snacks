#pragma once

#include <functional>

#include "Futures.h"

template <class R, typename Notifier,
          typename = std::enable_if<std::is_invocable_v<Notifier>>>
class FutureWithResult : public Future<R> {
 public:
  FutureWithResult(
      Notifier&& notifier,
      folly::Executor::KeepAlive<> executor = folly::getGlobalCPUExecutor(),
      int8_t priority = 0)
      : Future<R>(executor, priority),
        result_(std::make_shared<std::variant<R, std::exception>>()),
        notifier_(std::forward<Notifier>(notifier)) {}

  void asyncConsumeResult() noexcept {
    Future<R>::template asyncConsume(
        [notifier = notifier_, result = result_](R&& r) {
          *result = std::move(r);
          notifier();
        },
        [notifier = notifier_, result = result_](const std::exception& error) {
          *result = std::move(error);
          notifier();
        });
  }

  std::shared_ptr<std::variant<R, std::exception>> get() noexcept {
    return result_;
  }

 private:
  std::shared_ptr<std::variant<R, std::exception>> result_;
  Notifier notifier_;
};