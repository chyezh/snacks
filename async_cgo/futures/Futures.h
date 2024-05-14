#pragma once

#include <folly/futures/Future.h>
#include <glog/logging.h>

#include <atomic>
#include <functional>
#include <memory>
#include <variant>

template <typename R>
using Contract = std::pair<folly::Promise<R>, folly::SemiFuture<R>>;

template <typename R>
using ContractPtr = std::shared_ptr<Contract<R>>;

/// @brief a virtual class that represents a future that can be run,
/// cancelled, and get.
class IFuture {
 public:
  /// @brief cancel the future with the given exception,
  /// and the get method
  /// throws a folly::FutureException .
  virtual void cancel(folly::FutureException&& e) = 0;
};

/// @brief  Future is a implementation of IFuture.
/// @tparam R is the return type of the wraped function, which must be a
/// pointer.
/// @tparam Fn is the type of the wrapped function, which must be nothrow and
/// return a R type.
template <class R, typename = std::enable_if<std::is_pointer_v<R>>>
class Future : public IFuture {
 public:
  Future(folly::Executor::KeepAlive<> executor = folly::getGlobalCPUExecutor(),
         int8_t priority = 0) noexcept
      : contract_(
            std::make_shared<Contract<R>>(folly::makePromiseContract<R>())),
        executor_(executor),
        priority_(priority) {}

  Future(const Future&) = delete;

  Future(const Future&&) = delete;

  template <typename Fn, typename... Args,
            typename = std::enable_if<std::is_invocable_r_v<R, Fn, Args...>>>
  void asyncProduce(Fn&& fn, Args&&... args) noexcept {
    asyncProduce(executor_, priority_, std::forward<Fn>(fn),
                 std::forward<Args>(args)...);
  }

  template <typename Fn, typename... Args,
            typename = std::enable_if<std::is_invocable_r_v<R, Fn, Args...>>>
  void asyncProduce(folly::Executor::KeepAlive<> executor, int8_t priority,
                    Fn&& fn, Args&&... args) noexcept {
    // initialize the interrupt handler for the promise of contract.
    auto interrupt_handler = [contract = contract_](auto& e) {
      // just raise the exception to the semi future if future sent the
      // interrupt signal.
      contract->first.setException(std::move(e));
    };
    contract_->first.setInterruptHandler(std::move(interrupt_handler));

    // start produce process async.
    auto runner = [fn = std::move(fn), &args...]() {
      return fn(std::forward<Args>(args)...);
    };
    auto thenRunner = [contract = contract_,
                       runner = std::move(runner)](auto&&) {
      contract->first.setWith(std::move(runner));
    };
    folly::makeSemiFuture().via(executor, priority).then(thenRunner);
  }

  template <typename RFn, typename EFn,
            typename = std::enable_if<std::is_invocable_v<RFn, R>>,
            typename =
                std::enable_if<std::is_invocable_v<EFn, const std::exception&>>>
  void asyncConsume(RFn&& rfn, EFn&& efn) {
    asyncConsume(executor_, priority_, std::forward<RFn>(rfn),
                 std::forward<EFn>(efn));
  }

  template <typename RFn, typename EFn,
            typename = std::enable_if<std::is_invocable_v<RFn, R>>,
            typename =
                std::enable_if<std::is_invocable_v<EFn, const std::exception&>>>
  void asyncConsume(folly::Executor::KeepAlive<> executor, int8_t priority,
                    RFn&& rfn, EFn&& efn) {
    std::move(contract_->second)
        .via(executor, priority)
        .thenValue([contract = contract_, rfn = std::move(rfn)](auto&& result) {
          rfn(std::move(result));
        })
        .thenError(
            folly::tag_t<std::exception>{},
            [contract = contract_, efn = std::move(efn)](auto&& e) { efn(e); });
  }

  void cancel(folly::FutureException&& e) noexcept {
    contract_->second.raise(e);
  }

 private:
  ContractPtr<R> contract_;
  folly::Executor::KeepAlive<> executor_;
  int8_t priority_;
};
