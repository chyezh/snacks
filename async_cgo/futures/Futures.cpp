#include "Futures.h"

#include <folly/executors/ThreadedExecutor.h>
#include <folly/futures/Future.h>

#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <variant>

#include "FutureWithResult.h"

int main() {
  std::mutex locker;
  locker.lock();

  auto future = new FutureWithResult<int, std::function<void()>>(
      [&locker]() { locker.unlock(); });

  future->asyncProduce([]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    int x = 10076;
    // do something with x
    std::cout << "foo(" << x << ")" << std::endl;
    return x;
  });

  future->asyncConsumeResult();
  // future->asyncConsume([](int r) {}, [](const std::exception& error) {});
  //     [](int x) { std::cout << "consume(" << x << ")" << std::endl; },
  //     [](std::exception& e) {
  //       std::cout << "exception: " << e.what() << std::endl;
  //     });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  future->cancel(folly::FutureCancellation());

  // future->asyncConsumeResult();
  locker.lock();

  auto result = future->get();

  try {
    std::cout << std::get<int>(*result) << std::endl;
  } catch (const std::exception& e) {
    std::cout << e.what() << std::endl;
  }

  try {
    std::cout << std::get<std::exception>(*result).what() << std::endl;
  } catch (const std::exception& e) {
    std::cout << e.what() << std::endl;
  }

  delete future;
  return 0;
}