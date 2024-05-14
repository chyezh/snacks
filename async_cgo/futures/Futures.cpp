#include "Futures.h"

#include <folly/executors/ThreadedExecutor.h>
#include <folly/futures/Future.h>

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

int main() {
  auto future = new Future<int*>();

  future->asyncProduce([]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    int* x = new int{10076};
    // do something with x
    std::cout << "foo(" << *x << ")" << std::endl;
    return x;
  });

  future->asyncConsume(
      [](int* x) { std::cout << "consume(" << *x << ")" << std::endl; },
      [](std::exception& e) {
        std::cout << "exception: " << e.what() << std::endl;
      });

  future->cancel(folly::FutureCancellation());

  std::cout << "main thread is running..." << std::endl;
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  std::cout << "main thread is done..." << std::endl;

  delete future;
  return 0;
}