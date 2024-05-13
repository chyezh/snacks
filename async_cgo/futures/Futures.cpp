#include "Futures.h"

#include <folly/executors/ThreadedExecutor.h>
#include <folly/futures/Future.h>

#include <iostream>
#include <string>

int* foo() {
  int* x = new int{10076};
  // do something with x
  std::cout << "foo(" << *x << ")" << std::endl;
  throw std::runtime_error("foo error");
}

int main() {
  IFuture* future = new Future<int*, std::function<int*()>>(foo);

  future->run();
  // future->cancel(folly::FutureCancellation());
  try {
    auto result = future->get();
    std::cout << "result: " << *static_cast<int*>(result) << std::endl;
  } catch (const std::exception& e) {
    std::cout << "exception: " << e.what() << std::endl;
  }
}