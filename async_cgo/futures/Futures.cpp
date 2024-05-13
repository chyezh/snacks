#include "Futures.h"

#include <folly/executors/ThreadedExecutor.h>
#include <folly/futures/Future.h>

#include <iostream>
#include <string>

int foo(int x) {
  // do something with x
  std::cout << "foo(" << x << ")" << std::endl;
  return x;
}

int main() {
  Future<int, std::function<int(int)>> future(foo);
  // future.run(48);
  future.cancel(std::runtime_error("cancelled"));
  try {
    auto result = future.get();
  } catch (const std::exception& e) {
    std::cout << "exception: " << e.what() << std::endl;
  }
}