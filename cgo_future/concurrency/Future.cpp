#include <folly/executors/ThreadedExecutor.h>
#include <folly/futures/Future.h>

#include <iostream>

void foo(int x) {
  // do something with x
  std::cout << "foo(" << x << ")" << std::endl;
}

int main() {
  try {
    folly::Promise<int> p;
    p.setInterruptHandler(
        [](auto&& e) { std::cout << "interrupted:" << e << std::endl; });
    folly::Future<int> f = p.getSemiFuture()
                               .via(folly::getGlobalCPUExecutor())
                               .onTimeout(std::chrono::seconds(1), []() {
                                 std::cout << "timeout" << std::endl;
                                 return 42;
                               });
    f.cancel();
    auto result = std::move(f).get();
    std::cout << "result: " << result << std::endl;
  } catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
  }
}