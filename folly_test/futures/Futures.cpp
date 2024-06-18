#include <folly/executors/ThreadedExecutor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>

#include <iostream>

using namespace std;
using namespace folly;

void foo(int x)
{
    int y = x * 10;
    double result = x;
    for (int i = 0; i < y; i++)
    {
        result = result * 1.000001;
    }
    std::cout << "foo: " << int(result) << " at " << std::this_thread::get_id() << std::endl;
}

int main()
{
    CPUThreadPoolExecutor executor = CPUThreadPoolExecutor(
        std::pair<size_t, size_t>(2, 1),
        CPUThreadPoolExecutor::makeThrottledLifoSemQueue(std::chrono::nanoseconds(1000000)),
        std::make_shared<NamedThreadFactory>("CPUThreadPool"));
    std::cout << "making Promise" << " at " << std::this_thread::get_id() << std::endl;

    std::vector<Future<Unit>> futures;
    std::vector<Promise<int>> promises;
    for (int i = 0; i < 20; i++)
    {
        Promise<int> p;
        Future<int> f = p.getSemiFuture().via(&executor);
        auto f2 = move(f).thenValue(foo);
        promises.push_back(std::move(p));
        futures.push_back(std::move(f2));
    }

    for (int i = 0; i < 20; i++)
    {
        promises[i].setValue(i);
    }

    for (int i = 0; i < 20; i++)
    {
        futures[i].wait();
    }
}