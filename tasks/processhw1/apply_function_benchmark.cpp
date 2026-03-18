#include "apply_function.h"

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <functional>
#include <thread>
#include <vector>

namespace {

int MultiThreadCount() {
    const int hc = static_cast<int>(std::thread::hardware_concurrency());
    if (hc <= 0) {
        return 2;
    }
    return std::max(2, std::min(4, hc));
}

void FastTransform(int& x) {
    ++x;
}

void SlowTransform(int& x) {
    volatile std::uint64_t acc = static_cast<std::uint64_t>(x);
    for (int i = 0; i < 5000; ++i) {
        acc = acc * 1664525u + 1013904223u;
        acc ^= (acc >> 13);
    }
    x = static_cast<int>(acc & 0x7fffffff);
}

const int kMultiThreadCount = MultiThreadCount();

void RunBenchmark(
    benchmark::State& state,
    int thread_count,
    const std::function<void(int&)>& transform) {
    const std::size_t size = static_cast<std::size_t>(state.range(0));
    const std::vector<int> baseline(size, 1);

    for (auto _ : state) {
        std::vector<int> data = baseline;
        ApplyFunction<int>(data, transform, thread_count);
        benchmark::DoNotOptimize(data);
        benchmark::ClobberMemory();
    }
}

void BM_FastTransformSingleThread(benchmark::State& state) {
    static const std::function<void(int&)> kTransform = FastTransform;
    RunBenchmark(state, 1, kTransform);
}
BENCHMARK(BM_FastTransformSingleThread)
    ->UseRealTime()
    ->Unit(benchmark::kMicrosecond)
    ->Arg(32)
    ->Arg(128)
    ->Arg(512);

void BM_FastTransformMultiThread(benchmark::State& state) {
    static const std::function<void(int&)> kTransform = FastTransform;
    RunBenchmark(state, kMultiThreadCount, kTransform);
}
BENCHMARK(BM_FastTransformMultiThread)
    ->UseRealTime()
    ->Unit(benchmark::kMicrosecond)
    ->Arg(32)
    ->Arg(128)
    ->Arg(512);

void BM_SlowTransformSingleThread(benchmark::State& state) {
    static const std::function<void(int&)> kTransform = SlowTransform;
    RunBenchmark(state, 1, kTransform);
}
BENCHMARK(BM_SlowTransformSingleThread)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond)
    ->Arg(10000)
    ->Arg(50000)
    ->Arg(100000);

void BM_SlowTransformMultiThread(benchmark::State& state) {
    static const std::function<void(int&)> kTransform = SlowTransform;
    RunBenchmark(state, kMultiThreadCount, kTransform);
}
BENCHMARK(BM_SlowTransformMultiThread)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond)
    ->Arg(10000)
    ->Arg(50000)
    ->Arg(100000);

}  // namespace

BENCHMARK_MAIN();
