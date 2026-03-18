#include "apply_function.h"

#include <gtest/gtest.h>

#include <atomic>
#include <numeric>
#include <stdexcept>
#include <vector>

TEST(ApplyFunction, EmptyVector) {
    std::vector<int> data;
    ApplyFunction<int>(data, [](int& value) { value += 1; }, 4);
    EXPECT_TRUE(data.empty());
}

TEST(ApplyFunction, SingleThreadTransform) {
    std::vector<int> data{1, 2, 3, 4, 5};
    ApplyFunction<int>(data, [](int& value) { value *= value; }, 1);
    EXPECT_EQ(data, (std::vector<int>{1, 4, 9, 16, 25}));
}

TEST(ApplyFunction, MultiThreadTransform) {
    std::vector<int> data(10000);
    std::iota(data.begin(), data.end(), 0);
    ApplyFunction<int>(data, [](int& value) { value = value * 3 + 1; }, 4);

    for (std::size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ(data[i], static_cast<int>(i * 3 + 1));
    }
}

TEST(ApplyFunction, ThreadCountGreaterThanDataSize) {
    std::vector<int> data{10, 20, 30};
    std::atomic<int> calls{0};

    ApplyFunction<int>(data, [&calls](int& value) {
        ++calls;
        value += 5;
    }, 64);

    EXPECT_EQ(calls.load(), static_cast<int>(data.size()));
    EXPECT_EQ(data, (std::vector<int>{15, 25, 35}));
}

TEST(ApplyFunction, ExceptionFromTransformIsPropagated) {
    std::vector<int> data{1, 2, 3, 4, 5, 6, 7, 8};
    EXPECT_THROW(ApplyFunction<int>(data, [](int& value) {
        if (value == 5) {
            throw std::runtime_error("transform failed");
        }
        value += 10;
    }, 4), std::runtime_error);
}
