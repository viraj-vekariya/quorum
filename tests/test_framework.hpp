#pragma once
// Tiny dependency-free test harness: no gtest, just enough to run named
// checks and report pass/fail with a final summary and non-zero exit code
// on any failure (so `make test` fails CI-style, not just prints).

#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace testing {

inline int& FailCount() {
    static int n = 0;
    return n;
}

inline void Check(bool cond, const std::string& expr, const char* file, int line) {
    if (!cond) {
        std::cerr << "  FAIL: " << expr << " (" << file << ":" << line << ")\n";
        FailCount()++;
    }
}

struct TestCase {
    std::string name;
    std::function<void()> fn;
};

inline std::vector<TestCase>& Registry() {
    static std::vector<TestCase> r;
    return r;
}

struct Registrar {
    Registrar(const std::string& name, std::function<void()> fn) {
        Registry().push_back({name, std::move(fn)});
    }
};

inline int RunAll() {
    int passed = 0, failed = 0;
    for (auto& tc : Registry()) {
        int before = FailCount();
        std::cerr << "RUN  " << tc.name << "\n";
        tc.fn();
        if (FailCount() == before) {
            std::cerr << "PASS " << tc.name << "\n";
            passed++;
        } else {
            std::cerr << "FAIL " << tc.name << "\n";
            failed++;
        }
    }
    std::cerr << "\n" << passed << " passed, " << failed << " failed (" << (passed + failed)
               << " total)\n";
    return failed == 0 ? 0 : 1;
}

}  // namespace testing

#define TEST(name)                                                    \
    void test_##name();                                               \
    static testing::Registrar registrar_##name(#name, test_##name);   \
    void test_##name()

#define ASSERT_TRUE(cond) testing::Check((cond), #cond, __FILE__, __LINE__)
#define ASSERT_EQ(a, b) testing::Check((a) == (b), #a " == " #b, __FILE__, __LINE__)
#define ASSERT_GE(a, b) testing::Check((a) >= (b), #a " >= " #b, __FILE__, __LINE__)
