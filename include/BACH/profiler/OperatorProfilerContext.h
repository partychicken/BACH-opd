#pragma once
#include "BACH/profiler/profiler.h"

namespace BACH {
    class OperatorProfilerContext {
    public:
        // 设置当前线程的活跃算子 Profiler（在算子入口处调用）
        static void SetCurrentProfiler(OperatorProfiler* profiler);

        // 获取当前线程的活跃算子 Profiler（在底层函数内调用）
        static OperatorProfiler* GetCurrentProfiler();

    private:
        // 线程局部变量，每个线程都有一份独立的 current_profiler_
        static thread_local OperatorProfiler* current_profiler_;
    };
}