#pragma once
#include "BACH/profiler/profiler.h"


namespace BACH {
    class ThreadProfilerContext {
    public:
        static void SetCurrent(ThreadProfiler* profiler);
        static ThreadProfiler* GetCurrent();

    private:
        static thread_local ThreadProfiler* current_profiler_;
    };

}