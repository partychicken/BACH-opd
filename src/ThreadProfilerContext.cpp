#include "BACH/profiler/ThreadProfilerContext.h"

namespace BACH {

    thread_local ThreadProfiler* ThreadProfilerContext::current_profiler_ = nullptr;

    void ThreadProfilerContext::SetCurrent(ThreadProfiler* profiler) {
        current_profiler_ = profiler;
    }

    ThreadProfiler* ThreadProfilerContext::GetCurrent() {
        return current_profiler_;
    }

}