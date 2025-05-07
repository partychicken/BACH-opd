#include "BACH/profiler/OperatorProfilerContext.h"

namespace BACH {

    thread_local OperatorProfiler* OperatorProfilerContext::current_profiler_ = nullptr;

    void OperatorProfilerContext::SetCurrentProfiler(OperatorProfiler* profiler) {
        current_profiler_ = profiler;
    }

    OperatorProfiler* OperatorProfilerContext::GetCurrentProfiler() {
        return current_profiler_;
    }


}