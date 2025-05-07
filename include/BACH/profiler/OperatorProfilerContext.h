#pragma once
#include "BACH/profiler/profiler.h"

namespace BACH {
    class OperatorProfilerContext {
    public:
        // ���õ�ǰ�̵߳Ļ�Ծ���� Profiler����������ڴ����ã�
        static void SetCurrentProfiler(OperatorProfiler* profiler);

        // ��ȡ��ǰ�̵߳Ļ�Ծ���� Profiler���ڵײ㺯���ڵ��ã�
        static OperatorProfiler* GetCurrentProfiler();

    private:
        // �ֲ߳̾�������ÿ���̶߳���һ�ݶ����� current_profiler_
        static thread_local OperatorProfiler* current_profiler_;
    };
}