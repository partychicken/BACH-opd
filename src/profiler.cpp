#include "BACH/profiler/profiler.h"

namespace BACH {
	// OperatorProfiler
    void OperatorProfiler::Start() {
        finished_ = false;
        start_time_ = Clock::now();
    }

    void OperatorProfiler::End() {
        end_time_ = Clock::now();
        finished_ = true;
    }

    double OperatorProfiler::Elapsed() const {
        auto end = finished_ ? end_time_ : Clock::now();
        return std::chrono::duration_cast<std::chrono::duration<double>>(end - start_time_).count();
    }

    void OperatorProfiler::StartRead() {
        if (!reading_) {
            read_start_ = Clock::now();
            reading_ = true;
        }
    }

    void OperatorProfiler::EndRead() {
        if (reading_) {
            auto end = Clock::now();
            total_read_time_ += std::chrono::duration_cast<std::chrono::duration<double>>(end - read_start_).count();
            reading_ = false;
        }
    }

    void OperatorProfiler::StartWrite() {
        if (!writing_) {
            write_start_ = Clock::now();
            writing_ = true;
        }
    }

    void OperatorProfiler::EndWrite() {
        if (writing_) {
            auto end = Clock::now();
            total_write_time_ += std::chrono::duration_cast<std::chrono::duration<double>>(end - write_start_).count();
            writing_ = false;
        }
    }

    double OperatorProfiler::TotalReadTime() const {
        return total_read_time_;
    }

    double OperatorProfiler::TotalWriteTime() const {
        return total_write_time_;
    }


    // ThreadProfiler
    void ThreadProfiler::AddOperator(const std::string& op_name, const OperatorProfiler& op_profiler) {
        double elapsed = op_profiler.Elapsed();
        double read = op_profiler.TotalReadTime();
        double write = op_profiler.TotalWriteTime();

        latency_samples_[op_name].push_back(elapsed);
        total_time_[op_name] += elapsed;
        total_read_time_[op_name] += read;
        total_write_time_[op_name] += write;
    }

    double ThreadProfiler::GetTotalTime(const std::string& op_name) const {
        auto it = total_time_.find(op_name);
        return it != total_time_.end() ? it->second : 0.0;
    }

    double ThreadProfiler::GetTotalReadTime(const std::string& op_name) const {
        auto it = total_read_time_.find(op_name);
        return it != total_read_time_.end() ? it->second : 0.0;
    }

    double ThreadProfiler::GetTotalWriteTime(const std::string& op_name) const {
        auto it = total_write_time_.find(op_name);
        return it != total_write_time_.end() ? it->second : 0.0;
    }

    const std::unordered_map<std::string, std::vector<double>>& ThreadProfiler::GetLatencySamples() const {
        return latency_samples_;
    }


	// DBProfiler
    void DBProfiler::AddThreadProfiler(const ThreadProfiler& profiler) {
        std::lock_guard<std::mutex> lock(mutex_);

        auto thread_samples = profiler.GetLatencySamples();
        for (const auto& [op, samples] : thread_samples) {
            auto& vec = latency_samples_[op];
            vec.insert(vec.end(), samples.begin(), samples.end());

            total_times_[op] += profiler.GetTotalTime(op);
            total_read_[op] += profiler.GetTotalReadTime(op);
            total_write_[op] += profiler.GetTotalWriteTime(op);
        }
    }

    std::unordered_map<std::string, double> DBProfiler::GetP99Latency() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::unordered_map<std::string, double> result;
        for (const auto& [op, samples] : latency_samples_) {
            result[op] = ComputeP99(samples);
        }
        return result;
    }

    double DBProfiler::GetTotalTime(const std::string& op_name) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = total_times_.find(op_name);
        return it != total_times_.end() ? it->second : 0.0;
    }

    double DBProfiler::GetTotalReadTime(const std::string& op_name) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = total_read_.find(op_name);
        return it != total_read_.end() ? it->second : 0.0;
    }

    double DBProfiler::GetTotalWriteTime(const std::string& op_name) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = total_write_.find(op_name);
        return it != total_write_.end() ? it->second : 0.0;
    }

    double DBProfiler::ComputeP99(const std::vector<double>& samples) const {
        if (samples.empty()) return 0.0;
        size_t idx = static_cast<size_t>(0.99 * samples.size());
        std::vector<double> copy = samples;
        std::nth_element(copy.begin(), copy.begin() + idx, copy.end());
        return copy[idx];
    }

    void DBProfiler::PrintSummary() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << "=== DB Profiler Summary ===\n";
        for (const auto& [op, samples] : latency_samples_) {
            std::cout << "Operator: " << op << "\n";
            std::cout << "  Total time: " << total_times_.at(op) << "s\n";
            std::cout << "  Total read time: " << total_read_.at(op) << "s\n";
            std::cout << "  Total write time: " << total_write_.at(op) << "s\n";
            std::cout << "  p99 latency: " << ComputeP99(samples) << "s\n";
        }
    }

}