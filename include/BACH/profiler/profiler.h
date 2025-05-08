#pragma once
#include <chrono>
#include <mutex>
#include <vector>
#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <numeric>
#include <iostream>

namespace BACH {
	// ?? git
	using std::chrono::duration;
	using std::chrono::duration_cast;
	using std::chrono::high_resolution_clock;
	using std::chrono::milliseconds;
	using std::chrono::system_clock;
	using std::chrono::time_point;

	template <typename T>
	class BaseProfiler {
	public:
		//! Starts the timer
		void Start() {
			finished = false;
			start = Tick();
		}
		//! Finishes timing
		void End() {
			end = Tick();
			finished = true;
		}

		//! Returns the elapsed time in seconds. If End() has been called, returns
		//! the total elapsed time. Otherwise returns how far along the timer is
		//! right now.
		double Elapsed() const {
			auto measured_end = finished ? end : Tick();
			return std::chrono::duration_cast<std::chrono::duration<double>>(measured_end - start).count();
		}

	private:
		time_point<T> Tick() const {
			return T::now();
		}
		time_point<T> start;
		time_point<T> end;
		bool finished = false;
	};

	using Profiler = BaseProfiler<system_clock>;

	// time the operator in transaction/compaction
	class OperatorProfiler {
	public:
		void Start();                 // 开始计时
		void End();                   // 结束计时

		void StartRead();             // 一次读IO开始
		void EndRead();               // 一次读IO结束

		void StartWrite();            // 一次写IO开始
		void EndWrite();              // 一次写IO结束

		double Elapsed() const;       // 总时间
		double TotalReadTime() const;
		double TotalWriteTime() const;

	private:
		using Clock = std::chrono::high_resolution_clock;
		using TimePoint = std::chrono::time_point<Clock>;

		TimePoint start_time_;
		TimePoint end_time_;
		bool finished_ = false;

		double total_read_time_ = 0.0;
		double total_write_time_ = 0.0;

		TimePoint read_start_;
		bool reading_ = false;

		TimePoint write_start_;
		bool writing_ = false;
	};

	class ThreadProfiler {
	public:
		// 添加一次算子的执行数据
		void AddOperator(const std::string& op_name, const OperatorProfiler& op_profiler);

		// 获取某个算子的累计总时间/读写 IO 时间
		double GetTotalTime(const std::string& op_name) const;
		double GetTotalReadTime(const std::string& op_name) const;
		double GetTotalWriteTime(const std::string& op_name) const;

		// 获取用于 p99 计算的样本
		const std::unordered_map<std::string, std::vector<double>>& GetLatencySamples() const;

	private:
		std::unordered_map<std::string, std::vector<double>> latency_samples_;
		std::unordered_map<std::string, double> total_time_;
		std::unordered_map<std::string, double> total_read_time_;
		std::unordered_map<std::string, double> total_write_time_;
	};

	class DBProfiler {
	public:
		// 收集来自一个线程/事务的统计数据
		void AddThreadProfiler(const ThreadProfiler& profiler);

		// 获取每个算子的 p99 延迟（单位：秒）
		std::unordered_map<std::string, double> GetP99Latency() const;

		// 获取算子的总运行时间、读/写 IO 时间
		double GetTotalTime(const std::string& op_name) const;
		double GetTotalReadTime(const std::string& op_name) const;
		double GetTotalWriteTime(const std::string& op_name) const;

		// 打印当前所有统计数据
		void PrintSummary() const;

	private:
		mutable std::mutex mutex_;
		std::unordered_map<std::string, std::vector<double>> latency_samples_;
		std::unordered_map<std::string, double> total_times_;
		std::unordered_map<std::string, double> total_read_;
		std::unordered_map<std::string, double> total_write_;

		double ComputeP99(const std::vector<double>& samples) const;
	};
}
