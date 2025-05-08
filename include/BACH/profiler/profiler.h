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
		void Start();                 // ��ʼ��ʱ
		void End();                   // ������ʱ

		void StartRead();             // һ�ζ�IO��ʼ
		void EndRead();               // һ�ζ�IO����

		void StartWrite();            // һ��дIO��ʼ
		void EndWrite();              // һ��дIO����

		double Elapsed() const;       // ��ʱ��
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
		// ���һ�����ӵ�ִ������
		void AddOperator(const std::string& op_name, const OperatorProfiler& op_profiler);

		// ��ȡĳ�����ӵ��ۼ���ʱ��/��д IO ʱ��
		double GetTotalTime(const std::string& op_name) const;
		double GetTotalReadTime(const std::string& op_name) const;
		double GetTotalWriteTime(const std::string& op_name) const;

		// ��ȡ���� p99 ���������
		const std::unordered_map<std::string, std::vector<double>>& GetLatencySamples() const;

	private:
		std::unordered_map<std::string, std::vector<double>> latency_samples_;
		std::unordered_map<std::string, double> total_time_;
		std::unordered_map<std::string, double> total_read_time_;
		std::unordered_map<std::string, double> total_write_time_;
	};

	class DBProfiler {
	public:
		// �ռ�����һ���߳�/�����ͳ������
		void AddThreadProfiler(const ThreadProfiler& profiler);

		// ��ȡÿ�����ӵ� p99 �ӳ٣���λ���룩
		std::unordered_map<std::string, double> GetP99Latency() const;

		// ��ȡ���ӵ�������ʱ�䡢��/д IO ʱ��
		double GetTotalTime(const std::string& op_name) const;
		double GetTotalReadTime(const std::string& op_name) const;
		double GetTotalWriteTime(const std::string& op_name) const;

		// ��ӡ��ǰ����ͳ������
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
