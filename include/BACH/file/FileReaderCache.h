#pragma once

#include <iostream>
#include <memory>
#include <shared_mutex>
#include <string_view>
#include <unordered_map>
#include "FileReader.h"
#include "BACH/utils/Options.h"

namespace BACH
{
	class FileReaderCache
	{
	public:
		FileReaderCache(int32_t _max_size, std::string _prefix) :
			max_size(_max_size), prefix(_prefix) {}
		std::shared_ptr<FileReader> find(std::string file_name)
		{
			std::shared_lock<std::shared_mutex> lock(mutex);
			auto it = cache.find(file_name);
			if (it != cache.end())
			{
				return it->second;
			}
			lock.unlock();
			return add(file_name);
		}
		std::shared_ptr<FileReader> add(std::string file_name)
		{
			std::unique_lock<std::shared_mutex> lock(mutex);
			if (size >= max_size)
			{
				for(auto &i:cache)
					i.second.reset();
				cache.clear();
				size = 0;
			}
			auto x = std::make_shared<FileReader>(prefix + file_name);
			cache[file_name] = x;
			size++;
			//std::cout << size << std::endl;
			return x;
		}
	private:
		std::unordered_map<std::string, std::shared_ptr<FileReader>> cache;
		std::shared_mutex mutex;
		int32_t max_size, size = 0;
		std::string prefix;
	};
}