#include "BACH/file/FileReader.h"
#include "BACH/sstable/FileMetaData.h"

namespace BACH {
	FileReader::FileReader(const std::string& file_path, FileMetaData* _file_data) :
		file_data(_file_data), id(_file_data->identify),
		ref(util::make_ref(_file_data->identify, (idx_t)1))
	{
		//if (file_data != NULL)
		//	std::printf("new filereader for %s pointer is %x\n",
		//		file_data->file_name.data(), this);
		fd = open(file_path.c_str(), O_RDONLY);
		if (fd == -1)
		{
			std::cout << "cannot open file " << file_path << std::endl;
		}
		else
		{
			fstat(fd, &sb);
		}
	}

	FileReader::~FileReader() {
		//if (file_data != NULL)
		//	std::printf("delete ilereader for %s pointer is %x\n",
		//		file_data->file_name.data(), this);
		if (fd != -1) {
			close(fd);
			fd = -1;
		}
	}

	void FileReader::add_ref()
	{
		ref.fetch_add((ref_t)1);
		//std::cout << "filereader of " + file_data->file_name+ " ref++, now is" + std::to_string(ref) +"\n";
	}
	//#pragma GCC push_options
	//#pragma GCC optimize("O0")
	bool FileReader::dec_ref(identify_t id, bool deadby)
	{
		ref_t x = util::make_ref(id, util::unzip_ref_num(ref.load()));
		if (ref.compare_exchange_weak(x, x - 1))
		{
			std::printf("filereader of %s %x ref--, now is %d, from parser?:%d\n",
				file_data->file_name.data(), this, util::unzip_ref_num(x - 1), deadby);
			ref_t zero = util::make_ref(id, (idx_t)0);
			if (ref.compare_exchange_weak(zero, (ref_t)(-1)))
			{
				file_data->reader.store(NULL);
				file_data->reader_pos = -1;
				delete this;
			}
			return true;
		}
		else
		{
			std::printf("filereader of %s %x ref-- failed, from parser?:%d\n",
				file_data->file_name.data(), this, util::unzip_ref_num(x - 1), deadby);
			return false;
		}
		//int k = util::unzip_id(ref.load());
		//if (k != id)
		//{
		//	std::printf("err ref%d 0x%x\n", k, this);
		//	//std::cout << k << "error in dec_ref\n";
		//	//exit(0);
		//}


	}
	//#pragma GCC pop_options

	bool FileReader::fread(void* buf, int32_t count, int32_t offset) const {
		if (buf == nullptr) {
			return false;
		}
		if (fd == -1) {
			return false;
		}
		if (fcntl(fd, F_GETFL) == -1)
		{
			std::printf("fd: %d error! this %lx\n", fd, this);
			exit(-1);
		}
		auto cnt = pread(fd, buf, count, offset);
		if (cnt != count) {
			cnt = pread(fd, buf, count, offset);
			std::cout << errno << " read error\n";
			exit(-1);
			return false;
		}
		return true;
	}

	bool FileReader::rread(void* buf, int32_t count, int32_t offset) const {
		return fread(buf, count, sb.st_size - offset);
	}
}