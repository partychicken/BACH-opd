#pragma once

#include <string>
#include <cstddef>
#include <cstdint>

namespace BACH
{
    class FileWriter final {
    private:
        static constexpr int32_t BUFFER_SIZE = 4096*16; //默认以 64 KB为单位进行批量写
        int32_t buffer_offset = 0; // 缓冲区的offset，即下一条数据的写入位置
        char buffer[BUFFER_SIZE]{}; // 缓冲区

        // 文件描述符
        int32_t fd;

    public:
        FileWriter(const std::string& file_path, bool append = false);

        ~FileWriter();

        // 将长度为len的data追加到文件中。
        // 默认情况下，是追加到buffer中（flush=false）。如果开启flush=true，则追加后立即刷盘(性能较低)。
        bool append(const char* data, int32_t len, bool flush = false);

        // flush: 底层是write，也就是将buf写到C库的缓冲区
        // C库中的flush指的是将C库的缓冲区刷到内核缓冲区，这里存在区别。
        // todo:语义存在缺陷，以后再弄
        bool flush();

        // sync底层是fsync，从内核缓冲区刷到磁盘
        void sync();

        void close();
    private:
        // 真正的持久化，由flush调用
        bool buf_persist(const char* data, int32_t len);

        // 检查buffer是否写满
        bool is_buffer_full() const {
            return buffer_offset == BUFFER_SIZE;
        }
    };
}