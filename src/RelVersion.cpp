#include "BACH/sstable/RelVersion.h"
#include "BACH/db/DB.h"

namespace BACH {
    RelVersion::RelVersion(DB *_db) : next(nullptr), epoch(0), next_epoch(-1), ref(2), db(_db) {
    }

    RelVersion::RelVersion(RelVersion *_prev, VersionEdit *edit, time_t time) : next(nullptr),
        epoch(std::max(_prev->epoch, time)), next_epoch(-1),
        db(_prev->db) {
        FileIndex = _prev->FileIndex;
        FileTotalSize = _prev->FileTotalSize;
        for (auto i: edit->EditFileList) {
            if (i->deletion) {
                auto x = std::lower_bound(FileIndex[i->level].begin(),
                                          FileIndex[i->level].end(), i, RelFileCompare);
                if ((*x)->file_id != i->file_id) {
                    //error
                    std::cout << "delete a file that not exist" << std::endl;
                    exit(-1);
                }
                FileIndex[i->level].erase(x);
                FileTotalSize[i->level] -= i->file_size;
            } else {
                if (FileIndex.size() <= i->level)
                    FileIndex.resize(i->level + 2),
                    FileTotalSize.resize(i->level + 2);
                auto x = std::upper_bound(FileIndex[i->level].begin(),
                                          FileIndex[i->level].end(), i, RelFileCompare);
                auto f = new RelFileMetaData(*static_cast<RelFileMetaData<std::string> *>(i));
                FileIndex[i->level].insert(x, f);
                FileTotalSize[i->level] += i->file_size;
            }
        }
        for (auto &i: FileIndex)
            for (auto &j: i)
                j->ref.fetch_add(1, std::memory_order_relaxed);
        _prev->next = this;
        _prev->next_epoch = epoch;
    }

    RelVersion::~RelVersion() {
        if (size_entry != nullptr)
            size_entry->delete_entry();
        for (auto &i: FileIndex)
            for (auto &j: i) {
                auto r = j->ref.fetch_add(-1);
                if (r == 1) {
                    unlink((db->options->STORAGE_DIR + "/"
                            + j->file_name).c_str());
                    //if(k->filter->size() == util::ClacFileSize(db->options, k->level))
                    delete j->filter;
                    delete j->reader;
                    db->ReaderCaches->deletecache(j);
                    delete j;
                }
            }
    }

    RelCompaction<std::string> *RelVersion::GetCompaction(VersionEdit *edit, bool force_level) {
        RelFileMetaData<std::string> *begin_file_meta = static_cast<RelFileMetaData<std::string> *>(*(edit->EditFileList
            .begin()));
        idx_t level = begin_file_meta->level;
        // the level which adds a new file
        RelCompaction<std::string> *c = NULL;
        RelFileMetaData<std::string> *down_file_meta;
        if (level == 0) {
            if (FileIndex[level].size() <= db->options->ZERO_LEVEL_FILES) {
                return c;
            }
            int down_fileid = 0;
            do {
                down_fileid = rand() % db->options->ZERO_LEVEL_FILES;
                down_file_meta = static_cast<RelFileMetaData<std::string> *>(FileIndex[level][down_fileid]);
            } while (down_file_meta->merging);
        } else {
            if (level == db->options->MAX_LEVEL - 1) {
                return c;
            }
            if (FileIndex[level].size() <= util::fast_pow(10, level - 1) * 4) {
                return c;
            }
            size_t edit_size = edit->EditFileList.size();
            if (edit_size & 1) {
                std::cerr << "FUCK edit size" << std::endl;
                return c;
            }
            int down_fileid = 0;
            do {
                down_fileid = rand() % FileIndex[level].size();
                down_file_meta = static_cast<RelFileMetaData<std::string> *>(FileIndex[level][down_fileid]);
            } while (down_file_meta->merging);
        }

        std::string key_min = down_file_meta->key_min;
        std::string key_max = down_file_meta->key_max;
        //specialize the first down file
        if (FileIndex[level + 1].size() == 0) {
            c = new RelCompaction<std::string>();
            c->file_list.push_back(down_file_meta);
            c->key_min = key_min;
            c->target_level = level + 1;
            return c;
        }

        //find the last file, whose key_min < current key_min
        auto iter1 = std::lower_bound(FileIndex[level + 1].begin(),
                                      FileIndex[level + 1].end(),
                                      std::make_pair(key_min, (idx_t) 0),
                                      RelFileCompareWithPair);
        if (iter1 != FileIndex[level + 1].begin()) iter1--;

        //find the first file, whose key_min > current key_max
        //thus, the file previous is the last one to compact
        auto iter2 = std::lower_bound(FileIndex[level + 1].begin(),
                                      FileIndex[level + 1].end(),
                                      std::make_pair(key_max, (idx_t) 0),
                                      RelFileCompareWithPair);
        if (iter2 != FileIndex[level + 1].end()) iter2++;
        if (c == nullptr)
            c = new RelCompaction<std::string>();
        c->file_list.push_back(down_file_meta);
        down_file_meta->merging = true;
        c->key_min = min(key_min, static_cast<RelFileMetaData<std::string> *>(*iter1)->key_min);
        c->target_level = level + 1;
        for (auto i = iter1; i != iter2; ++i)
            if (!(*i)->merging) {
                c->file_list.push_back(*i);
                (*i)->merging = true;
            }
        return c;
    }

    bool RelVersion::AddRef() {
        idx_t k;
        do {
            k = ref.load();
            if (k == 0)
                return false;
        } while (!ref.compare_exchange_weak(k, k + 1));
        return true;
    }

    void RelVersion::DecRef() {
        auto k = ref.fetch_add(-1);
        if (k == 1)
            delete this;
    }

    void RelVersion::AddSizeEntry(std::shared_ptr<relMemTable> x) {
        size_entry = x;
    }

    RelVersionIterator::RelVersionIterator(RelVersion *_version, std::string _key_min, std::string _key_max)
        : version(_version), key_min(_key_min), key_max(_key_max), file_size(1),
          size(version->db->options->MEMORY_MERGE_NUM) {
        nextlevel();
    }

    FileMetaData *RelVersionIterator::GetFile() const {
        if (end)
            return nullptr;
        return version->FileIndex[level][idx];
    }

    void RelVersionIterator::next() {
        if (end)
            return;
        if (level == 0 && idx < version->FileIndex[level].size() - 1) {
            ++idx;
        }
        if (idx == version->FileIndex[level].size() - 1 ||
            (static_cast<RelFileMetaData<std::string> *>(version->FileIndex[level][++idx])->key_min > key_max))
            nextlevel();
    }

    void RelVersionIterator::nextlevel() {
        ++level;
        while (level < version->FileIndex.size() && !findsrc()) {
            ++level;
        }
        if (level == version->FileIndex.size())
            end = true;
    }

    bool RelVersionIterator::findsrc() {
        if (version->FileIndex[level].empty())
            return false;
        if (level == 0) {
            for (auto x: version->FileIndex[level]) {
                if (static_cast<RelFileMetaData<std::string> *>(x)->key_min > key_max
                    || static_cast<RelFileMetaData<std::string> *>(x)->key_max < key_min) {
                    continue;
                }
                return true;
            }
            return false;
        }
        auto x = std::lower_bound(version->FileIndex[level].begin(),
                                  version->FileIndex[level].end(),
                                  std::make_pair(key_min, 0),
                                  RelFileCompareWithPair);
        if (x == version->FileIndex[level].begin())
            return false;
        idx = x - version->FileIndex[level].begin() - 1;
        if (static_cast<RelFileMetaData<std::string> *>(version->FileIndex[level][idx])->key_min > key_max
            || static_cast<RelFileMetaData<std::string> *>(version->FileIndex[level][idx])->key_max < key_min)
            return false;
        return true;
    }

    bool RelFileCompareWithPair(FileMetaData *lhs, const std::pair<std::string, idx_t> &rhs) {
        auto rlhs = static_cast<RelFileMetaData<std::string> *>(lhs);
        //return rlhs->key_min == rhs.first ? rlhs->file_id < rhs.second : rlhs->key_min < rhs.first;
        return rlhs->key_min < rhs.first;
    }

    bool RelFileCompareWithPairUpper(const std::pair<std::string, idx_t> &rhs, FileMetaData *lhs) {
        auto rlhs = static_cast<RelFileMetaData<std::string> *>(lhs);
        //return rlhs->key_min == rhs.first ? rlhs->file_id < rhs.second : rlhs->key_min < rhs.first;
        return rlhs->key_min < rhs.first;
    }

    bool RelFileCompare(FileMetaData *lhs, FileMetaData *rhs) {
        auto rlhs = static_cast<RelFileMetaData<std::string> *>(lhs);
        auto rrhs = static_cast<RelFileMetaData<std::string> *>(rhs);
        return rlhs->key_min < rrhs->key_min;
    }
}
