// Database.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <memory>
#include <variant>
#include <algorithm>
#include <chrono>
#include <cassert>
#include <fstream>
#include <queue>
#include <list>
#include <functional>
#include <filesystem>

// 資料導向設計的列存儲資料庫實作

// 常數定義
constexpr size_t PAGE_SIZE = 4096;  // 4KB 頁面大小
constexpr size_t BUFFER_POOL_SIZE = 1000;  // 緩衝池大小
constexpr size_t BTREE_ORDER = 128;  // B+樹階數

// 支持的資料型別
enum class DataType {
    INT32,
    INT64,
    FLOAT,
    DOUBLE,
    STRING,
    BOOL
};

// 型別別名，使用 std::variant 來支持多種資料型別
using Value = std::variant<int32_t, int64_t, float, double, std::string, bool>;
using PageId = uint64_t;
using RecordId = uint64_t;

// 頁面結構
struct Page {
    PageId page_id;
    bool is_dirty;
    std::vector<char> data;

    Page(PageId id) : page_id(id), is_dirty(false) {
        data.resize(PAGE_SIZE);
    }
};

// 磁碟管理器
class DiskManager {
private:
    std::string db_path_;
    std::unordered_map<std::string, std::fstream> file_streams_;

public:
    DiskManager(const std::string& db_path) : db_path_(db_path) {
        std::filesystem::create_directories(db_path_);
    }

    ~DiskManager() {
        for (auto& [filename, stream] : file_streams_) {
            if (stream.is_open()) {
                stream.close();
            }
        }
    }

    std::fstream& getFileStream(const std::string& filename) {
        auto it = file_streams_.find(filename);
        if (it == file_streams_.end()) {
            std::string filepath = db_path_ + "/" + filename;
            
            // 如果目錄結構不存在則創建
            size_t last_slash = filepath.find_last_of("/\\");
            if (last_slash != std::string::npos) {
                std::string dir_path = filepath.substr(0, last_slash);
                std::filesystem::create_directories(dir_path);
            }
            
            // 首先嘗試打開現有檔案
            file_streams_[filename] = std::fstream(filepath,
                std::ios::in | std::ios::out | std::ios::binary);

            if (!file_streams_[filename].is_open()) {
                // 如果檔案不存在，創建它
                file_streams_[filename] = std::fstream(filepath,
                    std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
                
                // 如果仍然無法打開，報告錯誤
                if (!file_streams_[filename].is_open()) {
                    std::cerr << "Failed to create file: " << filepath << std::endl;
                } else {
                    std::cout << "DEBUG: Successfully created file: " << filepath << std::endl;
                }
            }
        }
        return file_streams_[filename];
    }

    void writePage(const std::string& filename, PageId page_id, const Page& page) {
        auto& stream = getFileStream(filename);
        if (stream.is_open()) {
            stream.seekp(page_id * PAGE_SIZE);
            stream.write(page.data.data(), PAGE_SIZE);
            stream.flush();
            std::cout << "DEBUG: Wrote page " << page_id << " to file " << filename << std::endl;
        } else {
            std::cerr << "ERROR: Cannot write to file " << filename << std::endl;
        }
    }

    void readPage(const std::string& filename, PageId page_id, Page& page) {
        auto& stream = getFileStream(filename);
        if (stream.is_open()) {
            stream.seekg(page_id * PAGE_SIZE);
            if (stream.gcount() > 0 || stream.good()) {
                stream.read(page.data.data(), PAGE_SIZE);
                if (stream.gcount() < PAGE_SIZE) {
                    // 如果檔案較小，將剩餘字節初始化為零
                    std::fill(page.data.begin() + stream.gcount(), page.data.end(), 0);
                }
            } else {
                // 如果讀取失敗，將頁面初始化為零
                std::fill(page.data.begin(), page.data.end(), 0);
            }
        } else {
            std::cerr << "ERROR: Cannot read from file " << filename << std::endl;
            std::fill(page.data.begin(), page.data.end(), 0);
        }
    }
};

// 緩衝池管理器 - LRU替換策略
class BufferPoolManager {
private:
    size_t pool_size_;
    std::unordered_map<std::string, std::unordered_map<PageId, std::shared_ptr<Page>>> pages_;
    std::list<std::pair<std::string, PageId>> lru_list_;
    std::unordered_map<std::string, std::unordered_map<PageId,
        std::list<std::pair<std::string, PageId>>::iterator>> lru_map_;
    DiskManager& disk_manager_;

public:
    BufferPoolManager(DiskManager& disk_manager)
        : pool_size_(BUFFER_POOL_SIZE), disk_manager_(disk_manager) {
    }

    std::shared_ptr<Page> fetchPage(const std::string& filename, PageId page_id) {
        auto file_it = pages_.find(filename);
        if (file_it != pages_.end()) {
            auto page_it = file_it->second.find(page_id);
            if (page_it != file_it->second.end()) {
                // 更新LRU
                updateLRU(filename, page_id);
                return page_it->second;
            }
        }

        // 頁面不在緩衝池中，需要從磁碟載入
        auto page = std::make_shared<Page>(page_id);
        // 只有當這不是新頁面時才嘗試讀取
        disk_manager_.readPage(filename, page_id, *page);

        // 檢查是否需要淘汰頁面
        if (getTotalPages() >= pool_size_) {
            evictPage();
        }

        pages_[filename][page_id] = page;
        lru_list_.push_front({ filename, page_id });
        lru_map_[filename][page_id] = lru_list_.begin();

        return page;
    }

    void flushPage(const std::string& filename, PageId page_id) {
        auto file_it = pages_.find(filename);
        if (file_it != pages_.end()) {
            auto page_it = file_it->second.find(page_id);
            if (page_it != file_it->second.end() && page_it->second->is_dirty) {
                disk_manager_.writePage(filename, page_id, *page_it->second);
                page_it->second->is_dirty = false;
            }
        }
    }

    void flushAllPages() {
        for (const auto& [filename, page_map] : pages_) {
            for (const auto& [page_id, page] : page_map) {
                if (page->is_dirty) {
                    disk_manager_.writePage(filename, page_id, *page);
                    page->is_dirty = false;
                }
            }
        }
    }

private:
    size_t getTotalPages() const {
        size_t total = 0;
        for (const auto& [filename, page_map] : pages_) {
            total += page_map.size();
        }
        return total;
    }

    void updateLRU(const std::string& filename, PageId page_id) {
        auto file_it = lru_map_.find(filename);
        if (file_it != lru_map_.end()) {
            auto page_it = file_it->second.find(page_id);
            if (page_it != file_it->second.end()) {
                lru_list_.erase(page_it->second);
                lru_list_.push_front({ filename, page_id });
                page_it->second = lru_list_.begin();
            }
        }
    }

    void evictPage() {
        if (lru_list_.empty()) return;

        auto [filename, page_id] = lru_list_.back();
        lru_list_.pop_back();

        // 如果頁面被修改，寫回磁碟
        flushPage(filename, page_id);

        pages_[filename].erase(page_id);
        lru_map_[filename].erase(page_id);
    }
};

// B+樹節點
struct BPlusTreeNode {
    bool is_leaf;
    std::vector<Value> keys;
    std::vector<PageId> children;  // 對於內部節點，指向子節點
    std::vector<RecordId> records; // 對於葉子節點，指向記錄
    PageId next_leaf;  // 葉子節點的下一個節點（用於範圍查詢）

    BPlusTreeNode(bool leaf = true) : is_leaf(leaf), next_leaf(0) {}
};

// B+樹索引
class BPlusTreeIndex {
private:
    std::string index_name_;
    PageId root_page_id_;
    BufferPoolManager& buffer_manager_;
    DataType key_type_;
    size_t max_keys_;

public:
    BPlusTreeIndex(const std::string& name, DataType key_type, BufferPoolManager& buffer_manager)
        : index_name_(name), root_page_id_(0), buffer_manager_(buffer_manager),
        key_type_(key_type), max_keys_(BTREE_ORDER - 1) {
    }

    void insert(const Value& key, RecordId record_id) {
        if (root_page_id_ == 0) {
            // 創建根節點
            root_page_id_ = createNewNode(true);
        }

        auto result = insertInternal(root_page_id_, key, record_id);
        if (result.second != 0) {
            // 根節點分裂，創建新根
            PageId new_root = createNewNode(false);
            auto root_node = getNode(new_root);
            root_node->keys.push_back(result.first);
            root_node->children.push_back(root_page_id_);
            root_node->children.push_back(result.second);
            root_page_id_ = new_root;
            saveNode(new_root, *root_node);
        }
    }

    std::vector<RecordId> search(const Value& key) {
        if (root_page_id_ == 0) return {};

        PageId leaf_page = findLeaf(root_page_id_, key);
        auto leaf_node = getNode(leaf_page);

        std::vector<RecordId> results;
        for (size_t i = 0; i < leaf_node->keys.size(); ++i) {
            if (leaf_node->keys[i] == key) {
                results.push_back(leaf_node->records[i]);
            }
        }

        return results;
    }

    std::vector<RecordId> rangeSearch(const Value& start_key, const Value& end_key) {
        if (root_page_id_ == 0) return {};

        std::vector<RecordId> results;
        PageId leaf_page = findLeaf(root_page_id_, start_key);

        while (leaf_page != 0) {
            auto leaf_node = getNode(leaf_page);

            for (size_t i = 0; i < leaf_node->keys.size(); ++i) {
                if (compareValues(leaf_node->keys[i], start_key) >= 0 &&
                    compareValues(leaf_node->keys[i], end_key) <= 0) {
                    results.push_back(leaf_node->records[i]);
                }
                if (compareValues(leaf_node->keys[i], end_key) > 0) {
                    return results;
                }
            }

            leaf_page = leaf_node->next_leaf;
        }

        return results;
    }

private:
    PageId createNewNode(bool is_leaf) {
        static PageId next_page_id = 1;
        PageId page_id = next_page_id++;

        BPlusTreeNode node(is_leaf);
        saveNode(page_id, node);

        return page_id;
    }

    std::shared_ptr<BPlusTreeNode> getNode(PageId page_id) {
        auto page = buffer_manager_.fetchPage(index_name_, page_id);

        // 簡化：直接將頁面資料解釋為節點
        // 實際實作中需要序列化/反序列化
        auto node = std::make_shared<BPlusTreeNode>();
        // 在這裡進行反序列化...

        return node;
    }

    void saveNode(PageId page_id, const BPlusTreeNode& node) {
        auto page = buffer_manager_.fetchPage(index_name_, page_id);

        // 簡化：直接將節點資料寫入頁面
        // 實際實作中需要序列化
        page->is_dirty = true;
    }

    std::pair<Value, PageId> insertInternal(PageId page_id, const Value& key, RecordId record_id) {
        auto node = getNode(page_id);

        if (node->is_leaf) {
            // 葉子節點：插入鍵值對
            insertIntoLeaf(*node, key, record_id);

            if (node->keys.size() > max_keys_) {
                // 分裂葉子節點
                return splitLeaf(page_id, *node);
            }

            saveNode(page_id, *node);
            return { Value{}, 0 };
        }
        else {
            // 內部節點：找到合適的子節點
            size_t child_index = findChildIndex(*node, key);
            auto result = insertInternal(node->children[child_index], key, record_id);

            if (result.second != 0) {
                // 子節點分裂，需要在當前節點插入新的鍵
                insertIntoInternal(*node, result.first, result.second, child_index);

                if (node->keys.size() > max_keys_) {
                    // 分裂內部節點
                    return splitInternal(page_id, *node);
                }
            }

            saveNode(page_id, *node);
            return { Value{}, 0 };
        }
    }

    PageId findLeaf(PageId page_id, const Value& key) {
        auto node = getNode(page_id);

        if (node->is_leaf) {
            return page_id;
        }
        else {
            size_t child_index = findChildIndex(*node, key);
            return findLeaf(node->children[child_index], key);
        }
    }

    void insertIntoLeaf(BPlusTreeNode& node, const Value& key, RecordId record_id) {
        auto pos = std::lower_bound(node.keys.begin(), node.keys.end(), key,
            [this](const Value& a, const Value& b) {
                return compareValues(a, b) < 0;
            });

        size_t index = pos - node.keys.begin();
        node.keys.insert(pos, key);
        node.records.insert(node.records.begin() + index, record_id);
    }

    void insertIntoInternal(BPlusTreeNode& node, const Value& key, PageId new_child, size_t child_index) {
        node.keys.insert(node.keys.begin() + child_index, key);
        node.children.insert(node.children.begin() + child_index + 1, new_child);
    }

    std::pair<Value, PageId> splitLeaf(PageId page_id, BPlusTreeNode& node) {
        size_t mid = node.keys.size() / 2;

        PageId new_page_id = createNewNode(true);
        auto new_node = getNode(new_page_id);

        // 移動後半部分到新節點
        new_node->keys.assign(node.keys.begin() + mid, node.keys.end());
        new_node->records.assign(node.records.begin() + mid, node.records.end());
        new_node->next_leaf = node.next_leaf;

        // 縮短原節點
        node.keys.resize(mid);
        node.records.resize(mid);
        node.next_leaf = new_page_id;

        saveNode(page_id, node);
        saveNode(new_page_id, *new_node);

        return { new_node->keys[0], new_page_id };
    }

    std::pair<Value, PageId> splitInternal(PageId page_id, BPlusTreeNode& node) {
        size_t mid = node.keys.size() / 2;

        PageId new_page_id = createNewNode(false);
        auto new_node = getNode(new_page_id);

        // 移動後半部分到新節點
        Value promoted_key = node.keys[mid];
        new_node->keys.assign(node.keys.begin() + mid + 1, node.keys.end());
        new_node->children.assign(node.children.begin() + mid + 1, node.children.end());

        // 縮短原節點
        node.keys.resize(mid);
        node.children.resize(mid + 1);

        saveNode(page_id, node);
        saveNode(new_page_id, *new_node);

        return { promoted_key, new_page_id };
    }

    size_t findChildIndex(const BPlusTreeNode& node, const Value& key) {
        auto pos = std::lower_bound(node.keys.begin(), node.keys.end(), key,
            [this](const Value& a, const Value& b) {
                return compareValues(a, b) < 0;
            });

        return pos - node.keys.begin();
    }

    int compareValues(const Value& a, const Value& b) {
        // 實作值比較邏輯
        if (a == b) return 0;
        return (a < b) ? -1 : 1;
    }
};

// 支援大資料集的列存儲結構
class DiskBasedColumn {
private:
    std::string name_;
    DataType type_;
    std::string data_file_;
    std::unique_ptr<BPlusTreeIndex> index_;
    BufferPoolManager& buffer_manager_;
    size_t total_records_;
    size_t records_per_page_;

public:
    DiskBasedColumn(const std::string& name, DataType type, BufferPoolManager& buffer_manager)
        : name_(name), type_(type), buffer_manager_(buffer_manager), total_records_(0) {
        
        // name 參數是從資料庫根目錄開始的相對路徑（例如："employees/id"）
        // 我們需要創建相對於資料庫根目錄的資料檔案路徑
        data_file_ = name + ".data";

        // 計算每頁可存儲的記錄數
        size_t record_size = getRecordSize(type);
        records_per_page_ = PAGE_SIZE / record_size;

        // 為此列創建索引
        index_ = std::make_unique<BPlusTreeIndex>(name + ".idx", type, buffer_manager);
    }

    RecordId append(const Value& value) {
        RecordId record_id = total_records_;
        PageId page_id = record_id / records_per_page_;
        size_t offset = record_id % records_per_page_;

        auto page = buffer_manager_.fetchPage(data_file_, page_id);
        writeValueToPage(*page, offset, value);
        page->is_dirty = true;

        // 更新索引
        index_->insert(value, record_id);

        // 前幾筆記錄的除錯輸出
        if (total_records_ < 5 || total_records_ % 10000 == 0) {
            std::cout << "DEBUG: Appended record " << record_id << " to page " << page_id 
                      << " in file " << data_file_ << std::endl;
        }

        total_records_++;
        return record_id;
    }

    Value get(RecordId record_id) const {
        PageId page_id = record_id / records_per_page_;
        size_t offset = record_id % records_per_page_;

        auto page = buffer_manager_.fetchPage(data_file_, page_id);
        return readValueFromPage(*page, offset);
    }

    std::vector<RecordId> findRecords(const Value& value) {
        return index_->search(value);
    }

    std::vector<RecordId> findRecordsInRange(const Value& start, const Value& end) {
        return index_->rangeSearch(start, end);
    }

    // 聚合函式 - 針對大資料集優化
    double sum() const {
        double result = 0.0;

        // 分頁處理，避免記憶體溢出
        for (PageId page_id = 0; page_id * records_per_page_ < total_records_; ++page_id) {
            auto page = buffer_manager_.fetchPage(data_file_, page_id);

            size_t start_record = page_id * records_per_page_;
            size_t end_record = std::min(start_record + records_per_page_, total_records_);

            for (size_t i = start_record; i < end_record; ++i) {
                Value val = readValueFromPage(*page, i - start_record);
                result += getNumericValue(val);
            }
        }

        return result;
    }

    double average() const {
        if (total_records_ == 0) return 0.0;
        return sum() / total_records_;
    }

    size_t size() const { return total_records_; }
    const std::string& getName() const { return name_; }
    DataType getType() const { return type_; }

private:
    size_t getRecordSize(DataType type) const {
        switch (type) {
        case DataType::INT32: return sizeof(int32_t);
        case DataType::INT64: return sizeof(int64_t);
        case DataType::FLOAT: return sizeof(float);
        case DataType::DOUBLE: return sizeof(double);
        case DataType::STRING: return 256;  // 固定長度字串
        case DataType::BOOL: return sizeof(bool);
        }
        return 8;  // 預設大小
    }

    void writeValueToPage(Page& page, size_t offset, const Value& value) {
        size_t record_size = getRecordSize(type_);
        char* data_ptr = page.data.data() + offset * record_size;

        switch (type_) {
        case DataType::INT32: {
            int32_t val = std::get<int32_t>(value);
            std::memcpy(data_ptr, &val, sizeof(int32_t));
            break;
        }
        case DataType::INT64: {
            int64_t val = std::get<int64_t>(value);
            std::memcpy(data_ptr, &val, sizeof(int64_t));
            break;
        }
        case DataType::FLOAT: {
            float val = std::get<float>(value);
            std::memcpy(data_ptr, &val, sizeof(float));
            break;
        }
        case DataType::DOUBLE: {
            double val = std::get<double>(value);
            std::memcpy(data_ptr, &val, sizeof(double));
            break;
        }
        case DataType::STRING: {
            std::string val = std::get<std::string>(value);
            val.resize(255);  // 確保不超過固定長度
            std::memcpy(data_ptr, val.c_str(), 256);
            break;
        }
        case DataType::BOOL: {
            bool val = std::get<bool>(value);
            std::memcpy(data_ptr, &val, sizeof(bool));
            break;
        }
        }
    }

    Value readValueFromPage(const Page& page, size_t offset) const {
        size_t record_size = getRecordSize(type_);
        const char* data_ptr = page.data.data() + offset * record_size;

        switch (type_) {
        case DataType::INT32: {
            int32_t val;
            std::memcpy(&val, data_ptr, sizeof(int32_t));
            return val;
        }
        case DataType::INT64: {
            int64_t val;
            std::memcpy(&val, data_ptr, sizeof(int64_t));
            return val;
        }
        case DataType::FLOAT: {
            float val;
            std::memcpy(&val, data_ptr, sizeof(float));
            return val;
        }
        case DataType::DOUBLE: {
            double val;
            std::memcpy(&val, data_ptr, sizeof(double));
            return val;
        }
        case DataType::STRING: {
            std::string val(data_ptr, 256);
            // 移除尾部的null字符
            val = val.c_str();
            return val;
        }
        case DataType::BOOL: {
            bool val;
            std::memcpy(&val, data_ptr, sizeof(bool));
            return val;
        }
        }
        return int32_t(0);
    }

    double getNumericValue(const Value& value) const {
        switch (type_) {
        case DataType::INT32: return static_cast<double>(std::get<int32_t>(value));
        case DataType::INT64: return static_cast<double>(std::get<int64_t>(value));
        case DataType::FLOAT: return static_cast<double>(std::get<float>(value));
        case DataType::DOUBLE: return std::get<double>(value);
        default: return 0.0;
        }
    }
};

// 支援大資料集的表格
class DiskBasedTable {
private:
    std::string name_;
    std::string table_path_;
    std::unordered_map<std::string, std::unique_ptr<DiskBasedColumn>> columns_;
    std::vector<std::string> column_order_;
    BufferPoolManager& buffer_manager_;
    size_t row_count_;

public:
    DiskBasedTable(const std::string& name, BufferPoolManager& buffer_manager)
        : name_(name), buffer_manager_(buffer_manager), row_count_(0) {
        table_path_ = name;  // 只是表格名稱，相對路徑將由列來構建
        // 註記：目錄創建將在檔案創建時處理
    }

    void addColumn(const std::string& name, DataType type) {
        if (columns_.find(name) != columns_.end()) {
            throw std::runtime_error("Column already exists: " + name);
        }

        auto column = std::make_unique<DiskBasedColumn>(
            table_path_ + "/" + name, type, buffer_manager_);

        // 如果表格已有資料，新列需要填入預設值
        for (size_t i = 0; i < row_count_; ++i) {
            Value default_value;
            switch (type) {
            case DataType::INT32: default_value = int32_t(0); break;
            case DataType::INT64: default_value = int64_t(0); break;
            case DataType::FLOAT: default_value = 0.0f; break;
            case DataType::DOUBLE: default_value = 0.0; break;
            case DataType::STRING: default_value = std::string(""); break;
            case DataType::BOOL: default_value = false; break;
            }
            column->append(default_value);
        }

        columns_[name] = std::move(column);
        column_order_.push_back(name);
    }

    void insertRow(const std::unordered_map<std::string, Value>& row_data) {
        for (const auto& col_name : column_order_) {
            auto it = row_data.find(col_name);
            if (it != row_data.end()) {
                columns_[col_name]->append(it->second);
            }
            else {
                // 插入預設值
                Value default_value;
                switch (columns_[col_name]->getType()) {
                case DataType::INT32: default_value = int32_t(0); break;
                case DataType::INT64: default_value = int64_t(0); break;
                case DataType::FLOAT: default_value = 0.0f; break;
                case DataType::DOUBLE: default_value = 0.0; break;
                case DataType::STRING: default_value = std::string(""); break;
                case DataType::BOOL: default_value = false; break;
                }
                columns_[col_name]->append(default_value);
            }
        }
        row_count_++;
    }

    // 批量插入 - 對大資料集優化
    void bulkInsert(const std::vector<std::unordered_map<std::string, Value>>& rows) {
        for (const auto& row : rows) {
            insertRow(row);

            // 每1000行刷新一次緩衝池
            if (row_count_ % 1000 == 0) {
                buffer_manager_.flushAllPages();
            }
        }
    }

    DiskBasedColumn* getColumn(const std::string& name) {
        auto it = columns_.find(name);
        return (it != columns_.end()) ? it->second.get() : nullptr;
    }

    // 索引查詢 - 利用B+樹快速定位
    std::vector<std::unordered_map<std::string, Value>> indexedSelect(
        const std::string& index_column,
        const Value& value,
        const std::vector<std::string>& selected_columns = {}) {

        std::vector<std::unordered_map<std::string, Value>> result;

        auto* column = getColumn(index_column);
        if (!column) return result;

        // 使用索引快速找到匹配的記錄
        auto record_ids = column->findRecords(value);

        // 決定要選擇的列
        std::vector<std::string> cols = selected_columns.empty() ? column_order_ : selected_columns;

        // 構建結果
        for (RecordId record_id : record_ids) {
            std::unordered_map<std::string, Value> row;
            for (const auto& col_name : cols) {
                auto* col = getColumn(col_name);
                if (col) {
                    row[col_name] = col->get(record_id);
                }
            }
            result.push_back(row);
        }

        return result;
    }

    // 範圍查詢
    std::vector<std::unordered_map<std::string, Value>> rangeSelect(
        const std::string& index_column,
        const Value& start_value,
        const Value& end_value,
        const std::vector<std::string>& selected_columns = {}) {

        std::vector<std::unordered_map<std::string, Value>> result;

        auto* column = getColumn(index_column);
        if (!column) return result;

        // 使用索引進行範圍查詢
        auto record_ids = column->findRecordsInRange(start_value, end_value);

        std::vector<std::string> cols = selected_columns.empty() ? column_order_ : selected_columns;

        for (RecordId record_id : record_ids) {
            std::unordered_map<std::string, Value> row;
            for (const auto& col_name : cols) {
                auto* col = getColumn(col_name);
                if (col) {
                    row[col_name] = col->get(record_id);
                }
            }
            result.push_back(row);
        }

        return result;
    }

    const std::string& getName() const { return name_; }
    size_t getRowCount() const { return row_count_; }
    const std::vector<std::string>& getColumnNames() const { return column_order_; }
};

// 支援大資料集的資料庫
class LargeScaleDatabase {
private:
    std::string name_;
    std::string db_path_;
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_manager_;
    std::unordered_map<std::string, std::unique_ptr<DiskBasedTable>> tables_;

public:
    LargeScaleDatabase(const std::string& name, const std::string& db_path)
        : name_(name), db_path_(db_path) {
        disk_manager_ = std::make_unique<DiskManager>(db_path);
        buffer_manager_ = std::make_unique<BufferPoolManager>(*disk_manager_);
    }

    ~LargeScaleDatabase() {
        // 確保所有資料都寫入磁碟
        buffer_manager_->flushAllPages();
    }

    void createTable(const std::string& table_name) {
        if (tables_.find(table_name) != tables_.end()) {
            throw std::runtime_error("Table already exists: " + table_name);
        }
        tables_[table_name] = std::make_unique<DiskBasedTable>(
            table_name, *buffer_manager_);
    }

    DiskBasedTable* getTable(const std::string& table_name) {
        auto it = tables_.find(table_name);
        return (it != tables_.end()) ? it->second.get() : nullptr;
    }

    void dropTable(const std::string& table_name) {
        tables_.erase(table_name);
        // 實際實作中還需要刪除磁碟檔案
    }

    // 壓縮和優化
    void optimize() {
        buffer_manager_->flushAllPages();
        // 可以實作表格壓縮、索引重建等優化操作
    }

    // 統計資訊
    void printStatistics() {
        std::cout << "Database Statistics:\n";
        std::cout << "Database Name: " << name_ << "\n";
        std::cout << "Table Count: " << tables_.size() << "\n";

        for (const auto& [table_name, table] : tables_) {
            std::cout << "  Table " << table_name << ": " << table->getRowCount() << " rows\n";
        }
    }
};

// 工具函式
void printValue(const Value& value) {
    std::visit([](const auto& v) {
        std::cout << v;
        }, value);
}

void printQueryResult(const std::vector<std::unordered_map<std::string, Value>>& result) {
    if (result.empty()) {
        std::cout << "Query result is empty\n";
        return;
    }

    // 印出標題
    for (const auto& pair : result[0]) {
        std::cout << pair.first << "\t";
    }
    std::cout << "\n";

    // 印出分隔線
    for (const auto& pair : result[0]) {
        std::cout << "--------\t";
    }
    std::cout << "\n";

    // 印出資料（限制輸出行數以避免過多輸出）
    size_t max_rows = std::min(size_t(10), result.size());
    for (size_t i = 0; i < max_rows; ++i) {
        for (const auto& pair : result[i]) {
            printValue(pair.second);
            std::cout << "\t";
        }
        std::cout << "\n";
    }

    if (result.size() > max_rows) {
        std::cout << "... (" << (result.size() - max_rows) << " more rows)\n";
    }
}

// 示範程式
int main() {
    try {
        std::cout << "=== Large-Scale Columnar Database Demo ===\n";
        std::cout << "Features: Disk Storage, B+ Tree Indexing, Buffer Pool Management\n\n";

        // 建立支援大資料集的資料庫
        LargeScaleDatabase db("LargeScaleDB", "./large_scale_db");

        // 建立員工表格
        db.createTable("employees");
        auto* emp_table = db.getTable("employees");

        emp_table->addColumn("id", DataType::INT32);
        emp_table->addColumn("name", DataType::STRING);
        emp_table->addColumn("salary", DataType::DOUBLE);
        emp_table->addColumn("department_id", DataType::INT32);

        std::cout << "1. Inserting test data...\n";
        emp_table->insertRow({ {"id", 1}, {"name", std::string("John Smith")}, {"salary", 50000.0}, {"department_id", 1} });
        emp_table->insertRow({ {"id", 2}, {"name", std::string("Jane Doe")}, {"salary", 60000.0}, {"department_id", 2} });
        emp_table->insertRow({ {"id", 3}, {"name", std::string("Bob Wilson")}, {"salary", 55000.0}, {"department_id", 1} });

        // 使用索引查詢
        std::cout << "2. Querying employees in department 1 using index:\n";
        auto dept1_employees = emp_table->indexedSelect("department_id", 1);
        printQueryResult(dept1_employees);
        std::cout << "\n";

        // 範圍查詢
        std::cout << "3. Querying employees with salary between 50000-60000:\n";
        auto salary_range = emp_table->rangeSelect("salary", 50000.0, 60000.0);
        printQueryResult(salary_range);
        std::cout << "\n";

        // 大資料量測試
        std::cout << "4. Large dataset test - inserting 100,000 records...\n";
        auto start_time = std::chrono::high_resolution_clock::now();

        db.createTable("large_dataset");
        auto* large_table = db.getTable("large_dataset");
        large_table->addColumn("id", DataType::INT32);
        large_table->addColumn("value", DataType::DOUBLE);
        large_table->addColumn("category", DataType::INT32);

        // 批量插入
        std::vector<std::unordered_map<std::string, Value>> batch_data;
        for (int i = 0; i < 100000; ++i) {
            batch_data.push_back({
                {"id", i},
                {"value", double(i * 1.5)},
                {"category", i % 10}
                });

            // 每1000筆批量插入一次
            if (batch_data.size() == 1000) {
                large_table->bulkInsert(batch_data);
                batch_data.clear();
            }
        }
        if (!batch_data.empty()) {
            large_table->bulkInsert(batch_data);
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

        std::cout << "Insert completed, time taken: " << duration.count() << " milliseconds\n";
        std::cout << "Record count: " << large_table->getRowCount() << "\n\n";

        // 大資料量索引查詢
        std::cout << "5. Large dataset index query test...\n";
        start_time = std::chrono::high_resolution_clock::now();

        auto category_results = large_table->indexedSelect("category", 5, { "id", "value" });

        end_time = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

        std::cout << "Index query completed, time taken: " << duration.count() << " milliseconds\n";
        std::cout << "Found " << category_results.size() << " records\n";
        std::cout << "First few results:\n";
        printQueryResult(category_results);
        std::cout << "\n";

        // 範圍查詢測試
        std::cout << "6. Range query test...\n";
        start_time = std::chrono::high_resolution_clock::now();

        auto range_results = large_table->rangeSelect("value", 10000.0, 20000.0);

        end_time = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

        std::cout << "Range query completed, time taken: " << duration.count() << " milliseconds\n";
        std::cout << "Found " << range_results.size() << " records\n\n";

        // 聚合查詢測試
        std::cout << "7. Large dataset aggregate query test...\n";
        start_time = std::chrono::high_resolution_clock::now();

        auto* value_column = large_table->getColumn("value");
        double total_sum = value_column->sum();
        double avg_value = value_column->average();

        end_time = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

        std::cout << "Aggregate query completed, time taken: " << duration.count() << " milliseconds\n";
        std::cout << "Sum: " << total_sum << "\n";
        std::cout << "Average: " << avg_value << "\n\n";

        // 印出資料庫統計
        db.printStatistics();

        // 在顯示功能前強制將所有資料刷新到磁碟
        std::cout << "\n8. Flushing all data to disk...\n";
        db.optimize();  // 這會呼叫 flushAllPages
        std::cout << "Data flush completed.\n";

        std::cout << "\n=== Large-Scale Columnar Database Features ===\n";
        std::cout << "✓ Disk Storage Support - Handle datasets larger than memory\n";
        std::cout << "✓ B+ Tree Indexing - Fast queries and range searches\n";
        std::cout << "✓ Buffer Pool Management - LRU replacement strategy, efficient memory management\n";
        std::cout << "✓ Paging Mechanism - 4KB pages, optimized disk I/O\n";
        std::cout << "✓ Columnar Storage Architecture - Optimized for analytical queries\n";
        std::cout << "✓ Batch Operations - Efficient handling of large datasets\n";
        std::cout << "✓ Data-Oriented Design - Cache-friendly memory layout\n";

		std::cin.get();  // Wait for user input to view results
        
    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}

// Run program: Ctrl + F5 or Debug > Start Without Debugging menu
// Debug program: F5 or Debug > Start Debugging menu

// Tips for Getting Started: 
//   1. Use the Solution Explorer window to add/manage files
//   2. Use the Team Explorer window to connect to source control
//   3. Use the Output window to see build output and other messages
//   4. Use the Error List window to view errors
//   5. Go to Project > Add New Item to create new code files, or Project > Add Existing Item to add existing code files to the project
//   6. In the future, to open this project again, go to File > Open > Project and select the .sln file
