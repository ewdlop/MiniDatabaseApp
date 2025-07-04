# 大規模列存儲資料庫

一個用 C++ 實現的列存儲資料庫系統，專為大資料集分析查詢而優化。

## 🚀 特色功能

### 核心架構
- **列存儲設計** - 針對分析查詢優化的資料佈局
- **磁碟存儲支援** - 處理超過記憶體大小的資料集
- **B+ 樹索引** - 快速查詢和範圍搜尋
- **緩衝池管理** - LRU 替換策略，高效記憶體管理
- **分頁機制** - 4KB 頁面，優化磁碟 I/O

### 性能特點
- **資料導向設計** - 緩存友好的記憶體佈局
- **批量操作** - 高效處理大資料集
- **索引加速** - 支援等值查詢和範圍查詢
- **聚合函數** - 針對大資料集優化的 SUM、AVG 等運算
- **分頁載入** - 避免記憶體溢出的分頁處理機制

### 編譯步驟

1. **使用 Visual Studio**
   ```
   1. 開啟 DatabaseApp.sln
   2. 選擇 Debug 或 Release 配置
   3. 按 F5 或 Ctrl+F5 編譯並執行
   ```

2. **使用命令行**
   ```cmd
   cd C:\Games\DatabaseApp
   msbuild DatabaseApp.sln /p:Configuration=Release
   ```

### 執行程式
```cmd
.\x64\Debug\DatabaseApp.exe
```
或
```cmd
.\x64\Release\DatabaseApp.exe
```

## 📁 專案結構

```
DatabaseApp/
├── DatabaseApp.cpp          # 主程式碼檔案
├── DatabaseApp.sln          # Visual Studio 解決方案
├── DatabaseApp.vcxproj      # 專案檔案
├── README.md                # 專案說明文件
├── x64/                     # 編譯輸出目錄
│   └── Debug/
│       └── DatabaseApp.exe  # 可執行檔案
└── large_scale_db/          # 資料庫檔案目錄 (執行時自動創建)
    ├── employees/           # 員工表格目錄
    └── large_dataset/       # 大資料集表格目錄
```

## 🏗️ 系統架構

### 架構層次
```
┌─────────────────┐
│   應用程式層    │  <- 查詢接口、資料操作
├─────────────────┤
│   表格管理層    │  <- DiskBasedTable, DiskBasedColumn
├─────────────────┤
│   索引管理層    │  <- B+ Tree Index
├─────────────────┤
│   緩衝池層      │  <- BufferPoolManager (LRU)
├─────────────────┤
│   磁碟管理層    │  <- DiskManager
└─────────────────┘
```

### 核心元件

1. **DiskManager** - 磁碟檔案管理
   - 檔案流管理
   - 頁面讀寫操作
   - 目錄結構創建

2. **BufferPoolManager** - 記憶體緩衝池
   - LRU 替換策略
   - 頁面緩存管理
   - 髒頁寫回機制

3. **BPlusTreeIndex** - B+ 樹索引
   - 快速查詢支援
   - 範圍查詢優化
   - 自動分裂平衡

4. **DiskBasedColumn** - 列存儲引擎
   - 列式資料存儲
   - 聚合函數優化
   - 分頁處理機制

5. **DiskBasedTable** - 表格管理
   - 多列協調管理
   - 批量插入優化
   - 查詢接口封裝

## 💡 使用範例

### 基本操作

```cpp
// 創建資料庫
LargeScaleDatabase db("MyDatabase", "./my_db");

// 創建表格
db.createTable("employees");
auto* table = db.getTable("employees");

// 添加列
table->addColumn("id", DataType::INT32);
table->addColumn("name", DataType::STRING);
table->addColumn("salary", DataType::DOUBLE);

// 插入資料
table->insertRow({
    {"id", 1}, 
    {"name", std::string("張三")}, 
    {"salary", 50000.0}
});

// 索引查詢
auto results = table->indexedSelect("id", 1);

// 範圍查詢
auto salary_range = table->rangeSelect("salary", 40000.0, 60000.0);
```

### 大資料集處理

```cpp
// 批量插入
std::vector<std::unordered_map<std::string, Value>> batch_data;
for (int i = 0; i < 100000; ++i) {
    batch_data.push_back({
        {"id", i},
        {"value", double(i * 1.5)},
        {"category", i % 10}
    });
}
table->bulkInsert(batch_data);

// 聚合查詢
auto* column = table->getColumn("value");
double total = column->sum();
double average = column->average();
```

## 🔍 支援的資料型別

| 型別        | C++ 型別    | 描述           |
|-------------|-------------|----------------|
| `INT32`     | `int32_t`   | 32位元整數     |
| `INT64`     | `int64_t`   | 64位元整數     |
| `FLOAT`     | `float`     | 單精度浮點數   |
| `DOUBLE`    | `double`    | 雙精度浮點數   |
| `STRING`    | `std::string` | 字串 (最大256字元) |
| `BOOL`      | `bool`      | 布林值         |

## ⚡ 性能特點

### 大資料集測試結果
- **插入性能**: 100,000 筆記錄約需 100-500 毫秒
- **索引查詢**: 10,000+ 筆資料中查詢延遲 < 10 毫秒
- **範圍查詢**: 支援大範圍高效掃描
- **聚合運算**: 分頁處理避免記憶體溢出

### 記憶體管理
- **緩衝池大小**: 1000 頁面 (約 4MB)
- **頁面大小**: 4KB
- **替換策略**: LRU (最近最少使用)

### 磁碟 I/O 優化
- **批量寫入**: 減少磁碟 I/O 次數
- **預讀機制**: 順序讀取優化
- **髒頁管理**: 延遲寫入提升性能

## 🛠️ 技術細節

### B+ 樹配置
- **樹階數**: 128
- **分裂策略**: 中點分裂
- **葉節點鏈接**: 支援範圍查詢

### 列存儲優化
- **資料導向設計**: 提升緩存局部性
- **壓縮友好**: 相同型別資料聚集
- **向量化友好**: 支援 SIMD 優化潛力

### 事務與並發
- 當前版本: 單執行緒操作
- 未來規劃: MVCC、WAL 日誌

## 🚧 未來改進

### 短期目標
- [ ] 添加資料壓縮支援
- [ ] 實現更多聚合函數
- [ ] 優化字串處理性能
- [ ] 添加資料驗證機制

### 長期目標
- [ ] 多執行緒並發支援
- [ ] 分散式存儲支援
- [ ] SQL 查詢語言支援
- [ ] 事務處理機制

## 🐛 已知限制

1. **並發限制**: 目前僅支援單執行緒操作
2. **字串長度**: 固定最大 256 字元
3. **索引限制**: 每列僅支援一個 B+ 樹索引
4. **記憶體佔用**: 大資料集需要適當的緩衝池配置

## 🤝 貢獻指南

歡迎提交 Issue 和 Pull Request！

### 開發環境設定
1. Fork 專案
2. 創建功能分支: `git checkout -b feature/新功能`
3. 提交變更: `git commit -am '添加新功能'`
4. 推送分支: `git push origin feature/新功能`
5. 提交 Pull Request

## 📄 授權條款

本專案採用 MIT 授權條款 - 詳見 [LICENSE](LICENSE) 檔案

## 📞 聯絡資訊

如有問題或建議，歡迎透過以下方式聯絡：

- 📧 Email: your.email@example.com
- 🐛 Issues: [GitHub Issues](https://github.com/your-username/DatabaseApp/issues)
- 💬 討論: [GitHub Discussions](https://github.com/your-username/DatabaseApp/discussions)

---

**⭐ 如果這個專案對您有幫助，請給個星星！** 
