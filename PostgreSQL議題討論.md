

## Oracle vs PostgreSQL NULL 處理的關鍵差異

### 1. Index 中的 NULL 處理（最重要的差異！）

| 特性 | Oracle | PostgreSQL |
|------|--------|------------|
| **單欄 B-tree Index 是否包含 NULL** | ❌ **不包含** | ✅ **包含** |
| `WHERE col IS NULL` 能否使用 Index | ❌ 無法（需要 workaround） | ✅ 可以直接使用 |

Oracle 的 B-tree index 有一個重要特性：如果所有被索引的欄位都是 NULL，該筆記錄不會被加入 index。

而 PostgreSQL 的 index 可以處理 nullable 條件，單欄 index 也能支援 `IS NULL` 查詢。

**範例驗證（PostgreSQL）：**
```sql
-- PostgreSQL 可以直接用 index 查 NULL
EXPLAIN ANALYZE SELECT * FROM test WHERE col1 IS NULL;

-- 結果會顯示：Index Scan 或 Index Only Scan
-- Index Cond: (col1 IS NULL)  ← 這在 Oracle 單欄 index 做不到！
```

---

### 2. 你的情境：欄位預設 NULL，後續會被更新

這種模式在 PostgreSQL 中有一些注意事項：

#### 潛在問題：
1. **Index 膨脹（Bloat）**：頻繁更新會產生 dead tuples
2. **HOT Update 失效**：如果更新的欄位有 index，可能無法使用 Heap-Only Tuple 優化

#### 建議做法：

**方案 A：使用 Partial Index（推薦）**
```sql
-- 如果大多數時候查的是非 NULL 的資料
CREATE INDEX idx_col1_not_null ON table_name(col1) 
WHERE col1 IS NOT NULL;

-- 如果需要查 NULL（例如找「待處理」的資料）
CREATE INDEX idx_col1_null ON table_name(col1) 
WHERE col1 IS NULL;
```

Partial index 在需要索引的資料只佔表格一小部分時特別有效，可以減少 index 大小並加速查詢。

**方案 B：考慮使用 BRIN Index（如果資料有時序性）**
```sql
-- 如果資料依時間順序插入，BRIN 很適合
CREATE INDEX idx_col1_brin ON table_name USING BRIN(col1);
```

---

### 3. 排序時 Index 對 NULL 的影響

PostgreSQL 提供彈性的 NULL 排序控制：

```sql
-- 建立 index 時指定 NULL 排序位置
CREATE INDEX idx_col1 ON table_name(col1 NULLS FIRST);
-- 或
CREATE INDEX idx_col1 ON table_name(col1 NULLS LAST);

-- 查詢時也可以指定
SELECT * FROM table_name ORDER BY col1 NULLS LAST;
```

**重點**：如果查詢的 `ORDER BY` 排序方向與 index 定義一致，就能有效利用 index。

---

## 總結

| 問題 | Oracle | PostgreSQL |
|------|--------|------------|
| NULL 造成 Index 無法使用？ | ✅ 是的，單欄 index 不含 NULL | ❌ **不會**，NULL 正常被索引 |
| 很多 NULL 欄位影響效能？ | 可能（Index 問題） | **通常不會**，反而節省空間 |
| NULL 造成資料破碎？ | 某些情況下 | 頻繁更新才會（VACUUM 可解決）|
| 排序 Index + NULL？ | 需要特殊處理 | 原生支援 `NULLS FIRST/LAST` |

