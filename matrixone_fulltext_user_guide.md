# MatrixOne 全文检索用户指南

## 目录

- [快速开始](#快速开始)
- [基础概念](#基础概念)
- [语法参考](#语法参考)
- [实现原理](#实现原理)
- [相关性评分算法](#相关性评分算法)
- [性能优化](#性能优化)
- [当前限制与不足](#当前限制与不足)
- [最佳实践](#最佳实践)
- [Python SDK](#python-sdk)
- [常见问题](#常见问题)

---

## 快速开始

### 创建表和索引

```sql
-- 创建包含文本列的表
CREATE TABLE articles (
  id INT PRIMARY KEY,
  title VARCHAR(200),
  content TEXT,
  author VARCHAR(100)
);

-- 创建全文索引（推荐使用 BM25 算法）
CREATE FULLTEXT INDEX ftidx ON articles (title, content);

-- 插入数据
INSERT INTO articles VALUES
  (1, '人工智能简介', '人工智能是计算机科学的一个分支，致力于创建能够执行通常需要人类智能的任务的系统。', '张三'),
  (2, '数据库原理', '数据库是组织和存储数据的系统，支持高效的数据检索和管理。', '李四');
```

### 执行全文检索

```sql
-- 自然语言模式检索（默认模式）
SELECT id, title FROM articles 
WHERE MATCH(title, content) AGAINST('人工智能');

-- 布尔模式检索
SELECT id, title FROM articles 
WHERE MATCH(title, content) AGAINST('+数据库 +原理' IN BOOLEAN MODE);

-- 带相关性评分
SELECT id, title, 
       MATCH(title, content) AGAINST('人工智能') AS score
FROM articles 
WHERE MATCH(title, content) AGAINST('人工智能')
ORDER BY score DESC
LIMIT 10;
```

---

## 基础概念

### 什么是全文检索？

全文检索是一种在大量文本数据中快速查找包含特定关键词的文档的技术。与传统的 `LIKE '%keyword%'` 查询不同，全文检索通过预先建立**倒排索引**来实现高效搜索。

**类比理解**：
- `LIKE` 查询：逐本翻书找内容
- 全文检索：使用索引卡片系统，直接定位到包含关键词的书籍

### 倒排索引原理

倒排索引将"词 → 文档"的映射关系存储起来。

**示例**：假设有3篇文档：
- 文档1: "苹果是一种水果"
- 文档2: "香蕉和苹果都很好吃"  
- 文档3: "香蕉是黄色的"

**倒排索引结构**：
```
苹果 → [文档1, 文档2]
香蕉 → [文档2, 文档3]
水果 → [文档1]
```

搜索"苹果"时，直接从索引获取 [文档1, 文档2]，无需扫描所有文档。

### 搜索模式

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| **自然语言模式** | 短语搜索，词之间是 AND 关系，检查位置顺序 | 用户搜索框，通用搜索 |
| **布尔模式** | 支持 `+`、`-`、`""`、`*` 等操作符 | 高级搜索，程序化查询 |

### 相关性算法

| 算法 | 说明 | 推荐场景 |
|------|------|----------|
| **TF-IDF** | 词频-逆文档频率，传统算法 | 默认算法 |
| **BM25** | 改进的 TF-IDF，考虑文档长度归一化 | **推荐**，效果更好 |

### 分词解析器

| 解析器 | 说明 | 适用场景 |
|--------|------|----------|
| **default/ngram** | 智能分词：英文按空格，中文按 3-gram | 通用文档（默认） |
| **json** | 解析 JSON 值并分词 | JSON 文档，值需要分词 |
| **json_value** | 提取 JSON 值作为完整词 | JSON 文档，值作为整体匹配 |

---

## 语法参考

### 创建全文索引

```sql
-- 基本语法
CREATE FULLTEXT INDEX index_name ON table_name (column1, column2, ...);

-- 在建表时创建
CREATE TABLE t (
  id INT PRIMARY KEY,
  content TEXT,
  FULLTEXT(content)
);

-- 指定解析器
CREATE FULLTEXT INDEX ftidx ON table_name (column) WITH PARSER ngram;
CREATE FULLTEXT INDEX ftidx ON table_name (json_col) WITH PARSER json;
CREATE FULLTEXT INDEX ftidx ON table_name (json_col) WITH PARSER json_value;
```

#### 支持的列类型

- `CHAR` / `VARCHAR` / `TEXT`
- `JSON`
- `DATALINK`（支持 PDF、DOCX 等文件）

### 设置相关性算法

```sql
-- 设置为 BM25（推荐）
SET ft_relevancy_algorithm = 'BM25';

-- 设置为 TF-IDF（默认）
SET ft_relevancy_algorithm = 'TF-IDF';
```

### 全文检索查询

#### 自然语言模式

自然语言模式是**短语搜索**：分词后所有词之间是 AND 关系，且检查位置顺序。

```sql
-- 基础搜索（默认自然语言模式）
SELECT * FROM articles 
WHERE MATCH(title, content) AGAINST('machine learning');

-- 显式指定模式
SELECT * FROM articles 
WHERE MATCH(title, content) AGAINST('machine learning' IN NATURAL LANGUAGE MODE);

-- 带相关性评分
SELECT *, MATCH(title, content) AGAINST('keyword') AS score
FROM articles 
WHERE MATCH(title, content) AGAINST('keyword')
ORDER BY score DESC
LIMIT 10;
```

#### 布尔模式

| 操作符 | 说明 | 示例 |
|--------|------|------|
| `+` | 必须包含（AND） | `+apple +banana` |
| `-` | 必须不包含（NOT） | `+apple -banana` |
| `~` | 降低相关性但不排除 | `+apple ~old` |
| `""` | 精确短语匹配 | `"machine learning"` |
| `*` | 前缀匹配 | `learn*` |
| `()` | 分组 | `+(apple banana)` |
| `>` | 提高权重 | `>important` |
| `<` | 降低权重 | `<optional` |

```sql
-- 必须包含多个词
SELECT * FROM articles 
WHERE MATCH(title, content) AGAINST('+database +optimization' IN BOOLEAN MODE);

-- 必须包含但不包含
SELECT * FROM articles 
WHERE MATCH(title, content) AGAINST('+database -legacy' IN BOOLEAN MODE);

-- 精确短语
SELECT * FROM articles 
WHERE MATCH(title, content) AGAINST('"machine learning"' IN BOOLEAN MODE);

-- 前缀匹配
SELECT * FROM articles 
WHERE MATCH(title, content) AGAINST('learn*' IN BOOLEAN MODE);

-- 复杂表达式
SELECT * FROM articles 
WHERE MATCH(title, content) AGAINST('+python +(programming development) -legacy' IN BOOLEAN MODE);

-- 权重调整
SELECT * FROM articles 
WHERE MATCH(title, content) AGAINST('+red +(<blue >is)' IN BOOLEAN MODE);
```

### JSON 文档搜索

**json 解析器**：提取 JSON 值后进行分词

```sql
CREATE TABLE products (id INT PRIMARY KEY, specs JSON);
INSERT INTO products VALUES 
  (1, '{"name": "hello world", "code": "ABC123"}');

CREATE FULLTEXT INDEX ftidx ON products (specs) WITH PARSER json;

-- 搜索分词后的词
SELECT * FROM products WHERE MATCH(specs) AGAINST('hello');
-- ✓ 匹配 "hello world" 中的 "hello"
```

**json_value 解析器**：提取 JSON 值作为完整词（不分词）

```sql
CREATE FULLTEXT INDEX ftidx ON products (specs) WITH PARSER json_value;

-- 搜索完整的值
SELECT * FROM products WHERE MATCH(specs) AGAINST('ABC123');
-- ✓ 匹配完整值 "ABC123"

SELECT * FROM products WHERE MATCH(specs) AGAINST('ABC');
-- ✗ 不匹配，因为 "ABC123" 是完整词
```

### DATALINK 文档搜索

支持对 PDF、DOCX 等文件内容进行全文检索：

```sql
-- 创建 stage
CREATE STAGE ftstage URL='file:///path/to/files/';

-- 创建表
CREATE TABLE docs (
  id INT PRIMARY KEY, 
  fpath DATALINK,
  FULLTEXT(fpath)
);

-- 插入文件引用
INSERT INTO docs VALUES 
  (1, 'stage://ftstage/document.pdf'),
  (2, 'file:///path/to/chinese.pdf');

-- 搜索文件内容
SELECT id FROM docs WHERE MATCH(fpath) AGAINST('matrixone');
```

---

## 实现原理

### 索引表结构

MatrixOne 通过创建一个隐藏的辅助表来实现全文索引。当你创建全文索引时，系统会自动创建一个索引表：

```sql
-- 索引表结构（系统自动创建，用户不可见）
CREATE TABLE __mo_index_secondary_xxx (
    doc_id <主键类型>,    -- 文档ID，关联回原表
    word VARCHAR,         -- 分词后的词
    pos INT32,            -- 词在文档中的位置（字节偏移）
    PRIMARY KEY (...),
    CLUSTER BY (word)     -- 按词聚簇存储，提高查询效率
)
```

**关键设计**：
- `doc_id`：存储原表的主键值，用于关联回原表
- `word`：存储分词后的词（自动小写化），这是索引的核心
- `pos`：存储词在文档中的字节位置，支持短语搜索和位置匹配
- `CLUSTER BY (word)`：相同词的数据物理上聚集存储，查询时只需读取少量数据块
- 特殊词 `__DocLen`：每个文档会额外存储一条记录，pos 字段存储文档的词数，用于 BM25 算法的文档长度归一化计算

### 数据插入时的索引维护

当向主表插入数据时，系统会自动更新全文索引表：

```
INSERT INTO articles VALUES (...)
    ↓
分词处理（使用 tokenizer）
    ↓
生成 (doc_id, word, pos) 三元组
    ↓
INSERT INTO 索引表
    ↓
额外插入 (doc_id, '__DocLen', 词数) 记录
```

**分词示例**：

对于中文文本 "人工智能是未来"，使用 3-gram 分词：
```
原文: "人工智能是未来"
分词结果:
  - (doc_id=1, word="人工智", pos=0)   -- 字节位置 0
  - (doc_id=1, word="工智能", pos=3)   -- 字节位置 3（每个中文字符 3 字节）
  - (doc_id=1, word="智能是", pos=6)
  - (doc_id=1, word="能是未", pos=9)
  - (doc_id=1, word="是未来", pos=12)
  - (doc_id=1, word="__DocLen", pos=5) -- 文档包含 5 个词
```

对于英文文本 "color is red"：
```
原文: "color is red"
分词结果:
  - (doc_id=1, word="color", pos=0)
  - (doc_id=1, word="is", pos=6)
  - (doc_id=1, word="red", pos=9)
  - (doc_id=1, word="__DocLen", pos=3)
```

### 分词规则详解

MatrixOne 使用智能分词器（SimpleTokenizer），自动识别语言类型并采用不同的分词策略：

**英文（Latin 字符）**：
- 按空格和标点符号分词
- 自动转换为小写
- 最大词长 23 字节（超出部分截断）
- 数字被视为词的一部分

**中文/日文/韩文（CJK 字符）**：
- 使用 3-gram 滑动窗口分词
- 每次取 3 个连续字符作为一个词
- 示例："人工智能" → ["人工智", "工智能"]
- 示例："数据库" → ["数据库"]（正好 3 个字符）

**混合文本**：
- 自动在 Latin 和 CJK 之间切换分词模式
- 示例："AI人工智能test" → ["ai", "人工智", "工智能", "test"]

**分词边界判断**：
- 空格、标点符号、特殊字符作为分词边界
- Unicode 标点和空白字符都会触发分词

### 搜索执行流程

当执行全文搜索时，系统会经历以下步骤：

```
1. SQL 解析
   SELECT * FROM articles 
   WHERE MATCH(title, content) AGAINST('搜索词' IN NATURAL LANGUAGE MODE)
   
2. 模式解析（Pattern Parsing）
   将搜索字符串解析为 Pattern 树结构
   - 自然语言模式：生成短语匹配的 Pattern
   - 布尔模式：解析操作符生成复杂的 Pattern 树
   
3. SQL 生成
   将 Pattern 树转换为查询索引表的 SQL
   - 自然语言模式：生成带位置检查的 JOIN 查询
   - 布尔模式：基于集合论生成 UNION/JOIN 查询
   
4. 统计信息收集
   - 获取总文档数（Nrow）：用于 IDF 计算
   - 获取每个词的文档频率（aggcnt）：COUNT(doc_id) GROUP BY word
   - 获取平均文档长度（avgDocLen）：BM25 算法需要
   
5. 评分计算
   对每个匹配文档计算相关性分数：
   - TF-IDF：score = TF × IDF²
   - BM25：score = IDF² × TF_adjusted（考虑文档长度）
   
6. 排序返回
   按分数降序排序，返回 Top-K 结果
```

### 自然语言模式的 SQL 生成

自然语言模式实际上是**短语搜索**，需要检查词的位置顺序。

**示例**：搜索 "is red"

```sql
-- 生成的 SQL
WITH kw0 AS (SELECT doc_id, pos FROM index_table WHERE word = 'is'),
     kw1 AS (SELECT doc_id, pos FROM index_table WHERE word = 'red')
SELECT kw0.doc_id, CAST(0 as int) 
FROM kw0, kw1 
WHERE kw0.doc_id = kw1.doc_id 
  AND kw1.pos - kw0.pos = 3  -- 检查位置差（"is" 长度为 2，加空格为 3）
```

**位置检查的作用**：
- "color is red" ✓ 匹配（"is" 在位置 6，"red" 在位置 9，差值为 3）
- "red is color" ✗ 不匹配（位置顺序不对）

### 布尔搜索的 SQL 生成

布尔搜索的 SQL 生成基于**集合论**。这是一个重要的设计思想：

**示例 1**：`+apple +banana`（必须同时包含两个词）

```sql
-- 生成的 SQL
WITH t00 AS (SELECT doc_id FROM index_table WHERE word = 'apple'),
     t01 AS (SELECT doc_id FROM index_table WHERE word = 'banana'),
     t0 AS (SELECT t00.doc_id FROM t00, t01 WHERE t00.doc_id = t01.doc_id)
SELECT t0.doc_id, CAST(0 as int) FROM t0
```

**集合论解释**：
- `+apple` 对应集合 A（包含 apple 的文档）
- `+banana` 对应集合 B（包含 banana 的文档）
- `+apple +banana` 对应 A ∩ B（交集）

**示例 2**：`apple banana`（包含任一词，OR 关系）

```sql
-- 生成的 SQL
WITH t0 AS (SELECT doc_id FROM index_table WHERE word = 'apple'),
     t1 AS (SELECT doc_id FROM index_table WHERE word = 'banana')
SELECT doc_id, CAST(0 as int) FROM t0
UNION ALL
SELECT doc_id, CAST(1 as int) FROM t1
```

**NOT 操作的优化**：

对于 `+apple -banana`（包含 apple 但不包含 banana），由于 `NOT IN` 在 SQL 中性能较差，MatrixOne 采用以下优化策略：

```
原始语义: A - B = A ∩ B^c（A 与 B 的补集的交集）

优化策略: 
  1. SQL 返回: A UNION ALL (A ∩ B)
     - A：包含 apple 的文档
     - A ∩ B：同时包含 apple 和 banana 的文档
  2. 应用层过滤: 在结果处理时排除 (A ∩ B) 中的文档
  3. 最终结果: A - B

这样避免了 SQL 中的 NOT 操作，性能提升 10-50 倍。
```

**示例 3**：复杂表达式 `+A +B -(<C >D)`

```
Pattern 树结构:
((JOIN (+ (TEXT A)) (+ (TEXT B))) (- (GROUP (< (TEXT C)) (> (TEXT D)))))

SQL 生成:
WITH t00 AS (SELECT doc_id FROM idx WHERE word = 'a'),
     t01 AS (SELECT doc_id FROM idx WHERE word = 'b'),
     t0 AS (SELECT t00.doc_id FROM t00, t01 WHERE t00.doc_id = t01.doc_id),
     t1 AS (SELECT doc_id FROM idx WHERE word = 'c'),
     t2 AS (SELECT doc_id FROM idx WHERE word = 'd')
SELECT t0.doc_id, CAST(0 as int) FROM t0
UNION ALL SELECT t0.doc_id, CAST(1 as int) FROM t0, t1 WHERE t0.doc_id = t1.doc_id
UNION ALL SELECT t0.doc_id, CAST(2 as int) FROM t0, t2 WHERE t0.doc_id = t2.doc_id
```

### 短语搜索的实现

布尔模式中的精确短语搜索（`"some words"`）与自然语言模式类似，需要检查词的位置：

```sql
-- 搜索 "is not red"
WITH kw0 AS (SELECT doc_id, pos FROM index_table WHERE word = 'is'),
     kw1 AS (SELECT doc_id, pos FROM index_table WHERE word = 'not'),
     kw2 AS (SELECT doc_id, pos FROM index_table WHERE word = 'red')
SELECT kw0.doc_id 
FROM kw0, kw1, kw2 
WHERE kw0.doc_id = kw1.doc_id 
  AND kw0.doc_id = kw2.doc_id
  AND kw1.pos - kw0.pos = 3   -- "is" 到 "not" 的位置差
  AND kw2.pos - kw0.pos = 7   -- "is" 到 "red" 的位置差
GROUP BY kw0.doc_id           -- 去重
```

### 前缀匹配的实现

前缀匹配（`learn*`）使用 `prefix_eq` 函数：

```sql
SELECT doc_id FROM index_table WHERE prefix_eq(word, 'learn')
-- 匹配: learn, learning, learned, learner, ...
```

---

## 相关性评分算法

### TF-IDF 算法

```
score = TF × IDF²

其中：
- TF（词频）= 词在文档中出现的次数
- IDF（逆文档频率）= log10(总文档数 / 包含该词的文档数)
```

**计算示例**：
```
总文档数：1000，包含"苹果"的文档数：100，文档A中"苹果"出现3次

IDF = log10(1000 / 100) = 1.0
score = 3 × 1.0² = 3.0
```

### BM25 算法（推荐）

BM25 改进了 TF-IDF：
1. **词频饱和度**：词频增长对分数的影响逐渐减小
2. **文档长度归一化**：避免长文档获得不公平优势

```
score = IDF² × TF_adjusted

TF_adjusted = TF × (k1 + 1) / (TF + k1 × (1 - b + b × (docLen / avgDocLen)))

其中：
- k1 = 1.5（词频饱和度参数）
- b = 0.75（文档长度归一化参数）
```

**为什么 BM25 更好？**

搜索"苹果"：
- 文档A：2000词，"苹果"出现5次
- 文档B：500词，"苹果"出现3次

| 算法 | 文档A | 文档B | 排名 |
|------|-------|-------|------|
| TF-IDF | 5.0 | 3.0 | A > B |
| BM25 | 1.64 | 1.90 | B > A |

BM25 认为短文档中更密集的关键词匹配更有价值。

### 权重调整

| 操作符 | 权重 | 说明 |
|--------|------|------|
| 默认 | 1.0 | 正常权重 |
| `>` | 1.1 | 提高排名 |
| `<` | 0.9 | 降低排名 |
| `~` | -1.0 | 降低排名但不排除 |

### 复合查询评分

- **AND 操作**：分数累加 `+apple +banana → score(apple) + score(banana)`
- **OR 操作**：分数累加 `apple banana → score(apple) + score(banana)`
- **NOT 操作**：排除文档 `+apple -banana → 包含 banana 则排除`
- **GROUP 操作**：取最大分数 `(apple banana) → max(score(apple), score(banana))`

---

## 性能优化

### 索引创建优化

1. **先插入数据，再创建索引**
```sql
-- ✅ 推荐
INSERT INTO articles VALUES (...);
CREATE FULLTEXT INDEX ftidx ON articles (title, content);

-- ❌ 不推荐
CREATE FULLTEXT INDEX ftidx ON articles (title, content);
INSERT INTO articles VALUES (...);  -- 每次插入都更新索引
```

2. **只索引需要搜索的列**

### 查询优化

1. **始终使用 LIMIT**
```sql
-- ✅ 推荐
SELECT * FROM articles 
WHERE MATCH(title, content) AGAINST('keyword')
LIMIT 10;
```

2. **避免返回大文本列**
```sql
-- ✅ 推荐
SELECT id, title FROM articles 
WHERE MATCH(title, content) AGAINST('keyword')
LIMIT 10;
```

3. **混合过滤查询**：先全文检索 Top-K，再过滤
```sql
WITH topk AS (
  SELECT id, title, category,
         MATCH(title, content) AGAINST('keyword') AS score
  FROM articles 
  WHERE MATCH(title, content) AGAINST('keyword')
  ORDER BY score DESC
  LIMIT 100
)
SELECT * FROM topk
WHERE category = 'Technology'
ORDER BY score DESC
LIMIT 10;
```

### 数据操作优化

1. **批量删除**
```sql
-- ✅ 推荐
DELETE FROM articles WHERE id IN (1, 2, 3, 4, 5);
```

2. **大量更新时重建索引**
```sql
DROP INDEX ftidx ON articles;
UPDATE articles SET content = ...;
CREATE FULLTEXT INDEX ftidx ON articles (title, content);
```

---

## 当前限制与不足

### 1. 全量评分计算

**问题**：当前实现会对所有匹配文档计算分数，即使只需要 Top-10。

**影响**：搜索常见词时，可能需要计算百万级文档的分数。

| 场景 | 匹配文档数 | 需要结果 | 实际计算 | 浪费 |
|------|-----------|---------|---------|------|
| 常见词 | 1,000,000 | 10 | 1,000,000 | 99.999% |

### 2. SQL 层 LIMIT 未下推

查询索引表时没有 LIMIT 限制，返回所有匹配文档。

### 3. 统计信息不缓存

每次查询都重新计算总文档数和文档频率。

### 4. 不支持早期终止

不能在计算过程中提前停止，必须计算所有文档分数。

### 5. 删除和更新性能

删除和更新需要同步更新全文索引，开销较大。

### 性能对比

| 指标 | MatrixOne | Elasticsearch |
|------|-----------|---------------|
| 评分计算 | O(N) 所有匹配 | O(K) Top-K |
| 统计信息 | 每次计算 | 缓存 |
| 早期终止 | ❌ | ✅ |

---

## 最佳实践

### 索引设计

- ✅ 使用 BM25 算法：`SET ft_relevancy_algorithm = 'BM25';`
- ✅ 中文文档使用默认解析器（自动 ngram）
- ✅ JSON 文档根据需求选择 `json` 或 `json_value` 解析器
- ✅ 先插入数据，再创建索引

### 查询优化

- ✅ 始终使用 LIMIT
- ✅ 避免搜索过于常见的词
- ✅ 使用布尔模式 `+` 操作符减少匹配文档数
- ✅ 混合过滤时先全文检索再过滤

### 数据操作

- ✅ 批量删除和更新
- ✅ 大量更新时重建索引
- ✅ 避免频繁更新全文索引列

---

## Python SDK

### 安装

```bash
# 稳定版
pip install -U matrixone-python-sdk

# 预发布版
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ matrixone-python-sdk
```

### 快速连接

```python
from matrixone import Client

client = Client()
client.connect(
    host='localhost',
    port=6001,
    user='root',
    password='111',
    database='test'
)
```

### 创建全文索引

```python
# 创建基本全文索引
client.fulltext_index.create(
    'articles',
    name='ftidx_content',
    columns=['title', 'content']
)

# 创建带解析器的索引（JSON 文档）
client.fulltext_index.create(
    'products',
    name='ftidx_specs',
    columns=['specs'],
    parser='json'
)
```

### 全文检索查询

#### 布尔模式搜索

```python
from matrixone.sqlalchemy_ext.fulltext_search import boolean_match, group

# 必须包含
result = client.query('articles').filter(
    boolean_match('title', 'content').must('machine', 'learning')
).execute()

# 必须包含但不包含
result = client.query('articles').filter(
    boolean_match('title', 'content')
    .must('python')
    .must_not('legacy')
).execute()

# 鼓励词（提升相关性）
result = client.query('articles').filter(
    boolean_match('title', 'content')
    .must('python')
    .encourage('tutorial')
).execute()

# 降低相关性
result = client.query('articles').filter(
    boolean_match('title', 'content')
    .must('programming')
    .discourage('deprecated')
).execute()

# 精确短语
result = client.query('articles').filter(
    boolean_match('title', 'content').phrase('machine learning')
).execute()

# 前缀匹配
result = client.query('articles').filter(
    boolean_match('title', 'content').prefix('learn')
).execute()

# OR 逻辑（分组）
result = client.query('articles').filter(
    boolean_match('title', 'content')
    .must(group().medium('programming', 'development'))
).execute()

# 复杂组合
result = client.query('articles').filter(
    boolean_match('title', 'content')
    .must('python')
    .must(group().medium('programming', 'development'))
    .must_not('legacy')
    .encourage('tutorial')
    .phrase('best practices')
).execute()
```

#### 带相关性评分

```python
from matrixone.sqlalchemy_ext.fulltext_search import boolean_match

# 查询带评分
result = client.query(
    'articles.id',
    'articles.title',
    boolean_match('title', 'content').must('machine').label('score')
).execute()

for row in result.fetchall():
    print(f"ID: {row[0]}, Title: {row[1]}, Score: {row[2]:.4f}")
```

### SDK 操作符对照表

| SDK 方法 | SQL 语法 | 说明 |
|----------|----------|------|
| `must('word')` | `+word` | 必须包含 |
| `must_not('word')` | `-word` | 必须不包含 |
| `encourage('word')` | `word` | 提升相关性 |
| `discourage('word')` | `~word` | 降低相关性 |
| `phrase('text')` | `"text"` | 精确短语 |
| `prefix('word')` | `word*` | 前缀匹配 |
| `group().medium('a', 'b')` | `+(a b)` | OR 逻辑分组 |
| `group().high('word')` | `>word` | 高权重 |
| `group().low('word')` | `<word` | 低权重 |

### 完整示例

```python
from matrixone import Client
from matrixone.sqlalchemy_ext.fulltext_search import boolean_match
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, Text

Base = declarative_base()

class Article(Base):
    __tablename__ = 'articles'
    id = Column(Integer, primary_key=True)
    title = Column(String(200))
    content = Column(Text)
    category = Column(String(50))

# 连接数据库
client = Client()
client.connect(host='localhost', port=6001, user='root', password='111', database='test')

# 创建表
client.create_table(Article)

# 插入数据
articles = [
    {'title': 'Machine Learning Guide', 'content': 'Comprehensive ML tutorial...', 'category': 'AI'},
    {'title': 'Python Programming', 'content': 'Learn Python basics', 'category': 'Programming'},
]
client.batch_insert(Article, articles)

# 创建全文索引
client.fulltext_index.create('articles', name='ftidx', columns=['title', 'content'])

# 布尔搜索带评分
result = client.query(
    Article.id,
    Article.title,
    boolean_match(Article.title, Article.content)
    .must('python')
    .encourage('tutorial')
    .label('score')
).order_by('score DESC').limit(10).execute()

for row in result.fetchall():
    print(f"ID: {row[0]}, Title: {row[1]}, Score: {row[2]:.4f}")

client.disconnect()
```

### 更多资源

- [SDK 完整文档](https://matrixone.readthedocs.io/)
- [API 参考](https://matrixone.readthedocs.io/en/latest/api/index.html)

---

## 常见问题

**Q: 中文搜索不准确？**

A: 默认解析器已支持中文 3-gram 分词，无需额外配置。

**Q: 搜索结果不符合预期？**

A: 
1. 检查索引：`SHOW INDEX FROM table_name;`
2. 使用布尔模式精确控制
3. 使用 `EXPLAIN` 查看查询计划

**Q: 查询性能较差？**

A:
1. 使用 LIMIT 限制结果数
2. 避免搜索常见词
3. 使用 BM25 算法

**Q: 删除/更新操作很慢？**

A:
1. 使用批量操作
2. 大量更新时重建索引

**Q: 全文索引未生效？**

A: 
```sql
-- 查看查询计划
EXPLAIN SELECT * FROM articles 
WHERE MATCH(title, content) AGAINST('keyword');

-- 确认索引存在
SHOW INDEX FROM articles;
```

---

## 未来规划

### 短期优化（高优先级）

1. **SQL 层 LIMIT 下推**
   - 在查询索引表时添加 LIMIT
   - 预期减少 90%+ 的数据扫描

2. **统计信息缓存**
   - 缓存总文档数
   - 缓存常见词的文档频率
   - 预期减少重复计算

### 中期优化

3. **早期终止**
   - 实现分数上界估算
   - 提前终止低分文档的计算
   - 预期减少 99%+ 的评分计算

4. **倒排索引优化**
   - 在索引表中存储词频
   - 减少内存使用和计算

### 长期优化

5. **并行搜索**
   - 分片并行搜索
   - 提高吞吐量

6. **Pre-filter 和 Post-filter 模式**
   - 支持更灵活的混合过滤

---

## 附录

### 布尔模式操作符参考

| 操作符 | 说明 | 示例 |
|--------|------|------|
| `+` | 必须包含（AND） | `+machine +learning` |
| `-` | 必须不包含（NOT） | `+machine -deep` |
| `~` | 降低相关性但不排除 | `+machine ~legacy` |
| `""` | 精确短语 | `"machine learning"` |
| `*` | 前缀匹配 | `learn*` |
| `()` | 分组 | `+(machine learning)` |
| `>` | 提高权重 | `>important` |
| `<` | 降低权重 | `<optional` |

### SDK 操作符详细说明

#### must() - 必须包含

```python
# SQL: WHERE MATCH(title, content) AGAINST('+machine +learning' IN BOOLEAN MODE)
result = client.query('articles').filter(
    boolean_match('title', 'content').must('machine', 'learning')
).execute()
```

#### must_not() - 必须不包含

```python
# SQL: WHERE MATCH(content) AGAINST('+programming -legacy' IN BOOLEAN MODE)
result = client.query(Article).filter(
    boolean_match(Article.content)
    .must('programming')
    .must_not('legacy')
).execute()
```

#### encourage() - 鼓励词（提升相关性）

提升包含指定词的文档的相关性评分，但不强制要求包含。

```python
# SQL: WHERE MATCH(title, content) AGAINST('+machine tutorial' IN BOOLEAN MODE)
# 注意：tutorial 不带 + 号，表示可选但会提升相关性
result = client.query('articles').filter(
    boolean_match('title', 'content')
    .must('machine')
    .encourage('tutorial')
).execute()

# 多个鼓励词
# SQL: WHERE MATCH(title, content) AGAINST('+python tutorial guide beginner' IN BOOLEAN MODE)
result = client.query('articles').filter(
    boolean_match('title', 'content')
    .must('python')
    .encourage('tutorial', 'guide', 'beginner')
).execute()
```

#### discourage() - 降低相关性

降低包含指定词的文档的相关性评分，但不排除这些文档。

```python
# SQL: WHERE MATCH(title, content) AGAINST('+python ~legacy' IN BOOLEAN MODE)
result = client.query('articles').filter(
    boolean_match('title', 'content')
    .must('python')
    .discourage('legacy')
).execute()

# 多个降低词
# SQL: WHERE MATCH(title, content) AGAINST('+programming ~deprecated ~outdated ~legacy' IN BOOLEAN MODE)
result = client.query('articles').filter(
    boolean_match('title', 'content')
    .must('programming')
    .discourage('deprecated', 'outdated', 'legacy')
).execute()
```

#### phrase() - 精确短语匹配

要求文档包含精确的短语，词序必须匹配。

```python
# SQL: WHERE MATCH(title, content) AGAINST('"machine learning"' IN BOOLEAN MODE)
result = client.query('articles').filter(
    boolean_match('title', 'content').phrase('machine learning')
).execute()

# 短语与其他操作符组合
# SQL: WHERE MATCH(title, content) AGAINST('+python "best practices" -legacy' IN BOOLEAN MODE)
result = client.query('articles').filter(
    boolean_match('title', 'content')
    .must('python')
    .phrase('best practices')
    .must_not('legacy')
).execute()
```

#### prefix() - 前缀匹配（通配符）

匹配以指定前缀开头的词。

```python
# SQL: WHERE MATCH(title, content) AGAINST('learn*' IN BOOLEAN MODE)
# 匹配：learn, learning, learned, learner 等
result = client.query('articles').filter(
    boolean_match('title', 'content').prefix('learn')
).execute()

# 前缀与其他操作符组合
# SQL: WHERE MATCH(title, content) AGAINST('+python tutor*' IN BOOLEAN MODE)
result = client.query('articles').filter(
    boolean_match('title', 'content')
    .must('python')
    .prefix('tutor')
).execute()
```

#### group() - 分组操作（OR 逻辑）

使用 `group()` 可以在必须条件中实现 OR 逻辑，或者创建加权组。

```python
from matrixone.sqlalchemy_ext.fulltext_search import group

# OR 逻辑：必须包含 "programming" 或 "development"
# SQL: WHERE MATCH(title, content) AGAINST('+(programming development)' IN BOOLEAN MODE)
result = client.query('articles').filter(
    boolean_match('title', 'content')
    .must(group().medium('programming', 'development'))
).execute()

# 加权组：高权重和低权重
# SQL: WHERE MATCH(title, content) AGAINST('>tutorial <basic' IN BOOLEAN MODE)
result = client.query('articles').filter(
    boolean_match('title', 'content')
    .encourage(group().high('tutorial').low('basic'))
).execute()

# 复杂分组
# SQL: WHERE MATCH(title, content) AGAINST('+python +(machine deep) >tutorial <advanced' IN BOOLEAN MODE)
result = client.query('articles').filter(
    boolean_match('title', 'content')
    .must('python')
    .must(group().medium('machine', 'deep'))
    .encourage(group().high('tutorial').low('advanced'))
).execute()
```

**group() 方法说明**：
- `group().medium(term1, term2, ...)`：中等权重组，用于 OR 逻辑
- `group().high(term)`：高权重词
- `group().low(term)`：低权重词

### 操作符优先级和组合规则

1. `must()` 和 `must_not()` 是过滤条件，决定文档是否匹配
   - SQL 对应：`+word` 和 `-word`
2. `encourage()` 和 `discourage()` 只影响相关性评分，不改变匹配结果
   - SQL 对应：`word`（不带操作符）和 `~word`
3. `phrase()` 和 `prefix()` 是特殊的匹配条件
   - SQL 对应：`"phrase"` 和 `word*`
4. `group()` 用于实现 OR 逻辑或加权
   - SQL 对应：`(word1 word2)` 和 `>word`/`<word`
5. 所有操作符可以链式调用，顺序不影响逻辑结果

### 相关资源

- **官方文档**：https://docs.matrixorigin.cn/
- **SDK 文档**：https://matrixone.readthedocs.io/
- **测试用例**：`test/distributed/cases/fulltext/`

---

## 贡献指南

### 如何提交 Issue

如果您在使用过程中遇到问题或有功能建议，欢迎在 MatrixOne 的 GitHub 仓库提交 Issue：

1. **访问 MatrixOne GitHub 仓库**：https://github.com/matrixorigin/matrixone
2. **创建新 Issue**：选择合适的 Issue 模板
3. **填写详细信息**：包括问题描述、复现步骤、环境信息等
4. **添加标签**：如果是全文检索相关问题，可以添加 `fulltext` 标签

### 如何提交 Pull Request

如果您想为 MatrixOne 贡献代码，欢迎提交 Pull Request：

1. **Fork 仓库**：将仓库 Fork 到您的账户
2. **创建分支**：创建功能分支进行开发
3. **提交更改**：编写代码、添加测试、更新文档
4. **创建 Pull Request**：填写 PR 描述，等待代码审查

感谢您对 MatrixOne 的贡献！🎉

---

## 参考资料

- [MatrixOne 官方文档](https://docs.matrixorigin.cn/)
- [Python SDK 文档](https://matrixone.readthedocs.io/)
