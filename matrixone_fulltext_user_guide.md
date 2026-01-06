# MatrixOne 全文检索用户指南

## 目录

- [全文检索背景介绍](#全文检索背景介绍)
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

## 全文检索背景介绍

### 全文检索是什么？

想象一下，你需要在图书馆的几万本书中找到所有提到"人工智能"的内容。如果一本本翻书查找，那得花多少时间？全文检索就是解决这个问题的技术——它让你能够快速在大量文本中找到包含特定关键词的内容。

**日常场景**：
- 在搜索引擎输入关键词，瞬间找到相关网页
- 在文档库中搜索"合同条款"，快速定位相关文档
- 在代码仓库中搜索函数名，找到所有使用的地方

### 历史上的主流解决方案

全文检索技术发展至今，有几个重要的里程碑：

**Lucene Engine（2000年）**
- Apache 开源的全文检索引擎库
- 提供了倒排索引、分词、相关性评分等核心功能
- 是许多全文检索系统的基础

**Elasticsearch（2010年）**
- 基于 Lucene 构建的分布式搜索引擎
- 支持大规模数据、实时搜索、复杂查询
- 广泛应用于日志分析、企业搜索等场景

**ELK 技术栈**
- **E**lasticsearch：存储和搜索
- **L**ogstash：数据收集和处理
- **K**ibana：数据可视化
- 日常可见于企业日志监控、数据分析平台

### 为什么需要全文检索？

传统数据库的 `LIKE '%keyword%'` 查询存在严重的性能问题：

**传统 LIKE 查询的问题**：
- ❌ **全表扫描**：必须扫描每一行数据，性能随数据量线性下降
- ❌ **无法利用索引**：`LIKE '%keyword%'` 无法使用普通索引
- ❌ **无相关性排序**：无法知道哪些结果更相关
- ❌ **功能受限**：不支持复杂查询（AND、OR、NOT、短语匹配等）

**全文检索的优势**：
- ✅ **倒排索引**：预先建立"词 → 文档"的映射，查询时直接定位，性能提升成千上万倍
- ✅ **相关性评分**：使用 TF-IDF 或 BM25 算法，按相关性排序结果
- ✅ **复杂查询**：支持布尔搜索、短语匹配、前缀匹配等高级功能
- ✅ **多语言支持**：自动处理中英文混合文本

**性能对比示例**：
- 100万条记录中搜索关键词
- `LIKE` 查询：需要扫描 100万行，耗时数秒
- 全文检索：直接查索引，耗时毫秒级

这就是为什么现代应用都需要全文检索能力。

### MatrixOne 全文检索

MatrixOne 将全文检索能力直接集成到数据库中，让你无需部署额外的搜索引擎，就能在 SQL 中直接进行全文搜索。它支持：
- 中英文混合搜索
- 相关性评分（TF-IDF、BM25）
- 布尔搜索（AND、OR、NOT）
- JSON 文档搜索
- PDF、DOCX 文件搜索

---

## 快速开始

### MatrixOne vs Elasticsearch：设计理念对比

**在 Elasticsearch 中**，你需要：
- 部署独立的 Elasticsearch 集群
- 定义索引（Index）和映射（Mapping）
- 使用 REST API 或客户端库进行数据导入和搜索
- 维护两套系统：数据库（存储）和搜索引擎（检索）
- 处理数据同步问题（数据库更新后需要同步到 Elasticsearch）

**在 MatrixOne 中**，全文检索就像普通索引一样简单：
- ✅ **无需额外部署**：全文检索能力直接集成在数据库中，无需独立的搜索引擎
- ✅ **使用标准 SQL**：创建全文索引就像创建普通索引（如 `CREATE INDEX`）一样，语法简单直观
- ✅ **统一的数据管理**：数据存储和检索在同一个系统中，无需维护两套系统
- ✅ **自动同步**：插入、更新、删除数据时，全文索引自动维护，无需手动同步
- ✅ **事务一致性**：全文检索与数据操作在同一事务中，保证数据一致性

**简单对比**：

| 特性 | Elasticsearch | MatrixOne |
|------|---------------|-----------|
| 部署方式 | 独立集群 | 数据库内置 |
| 查询语言 | REST API / DSL | 标准 SQL |
| 数据同步 | 需要手动同步 | 自动同步 |
| 事务支持 | 不支持 | 支持 |
| 学习成本 | 需要学习 DSL | 只需 SQL |

**代码示例对比**：

```sql
-- MatrixOne：就像创建普通索引一样简单
-- 1. 创建表（普通表，无需特殊配置）
CREATE TABLE articles (
  id INT PRIMARY KEY,
  title VARCHAR(200),
  content TEXT
);

-- 2. 创建全文索引（就像创建普通索引一样简单）
CREATE FULLTEXT INDEX ftidx ON articles (title, content);
-- 对比：普通索引是 CREATE INDEX idx_name ON table(column)
--       全文索引是 CREATE FULLTEXT INDEX idx_name ON table(column)

-- 3. 插入数据（全文索引自动更新，无需额外操作）
INSERT INTO articles VALUES (1, '标题', '内容');

-- 4. 使用标准 SQL 搜索（就像 WHERE 条件一样）
SELECT * FROM articles
WHERE MATCH(title, content) AGAINST('关键词');
```

**核心优势**：在 MatrixOne 中，全文索引就是数据库的一个普通索引类型，使用方式与普通索引完全一致，无需学习新的 API 或工具。

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

本章节介绍 MatrixOne 全文检索的核心概念和配置选项，帮助你理解如何选择和使用不同的功能。

### 搜索模式

MatrixOne 支持两种搜索模式，适用于不同的使用场景：

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| **自然语言模式** | 短语搜索，词之间是 AND 关系，检查位置顺序 | 用户搜索框，通用搜索 |
| **布尔模式** | 支持 `+`、`-`、`""`、`*` 等操作符 | 高级搜索，程序化查询 |

**使用建议**：
- 普通用户搜索：使用自然语言模式（默认）
- 需要精确控制：使用布尔模式，支持复杂的查询逻辑

### 相关性评分算法

相关性算法决定了搜索结果如何排序。MatrixOne 支持两种算法：

| 算法 | 说明 | 推荐场景 |
|------|------|----------|
| **TF-IDF** | 词频-逆文档频率，传统算法 | 默认算法，简单场景 |
| **BM25** | 改进的 TF-IDF，考虑文档长度归一化 | **推荐**，效果更好，长文档场景 |

**BM25 的优势**：
- 考虑文档长度，避免长文档获得不公平优势
- 词频饱和度，避免高频词过度影响评分
- 更符合现代搜索引擎的评分标准

**设置方法**：
```sql
SET ft_relevancy_algorithm = 'BM25';  -- 推荐
SET ft_relevancy_algorithm = 'TF-IDF'; -- 默认
```

### 分词解析器

分词解析器决定了如何将文本分解为可搜索的词。MatrixOne 提供多种解析器：

| 解析器 | 说明 | 适用场景 |
|--------|------|----------|
| **default/ngram** | 智能分词：英文按空格，中文按 3-gram | 通用文档（默认），中英文混合 |
| **json** | 提取 JSON 值后**分词** | JSON 文档，搜索文本内容（如商品描述） |
| **json_value** | 提取 JSON 值作为**完整词**（不分词） | JSON 文档，搜索标识符（如订单号、产品编号） |

**选择建议**：
- 普通文本列：使用 `default`（默认），自动处理中英文
- JSON 列需要搜索值内容：使用 `json`
- JSON 列需要精确匹配值：使用 `json_value`

### 支持的列类型

全文索引可以创建在以下类型的列上：

- `CHAR` / `VARCHAR` / `TEXT`：文本类型，最常用
- `JSON`：JSON 文档，需要配合 `json` 或 `json_value` 解析器
- `DATALINK`：文件链接，支持 PDF、DOCX 文件（详见 [DATALINK 文档搜索](#datalink-文档搜索) 章节）

### 核心概念总结

**倒排索引**：全文检索的核心数据结构，将"词 → 文档"的映射关系存储起来，实现快速定位。

**工作流程**：
1. **索引创建**：文本被分词后，建立倒排索引（词 → 文档列表）
2. **查询执行**：搜索词被分词，通过倒排索引快速找到匹配文档
3. **评分排序**：使用 TF-IDF 或 BM25 算法计算相关性分数
4. **结果返回**：按分数排序返回最相关的文档

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

- `CHAR` / `VARCHAR` / `TEXT`：文本类型，最常用
- `JSON`：JSON 文档，需要配合 `json` 或 `json_value` 解析器（详见 [JSON 文档搜索](#json-文档搜索)）
- `DATALINK`：文件引用类型，支持 PDF、DOCX 文件的全文检索（详见 [DATALINK 文档搜索](#datalink-文档搜索)）

**DATALINK 简介**：
- DATALINK 存储文件路径/URL，而不是文件内容本身
- 支持 `file://`、`stage://`、`hdfs://` 等路径格式
- 可以对 PDF、DOCX 文件内容进行全文检索
- 适合文档管理系统、知识库等场景

### 设置相关性算法

```sql
-- 设置为 BM25（推荐）
SET ft_relevancy_algorithm = 'BM25';

-- 设置为 TF-IDF（默认）
SET ft_relevancy_algorithm = 'TF-IDF';
```

### 全文检索查询

#### 自然语言模式

自然语言模式是**短语搜索**：分词后所有词之间是 AND 关系，且**检查位置顺序**。这是默认的搜索模式，适合大多数用户搜索场景。

**核心特性**：
1. **所有词必须出现**：搜索词分词后，所有词都必须在文档中出现（AND 关系）
2. **检查位置顺序**：词在文档中的出现顺序必须与搜索词一致
3. **相关性评分**：返回的结果按相关性分数排序

**示例 1：基础搜索**

```sql
-- 搜索 "machine learning"（默认自然语言模式）
SELECT * FROM articles
WHERE MATCH(title, content) AGAINST('machine learning');

-- 显式指定模式（与上面等价）
SELECT * FROM articles
WHERE MATCH(title, content) AGAINST('machine learning' IN NATURAL LANGUAGE MODE);
```

**匹配规则**：
- ✅ 匹配："This article is about machine learning"（词顺序一致）
- ✅ 匹配："machine learning algorithms"（词顺序一致）
- ❌ 不匹配："learning machine"（词顺序相反）
- ❌ 不匹配："machine and deep learning"（中间有其他词，位置顺序不连续）

**示例 2：多个词搜索**

```sql
-- 搜索 "artificial intelligence future"
SELECT * FROM articles
WHERE MATCH(title, content) AGAINST('artificial intelligence future');
```

**匹配规则**：
- ✅ 匹配："artificial intelligence is the future"（三个词按顺序出现）
- ✅ 匹配："artificial intelligence and future development"（三个词按顺序出现，中间可以有其他词）
- ❌ 不匹配："The future of artificial intelligence"（"future"在"artificial"之前，顺序错误）
- ❌ 不匹配："intelligence artificial future"（"artificial"和"intelligence"顺序错误）

**示例 3：中文搜索**

```sql
-- 搜索中文短语
SELECT * FROM articles
WHERE MATCH(title, content) AGAINST('人工智能 未来');
```

**匹配规则**：
- ✅ 匹配："人工智能是未来的趋势"（词按顺序出现）
- ✅ 匹配："人工智能和未来发展趋势"（两个词按顺序出现）
- ❌ 不匹配："未来的人工智能发展"（"未来"在"人工智能"之前，顺序错误）
- ❌ 不匹配："智能人工 未来"（"人工智能"被拆分，顺序不对）

**示例 4：带相关性评分**

```sql
-- 获取搜索结果和相关性分数
SELECT id, title,
       MATCH(title, content) AGAINST('machine learning') AS score
FROM articles
WHERE MATCH(title, content) AGAINST('machine learning')
ORDER BY score DESC  -- 按相关性排序
LIMIT 10;
```

**使用场景**：
- ✅ 用户搜索框：用户输入自然语言查询
- ✅ 通用搜索：不需要复杂查询逻辑的场景
- ✅ 简单关键词搜索：查找包含特定短语的文档

**注意事项**：
- 自然语言模式会检查词的位置顺序，如果需要更灵活的词序匹配，使用布尔模式
- 对于单个词搜索，自然语言模式和布尔模式效果相同
- 自然语言模式不支持操作符（`+`、`-`、`*` 等），需要操作符时使用布尔模式

#### 布尔模式

布尔模式提供了强大的查询控制能力，支持多种操作符来构建复杂的搜索查询。

**重要提示：查询词分词**

布尔模式需要用户明确指定每个查询词，因此需要将查询文本分词。不同语言的分词难度不同：

| 语言 | 分词难度 | 方法 | 是否需要工具 |
|------|---------|------|------------|
| **英文** | 简单 | 按空格分隔 | 通常不需要 |
| **中文** | 复杂 | 需要识别词边界 | **需要分词库** |

**英文分词**（简单）：
- 英文有明确的词边界（空格），分词很简单
- 示例：`"machine learning"` → `+machine +learning`（按空格分隔即可）

**中文分词**（复杂）：
- 中文没有明显的词边界，需要专门的分词工具
- 示例：`"人工智能和机器学习"` → `+人工智能 +机器学习`（需要分词工具识别词边界）

**为什么需要分词？**

- **自然语言模式**：系统会自动分词，用户直接输入原始文本即可
- **布尔模式**：需要用户明确指定每个词，系统不会自动分词，因此需要用户先分词

**英文分词示例**（简单，通常不需要工具）：

```python
def english_to_boolean_query(text, mode='must'):
    """
    将英文查询转换为布尔模式查询
    英文分词很简单：按空格分隔即可
    """
    words = text.split()  # 按空格分词

    # 可选：去除标点符号
    import string
    words = [w.strip(string.punctuation) for w in words if w.strip()]

    if mode == 'must':
        return ' '.join(['+' + w for w in words])
    else:
        return ' '.join(words)

# 使用示例
query = english_to_boolean_query("machine learning deep")
# 结果：'+machine +learning +deep'

query = english_to_boolean_query("database optimization")
# 结果：'+database +optimization'
```

**中文分词库推荐**

中文分词比英文复杂得多，需要专门的分词工具。以下是常用的中文分词库：

**1. jieba（结巴分词）** - 最流行的中文分词库

```python
import jieba

# 安装：pip install jieba

# 精确模式分词（推荐）
query = "人工智能和机器学习"
words = jieba.cut(query, cut_all=False)
# 结果：['人工智能', '和', '机器学习']

# 转换为布尔模式查询
boolean_query = ' +'.join(['+' + word for word in words if len(word.strip()) > 1])
# 结果：'+人工智能 +机器学习'（忽略单字和标点）

# 完整示例
def build_boolean_query_chinese(text):
    """将中文文本转换为布尔模式查询"""
    words = jieba.cut(text, cut_all=False)
    # 过滤单字和标点，只保留有意义的词
    meaningful_words = [w for w in words if len(w.strip()) > 1 and w.strip() not in '，。！？、；：']
    return ' '.join(['+' + word for word in meaningful_words])

# 使用示例
query = build_boolean_query_chinese("人工智能 机器学习 深度学习")
# 结果：'+人工智能 +机器学习 +深度学习'
```

**2. pkuseg（北大分词）** - 准确度较高

```python
import pkuseg

# 安装：pip install pkuseg

seg = pkuseg.pkuseg()  # 默认模型
query = "人工智能和机器学习"
words = seg.cut(query)
# 结果：['人工智能', '和', '机器学习']

# 转换为布尔模式
boolean_query = ' '.join(['+' + w for w in words if len(w) > 1])
```

**3. HanLP** - 功能强大的自然语言处理库

```python
from pyhanlp import *

# 安装：pip install pyhanlp
# 注意：首次使用需要下载模型

query = "人工智能和机器学习"
words = HanLP.segment(query)
# 结果：词列表

# 提取词
word_list = [str(term.word) for term in words]
boolean_query = ' '.join(['+' + w for w in word_list if len(w) > 1])
```

**4. 完整示例：构建中文布尔查询**

```python
import jieba
import re

def chinese_to_boolean_query(text, mode='must'):
    """
    将中文查询转换为布尔模式查询

    Args:
        text: 中文查询文本
        mode: 'must' (必须包含) 或 'should' (可选)

    Returns:
        布尔模式查询字符串
    """
    # 分词
    words = jieba.cut(text, cut_all=False)

    # 过滤：只保留长度>1的词，去除标点
    meaningful_words = [
        w.strip() for w in words
        if len(w.strip()) > 1 and re.match(r'^[\u4e00-\u9fa5a-zA-Z0-9]+$', w.strip())
    ]

    if not meaningful_words:
        return text  # 如果没有有效词，返回原文

    # 构建布尔查询
    if mode == 'must':
        return ' '.join(['+' + w for w in meaningful_words])
    else:
        return ' '.join(meaningful_words)

# 使用示例
query1 = chinese_to_boolean_query("人工智能 机器学习")
# 结果：'+人工智能 +机器学习'

query2 = chinese_to_boolean_query("数据库优化", mode='must')
# 结果：'+数据库 +优化'

# 在 SQL 中使用
sql = f"""
SELECT * FROM articles
WHERE MATCH(title, content) AGAINST('{query1}' IN BOOLEAN MODE);
"""
```


**操作符说明**

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

**SQL 示例**

```sql
-- 英文查询示例

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

**中文查询示例（需要先分词）**

```sql
-- 中文查询：搜索"人工智能 机器学习"
-- 注意：需要先分词为 "+人工智能 +机器学习"
SELECT * FROM articles
WHERE MATCH(title, content) AGAINST('+人工智能 +机器学习' IN BOOLEAN MODE);

-- 中文查询：必须包含"数据库"，但不包含"旧版"
SELECT * FROM articles
WHERE MATCH(title, content) AGAINST('+数据库 -旧版' IN BOOLEAN MODE);

-- 中文查询：精确短语（用引号）
SELECT * FROM articles
WHERE MATCH(title, content) AGAINST('"人工智能 机器学习"' IN BOOLEAN MODE);

-- 中文查询：复杂组合
-- 必须包含"Python"，可选包含"编程"或"开发"，排除"旧版"
SELECT * FROM articles
WHERE MATCH(title, content) AGAINST('+Python +(编程 开发) -旧版' IN BOOLEAN MODE);
```

**Python 完整集成示例**

```python
import jieba
import pymysql  # 或使用 MatrixOne Python SDK

def search_articles_chinese(keywords, connection):
    """
    使用中文关键词搜索文章（布尔模式）

    Args:
        keywords: 中文关键词，如 "人工智能 机器学习"
        connection: 数据库连接
    """
    # 分词并构建布尔查询
    words = jieba.cut(keywords, cut_all=False)
    meaningful_words = [w.strip() for w in words if len(w.strip()) > 1]
    boolean_query = ' '.join(['+' + w for w in meaningful_words])

    # 构建 SQL
    sql = f"""
    SELECT id, title,
           MATCH(title, content) AGAINST('{boolean_query}' IN BOOLEAN MODE) AS score
    FROM articles
    WHERE MATCH(title, content) AGAINST('{boolean_query}' IN BOOLEAN MODE)
    ORDER BY score DESC
    LIMIT 10
    """

    # 执行查询
    cursor = connection.cursor()
    cursor.execute(sql)
    return cursor.fetchall()

# 使用示例
# results = search_articles_chinese("人工智能 深度学习", connection)
```

### JSON 文档搜索

MatrixOne 支持对 JSON 列进行全文检索，提供两种解析器来处理不同的搜索需求。

**核心区别**：

| 解析器 | 处理方式 | 适用场景 | 示例 |
|--------|---------|---------|------|
| **json** | 提取 JSON 值后**分词** | 搜索 JSON 中的文本内容 | 商品描述、文章内容 |
| **json_value** | 提取 JSON 值作为**完整词**（不分词） | 搜索 JSON 中的标识符、代码 | 产品编号、订单号、标签 |

---

#### json 解析器：搜索文本内容

**作用**：提取 JSON 中的所有值，然后对每个值进行分词，适合搜索 JSON 中的文本内容。

**使用场景**：
- ✅ 商品描述、产品详情
- ✅ 文章内容、评论
- ✅ 用户输入的文本字段
- ✅ 需要部分匹配的场景

**示例：电商商品搜索**

```sql
-- 创建商品表
CREATE TABLE products (
  id INT PRIMARY KEY,
  name VARCHAR(200),
  details JSON  -- 存储商品详细信息
);

-- 插入商品数据
INSERT INTO products VALUES
  (1, 'iPhone 15', '{"brand": "Apple", "description": "最新款智能手机，配备A17芯片", "tags": ["手机", "苹果", "5G"]}'),
  (2, 'MacBook Pro', '{"brand": "Apple", "description": "专业级笔记本电脑，适合编程和设计", "tags": ["电脑", "苹果", "M3芯片"]}'),
  (3, 'AirPods Pro', '{"brand": "Apple", "description": "降噪无线耳机", "tags": ["耳机", "苹果"]}');

-- 使用 json 解析器创建全文索引
CREATE FULLTEXT INDEX ftidx ON products (details) WITH PARSER json;

-- 搜索示例
-- 1. 搜索品牌（会被分词）
SELECT id, name FROM products WHERE MATCH(details) AGAINST('Apple');
-- ✓ 匹配所有商品（因为 "Apple" 在 brand 字段中）

-- 2. 搜索描述中的关键词（会被分词）
SELECT id, name FROM products WHERE MATCH(details) AGAINST('芯片');
-- ✓ 匹配 iPhone 15 和 MacBook Pro（因为描述中包含"芯片"）

-- 3. 搜索标签（会被分词）
SELECT id, name FROM products WHERE MATCH(details) AGAINST('苹果');
-- ✓ 匹配所有商品（因为 tags 中包含"苹果"）

-- 4. 搜索部分词（分词后的结果）
SELECT id, name FROM products WHERE MATCH(details) AGAINST('智能');
-- ✓ 匹配 iPhone 15（因为"智能手机"被分词后包含"智能"）
```

**工作原理**：
```
JSON 值: "最新款智能手机，配备A17芯片"
         ↓ json 解析器提取并分词
索引词: ["最新款", "智能", "手机", "配备", "A17", "芯片"]
         ↓ 可以搜索任意词
搜索 "智能" → ✓ 匹配
搜索 "芯片" → ✓ 匹配
```

---

#### json_value 解析器：搜索完整标识符

**作用**：提取 JSON 中的所有值，但将每个值作为**完整的词**（不分词），适合搜索标识符、代码、编号等。

**使用场景**：
- ✅ 产品编号、订单号
- ✅ 用户ID、账号
- ✅ 标签、分类代码
- ✅ 需要精确匹配的场景

**示例：订单管理系统**

```sql
-- 创建订单表
CREATE TABLE orders (
  id INT PRIMARY KEY,
  order_info JSON  -- 存储订单详细信息
);

-- 插入订单数据
INSERT INTO orders VALUES
  (1, '{"order_no": "ORD-2024-001", "user_id": "U12345", "status": "pending", "product_code": "PROD-ABC-123"}'),
  (2, '{"order_no": "ORD-2024-002", "user_id": "U67890", "status": "shipped", "product_code": "PROD-XYZ-789"}'),
  (3, '{"order_no": "ORD-2024-003", "user_id": "U12345", "status": "delivered", "product_code": "PROD-ABC-456"}');

-- 使用 json_value 解析器创建全文索引
CREATE FULLTEXT INDEX ftidx ON orders (order_info) WITH PARSER json_value;

-- 搜索示例
-- 1. 搜索完整订单号
SELECT id FROM orders WHERE MATCH(order_info) AGAINST('ORD-2024-001');
-- ✓ 匹配订单 1（精确匹配完整订单号）

-- 2. 搜索部分订单号（不匹配！）
SELECT id FROM orders WHERE MATCH(order_info) AGAINST('ORD-2024');
-- ✗ 不匹配！因为 "ORD-2024-001" 是完整词，不会被拆分

-- 3. 搜索完整用户ID
SELECT id FROM orders WHERE MATCH(order_info) AGAINST('U12345');
-- ✓ 匹配订单 1 和 3（精确匹配完整用户ID）

-- 4. 搜索部分用户ID（不匹配！）
SELECT id FROM orders WHERE MATCH(order_info) AGAINST('U123');
-- ✗ 不匹配！因为 "U12345" 是完整词

-- 5. 搜索完整产品代码
SELECT id FROM orders WHERE MATCH(order_info) AGAINST('PROD-ABC-123');
-- ✓ 匹配订单 1（精确匹配完整产品代码）
```

**工作原理**：
```
JSON 值: "ORD-2024-001"
         ↓ json_value 解析器提取（不分词）
索引词: ["ORD-2024-001"]  （作为完整词）
         ↓ 只能搜索完整值
搜索 "ORD-2024-001" → ✓ 匹配
搜索 "ORD-2024"     → ✗ 不匹配（不是完整词）
搜索 "001"          → ✗ 不匹配（不是完整词）
```

---

#### 对比示例：同一数据，不同解析器

让我们用同一个 JSON 数据，看看两种解析器的区别：

```sql
CREATE TABLE test_data (id INT PRIMARY KEY, data JSON);
INSERT INTO test_data VALUES
  (1, '{"product_code": "ABC-123-XYZ", "description": "高性能处理器"}');

-- 使用 json 解析器
CREATE FULLTEXT INDEX ftidx_json ON test_data (data) WITH PARSER json;

-- 使用 json_value 解析器
CREATE FULLTEXT INDEX ftidx_json_value ON test_data (data) WITH PARSER json_value;
```

**搜索对比**：

| 搜索词 | json 解析器 | json_value 解析器 | 说明 |
|--------|-----------|------------------|------|
| `"高性能"` | ✅ 匹配 | ❌ 不匹配 | json 会分词"高性能处理器" |
| `"处理器"` | ✅ 匹配 | ❌ 不匹配 | json 会分词"高性能处理器" |
| `"ABC-123-XYZ"` | ✅ 匹配 | ✅ 匹配 | 完整值，两种都匹配 |
| `"ABC-123"` | ✅ 匹配 | ❌ 不匹配 | json 会分词，json_value 不会 |
| `"ABC"` | ✅ 匹配 | ❌ 不匹配 | json 会分词，json_value 不会 |

**选择建议**：

```sql
-- ✅ 场景1：搜索商品描述、评论等文本内容
-- 使用 json 解析器
CREATE FULLTEXT INDEX ftidx ON products (description) WITH PARSER json;
-- 可以搜索：描述中的任意词

-- ✅ 场景2：搜索产品编号、订单号等标识符
-- 使用 json_value 解析器
CREATE FULLTEXT INDEX ftidx ON orders (order_info) WITH PARSER json_value;
-- 只能搜索：完整的编号

-- ✅ 场景3：混合场景（既有文本又有标识符）
-- 根据主要搜索需求选择：
-- - 主要搜索文本 → json
-- - 主要搜索标识符 → json_value
```

**完整示例：商品搜索系统**

```sql
-- 商品表，包含文本描述和产品编号
CREATE TABLE products (
  id INT PRIMARY KEY,
  info JSON
);

INSERT INTO products VALUES
  (1, '{"code": "PROD-001", "name": "iPhone 15 Pro", "description": "最新款苹果手机，配备A17 Pro芯片"}'),
  (2, '{"code": "PROD-002", "name": "MacBook Pro", "description": "专业级笔记本电脑，M3芯片"}');

-- 场景A：用户想搜索商品描述中的关键词（如"芯片"、"手机"）
CREATE FULLTEXT INDEX ftidx_desc ON products (info) WITH PARSER json;
SELECT * FROM products WHERE MATCH(info) AGAINST('芯片');
-- ✓ 匹配两个商品（因为描述中都包含"芯片"）

-- 场景B：用户想通过产品编号精确搜索（如"PROD-001"）
CREATE FULLTEXT INDEX ftidx_code ON products (info) WITH PARSER json_value;
SELECT * FROM products WHERE MATCH(info) AGAINST('PROD-001');
-- ✓ 只匹配商品1（精确匹配完整编号）

SELECT * FROM products WHERE MATCH(info) AGAINST('PROD');
-- ✗ 不匹配（因为"PROD-001"是完整词，不会被拆分）
```

---

#### 未来提案：增强 JSON 搜索能力（类似 MongoDB）

**当前限制**：
- 现有解析器只能搜索 JSON 中的所有值，无法针对特定字段搜索
- 无法搜索嵌套路径（如 `user.profile.name`）
- 无法区分不同字段的值（如同时搜索 `name` 和 `description` 字段）

**提案目标**：
实现类似 MongoDB 的 JSON 搜索能力，支持：
1. **字段级搜索**：针对特定 JSON 字段进行搜索
2. **嵌套路径搜索**：支持搜索嵌套 JSON 结构中的值
3. **多字段组合搜索**：可以同时搜索多个字段，并指定不同权重
4. **数组元素搜索**：支持搜索 JSON 数组中的元素

**提案设计**：

**1. 字段路径语法**

```sql
-- 提案：支持 JSON 路径语法
CREATE FULLTEXT INDEX ftidx ON products (info) 
WITH PARSER json_path('name', 'description', 'tags[*]');

-- 搜索特定字段
SELECT * FROM products 
WHERE MATCH(info) AGAINST('iPhone' IN JSON_FIELD 'name');
-- 只在 name 字段中搜索 "iPhone"

-- 搜索嵌套路径
SELECT * FROM products 
WHERE MATCH(info) AGAINST('Apple' IN JSON_FIELD 'brand.name');
-- 搜索 brand.name 字段

-- 搜索数组元素
SELECT * FROM products 
WHERE MATCH(info) AGAINST('手机' IN JSON_FIELD 'tags[*]');
-- 在 tags 数组的所有元素中搜索
```

**2. 多字段组合搜索**

```sql
-- 提案：支持多字段搜索，不同字段可以设置不同权重
CREATE FULLTEXT INDEX ftidx ON products (info) 
WITH PARSER json_multi(
  'name' WITH WEIGHT 2.0,      -- name 字段权重更高
  'description' WITH WEIGHT 1.0,
  'tags[*]' WITH WEIGHT 0.5
);

-- 搜索时，name 字段的匹配会获得更高分数
SELECT id, name, 
       MATCH(info) AGAINST('iPhone Pro' IN JSON_MULTI) AS score
FROM products
ORDER BY score DESC;
```

**3. 字段值精确匹配**

```sql
-- 提案：支持字段值的精确匹配（类似 MongoDB 的字段查询）
SELECT * FROM products 
WHERE MATCH(info) AGAINST('PROD-001' IN JSON_FIELD 'code' EXACT);
-- 只在 code 字段中精确匹配 "PROD-001"

-- 支持范围查询
SELECT * FROM products 
WHERE MATCH(info) AGAINST('price > 1000' IN JSON_FIELD 'price');
```

**4. 复杂查询示例**

```sql
-- 提案：类似 MongoDB 的查询能力
-- 示例数据
{
  "name": "iPhone 15 Pro",
  "brand": {
    "name": "Apple",
    "country": "USA"
  },
  "specs": {
    "cpu": "A17 Pro",
    "ram": "8GB",
    "storage": ["128GB", "256GB", "512GB"]
  },
  "tags": ["手机", "5G", "苹果"]
}

-- 搜索品牌名称
SELECT * FROM products 
WHERE MATCH(info) AGAINST('Apple' IN JSON_FIELD 'brand.name');

-- 搜索存储容量（数组元素）
SELECT * FROM products 
WHERE MATCH(info) AGAINST('256GB' IN JSON_FIELD 'specs.storage[*]');

-- 多字段组合搜索
SELECT * FROM products 
WHERE MATCH(info) AGAINST(
  '+iPhone +Pro' IN JSON_MULTI (
    'name' WITH WEIGHT 2.0,
    'description' WITH WEIGHT 1.0
  )
);
```

**5. 实现思路**

**索引结构扩展**：
```
当前索引表：
doc_id | word | pos

提案扩展：
doc_id | word | pos | json_path | field_weight
-------|------|-----|-----------|-------------
1      | iPhone | 0  | name      | 2.0
1      | Pro    | 7  | name      | 2.0
1      | 最新款 | 0  | description | 1.0
```

**查询优化**：
- 支持路径过滤：`WHERE json_path = 'name'`
- 支持权重计算：`score = base_score × field_weight`
- 支持数组展开：`tags[*]` 展开为多个索引项

**6. 与 MongoDB 的对比**

| 功能 | MongoDB | MatrixOne 当前 | MatrixOne 提案 |
|------|---------|---------------|---------------|
| 字段级搜索 | ✅ `{"name": "iPhone"}` | ❌ 搜索所有值 | ✅ `IN JSON_FIELD 'name'` |
| 嵌套路径 | ✅ `{"brand.name": "Apple"}` | ❌ | ✅ `IN JSON_FIELD 'brand.name'` |
| 数组搜索 | ✅ `{"tags": "手机"}` | ❌ | ✅ `IN JSON_FIELD 'tags[*]'` |
| 多字段组合 | ✅ `$or`, `$and` | ❌ | ✅ `JSON_MULTI` |
| 字段权重 | ✅ `$text` with weights | ❌ | ✅ `WITH WEIGHT` |
| 全文检索 | ✅ `$text` | ✅ `MATCH...AGAINST` | ✅ 增强版 |

**7. 使用场景**

- **电商搜索**：按商品名称、描述、标签分别搜索
- **用户搜索**：按用户名、邮箱、地址等字段搜索
- **日志分析**：按不同日志字段（level、message、source）搜索
- **配置管理**：按配置项的不同路径搜索

**8. 实施优先级**

- **Phase 1**：基础字段路径支持（`IN JSON_FIELD 'path'`）
- **Phase 2**：嵌套路径和数组支持（`brand.name`, `tags[*]`）
- **Phase 3**：多字段组合和权重（`JSON_MULTI`）
- **Phase 4**：复杂查询操作符（范围查询、精确匹配等）

这个提案将大大增强 MatrixOne 的 JSON 搜索能力，使其更接近 MongoDB 的查询灵活性，同时保持全文检索的高性能优势。

### DATALINK 文档搜索

#### 什么是 DATALINK？

**DATALINK** 是 MatrixOne 提供的一种特殊数据类型，用于存储**文件引用**（文件路径），而不是文件内容本身。它类似于数据库中的"链接"或"指针"，指向实际存储的文件。

**核心概念**：

| 特性 | 说明 | 类比 |
|------|------|------|
| **存储内容** | 文件路径/URL，不是文件内容 | 像书签，指向文件位置 |
| **文件位置** | 文件存储在外部（本地文件系统、HDFS、Stage等） | 文件不在数据库中 |
| **访问方式** | 通过 URL 协议访问（file://、stage://、hdfs://） | 像网页链接 |
| **优势** | 节省数据库存储空间，支持大文件 | 数据库只存"地址"，不存"房子" |

**DATALINK vs 其他类型**：

```sql
-- ❌ 传统方式：将文件内容存储在 BLOB 列中
CREATE TABLE docs_old (
  id INT PRIMARY KEY,
  file_content BLOB  -- 存储整个文件内容，占用大量空间
);

-- ✅ DATALINK 方式：只存储文件引用
CREATE TABLE docs (
  id INT PRIMARY KEY,
  file_path DATALINK  -- 只存储文件路径，文件在外部
);
```

**DATALINK 的优势**：
- ✅ **节省存储空间**：数据库只存储文件路径（通常几十字节），不存储文件内容（可能几MB到几GB）
- ✅ **支持大文件**：不受数据库存储限制，可以引用任意大小的文件
- ✅ **文件管理灵活**：文件可以存储在本地、HDFS、对象存储等不同位置
- ✅ **全文检索支持**：可以对 DATALINK 指向的文件内容进行全文检索

**支持的路径格式**：

```sql
-- 1. Stage 路径（推荐，便于管理）
'stage://stage_name/file.pdf'

-- 2. 本地文件路径
'file:///absolute/path/to/file.pdf'
'file://host/path/to/file.pdf'

-- 3. HDFS 路径
'hdfs://namenode:port/path/to/file.pdf'

-- 4. 支持指定文件片段（offset 和 size）
'file:///path/to/file.pdf?offset=100&size=1024'
```

**使用场景**：
- 📄 **文档管理系统**：存储大量 PDF、DOCX 文档的引用
- 📚 **知识库系统**：管理技术文档、手册等
- 📝 **内容管理系统**：存储文章、报告等文件
- 🔍 **文档搜索系统**：对文件内容进行全文检索

**示例：文档管理系统**

假设你有一个包含 10,000 个 PDF 文档的文档库，每个文档平均 5MB：

```sql
-- ❌ 传统方式：将文件内容存储在数据库中
CREATE TABLE documents_old (
  id INT PRIMARY KEY,
  title VARCHAR(200),
  file_content BLOB  -- 存储整个 PDF 内容
);
-- 问题：10,000 × 5MB = 50GB 的数据库存储空间！

-- ✅ DATALINK 方式：只存储文件引用
CREATE TABLE documents (
  id INT PRIMARY KEY,
  title VARCHAR(200),
  file_path DATALINK  -- 只存储文件路径（约 100 字节）
);
-- 优势：10,000 × 100 字节 ≈ 1MB 的数据库存储空间！
-- 文件实际存储在文件系统中，数据库只存"地址"
```

**工作流程**：

```
1. 文件存储在文件系统
   /documents/report_001.pdf (5MB)
   /documents/report_002.pdf (3MB)
   ...

2. 数据库中只存储路径
   INSERT INTO documents VALUES
     (1, '年度报告', 'file:///documents/report_001.pdf'),
     (2, '技术文档', 'file:///documents/report_002.pdf');

3. 查询时，系统通过路径访问文件
   SELECT * FROM documents WHERE id = 1;
   → 系统读取 file:///documents/report_001.pdf
```

---

#### 全文检索支持

MatrixOne 支持对 DATALINK 指向的 PDF、DOCX 文件内容进行全文检索。系统会自动提取文件中的文本内容，建立全文索引。

**工作原理**：

```
DATALINK 值: 'file:///documents/report.pdf'
            ↓
系统读取文件内容
            ↓
提取文本（PDF/DOCX 自动解析）
            ↓
分词并建立全文索引
            ↓
可以像普通文本列一样搜索
```

**实现原理**：
- 代码位置：`pkg/datalink/datalink.go` 的 `GetPlainText()` 方法
- PDF 处理：`pkg/datalink/pdf/pdf.go`（优先使用 pdftotext，fallback 到 gopdf）
- DOCX 处理：`pkg/datalink/docx/docx.go`（解析 ZIP 格式，提取 word/document.xml）
- 全文索引创建时：`pkg/sql/colexec/table_function/fulltext_tokenize.go` 自动调用 `GetPlainText()` 提取文本

**支持的文件格式**：
- ✅ **PDF**：`.pdf` 文件（自动提取文本内容）
- ✅ **DOCX**：`.docx` 文件（自动提取文本内容）
- ⚠️ **其他格式**：返回原始字节，无法进行有效的全文检索（更多格式支持正在开发中）

**使用方法**：

```sql
-- 1. 创建 stage（可选，用于管理文件）
CREATE STAGE ftstage URL='file:///path/to/files/';

-- 2. 创建表，包含 DATALINK 列
CREATE TABLE docs (
  id INT PRIMARY KEY,
  fpath DATALINK,
  FULLTEXT(fpath)  -- 在 DATALINK 列上创建全文索引
);

-- 3. 插入文件引用（支持两种路径格式）
INSERT INTO docs VALUES
  (1, 'stage://ftstage/document.pdf'),           -- 使用 stage 路径
  (2, 'file:///path/to/chinese.pdf'),            -- 使用 file:// 绝对路径
  (3, 'file:///path/to/file-sample_100kB.docx'); -- DOCX 文件

-- 4. 搜索文件内容（支持中英文）
-- 英文搜索
SELECT id FROM docs WHERE MATCH(fpath) AGAINST('matrixone');

-- 中文搜索（自然语言模式）
SELECT id FROM docs WHERE MATCH(fpath) AGAINST('慢慢地' IN NATURAL LANGUAGE MODE);

-- 布尔模式搜索（多文件组合）
SELECT id FROM docs
WHERE MATCH(fpath) AGAINST('+matrixone +慢慢地' IN BOOLEAN MODE);

-- 带相关性评分
SELECT id, MATCH(fpath) AGAINST('keyword') AS score
FROM docs
WHERE MATCH(fpath) AGAINST('keyword')
ORDER BY score DESC;
```

**文件路径格式**：
- `stage://stage_name/file.pdf`：使用已创建的 stage
- `file:///absolute/path/to/file.pdf`：使用绝对路径
- `file://host/path/to/file.pdf`：使用带主机的路径

**注意事项**：
1. 文件必须存在且可访问，否则插入或查询时会报错
2. PDF 提取需要系统安装 `pdftotext`（poppler 工具包），否则会使用 fallback 方案
3. 文件路径中的扩展名（`.pdf`、`.docx`）用于自动识别文件类型
4. 支持多个 DATALINK 列组合索引：`FULLTEXT(fpath1, fpath2)`

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

---

## 相关性评分算法

在理解搜索执行流程之前，我们需要先了解相关性评分算法，因为搜索过程中会用到这些概念（如 IDF、DF、TF-IDF、BM25）。

### TF-IDF 算法

```
score = TF × IDF²

其中：
- TF（词频）= 词在文档中出现的次数
- IDF（逆文档频率）= log10(总文档数 / 包含该词的文档数) // 压缩频率的动态范围，防止高频常见词过度主导相似度计算
```

**核心概念解释**：

- **TF（Term Frequency，词频）**：词在文档中出现的次数
  - 示例：文档A中"苹果"出现3次 → TF = 3
  - 含义：词在文档中出现越多，说明文档与查询越相关

- **DF（Document Frequency，文档频率）**：包含某个词的文档数量
  - 示例：1000个文档中，有100个包含"苹果" → DF = 100
  - 含义：常见词（如"的"、"是"）出现在很多文档中，DF 大；稀有词（如"人工智能"）出现在少数文档中，DF 小

- **IDF（Inverse Document Frequency，逆文档频率）**：衡量词的重要性
  - 公式：`IDF = log10(总文档数 / 包含该词的文档数) = log10(N / DF)`
  - 含义：DF 越小（词越稀有），IDF 越大，说明这个词越重要
  - 示例：总文档数1000，包含"苹果"的文档100个 → IDF = log10(1000/100) = 1.0

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
- docLen：文档的词数
- avgDocLen：平均文档长度
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

**SQL 详细解释**：

这个 SQL 查询实现了短语搜索的核心逻辑：不仅要求两个词都出现，还要求它们按顺序出现。

**1. CTE（公用表表达式）部分**：

```sql
WITH kw0 AS (SELECT doc_id, pos FROM index_table WHERE word = 'is'),
     kw1 AS (SELECT doc_id, pos FROM index_table WHERE word = 'red')
```

- `kw0`：查找所有包含 "is" 的记录，返回文档ID和位置
- `kw1`：查找所有包含 "red" 的记录，返回文档ID和位置

**示例数据**：
```
kw0 (word='is'):
doc_id | pos
-------|----
1      | 6    -- 文档1中"is"在位置6
2      | 10   -- 文档2中"is"在位置10

kw1 (word='red'):
doc_id | pos
-------|----
1      | 9    -- 文档1中"red"在位置9
2      | 5    -- 文档2中"red"在位置5
3      | 15   -- 文档3中"red"在位置15
```

**2. JOIN 部分**：

```sql
FROM kw0, kw1
WHERE kw0.doc_id = kw1.doc_id
```

- 对 `kw0` 和 `kw1` 做笛卡尔积
- 只保留同一文档的记录（`doc_id` 相同）
- 结果：同一文档中同时包含 "is" 和 "red" 的所有组合

**结果示例**：
```
doc_id | kw0.pos | kw1.pos
-------|---------|--------
1      | 6       | 9       -- 文档1：is在6，red在9
2      | 10      | 5        -- 文档2：is在10，red在5
```

**3. 位置顺序检查**：

```sql
AND kw1.pos - kw0.pos = 3
```

这是关键部分：检查 "red" 的位置是否紧跟在 "is" 之后。

**为什么位置差是 3？**
- "is" 长度为 2 字节
- 后面有一个空格（1 字节）
- 所以 "red" 应该在 "is" 之后 3 字节的位置

**位置验证示例**：

```
文档1: "color is red"
       0123456789...
       "is"在位置6（'c','o','l','o','r',' ','i','s'）
       "red"在位置9（'c','o','l','o','r',' ','i','s',' ','r','e','d'）
       
位置差 = 9 - 6 = 3 ✓ 匹配！

文档2: "red is color"
       "is"在位置10
       "red"在位置5
       
位置差 = 5 - 10 = -5 ✗ 不匹配（顺序错误）
```

**完整执行示例**：

假设索引表数据：
```
doc_id | word | pos
-------|------|----
1      | is   | 6
1      | red  | 9
2      | is   | 10
2      | red  | 5
3      | red  | 15
```

**执行过程**：
1. **CTE 阶段**：
   - `kw0`: [(1, 6), (2, 10)]
   - `kw1`: [(1, 9), (2, 5), (3, 15)]

2. **JOIN 阶段**（同一文档）：
   - (1, 6) × (1, 9) → doc_id=1, pos差=9-6=3 ✓
   - (2, 10) × (2, 5) → doc_id=2, pos差=5-10=-5 ✗

3. **位置检查**：
   - 只有 doc_id=1 满足 pos差=3

4. **最终结果**：
   ```
   doc_id
   1
   ```

**位置检查的作用**：
- "color is red" ✓ 匹配（"is" 在位置 6，"red" 在位置 9，差值为 3）
- "red is color" ✗ 不匹配（位置顺序不对）
- "is very red" ✗ 不匹配（位置差不是 3，中间有其他词）

**关键点总结**：
1. **短语匹配**：不仅要求两个词都出现，还要求顺序正确
2. **位置检查**：通过 `pos` 字段检查词的位置关系
3. **精确匹配**：位置差必须精确等于预期值（这里是 3）
4. **性能优化**：使用索引表直接查找，避免全表扫描

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
