# OLAP Scan Schema Alignment Design

本文档描述 Doris OLAP Scan 从 FE Physical ScanTuple 到 BE SegmentIterator 的 Schema
对齐重构。目标是让 Block 的列布局只由一个稠密的读取 Schema 描述，消除当前
`origin_return_columns`、`return_columns`、稀疏 `Schema` 和 Tablet column ordinal
混用造成的隐式映射。

本文档定义目标架构、接口边界、兼容策略和迁移步骤，不包含实现 patch。

## 背景

当前 OLAP Scan 同时存在多套列布局：

- FE ScanTuple：FE 下发的物理扫描 slots。
- `OlapScanner::_return_columns`：ScanTuple slot 映射到完整 `TabletSchema` 后得到的
  Tablet column ordinals。
- `TabletReader::ReaderParams::return_columns`：BE 为聚合、UNIQUE、sequence、binlog
  等路径追加列后的读取列集合。
- `origin_return_columns`：BlockReader 调用方原始输出 Block 的列集合。
- SegmentIterator `Schema`：内部保存完整 TabletSchema 大小的稀疏 column 数组，同时
  通过 `column_ids` 表示实际读取列。
- SegmentIterator Block：部分路径只包含 `return_columns`，delete predicate、TSO 等
  辅助列仅存在于稀疏 Schema 和 `_current_return_columns` 中。

这些布局导致同一个 `ColumnId` 在不同位置可能表示：

- 完整 TabletSchema ordinal；
- Schema 中的 selected column ordinal；
- Block position；
- FE SlotRef 的 column position；
- Segment file 中的物理 column ordinal；
- column unique ID。

例如：

- `BlockReader::init()` 通过 `origin_return_columns` 和 `return_columns` 构建
  `_return_columns_loc`。
- SegmentIterator 中表达式 `VSlotRef::column_id()` 是 Block position，但
  `ColumnPredicate::column_id()` 通常是 Tablet column ordinal。
- `Schema::_cols` 按完整 TabletSchema 大小分配，`_current_return_columns` 也因此按完整
  TabletSchema 大小分配。
- SegmentIterator 通过 `i >= block->columns()` 判断某列是否为 delete predicate
  辅助列，依赖输入 Schema 的前缀恰好等于输出 Block。

这些隐式约束在普通查询、AGG_KEYS、UNIQUE_KEYS、row binlog、schema evolution 和
dropped delete predicate 组合时很难维护。

## 目标

本次重构有以下目标：

1. FE Physical ScanTuple 完整描述 FE 已知的存储读取依赖。
2. BE 使用稠密 `Schema` 精确描述 Block 的列数、顺序、名称和类型。
3. TabletReader、VCollectIterator 和 BlockReader 使用同一个 ScanSchema。
4. BlockReader 所有 next-block 路径的 source Block 和 target Block 具有完全相同的
   Schema。
5. SegmentIterator 的输入输出 Block 与其 `schema()` 完全一致。
6. `ColumnPredicate::column_id()` 和 `VSlotRef::column_id()` 在 SegmentIterator 内统一
   表示当前 dense Schema 的局部 Block position。
7. dropped delete predicate 等 FE 无法获知的列通过 BE auxiliary Schema 显式追加，并
   通过显式 projection 移除。
8. 支持 FE/BE 滚动升级，不要求同时升级。
9. 不新增正向 `must_read` 标记。Schema 中的列默认读取，只有明确的负向优化条件允许
   跳过真实数据读取。

## 非目标

本次重构不包含：

- 修改 Segment 文件格式。
- 修改 TabletSchema 持久化格式。
- 修改事务、版本可见性或 delete bitmap 语义。
- 改变 SQL 可见列、`SELECT *` 或 LogicalOlapScan 输出语义。
- 将历史 dropped delete predicate 列暴露给 FE。
- 在第一阶段移除所有完整 TabletSchema 使用。TabletSchema 仍是存储语义和 schema
  evolution 的权威来源。

## 术语

### StorageSchema

完整的 `TabletSchema`，包含：

- 所有当前有效存储列；
- 合并后的 dropped delete predicate 列；
- KeysType、key 数量、sequence、seq-map；
- delete sign、version、rowid、binlog meta 等存储属性；
- column unique ID、parent unique ID 和 variant path；
- 聚合方法和聚合函数元数据。

StorageSchema 描述存储语义，不描述任意一个 Block 的布局。

### PlanScanSchema

PlanScanSchema 是 BE 从 FE Physical ScanTuple 直接解码得到的稠密 Schema：

```text
PlanScanSchema.fields[i] <=> Physical ScanTuple.slots[i]
```

layout version 1 中，PlanScanSchema 已包含全部 FE 可知的 storage dependencies。
legacy layout 中，PlanScanSchema 可能缺少 key、sequence、binlog meta 或 BEFORE，需要
Scanner adapter 生成更宽的 ScanSchema。

### ScanSchema

ScanSchema 是 TabletReader、VCollectIterator 和 BlockReader 共同使用的对齐后稠密
Schema。

```text
layout version 1:
    ScanSchema == PlanScanSchema

legacy layout:
    ScanSchema == LegacyReadSchemaAdapter(PlanScanSchema)
```

ScanSchema 包含：

- 查询输出和谓词需要的普通列；
- 聚合或 UNIQUE merge 所需的完整 key 前缀；
- 必要的 sequence 或 seq-map 列；
- row binlog 所需的 key、OP、LSN、TSO 和 BEFORE 列；
- FE 已知的 virtual、rowid、score 等 scan slots；
- 仅用于 scan-schema alignment 的 extra key slots。

ScanSchema 的顺序是物理扫描顺序，不是 SQL SELECT 列表顺序。

### SegmentSchema

SegmentSchema 是单个 Rowset/Segment 实际求值所需的 Schema：

```text
SegmentSchema = ScanSchema + BE-only auxiliary fields
```

BE-only auxiliary fields 包括：

- 历史 delete predicate 引用、但当前 FE schema 已不存在的 dropped columns；
- FE 无法表达的其他 storage-only predicate fields。

辅助列必须追加在 ScanSchema 后面，不允许插入 ScanSchema 中间。由此保证所有 FE
slots 在 ScanSchema 和 SegmentSchema 中具有相同 position。

### OutputTuple

OutputTuple 是 Scanner projection 之后交给上层执行算子的 SQL 输出。它可以只包含
`SELECT v` 中的 `v`，不包含 ScanSchema 为存储语义补齐的 key、sequence、BEFORE 等
内部 slots。

## 核心不变量

实现必须保证以下不变量：

1. `PlanScanSchema.fields.size() == Physical ScanTuple materialized slots`。layout version 1
   中 `ScanSchema == PlanScanSchema`；legacy layout 中 ScanSchema 可以由 adapter 补宽。
2. ScanSchema 的列顺序与 Scanner 内部交给 TabletReader 的 Block 顺序完全相同。
3. SegmentSchema 以 ScanSchema 为严格前缀。
4. `SegmentIterator::schema()` 与传入 `next_batch()` 的 Block 完全相同。
5. ProjectionIterator 之前使用 SegmentSchema；之后使用 ScanSchema。
6. VUnionIterator、VMergeIterator、VStatisticsIterator、VCollectIterator 和
   BlockReader 对外都使用 ScanSchema。
7. BlockReader source Block 和 target Block 的列数、顺序、名称、`DataTypePtr`、
   nullable 及 nested-prune 结果完全相同。
8. 在同一个 Schema 内：

   ```text
   ColumnPredicate.column_id == VSlotRef.column_id == Block position
   ```

9. local position 不跨 Schema 持久保存，也不用于定位历史物理列。
10. 物理列通过 column unique ID 定位；Variant extracted column 使用
    `parent unique ID + path` 定位。
11. 需要 merge 的 ScanSchema 必须包含完整 key 前缀，并显式保存
    `merge_key_count`。
12. ScanSchema 中的字段默认需要真实物化。只有显式证明安全的 extra-key 或 index-only
    路径可以填充 placeholder。

## 目标数据流

```text
LogicalOlapScan output
    -> FE physical translation
    -> Physical ScanTuple
    -> PlanScanSchema
         | layout v1
         | legacy: LegacyReadSchemaAdapter
         v
       ScanSchema

Storage read pipeline:

SegmentIterator(SegmentSchema)
    -> ProjectionIterator(ScanSchema)
    -> VUnion / VMerge(ScanSchema)
    -> VCollectIterator(ScanSchema)
    -> TabletReader / BlockReader(ScanSchema)
    -> Scanner projection
    -> SQL OutputTuple

StorageSchema
    -> provides physical identity and table semantics to builders, SegmentIterator and MergePlan
```

Scanner projection 和 Segment auxiliary projection 是两个不同边界：

- Segment projection：`SegmentSchema -> ScanSchema`，删除 BE-only auxiliary fields。
- Scanner projection：`ScanSchema -> OutputTuple`，删除 FE physical scan 的内部 slots。

## FE Physical ScanTuple

### Logical output 与 Physical ScanTuple 分离

`LogicalOlapScan.getOutput()` 继续表示 SQL 可见输出。`__BEFORE__*`、storage sequence 和
额外 key 不加入 LogicalOlapScan output，避免改变：

- 名称绑定；
- `SELECT *`；
- 上层 plan 的 slot 集合；
- 用户可见 schema。

PhysicalPlanTranslator 在生成 `OlapScanNode` 和 scan tuple descriptor 时追加内部 slots，
并由 scan 上层 projection 恢复 Logical output。即使原始物理计划没有用户
`PhysicalProject`，translator 也必须创建 scan-local projection，不能让内部 slots
直接成为 scan node 的算子输出。`SELECT *` 同样必须经过该边界。

[PR #64413](https://github.com/apache/doris/pull/64413) 的 extra storage key 处理是这一
设计的前置范式：

- FE 保留完整 storage key prefix；
- 额外 key 存在于 physical scan tuple；
- 上层 projection 删除额外 key；
- `extra_key_column_slot_ids` 标记仅用于 alignment 的 key。

row binlog、sequence 和其他 storage dependencies 复用相同 physical-slot 机制，但不能
复用 `extra_key` 的跳读语义。

### Physical slots 依赖集合

不同路径至少需要以下 physical slots：

| 路径 | Physical ScanTuple |
| --- | --- |
| DUP_KEYS / UNIQUE MOW direct | 查询输出、谓词、virtual expression 等实际依赖 |
| AGG_KEYS | 实际依赖、完整 key 前缀、需要聚合的 value |
| UNIQUE MOR | 实际依赖、完整 key 前缀、必要 sequence |
| UNIQUE seq-map | 实际依赖、完整 key 前缀、每个 value 对应的 sequence |
| row binlog APPEND_ONLY | 实际依赖、过滤所需的 OP/TSO；输出或排序需要时包含 LSN |
| row binlog MIN_DELTA | 实际依赖、完整 key、OP、LSN、TSO、所需 value 的 BEFORE |
| row binlog DETAIL | 实际依赖、完整 key、OP、LSN、TSO；启用历史值时包含所需 value 的 BEFORE |

“实际依赖”包括：

- SQL output expressions；
- pushed predicates；
- runtime filters；
- order-by/top-N；
- virtual column inputs；
- Scanner projection inputs。

FE 只追加当前请求需要的 value 和对应 BEFORE，不应无条件读取宽表的所有 value。

### Slot 顺序

Physical ScanTuple 按选中 index 的 storage schema 顺序构造。

对于需要 merge 的路径：

```text
[all key fields in storage order] [value/meta/internal fields in storage order]
```

不能按 SQL SELECT 顺序构造。例如：

```sql
SELECT v FROM agg_table;
```

如果 storage key 为 `(k1, k2)`，physical ScanTuple 至少是：

```text
[k1, k2, v]
```

Scanner projection 之后才输出：

```text
[v]
```

### `extra_key_column_slot_ids`

`extra_key_column_slot_ids` 只表示：

> 为 scan schema alignment 保留、并且在 direct 路径允许使用 placeholder 的 storage
> key slot。

它不是通用的“内部列”标记。

AGG/UNIQUE 中仅因为 schema alignment 而由 FE 补齐、但 FE 语义没有引用的 key 可以
标为 extra key。这与它在非 direct merge 中必须参与比较并不冲突：

- 非 direct path 忽略 extra-key 跳读 hint，读取真实 key；
- direct path 只有在 predicate、delete condition、expression 都不依赖它时才允许
  placeholder。

以下列不能标为 extra key：

- MIN_DELTA/DETAIL 分组所需 key，即使用户没有选择该 key；
- sequence 或 seq-map；
- binlog OP、LSN、TSO；
- BEFORE columns；
- delete predicate columns。

即使 direct reader 收到 extra key，也必须在确认该列不参与 predicate、delete
condition、virtual/common expression 后才能跳过真实读取。

### 不新增 `must_read`

新协议不增加正向 `must_read` 字段：

- slot 在 ScanSchema 中，默认读取；
- `extra_key` 是显式负向 hint；
- index-only 跳读必须由索引完整执行和 storage semantic dependency 共同证明。

新 BE 在判断是否可以跳过真实数据时采用以下优先级：

```text
if MergePlan requires real storage value at position:
    read real data
else if direct path
        and field is EXTRA_KEY_PLACEHOLDER
        and no predicate/delete/expression depends on it:
    allow placeholder
else if index result fully materializes the requested semantics
        and field is not a storage semantic dependency:
    allow index-only placeholder
else:
    read real data
```

`MergePlan requires real storage value` 至少覆盖：

- non-direct merge 使用的 key；
- sequence 和 seq-map；
- MIN_DELTA/DETAIL 分组 key；
- binlog OP、LSN、TSO；
- effective mode 要求的 BEFORE；
- delete predicate 和尚未完成求值的 query predicate/expression。

滚动升级期间，新 FE 需要把 storage semantic dependencies 的 unique IDs 合并到现有
`output_column_unique_ids`：

```text
output_column_unique_ids =
    SQL/project output unique IDs
    UNION storage semantic dependency unique IDs
```

这用于防止旧 BE 在 DUP/UNIQUE-MOW index-only 路径把 binlog key 等内部必读列填成
默认值。它不是新增长期 `must_read` 协议；只有兼容窗口结束并停止支持旧 BE 后，FE
才能停止合并这些 storage semantic dependency UIDs。

## Dense Schema

### 当前问题

当前 `Schema` 同时保存：

- 完整 TabletSchema 大小的 `_cols`；
- selected Tablet column ordinals `_col_ids`；
- Tablet ordinal 到 selected position 的 `_column_id_to_index`。

因此：

- `Schema::column(cid)` 的参数是 Tablet ordinal；
- `Schema::column_id(position)` 返回 Tablet ordinal；
- `Schema::column_index(cid)` 返回 Block position；
- `_current_return_columns` 必须按完整 TabletSchema 大小分配；
- 多数 SegmentIterator 代码需要在 Tablet ordinal 和 Block position 之间转换。

目标 Schema 不再保留稀疏形态。

### 迁移期类型隔离

上述是最终态，不能直接改变现有全局 `Schema(columns, column_ids)` 的语义。compaction、
schema change、RowCursor 等调用方仍依赖其中的 Tablet CID，若原构造函数静默变成 dense
语义，中间提交会发生类型正确但含义错误的 ID 混用。

迁移期先引入独立的 `DenseReadSchema` 强类型；本文后续代码示例中的 `Schema` 表示完成
迁移后的最终名称。两者必须满足：

- legacy `Schema` 和 `DenseReadSchema` 不提供隐式转换；
- 不复用同时可接收 `uint32_t`、但分别表示 Tablet CID/ReadPosition 的重载；
- 查询只在 OlapScanner 的 LegacyReadSchemaAdapter 边界完成一次转换；
- 非查询路径只在各 reader 创建边界通过 `ReadSchemaBuilder` 构造
  `DenseReadSchema`；
- TabletReader 以下的 dense 调用链不能再退回 legacy sparse Schema；
- 所有依赖 sparse Schema 的旁路迁移完成后，才删除 legacy 类型并将
  `DenseReadSchema` 收敛为最终 `Schema`。

### 建议数据结构

```cpp
using ReadPosition = uint32_t;
using TabletColumnId = uint32_t;

struct StorageColumnIdentity {
    int32_t unique_id;
};

struct VariantColumnIdentity {
    int32_t parent_unique_id;
    std::string normalized_path;
};

struct SyntheticColumnIdentity {
    int32_t slot_id;
    SyntheticColumnKind kind;
};

using ColumnIdentity =
        std::variant<StorageColumnIdentity, VariantColumnIdentity, SyntheticColumnIdentity>;

enum class ReadFieldRole : uint32_t {
    NONE = 0,
    MERGE_KEY = 1 << 0,
    MERGE_VALUE = 1 << 1,
    SEQUENCE = 1 << 2,
    BINLOG_OP = 1 << 3,
    BINLOG_LSN = 1 << 4,
    BINLOG_TSO = 1 << 5,
    BINLOG_BEFORE = 1 << 6,
    EXTRA_KEY_PLACEHOLDER = 1 << 7,
    DELETE_PREDICATE_AUX = 1 << 8,
    VIRTUAL = 1 << 9,
};

struct ReadField {
    // nullptr is allowed only for explicitly tagged synthetic fields.
    TabletColumnPtr storage_column;
    DataTypePtr block_type;
    std::string block_name;
    ColumnIdentity identity;

    // Transitional boundary mapping. It is not a Block position.
    std::optional<TabletColumnId> tablet_cid;

    uint32_t roles = 0;
};

class Schema {
public:
    size_t size() const;
    const ReadField& field(ReadPosition position) const;

    std::optional<ReadPosition> position_of(const ColumnIdentity& identity) const;
    std::optional<ReadPosition> position_of_tablet_cid(TabletColumnId cid) const;

    size_t merge_key_count() const;
    Block create_block(size_t reserve_rows = 0) const;
    Status validate_block(const Block& block) const;

private:
    std::vector<ReadField> _fields;
    std::unordered_map<ColumnIdentity, ReadPosition> _identity_to_position;
    std::unordered_map<TabletColumnId, ReadPosition> _tablet_cid_to_position;
    size_t _merge_key_count = 0;
};
```

`ColumnIdentity` 必须按值比较和 hash。Variant path 使用规范化后的 path 内容，不能比较
`PathInDataPtr` 地址。Variant identity 使用相对 parent 的规范化 path，包含区分
typed/nested extraction 所需的稳定属性，但不包含可能随 rename 改变的 root column
name。多个 uid 为 `-1` 的 virtual、score、rowid/global-rowid fields 使用
`SyntheticColumnIdentity` 和明确的 kind 区分，不能退化为同一个空 identity。

`tablet_cid` 只允许在 legacy adapter 和 Schema builder 的构造边界使用；它是
transitional lookup，不得进入 predicate、index 或 batch hot path。

`ReadField` 同时保存 `TabletColumn` 和 `block_type`，因为 Block 类型不一定能只从
TabletColumn 重建：

- FE SlotDescriptor 可以要求 nullable conversion；
- nested/Variant projection 会裁剪逻辑类型；
- virtual column 的结果类型来自 expression；
- schema evolution 下 file storage type 可能不同于 expected block type。

### 特殊列位置

delete sign、sequence、rowid、version、LSN、TSO、commit TSO 等位置全部保存为
ScanSchema 或 SegmentSchema 的局部 position：

```cpp
std::optional<ReadPosition> delete_sign_position() const;
std::optional<ReadPosition> sequence_position() const;
std::optional<ReadPosition> rowid_position() const;
std::optional<ReadPosition> version_position() const;
std::optional<ReadPosition> lsn_position() const;
std::optional<ReadPosition> tso_position() const;
```

位置 `0` 是合法值，所有判断使用 `std::optional` 或 `>= 0`，不能使用 `> 0`。

### Schema 构建

Schema 由统一 `ReadSchemaBuilder` 构建：

```cpp
class ReadSchemaBuilder {
public:
    Status add_scan_slot(const SlotDescriptor& slot);
    Status add_storage_field(TabletColumnId cid, ReadFieldRole role);
    Status append_auxiliary_field(const TabletColumn& column, ReadFieldRole role);
    Result<SchemaSPtr> build();
};
```

Builder 负责：

- SlotDescriptor 到 TabletColumn 的 UID/path 绑定；
- block type 和 nullable；
- selected index schema order；
- key prefix 校验；
- duplicate identity 检查；
- sequence/seq-map dependency；
- binlog BEFORE companion 绑定；
- special column positions；
- ScanSchema-prefix-SegmentSchema 不变量。

Schema 构建完成后使用 `std::shared_ptr<const Schema>`，读取期间不可变。

## TabletReader 收敛

目标 `ReaderParams` 中与 Block layout 相关的字段收敛为：

```cpp
struct ReaderParams {
    BaseTabletSPtr tablet;
    TabletSchemaSPtr storage_schema;
    SchemaSPtr read_schema;

    // Predicates and expressions are already bound to read_schema positions.
    std::vector<std::shared_ptr<ColumnPredicate>> predicates;
    VExprContextSPtrs common_expr_ctxs_push_down;
    std::map<ReadPosition, VExprContextSPtr> virtual_column_exprs;

    // Other version/range/runtime options remain unchanged.
};
```

最终删除：

- `ReaderParams::return_columns`；
- `ReaderParams::origin_return_columns`；
- `ReaderParams::tablet_columns_convert_to_null_set`；
- `TabletReader::_return_columns`；
- `TabletReader::_key_cids`；
- `TabletReader::_value_cids`；
- `RowsetReaderContext::return_columns`；
- `RowsetReaderContext::tso_predicate_column_id`，新 FE 协议下 TSO 是普通 ScanSchema
  dependency。

其他当前携带 layout 含义的字段必须显式迁移：

- `TabletReader::_sequence_col_idx` 和 `RowsetReaderContext::sequence_id_idx` 改为
  `std::optional<ReadPosition>`；
- `read_orderby_key_columns` 改为 ScanSchema local positions；
- `ReaderParams/StorageReadOptions::extra_columns` 收入 `ReadFieldRole`，不再单独传
  Tablet ordinals；
- `key_group_cluster_key_idxes` 改为对应 vertical group Schema 的 local positions；
- delete sign、LSN、TSO、rowid 等均从 Schema special positions 获取。

StorageSchema 仍保留在 TabletReader，用于：

- KeysType；
- key range 和 RowCursor；
- delete handler；
- schema evolution；
- sequence/seq-map 元数据；
- rowset/segment column identity；
- storage index 元数据。

`TabletReader::_init_conditions_param()` 也必须迁移到 local position：

- 通过 `read_schema->field(predicate->column_id()).storage_column` 读取列元数据；
- key/value predicate 分类使用 ReadField，而不是
  `_tablet_schema->column(predicate->column_id())`；
- FunctionFilter 先通过 name/UID/path 绑定 ReadPosition；
- ngram/inverted-index 判断从 ReadField 的 identity 查存储 index；
- MOR value predicate 判断使用 ReadField role/metadata；
- delete access-path 清理继续使用稳定 UID/path，不把 predicate local position 当成
  access-path key。

TabletReader 不再通过一组 Tablet ordinals 创建 Block，而是统一调用：

```cpp
Block block = read_schema->create_block();
```

非 FE 路径，例如 compaction、checksum、schema change，由本地 builder 从对应
TabletSchema 或 column group 构造同样的 dense Schema。

## BlockReader 收敛

### 单一 Block Schema

目标 BlockReader 只使用一个 `read_schema`：

```text
VCollect source Block schema == caller target Block schema == read_schema
```

source 和 target 仍是不同 Block 实例：

- AGG 会把多行聚合成一行；
- UNIQUE 会选择一个版本；
- seq-map 会按不同 sequence 拼接一行；
- MIN_DELTA 会折叠变更；
- DETAIL 会把 UPDATE 扩展成 BEFORE/AFTER 两行。

统一的是列布局，不是 Block 对象和行数。

### 删除的映射

最终删除：

- `_return_columns_loc`；
- `_seq_map_in_origin_block`；
- `_seq_map_not_in_origin_block`；
- source-to-target column map；
- 通过 `origin_return_columns` 回查 TabletColumn 的逻辑。

例如：

```cpp
target_columns[_return_columns_loc[idx]]
```

统一变为：

```cpp
target_columns[idx]
```

所有 sequence 都已经存在于 Physical ScanTuple 和 read_schema 中，因此不再区分
“sequence 是否在 origin block”。

seq-map replace 不仅要按各自 sequence 选择对应 value；输出行中的 sequence slot 本身
也必须更新为该规则实际选中的 sequence 值，不能残留 group 中最后一行或任意输入行的
值。

### MergePlan

BlockReader 在 init 时基于 immutable read_schema 和 StorageSchema 构建一次
`MergePlan`：

```cpp
enum class MergeMode {
    DIRECT,
    AGG_KEYS,
    UNIQUE_KEYS,
    SEQ_MAP_REPLACE,
    BINLOG_MIN_DELTA,
    BINLOG_DETAIL,
};

struct AggregateColumnPlan {
    ReadPosition position;
    AggregateFunctionPtr function;
};

struct SequenceReplacePlan {
    ReadPosition sequence_position;
    std::vector<ReadPosition> value_positions;
};

enum class ColumnCopyMode {
    DIRECT,
    UNWRAP_NULLABLE_SOURCE,
    WRAP_NON_NULL_SOURCE,
};

struct BeforeSourcePlan {
    ReadPosition source_position;
    ColumnCopyMode copy_mode;
};

struct MergePlan {
    MergeMode mode;
    size_t key_count = 0;
    std::vector<ReadPosition> normal_positions;
    std::vector<AggregateColumnPlan> aggregate_columns;
    std::vector<SequenceReplacePlan> sequence_rules;
    // Same size as ScanSchema. A value position maps to its BEFORE source position.
    std::vector<std::optional<BeforeSourcePlan>> before_source_by_output_position;
    std::optional<ReadPosition> delete_sign_position;
    std::optional<ReadPosition> binlog_op_position;
    std::optional<ReadPosition> binlog_lsn_position;
    std::optional<ReadPosition> binlog_tso_position;
};
```

聚合函数直接从 `read_schema->field(position).storage_column` 获取，不再通过
source position、output position 和 Tablet ordinal 三次转换。

binlog BEFORE companion 在 Schema/MergePlan 构建阶段显式绑定。对每个请求的 base/value
ReadField，builder 在 row-binlog StorageSchema 中解析对应 BEFORE TabletColumn，再将
`value identity -> BEFORE identity` 转成一对 ScanSchema positions。若当前存储元数据
只有 `__BEFORE__<name>__` 约定，canonical name 解析只发生在 builder 边界，并立即固化
为 identity/position；BlockReader 不在首个 Block 到达后按名称延迟搜索。

`before_source_by_output_position` 的长度始终等于 ScanSchema size。普通 value 有 companion
时保存 BEFORE position 和预计算的 nullability copy mode；binlog meta、BEFORE slot
自身和无 companion 的字段为 `std::nullopt`，读取源默认是自身。MIN_DELTA 或启用
historical value 的 DETAIL 对所需 value 缺 companion 时在 init 失败；未启用
historical value 的 DETAIL 保留读取自身的既有 fallback。

BEFORE storage column 固定为 nullable，而 public AFTER/value slot 可能是 non-nullable，
不能直接跨类型调用 `insert_from()`。builder 校验去掉 Nullable 后的 logical type 和
nested-prune 结果一致，并生成：

- `DIRECT`：source/target 的 nullable shape 相同；
- `UNWRAP_NULLABLE_SOURCE`：从 BEFORE `ColumnNullable` 的 nested column 复制到
  non-nullable target；实际使用行若为 NULL，返回带 tablet/rowset/column identity 的
  invariant error，不能填默认值继续；
- `WRAP_NON_NULL_SOURCE`：复制到 nullable target 的 nested column，并追加
  `null_map=0`。

BlockReader 用统一的 planned-column-copy helper 执行这三种模式，DETAIL pending row 和
MIN_DELTA 共用该 helper。Segment/VCollect 交给 BlockReader 的列必须已 materialize，
不能把 `ColumnConst(ColumnNullable(...))` 留给逐行路径。

### 六条 next-block 路径

| 路径 | 行语义 | Schema 契约 |
| --- | --- | --- |
| direct | VCollect 直接填 caller Block | caller Block 为 ScanSchema |
| AGG_KEYS | 同 key values 执行 SUM/MIN/MAX/REPLACE 等 | source/target 均为 ScanSchema |
| UNIQUE_KEYS | 同 key 保留最高版本/sequence 行 | source/target 均为 ScanSchema |
| seq-map replace | 每组 value 按各自 sequence 替换 | source/target 均为 ScanSchema |
| MIN_DELTA | 同 key 连续变更折叠为最小变更集 | source/target 均为 ScanSchema |
| DETAIL | UPDATE 展开为 UPDATE_BEFORE/UPDATE_AFTER | source/target 均为 ScanSchema |

`_direct_agg_key_next_block` 当前为空且没有有效选择路径，应在迁移时删除。

BlockReader 入口增加 invariant assertion：

```cpp
RETURN_IF_ERROR(_read_schema->validate_block(*block));
```

若该关系由调用链静态保证，debug 构建可使用 `DCHECK`；不能在布局不匹配时静默继续。

## SegmentIterator Column ID 统一

### Local position 是唯一运行期 ID

SegmentIterator 内所有参与 Block 运算的 column ID 统一为：

```text
ReadPosition == SegmentSchema position == Block position
```

包括：

- `ColumnPredicate::column_id()`；
- `VSlotRef::column_id()`；
- `_column_iterators` 下标；
- `_index_iterators` 下标；
- `_current_return_columns` 下标；
- `_predicate_column_ids`；
- `_non_predicate_columns`；
- `_common_expr_column_ids`；
- `_delete_range_column_ids`；
- `_delete_bloom_filter_column_ids`；
- `_is_pred_column`；
- `_is_common_expr_column`；
- `_converted_column_ids`；
- `col_id_to_predicates` key；
- virtual column expression map key；
- predicate/index execution status map key。

以下转换应消失：

```cpp
auto cid = _schema->column_id(slot_ref->column_id());
auto position = _schema->column_index(cid);
```

统一后直接使用：

```cpp
ReadPosition position = slot_ref->column_id();
const auto& field = _schema->field(position);
```

建议引入强类型或至少明确别名，避免重新传入 Tablet ordinal：

```cpp
using ReadPosition = uint32_t;
using TabletColumnId = uint32_t;
using ColumnUniqueId = int32_t;
```

长期可使用 wrapper strong type，在编译期禁止不同 ID 域混用。

### 不属于 local position 的边界标识

FE/BE 协议和存储元数据中仍有一些按 identity 表达的集合：

- `output_column_unique_ids`；
- `all_access_paths`；
- `predicate_access_paths`；
- Segment footer column ordinal；
- TabletSchema column ordinal。

它们不能直接用作 SegmentIterator 数组下标。SegmentIterator init 阶段应把协议或
StorageReadOptions 中的 UID/path 集合绑定为 local positions，例如：

```text
output_column_unique_ids
    -> SegmentSchema identity lookup
    -> _output_positions
```

完成绑定后，SegmentIterator hot path 只使用 `_output_positions`。access path 和 footer
ordinal 在进入具体 ColumnReader 时仍使用物理 identity，不属于 Block column ID。

### Segment 创建前的 predicate 绑定

local-position 契约从 `Segment::new_iterator()` 入口开始，而不是等到
`SegmentIterator::init()` 才成立。Segment-level pre-pruning 也必须迁移：

- `StorageReadOptions::col_id_to_predicates` 的 key 是 SegmentSchema ReadPosition；
- segment zonemap 遍历 predicate 时，先读取
  `schema->field(position).identity`，再查 ColumnReader；
- commit-TSO placeholder 与其他 special columns 通过 Schema 保存的 local position
  判断；
- expression zonemap 中的 VSlotRef 已重绑到同一个 SegmentSchema position；
- ColumnReader 完成 post-zonemap prune 后返回 UID/path identity，再通过
  `SegmentSchema::position_of()` 找回 local position；
- Variant extracted column 的 post-prune 使用 parent UID + path，不能用 parent
  Tablet CID 代替；
- 被 segment-level zonemap 直接裁掉时，`EmptySegmentIterator` 仍返回传入的精确
  SegmentSchema。

因此，Segment 创建前、SegmentIterator init 和逐 batch 执行使用的是同一个 ID 域，
不存在“pre-prune 用 Tablet CID，row-level predicate 用 ReadPosition”的过渡状态。

### Query predicate 绑定

query predicate 在 ScanSchema 创建后绑定：

```text
SlotId / UID / path
    -> ScanSchema ReadPosition
    -> predicate.clone(ReadPosition)
```

不再把 predicate clone 成完整 TabletSchema ordinal。

由于 SegmentSchema 只在 ScanSchema 尾部追加辅助列，query predicate 和 VExpr 的 position
进入 SegmentIterator 后保持不变。

### Delete predicate 绑定

DeleteHandler 不能直接把 complete TabletSchema ordinal 作为最终 predicate ID。

Delete predicate template 应保存稳定列身份：

```text
unique ID
or
parent unique ID + Variant path
```

历史 persisted delete predicate PB 可能没有 column UID。加载 template 时必须先使用该
delete predicate 所属的历史 TabletSchema 按名称解析 UID，再生成稳定 identity；不能
直接在当前 TabletSchema 按名称解析，否则 drop 后同名重建会绑定到错误列。只有无法
取得历史 UID 的 legacy 输入，才在兼容边界按其对应历史 schema 执行受校验的名称解析。

BetaRowsetReader 根据当前 rowset version 选择适用 delete conditions 后：

1. 收集其列身份；
2. 将 SegmentSchema 中尚不存在的列追加为 auxiliary field；这里既包括当前仍存在但
   未被 FE 扫描的列，也包括已经 dropped 的列；
3. 通过 SegmentSchema 查找 local position；
4. clone/rebind ColumnPredicate 到 local position；
5. 按原始 AND/OR 结构构建 `delete_condition_predicates`；
6. `del_predicates_for_zone_map` 同样使用 local position，但只保留原先允许
   segment/column zonemap 求值的单列条件，不能为了重绑而展开或改变条件树语义。

不能修改 DeleteHandler 内共享 predicate，因为不同 rowset 的 SegmentSchema 可能不同。

### Local position 到物理列

SegmentIterator 不再通过：

```cpp
_opts.tablet_schema->column(local_position)
```

查找物理列，而是：

```cpp
const ReadField& field = _schema->field(local_position);
if (std::holds_alternative<StorageColumnIdentity>(field.identity)) {
    _segment->new_column_iterator(*field.storage_column, ...);
} else if (std::holds_alternative<VariantColumnIdentity>(field.identity)) {
    _segment->new_variant_column_iterator(field.identity, field.block_type, ...);
} else {
    new_synthetic_iterator(field.identity, field.block_type, ...);
}
```

Segment/ColumnReader 内部继续通过 UID/path 定位：

```text
ReadPosition
    -> ReadField
    -> UID or parent UID + path
    -> ColumnMetaAccessor
    -> Segment footer ordinal
```

Segment footer ordinal 只存在于 `ColumnMetaAccessor` 和 column reader 内部，不向
SegmentIterator 暴露。

synthetic field 没有 Segment 物理 UID，必须根据 `SyntheticColumnKind` 使用专用的
virtual、rowid/global-rowid、score 或 constant/default iterator，不能无条件解引用
`storage_column`。

### Variant index 绑定

索引绑定与 Block position 分离。`IndexExecContext` 保存
`ColumnIdentity -> StorageIndexBinding`：

- 普通列按 UID 查 index；
- Variant extracted column 按 parent UID + normalized path 查 parent/子路径 index；
- parent column 即使不在 ScanSchema/SegmentSchema 中，也可从 StorageSchema 建立
  index binding；
- 仅为了使用 parent inverted index，不要求把 parent data column 加入 Block。

`vsearch` 等路径不再通过 parent name 得到 Tablet ordinal，再把该 ordinal 当作 local
position。若历史协议没有 UID，只允许在 legacy 构造边界按当前 StorageSchema 名称解析
一次；运行期始终使用稳定 identity。

### 特殊情况

- dropped 后同名重建必须按 unique ID 区分，不能按列名绑定。
- Variant extracted column 的 `unique_id` 可能为 `-1`，必须使用 parent UID + path。
- rowid 等特殊列位于 position 0 时仍是合法列。
- 旧 Segment 缺少新列时，default-value iterator 仍通过 ReadField 的 storage identity
  和 expected block type 创建。
- key seek 使用独立 dense key Schema，但 key fields 仍通过 UID/path 绑定物理 iterator。
- ANN、score、segment zonemap 和 expression zonemap 的 slot IDs 同样使用 local
  position，不允许在内部再次转换成 Tablet ordinal。

## SegmentSchema 与显式 Projection

### 删除隐式小 Block 契约

当前 SegmentIterator 允许：

```text
schema = [return columns..., delete auxiliary columns...]
block  = [return columns...]
```

并通过 `i >= block->columns()`、`loc < block->columns()` 跳过 auxiliary fields。

目标中禁止该形态：

```text
SegmentIterator.schema = SegmentSchema
SegmentIterator Block  = SegmentSchema
```

SegmentIterator 正常读取、过滤、类型转换并输出所有 SegmentSchema columns。

### ProjectionIterator

在 SegmentIterator 之上增加显式 ProjectionIterator：

```cpp
class ProjectionIterator final : public RowwiseIterator {
public:
    ProjectionIterator(RowwiseIteratorUPtr child, SchemaSPtr output_schema,
                       std::vector<ReadPosition> projection);

    Status next_batch(Block* output) override;
    const Schema& schema() const override;

private:
    RowwiseIteratorUPtr _child;
    SchemaSPtr _output_schema;
    std::vector<ReadPosition> _projection;
    Block _input_block;
};
```

对于 Segment auxiliary projection：

```text
input  = SegmentSchema
output = ScanSchema
projection = [0, 1, ..., ScanSchema.size() - 1]
```

因为 auxiliary fields 只追加在尾部，projection 可以移动或引用前缀 columns，不需要逐
行复制。

实现上复用 `_input_block`：每批将前缀 columns move 到 output 后，按 SegmentSchema
重新放回同类型的空 columns，供下一次 `next_batch()` 填充。该路径只做 column ownership
转移或 view，不做逐行 `insert_from()`。

ProjectionIterator 必须覆盖当前调用路径需要的：

- `Block`；
- `BlockWithSameBit`；
- `BlockView`；
- row location 转发；
- profile 转发；
- `data_id()`；
- `merged_rows()`；
- `empty()`；
- 两种 `init()` overload；
- 调用路径使用时的 `next_row()` 和 `unique_key_next_row()`。

这些接口不能依赖 RowwiseIterator 默认实现。例如 `data_id()` 默认返回 `0` 会改变
VMerge 在相同 key/sequence 下的 insert-order tie-break。

VUnion/VMerge 看到的 child schema 已经是 ScanSchema，不再依赖“child schema 比内部
Block 大”的隐式行为。

包装位置是每个 `LazyInitSegmentIterator` child 的外层、加入 VUnion/VMerge 之前：

```text
SegmentIterator / EmptySegmentIterator / VStatisticsIterator
    -> LazyInitSegmentIterator
    -> ProjectionIterator
    -> VUnionIterator or VMergeIterator
```

若 projection 需要初始化 child，它必须透传 `init(opts)` 和
`init(opts, CompactionSampleInfo*)`。此外还必须原样转发 `is_merge_iterator()` 和
`update_profile()`；不能依赖基类默认值改变 iterator 类型判断或 profile 归属。

### 其他 Segment-level iterators

以下迭代器必须遵守相同精确 Schema 契约：

- EmptySegmentIterator；
- VStatisticsIterator；
- AutoIncrementIterator；
- LazyInitSegmentIterator；
- Segment cache 返回的 iterator；
- TopN/ordered read path。

若 fast path 无法执行 auxiliary predicate，必须在选择 fast path 前明确禁用，而不是
返回错误形状的 Block。

## Dropped Delete Predicate

历史 delete predicate 可能引用当前 schema 已删除的列。FE 无法知道该依赖，因此它是
允许在 BE 追加 auxiliary field 的主要场景。

处理流程：

```text
Tablet current schema
    +
delete-predicate historical schemas
    |
    v
StorageSchema with merged dropped columns
    |
    +-- FE ScanTuple -> ScanSchema
    |
    +-- applicable delete predicates for rowset version
            |
            v
       SegmentSchema = ScanSchema + referenced dropped columns
```

要求：

- `TabletSchema::merge_dropped_columns()` 继续按 UID 合并。
- drop 后重新创建同名列时，两列 UID 不同，不得合并成同一 ReadField。
- 只追加当前 rowset 实际适用 delete predicates 引用的列。
- delete predicate template 先按历史 schema 解析 identity；每个 rowset 在最终
  SegmentSchema 完成后独立 clone/rebind。
- delete auxiliary columns 在 SegmentIterator 完成过滤后立即 projection 掉。
- delete auxiliary columns 不进入 BlockReader，也不进入 Scanner projection。

## TSO 与 Row Binlog

### FE 已知 TSO

当 TSO 来自 table stream / `@incr` 的 start/end 参数时，FE 知道该过滤依赖。目标协议
应把 TSO 加入 Physical ScanTuple，而不是由 BE 通过
`tso_predicate_column_id` 隐式 widening。

旧 FE 兼容路径仍可由 LegacyReadSchemaAdapter 追加 TSO。

### MIN_DELTA 和 DETAIL

binlog effective mode 必须由 `scanParams` 和 `RowBinlogTableWrapper.getOriginTable()` 推导。
不能根据 RowBinlogTableWrapper 自身的 KeysType 推导，因为 wrapper 为读取 row-binlog
存储而固定表现为 DUP_KEYS。

MIN_DELTA 必须真实读取：

- 完整 key；
- OP；
- LSN；
- TSO；
- 请求 value 对应的 BEFORE companion；
- 参与 predicate 或 output expression 的其他字段。

MIN_DELTA 仅允许在启用 historical value 的受支持表模型上执行，因此请求 value 的
BEFORE companion 是强制依赖。

DETAIL 在启用 historical value 时读取相同的 BEFORE dependencies。未启用 historical
value 时，DETAIL 仍可输出记录中的 change rows；此时缺失 BEFORE companion 不是
Schema 错误，before-image 取值遵循当前 DETAIL fallback 语义。FE 和 BE 必须基于
effective mode 与 `need_historical_value` 使用相同依赖规则。

这些字段都属于 ScanSchema，不是 Segment auxiliary fields。

BlockReader 输出仍保持完整 ScanSchema：

- public value slot 在 UPDATE_BEFORE 行读取 BEFORE companion；
- public value slot 在 UPDATE_AFTER 行读取 AFTER value；
- OP slot 被改写为对外的 INSERT/DELETE/UPDATE_BEFORE/UPDATE_AFTER；
- BEFORE companion slot 自身仍需保持与输出行数一致，随后由 Scanner projection 删除。

### 禁止错误 fast path

MIN_DELTA/DETAIL 依赖真实行序和同 key 分组，必须禁用会绕过 BlockReader 或 predicate
求值的：

- COUNT/MINMAX statistics fast path；
- COUNT_ON_INDEX；
- 会改变 key/LSN 顺序的 TopN/order pushdown；
- 不能保证全局 key 顺序的 union optimization。

FE 和 BE 都应设置相应约束，BE 最终仍负责 correctness 校验。

## 兼容设计

### 协议版本

在 `TOlapScanNode` 增加 optional layout version：

```thrift
29: optional i32 scan_schema_layout_version
```

定义：

- 未设置或 `0`：legacy FE scan layout；
- `1`：Physical ScanTuple 已满足本文 ScanSchema 契约；
- 未识别的更高版本：返回明确错误，不能按 legacy 静默执行。

不要复用 Tablet schema version 或 `be_exec_version`，它们不表达 physical tuple layout
能力。

所有能够生成 `TOlapScanNode` 的 FE plan/serialization 入口都必须设置同一版本语义，
不能只修改 Nereids translator。shared-nothing 与 Cloud BE 使用相同解码规则；该能力
不受配置项控制。

### New FE -> Old BE

新 FE：

- 下发完整 Physical ScanTuple；
- 设置 optional layout version，新字段会被旧 BE 忽略；
- 将 storage semantic dependency UIDs 合并进已有 `output_column_unique_ids`；
- 将 FE 语义未引用、仅为 alignment 补齐的 key 标记到
  `extra_key_column_slot_ids`；是否可使用 placeholder 仍由每个 BE reader 根据
  direct/non-direct 和 predicate dependencies 决定。
- 对 MIN_DELTA/DETAIL 将 `push_down_agg_type` 设置为 `NONE`，使旧 BE 也不会选择
  COUNT、MINMAX 或 COUNT_ON_INDEX fast path。

旧 BE 看到完整 slots 后，已有 key/sequence widening 应保持幂等。兼容测试必须验证
旧 BE 不会因为重复追加而产生重复列。

### Old FE -> New BE

新 BE 在 layout version 未设置时，在 OlapScanner 边界使用
`LegacyReadSchemaAdapter`：

1. 读取 legacy FE tuple；
2. 根据 KeysType、direct mode、sequence、binlog mode 补齐 storage dependencies；
3. 按 storage order 构造内部 aligned ScanSchema，同时生成
   `legacy_position -> aligned_position` 和反向 output projection；
4. clone/rebind query/value predicates、runtime filters、common/virtual expressions、
   ANN/score、order-by/TopN 和其他 SlotRef consumers 到 aligned positions；
5. 使用 aligned ScanSchema Block 调用 TabletReader/BlockReader；
6. 在 Scanner 内部按反向 mapping 显式 projection 回 legacy tuple layout。

例如 legacy tuple `[v]` 适配为 `[k1, k2, v]` 后，原来的 SlotRef position `0` 必须
重绑为 `2`，不能只补列而继续执行旧 ordinal。legacy SlotDescriptor 的
`col_unique_id < 0` 时，adapter 保留既有兼容语义，在当前 selected index schema 中按
名称唯一解析；解析后立即转成 UID/path 或 synthetic identity，名称不进入下层运行期
ID 域。

Legacy adapter 位于 Scanner 之上。TabletReader 以下不重新引入
`origin_return_columns` 或 source-to-target mapping。

### New FE -> New BE

layout version 为 `1` 时：

- BE 不再 widening FE 已知列；
- 构建 dense ScanSchema；
- 严格校验 key prefix、sequence、binlog meta 和 BEFORE dependencies；
- 仅追加 dropped delete predicate 等 BE-only auxiliary fields。

### 移除 legacy

至少经过一个完整兼容窗口，并完成 mixed-version 测试后，才能：

- 删除 LegacyReadSchemaAdapter；
- 删除旧 ReaderParams column vectors；
- 删除 layout version 0 分支。

BE-first 发布，使新 BE 能同时接收 old/new FE plan。

## 非查询路径

以下路径没有 FE Physical ScanTuple，但必须使用同一个 Schema 模型：

- horizontal compaction；
- vertical compaction；
- segment compaction；
- full/base/cumulative/cold-data/binlog compaction；
- local schema change；
- cloud schema change；
- checksum；
- index builder；
- rowid conversion/fetch；
- statistics reader；
- historical row retriever。

这些路径通过本地 `ReadSchemaBuilder` 构造 ScanSchema：

- full-row reader：按目标 TabletSchema 构造完整 dense Schema；
- vertical compaction：按 column group 构造 dense group Schema；
- checksum/index builder：按请求列构造 dense Schema；
- schema change：明确区分 source read Schema 和 target write Schema，由 schema-change
  projection/converter 连接，不能复用 BlockReader 的 origin mapping。

所有路径都必须满足：

```text
iterator.schema() == next_batch Block schema
```

### VerticalBlockReader 与 Segment Compaction

Vertical compaction 不能继续隐式依赖完整 TabletSchema ordinal。每个 column group 都有
独立、连续的 dense group Schema：

- key group 包含 merge 所需完整 key、sequence、delete sign 和本组输出列；
- value group 只包含本组 value 及其必要辅助列；
- `VerticalBlockReader` 的 direct、AGG 和 UNIQUE 路径中，source/target Block 都必须
  与当前 group Schema 完全同形；
- `key_group_cluster_key_idxes`、sequence position 和 delete-sign position 都是当前
  group Schema 的 local position，而不是 Tablet CID。

key group 生成 `RowSourcesBuffer`；value group 必须按该 buffer 对齐同一批逻辑行，
不能通过 Tablet ordinal 或另一个 group 的 Block position 推断对应关系。cluster-key
比较也只使用 key group 中已经绑定好的 local positions。

Segment compaction 可能直接构造 SegmentIterator，而不经过 BetaRowsetReader。因此
`SegmentSchema -> group ScanSchema` 的 ProjectionIterator 必须放在 SegmentIterator
构造边界，不能只在 BetaRowsetReader 中包装。所有能够直接创建 SegmentIterator 的入口
都要接受同一条 exact-schema 校验。

## 性能与内存

### 预期收益

- `_current_return_columns` 从完整 TabletSchema 大小缩小为 SegmentSchema 大小。
- `_column_iterators`、`_index_iterators` 和多个 bool vectors 同样按实际读取列分配。
- 删除 Tablet CID/position 的重复线性查找。
- MergePlan 和 identity map 在 init 阶段一次构建，batch hot path 直接按 position 访问。
- Segment auxiliary projection 是前缀 column move，不逐行复制。

### 需要避免的退化

- 不得为 `SELECT v` 无条件读取宽表全部 value/BEFORE。
- identity lookup 不得在逐行或逐 value 聚合循环中执行。
- Schema validation 的完整名称/类型比较仅在 init 或 debug path 执行。
- ProjectionIterator 必须保留 BlockView 和 row-location 优化。
- immutable Schema 由 reader 共享，不在每个 batch 重建。

## 并发与生命周期

Schema 和 MergePlan 在 Scanner/Reader init 阶段构建，读取期间不可变：

```cpp
std::shared_ptr<const Schema>
std::shared_ptr<const MergePlan>
```

本重构不增加共享可变状态和锁。

生命周期要求：

- ReaderParams 拥有 Schema shared pointer，不保存指向临时 vector 的裸指针。
- RowsetReaderContext 共享 immutable Schema，不指向 TabletReader 内部
  `return_columns` vector。
- ProjectionIterator 持有 child 和 input/output Schema。
- ColumnPredicate clone 由对应 reader/rowset options 持有。
- DeleteHandler predicate templates 不被原地修改。

## 错误处理与可观测性

以下情况是 invariant violation，应返回错误或触发 correctness assertion，不能降级继续：

- ScanSchema 与 Physical ScanTuple 列数或类型不一致；
- merge path 缺失完整 key prefix；
- sequence/seq-map dependency 缺失；
- MIN_DELTA 缺失 OP/LSN/必要 BEFORE；
- DETAIL 缺失 OP/LSN，或启用 historical value 时缺失必要 BEFORE；
- historical mode 将 nullable BEFORE 复制到 non-nullable value 时遇到 NULL；
- ColumnPredicate position 超出 SegmentSchema；
- 同一 ColumnIdentity 被绑定到多个 positions；
- SegmentIterator Block 与 SegmentSchema 不一致；
- 未识别的 `scan_schema_layout_version`。

错误信息至少包含：

- tablet ID；
- rowset ID/segment ID，若已知；
- reader type；
- layout version；
- position；
- column UID/path/name；
- expected 和 actual Block structure。

Profile 可增加 init-only counters：

- ScanSchema column count；
- Segment auxiliary column count；
- legacy adapter 使用次数；
- auxiliary projection 次数。

不建议为每个 batch 增加 INFO 日志。

## 迁移计划

### 阶段 1：引入 dense Schema

- 新增强类型 `DenseReadSchema`、`ReadField`、ColumnIdentity 和 builder；保留现有
  sparse `Schema`，且两者禁止隐式转换。
- 引入 PlanScanSchema 和 LegacyReadSchemaAdapter；先产出 aligned Schema、双向
  position mapping 和重绑结果，使用单元测试验证，但暂不切换生产读取链。
- 保留现有 Scanner/TabletReader/SegmentIterator legacy 运行链，不在
  TabletReader/Rowset 边界临时把 dense position 转回 Tablet CID。
- 增加 Schema/Block validation。
- 不改变查询结果。

### 阶段 2：SegmentIterator local position

- 在独立的 dense iterator 接口上实现 query predicates local-position binding。
- delete predicates 按 UID/path clone/rebind。
- `Segment::new_iterator()` 的 pre-zonemap、expression zonemap 和 post-zonemap prune
  一并改为 ReadPosition/identity，不能留到 SegmentIterator init 后再转换。
- SegmentIterator vectors/maps 改为 dense local positions。
- Segment/ColumnReader 物理查找改走 ReadField UID/path。
- 覆盖 zonemap、inverted index、ANN、score、Variant、key seek。
- production reader 仍走完整 legacy 链；dense path 通过直接 UT 验证，禁止出现半条
  legacy、半条 dense 的调用链。

### 阶段 3：精确 Segment Block 与 projection

- SegmentIterator 输入输出使用完整 SegmentSchema Block。
- 删除 `i >= block->columns()`、`loc < block->columns()` auxiliary 判断。
- 引入 ProjectionIterator。
- VUnion/VMerge/VStatistics/TopN 路径迁移。
- 完成后在同一个提交中原子切换从 OlapScanner、TabletReader、RowsetReader、
  SegmentIterator 到 VCollect 的整条 query/binlog 链：adapter 创建 aligned
  ScanSchema/Block、VCollect 使用 `Schema::create_block()`，Scanner 最后按反向
  mapping projection。
- 切换前 legacy 链完整存在；切换后 TabletReader 以下全部使用 dense Schema，不允许在
  中间边界转换 ID 域。BlockReader 的 legacy mapping 暂时保留，但 source/target 已经
  同形，因此 mapping 必须是 identity，并由 assertion 验证。

### 阶段 4：非查询路径切换

- 迁移 compaction、schema change、checksum、index builder、Cloud 和所有直接创建
  SegmentIterator/BlockReader 的入口。
- 每个入口通过本地 builder 构造 dense full-row/group Schema；schema-change 以显式
  converter 连接 source read Schema 和 target write Schema。
- Vertical key/value groups、segment compaction 和 dropped delete auxiliary projection
  完整切换。
- 阶段结束时，所有进入 BlockReader 的 source/target Block 都已同形；legacy mapping
  暂时存在但必须是 identity。

### 阶段 5：BlockReader 与 Reader 参数收敛

- 前置条件：阶段 3 的 query/binlog 链和阶段 4 的全部非查询调用方均已切换；本阶段不能
  依赖新 FE 同时上线。
- 引入 MergePlan。
- 六条 next-block 路径改成 source/target 相同 ScanSchema。
- 删除 `_return_columns_loc` 和两套 seq-map。
- 删除 `origin_return_columns`。
- 删除空的 `_direct_agg_key_next_block`。
- 删除 ReaderParams/RowsetReaderContext 中描述 Block layout 的 `return_columns` 和
  nullable conversion set。

### 阶段 6：FE Physical ScanTuple

- 扩展 #64413 的 physical slot preservation。
- 加入 sequence、seq-map、binlog key/meta/BEFORE dependencies。
- 保持 LogicalOlapScan output 不变。
- 设置 `scan_schema_layout_version = 1`。
- 更新 `output_column_unique_ids` 兼容旧 BE。
- 增加 FE plan/tuple/projection 单元测试。
- 在发布新 FE 前完成 New FE + Old BE、Old FE + New BE 和 New FE + New BE
  mixed-version 测试。

### 阶段 7：兼容窗口后清理

- 全部 sparse Schema 调用方迁移后，删除 legacy Schema 构造/API，并将
  DenseReadSchema 收敛为最终 `Schema` 名称。
- 经过完整兼容窗口并确认 layout version 0 已退出支持范围后，删除 legacy adapter。

每个阶段都应是可独立构建、可测试、行为正确的提交，不能在中间状态依赖 FE/BE
同时升级。

## 测试计划

### Dense Schema 单元测试

- Tablet ordinals 稀疏且 ScanSchema positions 连续。
- 乱序 Scan slots 输入被 builder 规范化或明确拒绝，不能破坏 storage key prefix。
- key prefix validation。
- nullable conversion。
- nested/Variant pruned type。
- Variant parent UID + path。
- special column 位于 position 0。
- duplicate UID/path 拒绝。
- SegmentSchema 追加 auxiliary fields 后 ScanSchema positions 不变。
- Block structure validation。

### Predicate binding 单元测试

- query ColumnPredicate 与 VSlotRef 使用相同 position。
- runtime filter 和 common expression。
- dropped delete predicate。
- persisted delete PB 无 UID 时按其历史 schema 解析，drop/re-add 同名列不误绑。
- delete predicate 引用当前存在但未进入 ScanSchema 的列。
- drop 后同名重建。
- 多 rowset 使用不同 applicable delete predicates。
- AND/OR delete predicate tree clone。
- zonemap/inverted-index predicate map 使用 local position。
- Segment pre-zonemap、expression zonemap 和 ColumnReader post-zonemap prune 使用同一
  local position/identity。
- Variant parent 不在 ScanSchema 时，parent inverted index 仍能按 identity 绑定。
- synthetic virtual/rowid/global-rowid/score fields 使用专用 iterator，不访问物理 UID。
- key seek Schema 不是完整 Tablet prefix 时仍按 UID/path 绑定。
- old Segment 缺列 default iterator。

### BlockReader 单元测试

- direct；
- AGG SUM/MIN/MAX/REPLACE；
- UNIQUE 无 sequence；
- UNIQUE single sequence；
- seq-map multiple sequence；
- MIN_DELTA 各 op 组合；
- DETAIL UPDATE BEFORE/AFTER；
- 多个 value、部分列读取且 BEFORE slots 非相邻时，value-to-BEFORE mapping 正确；
- DETAIL 未启用 historical value 时无 companion 使用自身 fallback；MIN_DELTA 缺
  companion 必须失败；
- non-nullable AFTER/value + nullable BEFORE(non-NULL) 按计划 unwrap 后输出正确；
  historical BEFORE 为 NULL 时返回错误，不能插入默认值；
- nullable target 从 non-nullable source wrap 时追加正确 null map；
- batch boundary pending row；
- adaptive byte budget group boundary；
- delete sign position 不等于 Tablet ordinal；
- source/target Block structure mismatch 必须失败。

### Segment/iterator 单元测试

- SegmentIterator Block 与 SegmentSchema 完全一致。
- auxiliary projection 删除 dropped columns。
- ProjectionIterator move 前缀 columns 后能恢复 reusable input Block。
- ProjectionIterator 完整转发 `data_id()`、`merged_rows()`、`empty()`、init、row API、
  BlockView、same-bit 和 row location。
- VUnion 和 VMerge。
- BlockView/BlockWithSameBit。
- TopN/order-by。
- VStatistics fast path。
- row location。
- ANN/score。
- expression zonemap。
- sparse wide table allocation。
- VerticalBlockReader key/value group 使用各自 dense Schema。
- Vertical key group 覆盖 cluster key、sequence、delete sign；value group 按
  RowSourcesBuffer 对齐。
- segment compaction 直接创建 SegmentIterator 时仍执行 auxiliary projection。

### FE 单元测试

- Nereids 以及其他仍能构造 OlapScanNode 的 planner/plan 入口都覆盖相同依赖规则。
- DUP/UNIQUE MOW 不添加不必要 key。
- AGG/UNIQUE MOR 添加完整 key prefix。
- sequence 和 seq-map dependencies。
- MIN_DELTA/DETAIL 添加完整 key、OP、LSN、TSO 和对应 BEFORE。
- APPEND_ONLY 最小依赖。
- internal slots 不进入 Logical output。
- internal slots 被上层 projection 删除。
- binlog semantic fields 不属于 `extra_key_column_slot_ids`。
- `output_column_unique_ids` 包含兼容期 storage semantic dependencies。

### Regression

- `SELECT v FROM agg_table`，多 key。
- UNIQUE MOR/MOW。
- sequence 和 seq-map partial update。
- `@incr` APPEND_ONLY/MIN_DELTA/DETAIL。
- binlog 只选择部分 value。
- UPDATE BEFORE/AFTER。
- 跨 rowset、跨 segment、overlapping segments。
- add/drop/re-add column。
- 历史 delete predicate 引用 dropped column。
- Variant/nested columns。
- count/minmax/count-on-index。
- local 和 Cloud mode。
- horizontal/vertical compaction 后结果一致。

### Mixed-version

- New FE + Old BE。
- Old FE + New BE。
- New FE + New BE。
- legacy tuple 缺 key、sequence、BEFORE 时 adapter 正确补齐。
- legacy `[v] -> [k1, k2, v]` 后，predicate、runtime filter、expression、ANN/score、
  order-by/TopN 的 position 全部正确重绑，并按反向 mapping 输出 `[v]`。
- legacy SlotDescriptor 缺 UID 时仅在 adapter 边界按 selected index schema 名称解析。
- 新 FE 完整 tuple 在旧 BE widening 下不重复列。
- 同一 query 扫描多个 tablets，其中一部分 reader 为 direct、另一部分为 non-direct；
  `extra_key` 只能在满足跳读条件的 direct reader 使用 placeholder，结果必须一致。

## 验收标准

重构完成需满足：

1. TabletReader/BlockReader 不再存在 `origin_return_columns`。
2. BlockReader 不再存在 source-to-target column map。
3. query/binlog ReaderParams 不再使用 `return_columns` 描述 Block。
4. `Schema` 不再按完整 TabletSchema 大小保存稀疏 `_cols`。
5. SegmentIterator 不再使用 Tablet ordinal 索引 Block 或 internal vectors。
6. SegmentIterator 不再通过 Block 列数判断 auxiliary columns。
7. ColumnPredicate 和 VSlotRef 在 SegmentIterator 中使用同一 local position。
8. physical column lookup 明确通过 UID/path。
9. FE internal scan slots 不改变 Logical output 和 `SELECT *`。
10. 所有六条 BlockReader 路径返回 ScanSchema Block。
11. 所有非查询 reader 路径满足精确 Schema/Block 契约。
12. FE/BE mixed-version 测试通过。

## 已确定的设计决策

- 不新增 `must_read`。
- 不把 `__BEFORE__*` 暴露为 LogicalOlapScan output。
- TabletSchema 继续存在，但不再充当 Block layout。
- BlockReader source/target 使用同一个 ScanSchema。
- SegmentIterator 使用 dense SegmentSchema。
- predicate 和 expr 的 column ID 统一为 local position。
- dropped delete predicate 是 BE auxiliary Schema 的主要桥接场景。
- storage identity 使用 UID；Variant 使用 parent UID + path。
- compatibility mapping 放在 Scanner adapter，不放回 BlockReader。
- auxiliary projection 必须显式，不依赖稀疏 Schema 或较小 Block。
