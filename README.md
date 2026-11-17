# 受管整备批次账本 + 选择性撤销（managed batch ledger with selective revocation）

从零实现的 Python + SQLite 数据仓库入库/加工账本：每一批是一个**确定性的结构化程序**，
按不可改的入库时间顺序执行；每批的输入、输出、读写足迹、guard 判定与失败决定都被
持久化并可复核。当你需要撤销某个**已成功**批次时，系统不会删行、也不会回滚备份，
而是从该批之前的合法世代出发、**不执行目标批**、把之后各成功批的原始程序按原顺序
重新判定，生成一个**新世代**；原批次的全部运行证据保持只读。

```
revoke_ledger/          协议校验、解释器、SQLite 物理层、账本/计划/应用/恢复
revoke_ledger/constraints.py  声明式关系完整性：规则、批末检查、级联闭包、依赖域
revoke_ledger/rprotocol.py    managed-relational/v1 程序校验（连接/投影/分组/派生写）
revoke_ledger/relational.py   managed-relational/v1 确定性执行器（关系算子与证据）
fixtures/scenarios.py   演练用初始数据与批次程序
fixtures/relational_scenarios.py  入仓加工（订单->渠道归属->客户汇总）夹具与手算预期
fixtures/relational/    提交的 CLI 示例 JSON（种子/约束/加工程序）
oracle.py               独立参考解释器：从初始状态全量重放（不做跳过优化）
constraint_oracle.py    独立的约束参考实现（朴素重复扫描，不 import 产品包）
reference_interpreter.py  oracle 的实现，不 import 产品包
drills/run_drills.py    可执行演练，逐条打印并断言产品 == oracle
drills/run_constraint_drills.py  约束功能演练（级联、唯一键、撤销阻断）
drills/run_relational_drills.py  关系加工演练（配对/分组/金额逐项断言 + CLI）
tests/test_ledger.py    60 项 unittest
tests/test_constraints.py 31 项约束 unittest
tests/test_relational.py  62 项关系加工程序 unittest
```

```bash
cd /workspace
python3 -m unittest discover -s tests -v     # 153 tests
python3 drills/run_drills.py                 # full drill, prints evidence
python3 drills/run_constraint_drills.py      # constraint drills, prints evidence
python3 drills/run_relational_drills.py      # relational drills, prints evidence
python3 drills/run_drills.py --keep /tmp/d   # keep the main SQLite db
python3 -m revoke_ledger.cli --help  # config/ingest/run/plan/apply/explain
```

---

## 1. 数据模型：稳定 row_id、四种值、显式身份

* 每行身份是 `(table, row_id)`，`row_id` 是显式 **int64**（`-2^63 .. 2^63-1`），
  绝不从值推断身份。
* 值域严格为 `int64 / text / bool / null`。`bool` 与 `int` 在比较中**不同型**：
  `eq(True, 1)` 为假；`range` 只匹配 int 值；`null` 不等于任何常量。
* 表结构是开放的：列由写入值的列名决定；未写的列在 `replace` 后为 NULL
  （`replace` 是整行替换）。

程序写入的每一行都必须能追溯到本批某次读结果中的显式 `(row_id)`，见 §2。

## 2. 程序协议 `managed-batch/v1`（封闭、无代码、无外部效应）

```json
{
  "logical_batch_id": "T",
  "schema": "managed-batch/v1",
  "steps": [ <step>, ... ],
  "partition": {"table": "gamma", "ranges": [[100,199]]}   // 可选
}
```

### 2.1 读步骤

```json
{"op": "read",
 "select": {"table": "alpha", "where": <pred>},
 "alias": "r",
 "map": [{"row_id": 1, "table": "alpha"}, ...]}
```

**读结果是：显式 `map` 中、且在当前时刻满足 `where` 的那些行。** 身份只来自 `map`，
值只来自被读行本身和 `where`。这使“谓词范围”“空选择”“原来未选中的行”都被固定在
可复核的显式身份域内。

### 2.2 谓词（两值逻辑，NULL 明确处理）

```
all | and(args) | or(args) | not(arg)
eq(field, value)        -> (v IS NOT NULL) AND (type 相同) AND v == value
range(field, lo, hi)    -> (v IS NOT NULL) AND int(lo <= v <= hi)
is_null(field, true/false) -> (v IS NULL) == 布尔
```

字段引用：`"<col>"`（当前读表）或 `"<earlier alias>.<col>"`（本批更早的读步骤）。
`not` 是两值取反，因此 `not(eq(n, 7))` 对 `n IS NULL` 也为真——这一点在证据里逐行记录。

### 2.3 写步骤

```json
{"op": "write", "effect": "insert|replace|delete",
 "table": "beta", "rows_alias": "r",
 "rows": [{"source_row_id": 1, "row_id": {"const": 500},
           "values": {"v": {"add": [{"field": "a.amount"},
                                    {"field": "a.amount"}]}}}]}
```

* 目标身份 = `source_row_id`（必须在该读的显式 `map` 内）+ `row_id` 表达式
  （`{"const": int64}` 或 `{"add": [{"const": int64}, int]}`）。
* 值表达式只有三种：`{"const": scalar}`、`{"field": ref}`、`{"add"|"sub": [a,b]}`
  （仅 int，精确运算，溢出即失败）。**没有当前时间、随机数、外部请求、SQL、函数。**
* `insert` 目标必须不存在，`delete` 目标必须存在，`replace` 是整行 upsert。
  写源行必须仍被该读选中（写前置条件）。
* 同一批内 `source_row_id` 不得重复；同一写步骤内目标 `row_id` 不得重复。

### 2.4 guard

```json
{"op": "guard", "test": "row_count", "alias": "r", "eq": 1}
{"op": "guard", "test": "int_sum", "field": "amount", "alias": "r", "ge": 15}
```

`row_count` 统计该读的选中行数；`int_sum` 对选中行的 int 字段求和（NULL 跳过，非 int 失败）。
给定的 `eq/ge/le` 必须全部成立，否则该批在**该步骤**失败。

### 2.5 执行与原子性

步骤按序执行，读看到本批更早步骤的写入。任一 guard 失败、主键冲突、删除缺行、
写源缺失、类型错误都是失败点：**该批不写入任何行、不产生世代**，失败证据与决定
写入账本。成功批次在**一个 SQLite 事务**里写入全部行并切换原子指针。

### 2.6 可选键分区声明 `partition`

一个批次可声明其读写只发生在 `partition.table` 的 `partition.ranges`（有序不交叠的
int64 闭区间）内。撤销计划只有在所有读写身份**都真的落在**该表该区间内时，才会把这
一声明用作“可跳过”的结构性依据之一（见 §6.3）。

### 2.7 第二套协议

除 `managed-batch/v1` 外，账本还接受关系加工程序协议 `managed-relational/v1`
（读取显式候选域 → 不可变关系别名 → 连接/投影/分组 → 对派生关系每行写入），
用于“按订单明细匹配渠道归属、再按客户汇总”这类关联汇总加工。完整定义见 §11。
两套协议共用同一入批、世代、约束与撤销合同；`managed-batch/v1` 的行为与历史证据
完全不变。

---

## 3. 入批：不可变时间顺序、输入指纹

* `ingest(program)` 先做完整协议校验，然后在一个事务里分配**永不改变的**
  `batch_ord`（按到达顺序）并落 `ingest_record`（含程序规范化 JSON 与 hash）。
* 同一 `logical_batch_id` 只能对应一份程序字节：重复提交完全相同的字节是幂等；
  不同字节一律拒绝。`idempotency_key` 用于入口重试去重，同一 key 对应不同程序也拒绝。
* `run_ingested` 只允许按 `batch_ord` 顺序执行：任一更早的入批还没有决定时拒绝执行
  后面的批。原始时间顺序不可改。
* `ledger_batch` 的 `UPDATE/DELETE` 被 SQLite 触发器禁止；`ingest_record`、`generation`、
  `managed_rows` 同样只读/只追加。失败批只留证据，`generation` 为 NULL。

## 4. 世代与原子可见性

```
generation         (generation, epoch, parent, kind, state_hash, state_digest, ...)
managed_rows       (generation, tbl, row_id, row_json)   -- 每代完整快照，只插不改
generation_head    (singleton=1, epoch, generation, ...) -- 唯一的可写指针行
```

一次提交 = 插入新世代的行 + 在**同一事务**内切换 `generation_head`。读取者总是先从
head 取一个 generation 号，然后只读该 generation，因此只能看到完整的旧结果或完整的
新结果，看不到“撤销一半”。

`epoch` 每次成功提交递增，撤销计划记录它捕获的 head 世代与 epoch，应用时逐项核对。

## 5. 声明式关系完整性（SQLite 之外的确定性约束层）

约束不是 SQLite 外键/触发器：它们是**账本自己管理的规则集**，在批末对
“所有显式步骤都成功之后的候选状态”统一计算。这样中间读写可以完全自由，
而每个提交的世代都满足约束。

### 5.0 配置协议 `managed-constraints/v1`

```json
{
  "schema": "managed-constraints/v1",
  "rules": [
    {"name": "account_owner_fk", "kind": "foreign_key",
     "child": "account", "field": "owner_id",
     "parent": "owner", "on_delete": "CASCADE"},
    {"name": "owner_code_u", "kind": "unique",
     "table": "owner", "fields": ["code"]}
  ]
}
```

* 每条规则有**稳定且唯一**的 `name`；外键指定子表一个字段、父表 `row_id`，
  `on_delete` 只能是 `CASCADE` 或 `RESTRICT`。支持同表引用、多层引用、循环引用。
* 规则集合在 seed 或首次入批前安装一次（`Ledger.install_constraints`），
  落 `constraint_config` 单行并由触发器禁止 `UPDATE/DELETE`；之后不可更换。
  同一配置重复安装是幂等的，不同配置一律拒绝。
* 配置先整体校验再落盘：非法配置**不会安装半份**（`state` 仍为未配置）。
* seed 本身必须满足规则，否则 seed 不提交、不产生世代；旧库未配置时完全沿用
  原有语义（不做在线加规则）。
* `Ledger.constraints_view()` 读回持久化配置、`config_hash`、安装世代与语义说明；
  重新打开数据库结果一致。

### 5.1 值语义

* 外键值只能是 int64 或 null；**bool 不算整数**（记 `fk_type` 原因）。
  null 不要求父行存在；字段缺失按 null 处理。
* 唯一键中**只要含 null 该元组就不参与冲突**；其余元组按**类型与值**比较，
  因此 `1`、`true`、`"1"` 三者互不相同。

### 5.2 批末检查顺序（终态检查）

对每个批，显式步骤按序执行，读能看到本批更早的显式写入（与原有语义完全一致，
主键冲突/guard/表达式规则照常生效）。全部显式步骤与 guard 成功后，才做**一次**
关系检查：

1. **删除起点（按行身份）**：比较批前状态与显式步骤完成后的候选状态，凡是
   “原来存在、现在不存在”的 `(table, row_id)` 都是删除起点。同一个 `row_id`
   删除后又在同批重建，则不算消失。
2. **CASCADE 闭包**：从起点出发，反复删除候选状态中仍引用已删父行的子行，
   直到没有新增删除。每个行至多删除一次，环因此必然终止。**新插入**的、引用
   本批被删父行的子行也进入闭包。
3. **存活行检查**：闭包完成后才检查存活行的 RESTRICT 引用、其它外键存在性与
   唯一键冲突。因此被另一条级联路径删掉的 RESTRICT 子行不拦截；凭空写一个
   不存在的父 `row_id` 是 `fk_missing` 违规，绝不会被当成“合法级联”。
4. 任一原因 → **整批失败**：所有显式写入与级联删除都不落正式数据，不产生世代；
   原因记录到 `ledger_batch.abort_json`。

### 5.3 多条原因的稳定记录

原因按 `(rule, code, table, 行身份, 规范化 JSON)` 排序去重，每条含规则名、
表、行身份、字段，以及父身份或唯一键组：

```json
{"code": "fk_missing", "rule": "account_owner_fk", "table": "account",
 "row_id": 2, "field": "owner_id", "parent": "owner", "referenced_row_id": 5}
{"code": "restrict_violation", "rule": "note_owner_fk", "table": "note",
 "row_id": 900, "deleted_parent": ["owner", 1]}
{"code": "unique_violation", "rule": "owner_code_u", "table": "owner",
 "row_ids": [1, 3], "fields": ["code"], "values": [{"type":"text","value":"c"}]}
```

成功批的证据里区分**显式写入**与**隐式删除**，并给出可追溯的触发链：

```json
"deleted": [
  {"table": "node", "row_id": 3, "explicit": false, "implicit": true,
   "rule": "node_parent_fk", "root": ["owner", 1],
   "trigger_chain": [
     {"from_table":"owner","from_row_id":1,"to_table":"node","to_row_id":1,
      "rule":"node_owner_fk","on_delete":"CASCADE"},
     {"from_table":"node","from_row_id":1,"to_table":"node","to_row_id":2,
      "rule":"node_parent_fk","on_delete":"CASCADE"},
     {"from_table":"node","from_row_id":2,"to_table":"node","to_row_id":3,
      "rule":"node_parent_fk","on_delete":"CASCADE"}],
   "inserted_in_batch": false, "extra_parents": []}
]
```

多条路径到达同一行时只删除一次；第一条到达路径记为 `trigger_chain`，其余
已删父行记入 `extra_parents`。起点、显式/隐式集合、规则快照与检查结果同时写入
世代 `trace.components`，可用 `Ledger.explain_batch(id)` 读回。

## 6. 撤销：计划 → 应用

### 6.1 业务定义

撤销目标是**已成功**批次 `T`（失败批不能撤销，且撤销时也不会被激活）。
`T` 之前已成功的批保持原样（`Kept / upstream_of_target`），它们定义了撤销的出发点。
之后各批按原顺序处理：

| 原状态 | 结果 | 写行为 |
|---|---|---|
| 目标 `T` | `Superseded` | 不执行，效果被“从 T 之前出发”的推导自然移除 |
| 之后成功批 | `Kept / reused_identical` | 重放后读结果与写输出逐值等于原执行 |
| 之后成功批 | `Kept / independent_partition` | **不执行**，原输出逐值保留（见 §6.3） |
| 之后成功批 | `Changed / changed_replay` | 重放后结果不同；**新结果是该批的正式结果** |
| 之后成功批 | `Blocked / blocked_guard` | guard 不成立，零写入，不产生世代 |
| 之后成功批 | `Blocked / blocked_pk_conflict` | 主键/删除/写源前置条件失败，零写入 |
| 之后成功批 | `Blocked / blocked_constraint_violation` | 重放后候选状态违反外键/唯一键/级联约束，零写入、零级联 |
| 之后原失败批 | `Blocked / blocked_historical_failure` | 只保留历史决定，**不激活** |

撤销的结果是**在新语义下从头推导**的，不是把原级联日志反向写回：从目标前的世代
出发重放，级联在每次重放的批末重新计算；若某个后续批因引用或唯一键不再可行，
它以 `blocked_constraint_violation` 结束，绝不留局部级联。计划载荷记录规则快照
（`constraint_config`）、全部 CASCADE 边（`cascade_dependencies`）与目标的隐式删除
（`target_implicit_deletes`）；应用前会核对规则集未变，并对最终世代独立复核一次
约束，任何未解决原因都会拒绝提交。`Ledger.explain_revocation(id)` 读回这些内容。

关键点：**不是沿用后来批次的旧输出**。`B=2*A` 在新输入 `A=10` 上重新执行得到 20，
而不是保留 40；也不会删掉 `beta`。原来失败的批不会因为撤销而“变成成功”。

### 6.2 计划阶段（不改正式世代）

`plan_revocation(revocation_id, target_logical_id)` 在一个事务里分三段落盘：

1. `planning`：绑定 `revocation_id → target`（一次性、不可改绑），记录捕获的
   `current_generation/epoch`、当前的**完整数据库承诺**（fingerprint）与
   `base_generation` 的状态承诺；
2. 纯内存计算：从 `base_generation` 的状态出发，按原程序重放/跳过/阻断各后续批；
3. `planned`：写计划载荷、逐批映射（`revocation_mapping`）与逐行差异
   （`revocation_diff`），并把载荷摘要写成本计划摘要。

计划阶段**不写** `managed_rows / generation / generation_head`；head 的 generation 与
epoch 在计划前后完全相同（测试与演练都会断言）。

计划载荷包含：
* 每批 disposition / reason_code / reuse_kind / 细节（含验证读数、写前置条件、
  实际改变键交集、原始世代号）；
* `influence`：目标的写键 + **完整读作用域**（每个 map 身份，包括 absent/excluded）；
* `skip_eligibility`：哪些后续批有声明分区、为什么有/没有资格跳过；
* `diff`：旧 head → 新推导状态的逐行差异，并标注造成差异的批次；
* `expected_state_hash` / `expected_state_promise` / `expected_counts`。

重复对同一 revocation id + 同一目标调用 plan 是幂等的：从同一已提交 base 重算，
载荷与摘要逐字节相同，只替换尚未应用的 `planned` 记录。

### 6.3 跳过（skip）的证明：不是“写集合不相交”

空选择与谓词范围意味着：撤掉一行可能让另一批**首次**满足 `row_count == 0/1`，
也可能让原先未选中的行**进入**选择窗口。仅跟踪“原来读出的 row_id”或仅比较两批写
集合都不足以证明不受影响。本实现的跳过必须同时满足：

1. 该批**声明了**键分区；
2. 该批**所有**读 map 身份与**所有**写目标都在该分区的表/区间内；
3. 该批**记录过的隐式级联删除**也必须落在该分区的表/区间内——否则复用证明
   会碰触声明之外的行，直接保守重放；
4. 分区与目标的**写键**、目标的**完整读作用域**在同一表上不相交；
5. 用当前推导状态**逐项复核**该批每个读步骤的完整 map 分类（absent/excluded/selected
   以及观察到的行值）、每个 guard 的值、每个写步骤的目标前置镜像与计算输出字节、
   以及**约束结果**（级联删除集合与违规原因列表必须与原执行逐项相等），
   全部与原执行一致；
6. 在推导结束后再取“旧 head ↔ 新推导状态”的**实际改变键集合**，要求该批所有读写
   身份**及其约束依赖闭包**都与实际改变键不相交。闭包由声明规则计算：
   外键两个方向（引用的父行、引用它的子行）、唯一键组、以及沿 CASCADE 边的可达集，
   逐层传递（覆盖多层引用和环）。若因**其他 Changed 批**或**目标的级联**间接改变了
   闭包内的键，则撤销跳过、降级为真实重放，并重新推导（可能级联多次，直到稳定）。

约束依赖是第 6 条的关键：一个“显式读/写只在自己的表里”的批仍可能因为**别的表**
上的父行消失（外键存在性）、唯一值被占（唯一键）、或被级联扫到而改变结论。
因此这类批必须进入解释器重放，得到 `Blocked / blocked_constraint_violation`
及可定位的原因；真正无关的分区（依赖闭包与改变键不相交）仍然跳过。

只要任一条件不满足（例如声明分区与目标的读作用域相交、缺少声明、实际改变键落入其
作用域或依赖闭包），该批一律**保守重放**。跳过时输出行与记录的隐式级联删除逐值从
原世代拷入推导状态，轨迹里记录 `executed=false` 与证明对象；
`actual_changed_keys_in_scope`、`actual_dependency_overlap` 必须为空；
`constraint_dependency_keys` 记录被检查过的依赖行。`skip_eligibility` 会记录每个
没有资格跳过的批的具体原因。

`managed-relational/v1` 程序**永不进入跳过集合**：派生关系经连接/分组依赖整个读
作用域，产品选择对它们一律保守重放（见 §11.7），`skip_eligibility` 会记录该原因。
`managed-batch/v1` 分区批的跳过路径不受影响。

### 6.4 应用阶段（原子提交，拒绝陈旧计划）

`apply_plan(plan_id)` 先做完整陈旧性核对，然后才提交：

1. 计划存在且 `phase='planned'`（已应用则直接返回已记录结果，见 §6.5）；
2. 计划载荷摘要自校验；
3. **当前 head 的 generation/epoch 与计划捕获值相同**；
4. **当前完整数据库承诺（fingerprint）与计划捕获值相同**。该承诺覆盖所有世代的
   行字节、head、ingest 记录、批次账本（原始证据）、generation 元数据与 meta。
   因此计划之后出现的**新入批、新批决定、新撤销、乃至入口之外的 SQLite 篡改**
   都会让旧计划被拒绝，而不是覆盖新数据；
5. `base_generation` 的状态承诺仍成立；
6. 用计划里的 skip_set 重新推导，重放结果必须与计划映射逐批一致、最终状态 hash/promise
   与计划记录一致；否则拒绝（`replay_diverged` / `state_diverged`）。

核对全部通过后，在**一个事务**里：再次核对 head+承诺 → 插入新撤销世代 → 切换 head →
把计划标为 `applied` 并记录不可变结果 → 写 `lineage`。并发应用同一计划时，
`UPDATE ... WHERE phase='planned'` 只有一个事务能成功，其余返回同一条已记录结果。

### 6.5 响应丢失与恢复

* **应用提交后响应丢失**：`revocation_result(revocation_id)` 按撤销 ID（或 plan ID）
  返回同一份已提交决定（phase/applied 世代/mapping/diff/state_promise）。重复
  `apply_plan` 返回同一结果，不会产生第二个撤销世代。
* **计划构建被打断**：计划显式按 `planning → planned → applied` 分段落盘。
  `recover_interrupted_plan(rev_id, target)`：
  * 没有任何撤销行（且 `crash_before_bind=True`）→ `nothing_persisted`，让调用者重新发起；
  * `planning` → 丢弃半成品，从**已提交的 base 世代**重新计划（并先校验 base 承诺）；
  * `planned` → 直接返回已完成计划；
  * `applied` → 返回已记录结果。
  恢复过程从不修改原批记录（触发器保证）。
* **撤销 ID 复用**：一个 revocation id 永远只绑定一个目标；已应用的撤销不能再规划、
  也不能改绑；同一个目标不能被两次撤销（`LedgerError`）。

## 7. 原证据只读，新结果以新世代保存

`ledger_batch`（程序/输入指纹/读写足迹/逐行分类证据/guard 证据/失败决定）与旧世代
行全部只追加、触发器禁止改删。撤销只新增：`revocation`、`generation_plan`、
`revocation_mapping`、`revocation_diff`、`lineage` 和一个 `kind='revocation'` 的新世代。
撤销世代的 `trace` 明确记录执行的批号、**跳过的批号**、blocked 批号与映射摘要。

## 8. 演练与独立对照

`oracle.py` + `reference_interpreter.py` 是完全独立的参考实现（不 import 产品包），
从**初始种子世代**出发、按公开定义**全量重放**（跳过目标，不做任何跳过优化）。
`drills/run_drills.py` 对每个场景：

1. 产品计划/应用；
2. oracle 全量重放；
3. 逐表、逐行、逐值断言一致；
4. 打印实际执行轨迹，证明独立分区批**没有执行**、其他实际受影响的批被重放。

覆盖的业务点：

| 场景 | 期望 |
|---|---|
| 主演练 A=10→T 置 20→B=2A→独立 C=7 | 撤销后 A=10、B=20、C=7；B 不沿用 40 也不被删；C 跳过未重放 |
| 后批 guard `A>=15` | 撤销后该批 `Blocked / blocked_guard`，零写入 |
| 空读 guard + 后来插入 | 原先失败的批只留历史；原依赖“非空”的插入批 blocked |
| 谓词选中范围变化 | 原未选中行进入窗口使 `row_count==1` guard 失败 → blocked |
| 同 row_id 被后批覆盖 | 后批 `Changed`，按新输入重新算出覆盖值 |
| 多表/多步中途失败 | 批原子失败，零写入 |
| 计划后新增批 / 仅入批 / 外部改值 / 篡改计划载荷 | 全部 `StaleError` 拒绝，新数据不被覆盖 |
| 切换后响应丢失 | 按撤销 ID 取回同一决定，重复 apply 不新增世代 |
| 中断的计划构建 | 从 `planning` 阶段重做，结果与正常计划一致 |

### 8.1 约束演练（`drills/run_constraint_drills.py`）

约束演练的期望删除集合与终态由 `constraint_oracle.py` 计算：它只复用无产品依赖
的 `reference_interpreter`（显式程序步骤），关系部分用**朴素重复扫描**独立实现
（集合差求删除起点、`while changed` 重扫求闭包、逐行枚举求违规），**不复用**
产品的 `ConstraintSet.finalize` 或依赖域函数。演练逐条打印并断言产品 == oracle：

| 场景 | 期望 |
|---|---|
| 子行 `entry[200]`→`account[20]`→`owner[3]` 在同一批、父行最后插入 | 成功；批末检查 `ok=True` |
| 同一批交换 `owner[1]`/`owner[2]` 的唯一值 | 成功 |
| 交换后终态重复 | 整批 `constraint_violation`，原因定位 `unique_violation` + `row_ids [1,3]`，零写入、head 不变 |
| 一次删除经两条路径并穿过 `node 7↔8` 环 | 删除集合恰为 `{owner1, node1,2,3,7,8,9}`；`node[9]` 只删一次，第二条路径记入 `extra_parents`；触发链逐跳可读 |
| RESTRICT 子行 `note[900]` 已被另一条 CASCADE（`ledger[50]`）清掉 | 允许删除（仍记录显式/隐式与被触发规则） |
| 同一 RESTRICT 子行存活 | 整批拒绝（`restrict_violation`），旧数据逐值不变 |
| 目标批新增 `owner[5]`，后批 `L` 在**另一张表** `account` 上写引用它的子行 | 撤销目标后 `L` 必须 `Blocked / blocked_constraint_violation`（`fk_missing`），即使显式分区与目标写表不同；真正无关的 `note` 批仍 `skipped_independent`，执行轨迹里可见 |
| 父行删后重建（同批） | 不是删除起点，不级联 |
| 新插子行引用本批被删父行 | 进入级联（`inserted_in_batch=true`, `implicit=true`） |
| 引用从未存在的父行 | `fk_missing` 拒绝，**不同**于级联 |
| 决策后重开数据库 | 规则、隐式影响、约束原因与终态一致 |

运行示例（摘自约束演练输出）：

```
  disposition by batch (original order):
    #1  T   Superseded  superseded_target              removed
    #2  L   Blocked     blocked_constraint_violation   not_written
    #3  Z   Kept        independent_partition          skipped_independent
  L's constraint reasons: [{"code": "fk_missing", "rule": "account_owner_fk",
                            "table": "account", "row_id": 2, "referenced_row_id": 5}]
  interpreter invoked for: ['L']
  replayed batch ords : []
  skipped batch ords  : [3]
  blocked batch ords  : [2] reasons={'2': 'blocked_constraint_violation'}
  trace: executed=[2] skipped=[3] blocked=[2]
  oracle[revocation]: MATCH on every table/row/value
```

`tests/test_constraints.py` 覆盖同一组语义的点对点断言（31 项），
`tests/test_ledger.py` 的 60 项旧测试保证未配置账本的语义未变。

运行示例（摘自主演练输出）：

```
  interpreter actually invoked for: ['B']
  -> only the affected batch B was executed; C never entered the interpreter
  disposition by batch (original order):
    #1  A        Kept        upstream_of_target           prior_generation
    #2  T        Superseded  superseded_target            removed
    #3  B        Changed     changed_replay               replayed_new
    #4  C        Kept        independent_partition        skipped_independent
  new generation 7 (epoch 7), previous 6
  replayed batch ords : [3]
  skipped batch ords  : [4]
  after revocation:  alpha[1]={"amount":10}  beta[500]={"value":20}  gamma[100]={"value":7}
  oracle[main]: MATCH on every table/row/value
  executed_batch_ords = [3]     skipped_batch_ords = [4]
```

## 9. 边界与不变量

* 撤销后该血缘不再接受新批执行（新结果已不是原时间顺序的自然延伸）；证据仍可读。
* 一个目标最多被撤销一次；一个 revocation id 最多应用一次。
* 一个批次的所有写入在失败时全部不生效（批级原子），世代只表示成功提交。
* 所有哈希/摘要基于规范化 JSON（键排序、无空白），证据可逐字节复算。
* 值域外类型、int64 溢出、跨类型算术、越界 row_id 都在协议/执行层被拒绝。
* 配置安装是**一次性的**：已配置账本的规则集不可替换；未配置账本没有在线加规则入口。
* 约束检查是纯函数，不依赖 SQLite 外键/触发器，也不做物理级联；失败批不落任何行。
* 任何提交（seed、批、撤销世代）都必须满足已安装规则；撤销应用前会独立复核终态。
* 关系批（§11）的行出现身份只由程序与读结果决定，与读 map 的排列无关；同一程序
  重放必产生逐字节相同的关系摘要、配对与分组。
* 关系批的写目标只来自声明的整数字段或封闭安全整数表达式；同步骤目标碰撞整批
  拒绝；`sum` 只判最终 int64 溢出，中间累计精确。

## 10. 已知取舍

* `int_sum` 跳过 NULL 字段；若字段存在非 int 值则该批失败（保守）。
* 键分区是**显式声明**，不是自动推断；没有声明就永远重放，宁可保守。
* 计划与应用的重新推导是确定性的纯重放，成本与受影响批次规模成正比；独立分区批
  完全不执行。
* 约束闭包按行身份与规则名排序做 BFS，结果确定；多条路径只保留首条触发链并将
  其余父行记入 `extra_parents`，便于审计而非穷举所有路径。
* 带约束依赖的跳过证明比纯显式读写证明更保守：依赖闭包（外键双向、唯一键组、
  CASCADE 可达集）与实际改变键相交就重放，宁可多执行也不误跳。
* 该原型为单文件 SQLite、单进程入口设计；并发应用已用事务与条件更新做了串行化，
  但未做跨数据库分布式事务（任务明确无需）。

---

## 11. 关系加工程序协议 `managed-relational/v1`

入仓加工需要“按订单明细匹配渠道归属、再按客户汇总金额”：同一渠道可能对应多条
有效归属（一对多），尚未匹配的明细也要留下来。逐行显式映射表达不了这种关联汇总，
因此账本接受第二套有版本协议，把**关联与分组作为真正的批程序能力**接入同一套
入批/世代/约束/撤销合同。程序仍是惰性数据：不执行调用者代码、SQL 或外部请求。

```json
{
  "logical_batch_id": "SETTLE-INNER",
  "schema": "managed-relational/v1",
  "steps": [ <step>, ... ],
  "partition": {"table": "gamma", "ranges": [[100,199]]}   // 可选，同 v1
}
```

### 11.1 步骤

```json
{"op": "read", "alias": "o",
 "select": {"table": "orders", "where": <pred>},
 "map": [{"row_id": 1, "table": "orders"}, ...],
 "row_id_field": "oid"}                       // 可选：把源 row_id 暴露为字段

{"op": "join", "left": "o", "right": "a", "how": "inner"|"left",
 "on": [{"left": "channel", "right": "channel"}, ...], "alias": "j"}

{"op": "project", "from": "j", "alias": "p",
 "select": [{"from": "left.amount", "as": "amount"},
            {"from": "right.customer_id", "as": "customer"}]}

{"op": "group", "from": "p", "alias": "g",
 "keys": ["customer"],
 "aggregates": [{"op": "count_star", "as": "n"},
                {"op": "count", "field": "amount", "as": "na"},
                {"op": "sum", "field": "amount", "as": "total"},
                {"op": "min"|"max", "field": "amount", "as": "lo"}]}

{"op": "write", "effect": "insert"|"replace"|"delete",
 "table": "customer_summary", "from": "g",
 "target": {"field": "customer"},             // 或 {"const": int64} / {"add"|"sub": [a,b]}
 "values": {"total": {"field": "total"}, "n": {"field": "n"}}}

{"op": "guard", "test": "row_count"|"int_sum", "relation": "g",
 "field": "total", "eq"|"ge"|"le": <int>}
```

* `read` 与 v1 同形：显式候选 `map` + 谓词过滤，形成**不可变关系别名**——每个别名
  都是当时结果的快照，本批后续写表不会回头改变它。`row_id_field` 把源行 row_id
  显式暴露为关系字段（覆盖同名列），供投影与写目标使用。
* 字段引用按关系模式静态校验：表读是**开放**模式（任意简单列名都有定义，缺列读作
  null）；project/group 输出是**封闭**模式（恰为声明的名字）；join 输出用
  `left.`/`right.` 限定（嵌套连接逐层嵌套）。**名称冲突**（投影/聚合重名、分组键与
  聚合同名）与**未定义字段引用**在入批校验时即拒绝，诊断含步骤号、引用与已定义
  名字。字段关联只走声明的等值键，绝不依靠“两表恰好有相同 row_id”。

### 11.2 行出现身份（稳定、与 map 排列无关）

关系中每次行出现都有稳定身份：

```
read:    ["src", <step>, <row_id>]
join:    ["join", <step>, <左身份>, <右身份|null>]
project: ["proj", <step>, <源身份>]
group:   ["grp", <step>, [<类型化键分量>, ...]]
```

相同值的不同输入行身份不同（源 row_id 不同）；同一对输入的不同连接配对身份不同
（配对身份不同）。关系按身份的规范化形式排序，**换掉输入 map 的排列不会改变身份、
关系摘要或任何下游结果**（有专项测试逐摘要断言）。身份贯穿证据：连接配对、未匹配
左行、分组成员、写目标的 `source_identity` 都可逐级回溯到源读行。

### 11.3 连接语义

* 支持 `inner` 与 `left`，等值键为**一个或多个**键对。
* 键按**类型和值**相等：整数 `1`、布尔 `true`、文本 `"1"` 是三个不同的键。
  任一键分量为 null 或缺字段（任一侧）都**不匹配**——null 与 null 也不匹配。
* 保留重复行贡献：两条左行与三条右行共享同一键时 inner 产生 **6** 条结果，
  不按值去重。left 对每条无匹配左行保留一条结果，右侧字段全为 null。
* 连接结果是普通关系，可再次连接或分组（不限两表模板）；嵌套连接的字段逐层
  加 `left.`/`right.` 前缀，投影可把需要的字段重新命名为简单名。

### 11.4 分组与聚合

* 分组键由多个（投影）字段组成，**按类型区分**（`1`/`true`/`"1"` 不同组），但
  **null 与缺字段归为同一种空值**——这与连接不匹配空值的语义不同，是有意的。
* `count(*)` 计行出现数；`count(field)` 跳过 null；`sum` 只接收非 bool 的 int64
  并跳过 null，全为 null 时结果为 null；`min`/`max` 跳过 null，同组非空值必须同型
  且只能为整数或文本。
* **空输入**：有分组键时没有组；无分组键的全局聚合仍产生一行，两个 count 为 0，
  其余结果为 null。
* `sum` 用**精确整数**累计，只判断**最终结果**是否超出 int64：`2**62 + 2**62 -
  2**62 = 2**62` 这类最终可表示的正负抵消**不会**因中间和溢出而被拒绝。
* 其他类型错误与最终溢出都使**整批不提交**，诊断给出步骤号、聚合项名与组键
  （`abort.detail` 含 `aggregate`/`op`/`field`/`group_key`/`offending`）。

### 11.5 对派生关系每行写入

* 目标 row_id 由**调用者声明的整数字段**（如分组键 `customer`、读时声明的
  `oid`）或封闭安全整数表达式（`const`/`field`/`add`/`sub`，精确 int64 检查）确定；
  **不从遍历序号分配，也不把哈希截成整数**。目标为 null/非整数即整批失败。
* 一个写步骤中不同派生行算出同一目标 row_id，**即使内容相等也整批拒绝**
  （`pk_conflict`，`detail.duplicates` 定位）。
* `insert` 目标必须不存在、`delete` 目标必须存在、`replace` 为整行 upsert；
  派生关系可直接用于 `row_count`/`int_sum` guard，guard 失败后**同批早先的写入
  也不留下**（批级原子，与 v1 相同）。
* 写入后的唯一键、外键与级联仍走**现有批末检查**（§5）：显式步骤全部成功后才对
  候选状态统一计算级联闭包与违规，关系批没有另一条无约束导入入口。

### 11.6 证据与解释

关系批的执行证据在 v1 骨架（reads/writes/guards/footprints/read_contents/
constraints）之上增加 `relations`：每个别名的行数与内容摘要；每个连接的**输入配对**
（成对的行出现身份）、**未匹配左行**与未匹配右行；每个分组的**成员身份**、类型化
组键与聚合值；每个写目标的 `source_identity` 与前后镜像摘要。

* `Ledger.explain_derived(id)` / `python3 -m revoke_ledger.cli explain-derived DB ID`
  从**不可变批记录**读回：连接配对、未匹配左行、输出组的成员与聚合值、最终写到的
  受管行。重开数据库读回完全一致。
* `Ledger.explain_batch(id)` 增加 `derived` 区；撤销映射详情中的 `replay_derived`
  保存**新世代**的派生证据（见下）。

### 11.7 参与选择性撤销

* 关系批的读作用域与 v1 相同：完整 map 身份（**空匹配、被过滤掉的候选和缺失候选
  都算依赖依据**）进入目标批的影响域与跳过证明；撤销某个来源批可能恢复一条右行，
  让 left 结果从补空行变成匹配、使一个组出现/消失、改变合计并触发 guard。
* 后续加工批**按新输入重新得出结果**：从读步骤重放整条管道，不修补旧汇总，也不凭
  旧的最终命中行或显式目标表证明可复用。重放结果按 v1 同一合同判定
  Kept/Changed/Blocked（guard 失败 → `blocked_guard`，约束违反 →
  `blocked_constraint_violation`，历史失败批永不激活）。
* **保守重放**：关系程序永不进入跳过集合（`skip_eligibility` 记录原因）；但
  `managed-batch/v1` 下确实无关的分区批**继续真实跳过**，撤销世代的 trace
  （`executed_batch_ords`/`skipped_batch_ords`）证明它们未进入解释器。
* **证据分别保存**：原始入批证据在 `ledger_batch.evidence_json`（触发器只读），
  新世代的派生证据在 `revocation_mapping.detail.replay_derived` 与计划载荷中；
  `explain_revocation` 可同时读回两者。计划、应用与重开后的解释沿用 §6 的世代
  合同（陈旧计划照常拒绝）。

### 11.8 命令行示例（提交的夹具）

```bash
cd /workspace
python3 -m revoke_ledger.cli init /tmp/rel.db \
    fixtures/relational/seed.json fixtures/relational/constraints.json
python3 -m revoke_ledger.cli ingest /tmp/rel.db fixtures/relational/settle_inner.json
python3 -m revoke_ledger.cli run /tmp/rel.db SETTLE-INNER
python3 -m revoke_ledger.cli explain-derived /tmp/rel.db SETTLE-INNER
python3 -m revoke_ledger.cli show /tmp/rel.db
python3 -m revoke_ledger.cli run /tmp/rel.db SETTLE-FK       # 末端约束拒绝整批
```

### 11.9 演练与验收（`drills/run_relational_drills.py`）

小样本入仓流程：订单 ⋈ 渠道归属 → 按客户汇总 → 受管 `customer_summary` 行。
**预期关联配对、分组成员与最终受管行全部来自手算/独立枚举**
（`fixtures/relational_scenarios.py` 的 `EXPECTED_*` 常量），不调用生产算子自证；
验收逐项断言配对与金额，不只核对输出条数。覆盖：

| 场景 | 期望 |
|---|---|
| SETTLE-INNER | 一对多：web 订单金额同时计入客户 101/102；int `1` ≠ text `"1"`；汇总行逐值等于手算 |
| SETTLE-LEFT | 未匹配左行保留（右侧 null），以原明细 row_id 落 `order_enriched` |
| SETTLE-COMPOSITE | 复合键中 null/缺字段任一侧都不匹配（null≠null） |
| SETTLE-CHAIN | 连接→再连接→分组；复合分组键 (customer, name) |
| SETTLE-EMPTY | 空输入：全局聚合一行（count 0、其余 null），带键则零组 |
| SETTLE-CANCEL | `2**62+2**62-2**62=2**62` 最终可表示，不拒绝 |
| SETTLE-OVERFLOW/COLLIDE/TYPEERR | 最终溢出 / 目标碰撞（同内容也拒）/ 聚合类型错误，均整批不提交、head 不变 |
| SETTLE-FK / SETTLE-UNIQUE | 末端外键/唯一键违反经现有批末检查拒绝整批 |
| 撤销演练 | 恢复归属行 → PROC 重推导（102 组出现、明细从补空变匹配）、G guard 阻塞零写入、v1 分区批 Z 跳过（trace 证明）；重开库读回同一组派生来源解释 |
| CLI | init/ingest/run/explain-derived/show 全流程与手算一致 |

### 11.10 关系协议的已知取舍

* 关系程序在撤销时一律保守重放、从不跳过；只有 `managed-batch/v1` 分区批可跳过。
* 派生关系行数有硬上限（100 000，超出 `limit_exceeded` 整批失败），防止连接扇出
  失控；读 map 仍受 v1 的 10 000 上限约束。
* 表读关系是开放模式：字段拼写错误在开放侧不会静态报错（值读作 null），一旦经过
  project/group 形成封闭模式，未定义引用即在入批校验拒绝。
* 关系批证据（配对、成员）随批记录持久化，体量与派生关系规模成正比；原型按小样本
  设计，未做证据外存。
