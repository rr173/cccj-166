# 受管整备批次账本 + 选择性撤销（managed batch ledger with selective revocation）

从零实现的 Python + SQLite 数据仓库入库/加工账本：每一批是一个**确定性的结构化程序**，
按不可改的入库时间顺序执行；每批的输入、输出、读写足迹、guard 判定与失败决定都被
持久化并可复核。当你需要撤销某个**已成功**批次时，系统不会删行、也不会回滚备份，
而是从该批之前的合法世代出发、**不执行目标批**、把之后各成功批的原始程序按原顺序
重新判定，生成一个**新世代**；原批次的全部运行证据保持只读。

```
revoke_ledger/          协议校验、解释器、SQLite 物理层、账本/计划/应用/恢复
fixtures/scenarios.py   演练用初始数据与批次程序
oracle.py               独立参考解释器：从初始状态全量重放（不做跳过优化）
reference_interpreter.py  oracle 的实现，不 import 产品包
drills/run_drills.py    可执行演练，逐条打印并断言产品 == oracle
tests/test_ledger.py    60 项 unittest
```

```bash
cd /workspace
python3 -m unittest discover -s tests -v     # 60 tests
python3 drills/run_drills.py                 # full drill, prints evidence
python3 drills/run_drills.py --keep /tmp/d   # keep the main SQLite db
python3 -m revoke_ledger.cli --help  # thin CLI: init/ingest/run/plan/apply/result/show/history
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
一声明用作“可跳过”的结构性依据之一（见 §5.3）。

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

## 5. 撤销：计划 → 应用

### 5.1 业务定义

撤销目标是**已成功**批次 `T`（失败批不能撤销，且撤销时也不会被激活）。
`T` 之前已成功的批保持原样（`Kept / upstream_of_target`），它们定义了撤销的出发点。
之后各批按原顺序处理：

| 原状态 | 结果 | 写行为 |
|---|---|---|
| 目标 `T` | `Superseded` | 不执行，效果被“从 T 之前出发”的推导自然移除 |
| 之后成功批 | `Kept / reused_identical` | 重放后读结果与写输出逐值等于原执行 |
| 之后成功批 | `Kept / independent_partition` | **不执行**，原输出逐值保留（见 §5.3） |
| 之后成功批 | `Changed / changed_replay` | 重放后结果不同；**新结果是该批的正式结果** |
| 之后成功批 | `Blocked / blocked_guard` | guard 不成立，零写入，不产生世代 |
| 之后成功批 | `Blocked / blocked_pk_conflict` | 主键/删除/写源前置条件失败，零写入 |
| 之后原失败批 | `Blocked / blocked_historical_failure` | 只保留历史决定，**不激活** |

关键点：**不是沿用后来批次的旧输出**。`B=2*A` 在新输入 `A=10` 上重新执行得到 20，
而不是保留 40；也不会删掉 `beta`。原来失败的批不会因为撤销而“变成成功”。

### 5.2 计划阶段（不改正式世代）

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

### 5.3 跳过（skip）的证明：不是“写集合不相交”

空选择与谓词范围意味着：撤掉一行可能让另一批**首次**满足 `row_count == 0/1`，
也可能让原先未选中的行**进入**选择窗口。仅跟踪“原来读出的 row_id”或仅比较两批写
集合都不足以证明不受影响。本实现的跳过必须同时满足：

1. 该批**声明了**键分区；
2. 该批**所有**读 map 身份与**所有**写目标都在该分区的表/区间内；
3. 分区与目标的**写键**、目标的**完整读作用域**在同一表上不相交；
4. 用当前推导状态**逐项复核**该批每个读步骤的完整 map 分类（absent/excluded/selected
   以及观察到的行值）、每个 guard 的值、每个写步骤的目标前置镜像与计算输出字节，
   全部与原执行一致；
5. 在推导结束后再取“旧 head ↔ 新推导状态”的**实际改变键集合**，要求该批所有读写
   身份都与实际改变键不相交；若因**其他 Changed 批**间接改变了它的输入，则撤销跳过、
   降级为真实重放，并重新推导（可能级联多次，直到稳定）。

只要任一条件不满足（例如声明分区与目标的读作用域相交、缺少声明、实际改变键落入其
作用域），该批一律**保守重放**。跳过时输出行逐值从原世代拷入推导状态，轨迹里记录
`executed=false` 与证明对象；`actual_changed_keys_in_scope` 必须为空。
`skip_eligibility` 会记录每个没有资格跳过的批的具体原因。

### 5.4 应用阶段（原子提交，拒绝陈旧计划）

`apply_plan(plan_id)` 先做完整陈旧性核对，然后才提交：

1. 计划存在且 `phase='planned'`（已应用则直接返回已记录结果，见 §5.5）；
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

### 5.5 响应丢失与恢复

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

## 6. 原证据只读，新结果以新世代保存

`ledger_batch`（程序/输入指纹/读写足迹/逐行分类证据/guard 证据/失败决定）与旧世代
行全部只追加、触发器禁止改删。撤销只新增：`revocation`、`generation_plan`、
`revocation_mapping`、`revocation_diff`、`lineage` 和一个 `kind='revocation'` 的新世代。
撤销世代的 `trace` 明确记录执行的批号、**跳过的批号**、blocked 批号与映射摘要。

## 7. 演练与独立对照

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

## 8. 边界与不变量

* 撤销后该血缘不再接受新批执行（新结果已不是原时间顺序的自然延伸）；证据仍可读。
* 一个目标最多被撤销一次；一个 revocation id 最多应用一次。
* 一个批次的所有写入在失败时全部不生效（批级原子），世代只表示成功提交。
* 所有哈希/摘要基于规范化 JSON（键排序、无空白），证据可逐字节复算。
* 值域外类型、int64 溢出、跨类型算术、越界 row_id 都在协议/执行层被拒绝。

## 9. 已知取舍

* `int_sum` 跳过 NULL 字段；若字段存在非 int 值则该批失败（保守）。
* 键分区是**显式声明**，不是自动推断；没有声明就永远重放，宁可保守。
* 计划与应用的重新推导是确定性的纯重放，成本与受影响批次规模成正比；独立分区批
  完全不执行。
* 该原型为单文件 SQLite、单进程入口设计；并发应用已用事务与条件更新做了串行化，
  但未做跨数据库分布式事务（任务明确无需）。
