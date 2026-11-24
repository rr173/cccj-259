# 仓表通告服务 (feedtable)

一个本地、可持久化的「仓表通告」服务：记录表 + 订阅区间 + 通知日志都在同一个
SQLite 文件里。质检员按半开整数区间订阅货号（`[lo, hi)`），可以左右挪动区间，
**已收过的数据和未处理消息继续沿用**，不需要清空重订，也不需要客户端偷偷补拉。

无网络依赖，纯离线；进程在任何时刻被杀掉，重启后从同一序列继续消费。

```
feedtable/
  db.py         schema + 事务封装（BEGIN IMMEDIATE / 读快照）
  intervals.py  半开区间与集合差
  batches.py    批次值对象、前置条件与内容指纹
  core.py       Feed：建订、提交事务、读取通告、确认、批次登记与撤回
  cli.py        命令行（每次调用一个进程，模拟随时崩溃的运维方式）
examples/consumer.py   只用投递内容的参考消费者
tests/                 102 个可执行检查（含随机差分测试）
```

## 快速开始

```python
from feedtable import Feed, RowSpec, MoveSpec

feed = Feed("roster.db")

# 先铺一些货号（此时还没有订阅，不占用通告序号）
feed.apply([RowSpec(f"sku-{i}", i, {"status": "ok"}) for i in range(200)])

# 建订：立即拿到起始视图 + 边界
view = feed.create_subscription("qc-a", 0, 100)
view.seq          # 边界：seq 大于它的提交才是接下来要消费的
view.rows         # 当时 [0,100) 内的全部记录

feed.ack("qc-a", view.seq)     # 起始视图确认完毕

# 一个写事务：改记录 + 挪区间。整个事务前/后可见结果决定变化。
out = feed.apply(
    [RowSpec("sku-60", 60, {"status": "hold"}),  # 保留区 [50,100) 内 → update
     RowSpec("sku-150", 150, {"status": "new"}), # 新区间外 → 不可见
     RowSpec.delete_pk("sku-7")],                # 被移除的区间内 → exit
    [MoveSpec("qc-a", 50, 150)],                 # 区间右移
)
out.seq                       # 本次提交通告的序号
out.changes["qc-a"]           # exit sku-0..49（sku-7 的删除并进这一条，只有一次）,
                              # update sku-60, enter sku-100..149
                              # sku-50..99 既非 exit 也非 enter：它们只是跟着挪
out.audit.to_dict()           # 可核查的候选读取计数

feed.pending("qc-a")          # 未确认的通告组，按原顺序、按当时内容
feed.ack("qc-a", out.seq)
feed.pending("qc-a")          # []
```

命令行等价：

```bash
python -m feedtable --db roster.db init
python -m feedtable --db roster.db load skus.jsonl
python -m feedtable --db roster.db subscribe qc-a 0 100
python -m feedtable --db roster.db apply --tx tx.json      # 见 cli.py 文档
python -m feedtable --db roster.db pending qc-a
python -m feedtable --db roster.db ack qc-a 3
```

## 语义

**记录**：`pk`（不变主键）、`route`（整数路由号）、`payload`（业务内容，
字符串或任意 JSON）。

**订阅**：一个名字 + 一个半开整数区间 `[lo, hi)`，外加独立的消费进度。

**建订交付起始视图**：`create_subscription` 在同一事务里读 `[lo, hi)` 的全部行，
存为不可变的起始视图，并返回边界 `seq`（建订时的日志头）。起始视图在确认前一直
以 `seq == 边界` 的消息出现，进程重启后原样重发。建订时使用的区间单独保存，因此
**先挪区间、后消费起始视图**也能正确衔接：起始视图仍是旧区间，随后的变化消息以
旧区间为基准表达。

**一次写事务** `apply(writes, moves)`：

- 记录增删改和订阅区间改动在**同一个 SQLite 事务**里提交；
- 变化由整个事务**前、后两份可见结果**决定：
  - 原来不可见、现在可见 → `enter`
  - 原来可见、现在不可见 → `exit`
  - 前后都可见，但交付的行不同（路由或内容） → `update`
  - 其余 → 不产生变化；
- 同一提交内每个主键每个订阅**只交一条净变化**（先删后插、先插后删都合并成最终
  状态；同一 pk 在事务内多次写入时最后一次生效）；
- 行只是**跟着区间一起挪**、仍在视图内时，不会出现「先删后建」——它既不会被读取，
  也不会被通告；
- 写事务若对任何订阅都没有可观察效果（例如只改了范围外的记录），则**不占用序号、
  不产生需要确认的消息**，业务写入照常落库。

**通告组**：每次提交按订阅生成一组有序变化，连同当时的区间边界一起持久化。
客户端按 `seq` 顺序读取、整组确认。

**确认**：`ack(subscription, seq)` 单调前进。确认前后都可以杀进程：
未确认的组仍在 `pending` 里，内容与顺序**按提交当时的样子**重发；历史组永远
按 `seq` 从日志读取，**绝不会用当前区间重算**。

**原子性**：数据、订阅定义、待发通告在同一个数据库、同一个写事务中落定
（WAL + `synchronous=FULL`）。提交前崩溃 → 全部回滚；提交后崩溃 → 全部持久。
客户端不可能看到部分结果。

## 读取代价

设订阅从 `O` 挪到 `N`。能改变可见性的行只可能是：

1. 本事务写入的行 —— 按主键**点查**前镜像；
2. `N \ O` 内的行 —— 候选进入；
3. `O \ N` 内的行 —— 候选退出。

`O ∩ N`（前后都可见，区间挪动本身不可能改变它们）以及 `O ∪ N` 之外的行**一律不读**；
也不会为了省事去扫两个区间的并集。

每次提交返回 `ReadAudit`，并随提交一起持久化（`Feed.commit_info(seq)` 可复查）：

```json
{
  "scanned": [
    {"label": "qc-a:enter[100,150)", "interval": [100, 150],
     "sql": "SELECT ... INDEXED BY records_route_pk WHERE route >= ? AND route < ? ...",
     "plan": ["SEARCH records USING INDEX records_route_pk (route>? AND route<?)"],
     "rows": 50, "excluded_already_written": 1, "candidates": 49,
     "for_subscriptions": ["qc-a"]}
  ],
  "rows_scanned": 50,
  "candidate_rows": 49,
  "point_lookups": 3,
  "stable_rows_read": 0,
  "violations": [],
  "allowed_segments": {"qc-a": [[100, 150], [0, 50]]},
  "retained_not_scanned": {"qc-a": [50, 100]}
}
```

- `candidate_rows`：真正作为「候选进入/退出」被读到的行数；
- `point_lookups`：按主键取写前镜像的次数；
- `stable_rows_read` / `violations`：任何落在保留区（或两个区间之外）的扫描都会被
  `violations()` 抓出来，这里恒为 0——它是由区间集合机械算出来的，不是自报；
- `plan` 来自 `EXPLAIN QUERY PLAN`，且 SQL 用 `INDEXED BY` 固定索引，
  schema 变化不会悄悄退化成全表扫描。

## 批次登记与按编号撤回

质检员发现某批货号资料采错时，可以在**现有数据库**上启用批次登记，之后按批次号
撤回：撤回后仓表的结果等同于「从未收过这一批」，但此后仍然成立的后续批次会保留。
订阅客户端**不需要清库或重新建订**，只会收到一组按净行差异生成的纠正通告。

### 启用

```python
feed.enable_batches()
```

启用时把当前整张 `records` 快照到 `baseline_records`，作为**不可撤回的起点**：
启用之前的写入不追认、也无法撤回。启用之后，记录的增删改只能通过批次入口提交；
`apply()` 若携带记录写入会被明确拒绝且不产生任何变化（纯区间挪动、建订、读取、确认
照常使用）。启用是幂等的。

### 批次结构

一个批次 (`BatchRequest`) 包含：

- 唯一编号 `batch_id`；
- 一组**可为空**的前置条件 `Condition`：
  - `Condition.absent(pk)`：要求该主键在仓表中**不存在**（存在但 `payload` 为
    `NULL` 仍是一行，不满足）；
  - `Condition.matches(pk, route, payload)`：要求存在一行且 `route`、`payload`
    与指定值**完全相同**，按仓表现有的行/值语义比较；
- 一组**非空**的记录写入 `RowSpec`，沿用既有的完整写入/删除语义；每个主键在一批
  只写一次，否则请求被拒绝。

所有条件都看本批生效前的仓表。全部满足才整批生效；否则一行不写，但该批仍会登记
（状态 `skipped`），保留首次登记顺序。

```python
from feedtable import BatchRequest, Condition

result = feed.submit_batch(BatchRequest(
    "jia",
    (Condition.matches("mark", 5, {"flag": "ok"}),),
    (RowSpec("d1", 6, {"v": 1}), RowSpec("d2", 7, {"v": 2})),
))
result.applied          # 条件是否满足、写入是否生效
result.notify_seq       # 生效且有可观察变化时的通告序号，否则 None
```

同编号、同内容重试不重新登记（`registered=False`）；同编号、不同内容冲突则拒绝，
无效请求不改变数据库。内容身份由规范化指纹 `batch_fingerprint` 决定（条件顺序无关，
JSON 键序无关）。

### 撤回与重新判断

```python
out = feed.withdraw_batch("jia")
out.audit.read_batches      # 本次实际读取了条件/写入明细的不同批次编号
```

撤回以启用时的仓表为起点理解结果：跳过所有已撤回批次，其余按**最初登记顺序**重新
判断。原来没通过的批次可能因此生效，原先生效的也可能整批失效；`batch_status` /
`batches` 反映最新结果（`registered` / `skipped` / `withdrawn`）。可以连续撤回不同
批次；重复撤回同一编号不再产生变化，未知编号报错。

例如甲写入校验标记、乙仅在标记存在且值符合预期时补录两条资料、丙在其中一条资料不
存在时转入待复核；撤回甲以后，乙的两条都不会留下，丙转而生效。

### 撤回的读取代价

撤回**不会全量扫仓表，也不会逐批读完整历史**。系统沿登记顺序按主键做闭包追踪：
一批写入的主键，只可能影响后来「把该键读作条件」或「写该键」的批次。允许重读撤回批
的条件/写入明细，以及沿这条链间接影响到的未撤回批；判断条件与净变化所需的历史行值
通过主键点查和启用基线取得。每次撤回返回的 `audit.read_batches` 列出**实际**打开过的
不同批次编号，便于检验无关批次没有被重放（长历史中这通常只占很小一部分）。

### 通告与原子性

一次撤回只按操作前、后的**实际行差异**向现有订阅发布纠正，沿用当前订阅区间和通告
规则：同一订阅只看到一组净变化，没有可见差异就不另发通告；旧起始视图、在途通告和已
确认进度保持原样。普通批次写入也照常产生通告。批次状态、当前仓表和新通告在同一个
SQLite 事务中落定，中断重开不会看到只更正了一部分的结果。

### 命令行

```bash
python -m feedtable --db roster.db enable-batches
python -m feedtable --db roster.db batch-submit --tx batch.json
python -m feedtable --db roster.db batch-withdraw jia
python -m feedtable --db roster.db batch-status          # 整个登记册
python -m feedtable --db roster.db batch-status jia      # 单个批次
```

`batch.json` 形如：

```json
{
  "batch_id": "jia",
  "conditions": [{"pk": "mark", "kind": "matches", "route": 5, "payload": {"flag": "ok"}}],
  "writes": [
    {"pk": "d1", "route": 6, "payload": {"v": 1}},
    {"pk": "ghost", "delete": true}
  ]
}
```

## 验证

```bash
python3 -m unittest discover -s tests -t . -v
```

覆盖内容（共 102 项）：

- **区间代数**（`test_intervals.py`）：半开边界、两段差集。
- **语义**（`test_feed.py`）：净变化、单主键单变更、挪区间不重建、写入与挪动同事务、
  读取代价、索引计划、**SQL 语句级追踪**（事务内对 `records` 的每条语句只可能是
  按主键点查或固定索引的双边界区间查询，且无 `SCAN records`）、大 payload。
- **批次登记**（`test_batches.py`）：启用快照与幂等、启用后旧写接口被拒且无变化、
  前置条件（含「NULL 行不等于不存在」与精确匹配）、失败仍登记、同编号幂等/冲突、
  登记顺序、生效才发通告。
- **批次撤回**（`test_withdrawal.py`）：甲/乙/丙示例、净纠正通告、连续撤回、重复与
  未知编号、跳过批撤回、状态翻转、无可见差异不发通告、只重放受影响批次、起始视图与
  确认进度保持原样。
- **批次持久化与读取代价**（`test_batch_durability.py`）：撤回失败整体回滚、重开后
  状态与纠正可复查、撤回期间对仓表/登记册只有点查和固定索引查询（无全表扫描）、
  索引计划、CLI 全流程。
- **批次随机差分测试**（`test_batch_differential.py`）：内置从启用基线全量重放的参考
  实现，随机登记与撤回多轮；逐步比对仓表、批次状态、订阅变化，并校验撤回实际读取的
  批次集合与按键影响闭包完全一致。
- **持久化与原子性**（`test_durability.py`）：部分确认后「崩溃」重放、历史组不被重算、
  另一连接看不到未提交事务、失败事务不留痕、审计随提交持久化并可复查、CLI 全流程。
- **运维流程**（`test_workflow.py`）：未消费起始视图时先挪区间、多订阅互不干扰、
  4 线程并发提交后序号连续且重放结果与源表一致。
- **随机差分测试**（`test_differential.py`）：内置一个全量扫描的参考实现，随机执行
  多轮事务；逐条比对净变化、代价审计，并让一个「只吃投递内容」的消费者在每一步
  之后与源表可见集合完全一致。
- **参考消费者**（`test_example.py`）：消费 → 改订 → 再消费的完整闭环；
  以及在「状态文件已写、确认未写」处崩溃后重跑只补确认、不重复应用。

参考消费者可独立运行（只读投递内容，状态写入 JSON 文件，可选交叉核对源表）：

```bash
python3 examples/consumer.py --db roster.db --name qc-a \
        --state qc-a.json --commit --source roster.db
```

## 取舍

- SQLite 单写者：并发提交会排队（`busy_timeout=30s`），读不受影响（WAL）。
  这是「同机离线」场景下的合理选择。
- 日志组按订阅存储（`commit_changes` 带 `subscription` 列）：同一条记录若被多个
  订阅看到，会在各自组里各写一份。这让「按订阅、按原样重放」成为一次索引查找，
  代价是日志体积随订阅数线性增长。
- 起始视图整份物化：换来确定性和可重放，代价是建订时读到 `[lo, hi)` 全量一次。
- 「无可观察变化的事务不占序号」意味着序号是稀疏的提交序号而非连续事务号；
  `pending` 与 `ack` 都按序号的顺序语义工作，稀疏无影响。带区间挪动的事务即使
  变化为空也占一个序号，因为新边界本身要投递。
- 只做本机持久化，不做网络推送；消费方按需拉取 `pending`。
