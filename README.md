# 车队资料更账窗口的分布式静止屏障

更换工地设备资料编号前，协调端先让真实写入拓扑上的参与方**停止接旧编号新单**、
确认**此前接纳的工作全部排空或可信交接**，再向车队负责人发放**窗口承诺**开始更账，
最后只按**已经确定的编号版本**逐步恢复接单。资料为本地合成的范围、角色及工作交接，
运行环境 Python 3.11，**只使用标准库**。

## 一分钟验证

```bash
# 1) 原有资料读取器
python3 scenario_reader.py examples

# 2) 逐行回放三个演练案例并核对关键判定，全部 PASS、退出码 0
python3 replay.py examples

# 3) 全部单元/HTTP 测试（含原有读取器测试，共 30 个）
python3 -m unittest discover -v

# 4) 不起外部进程，一键在临时 HTTP 服务上跑“真实形状”的仿真端（29 项核对）
python3 simulator.py --local
```

也可以起真正的服务，再用仿真端打它：

```bash
python3 server.py --host 127.0.0.1 --port 8765 --data-dir .runtime
python3 simulator.py --base-url http://127.0.0.1:8765
```

服务是多“案例(case)”的：每个演练文件/每套拓扑一个独立 `Coordinator` 和一份
append-only 日志目录（`.runtime/cases/<case>/journal.jsonl`）。杀掉进程后用同一
`--data-dir` 重启，窗口承诺、`unknown` 冻结、恢复记录都还在。

## 目录

| 文件 | 作用 |
| --- | --- |
| `coordinator.py` | 纯标准库核心引擎：拓扑闭包、静止/排空闸门、承诺与登记簿锁、版本化恢复、日志持久化与重启恢复、数据面准入探测 |
| `scenario_reader.py` | 原有 JSONL 契约读取器（未改动其校验语义） |
| `replay.py` | 回放 CLI，逐行打印决定并对演练案例做机器核对 |
| `server.py` | `http.server` 实现的 JSON API + 多案例管理 + 磁盘重启 |
| `simulator.py` | 本地仿真端；`--local` 可在后台线程起临时服务一键端到端演练 |
| `examples/*.jsonl` | 三个独立演练案例 + `message_types.json` 消息名契约 |
| `test_coordinator.py` / `test_http_api.py` | 引擎规则测试与 HTTP/回放核对测试 |

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 `at`、消息名称 `kind` 与对象 `record`。
`at` 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 `record` 中。

- **member**　服务 `service` 的生产代次 `boot`，覆盖负责的 `fleets` 和 `registers`，并声明可静止的最小 fleet 组 `pause_groups`。`new_work` 表示在该事件时仍接纳输入。可选 `downstreams` 描述向下游交付。
- **open**　发起者申请 `window`，引用 `fleets` 或 `registers`。`expand_accepted` 表示负责人确认窗口可以覆盖额外车队。
- **approve_expansion**　负责人确认一个尚未开始的 window 可覆盖所列的附加 fleets。
- **paused**　指定 boot 的服务已停止接纳目标范围的旧编号。`pending` 列所负责的旧工作及业务编号。
- **transfer**　实际交接 work，由来源服务及代次和接收服务及代次确认。接收方须能明确承担旧编号工作。
- **finished**　指定服务代次对指定 work 回报真正处理完成。
- **begin**　持有 window 的操作方申请进入更账；闸门通过时发放持久的窗口承诺 `commitment`。
- **result**　记录更账结果 `committed`、`aborted` 或 `unknown`，committed 携带 `new_numbering`。
- **resume**　指定 boot 的服务回报已按 `numbering` 恢复接单；版本必须等于窗口已确定的版本。
- **inspect**　按 window 或 fleet 查状态。`cancel` 在开始前撤回申请；`restart` 仿真协调端重启。
- **cancel**　在开始更账以前撤回 window。
- **restart**　模拟协调端重启，其余服务保留自身接单与暂停事实。

## 规则如何落到判定上

- **真实写入拓扑闭包（open 时 + 每次 begin 前）**：由当前 member 代次的
  `fleets/registers/pause_groups` 求不动点。共享服务 `joined` 只能把 `[a,b]`
  一起静止时，open `fleets:["a"]` 会闭包出 `expansion_required:["b"]`；
  未经 `approve_expansion`，`begin` 得到 `expansion_unapproved`，且**不会**对 `b`
  产生任何冻结 hold，车队 `c` 更是全程 `allow`。服务随后公布
  `pause_groups:[["a"],["b"]]` 这种“可独立停止”的新能力后，扩张要求自动消失。
- **新代次必须自证静止**：`yard#1` 的 paused 不能满足当前代次 `yard#2`，
  因此示例第 8 行首次 `begin` 同时吃到 `pause_missing(yard#2)` 与
  `work_outstanding(job-3)`；第 11 行在 `yard#2` paused、`yard#1` finish 后才成功。
  窗口形成前就发生接管时同理：旧代次确认只会被标 `stale_generation`。
- **退出登记 ≠ 结束**：工作只在 `finished` 且回报方是台账当前保管/承接代次时才终结；
  非保管代次的 finish 记 `finish_rejected_not_holder`。
- **交接不排空**：`transfer` 只移动保管责任；接收方真正 finish 前一直是
  `work_outstanding`。`paused.pending:[]` 只是某时点声明，不会删除未完成工作。
  同一 work 被多个窗口/服务提及时进**全局工作台账**，任何一方“我这边没有了”都不能销账
  （见 `examples/shared-residual-work.jsonl`）。交接给交接前未确认的参与方会留下
  `ineligible_transfer`。
- **共用登记簿互斥**：成功 `begin` 持有 register 锁直到终局并完成版本化恢复；
  另一窗口对同一 `R` 的 begin 会被 `register_conflict` 拒绝，错误体直接给出占用方窗口，
  让双方“知情”。未开始（含 begin 被拒）的窗口可以 `cancel`。
- **数据面准入**（`POST /intake` 或 fleet 视图里的 `default_probe`）：
  - `changing`：旧编号 `reject_old_number`；持新编号提交权的客户端 `wait_for_result`。
  - `unknown`：一律 `retry_later`，冻结不解除。
  - `committed v2`：旧版 `reject_obsolete`；本代次未按 v2 resume 前 `retry_restoring`。
  - `aborted`：编号仍是 `v1`，按 v1 resume 后 `allow`，新提案被拒。
  - 写入拓扑上所有当前代次都放行，车队整体才 `allow`；因此示例末尾 `yard#2` 已恢复而
    `desk#1` 未恢复时，车队 `a` 整体仍是 `retry_restoring`（逐步恢复，不提前全开）。
- **版本化 resume**：committed `v2` 后 `resume numbering:"v1"` 返回
  `stale_numbering_resume` 且不改变接单状态；aborted 后只接受 `v1`；
  unknown/changing 时版本未定，任何 resume 都是 `numbering_not_determined`。
- **unknown 与重启**：`result:"unknown"` 后状态为 `unknown`、
  `determined_numbering=null`，inspect 的 `live_blockers` 含 `result_unresolved`，
  承诺与锁保留。所有输入先 fsync 到 journal，`restart` 以及真实进程重启都从日志折叠恢复，
  操作者断开连接不会被当成完成。

## HTTP API

```text
GET  /health
GET  /api/cases
POST /api/cases                         {"name":"..."}
POST /api/cases/<case>/events           完整信封 {at,kind,record}（case 不存在会自动建档）
POST /api/cases/<case>/restart          丢弃内存对象并从同一 journal 目录全新加载
POST /api/cases/<case>/intake           {"fleet":"a","numbering":"v1",
                                        "client_authorized_new":false,"service":null}
GET  /api/cases/<case>/snapshot
GET  /api/cases/<case>/topology
GET  /api/cases/<case>/windows          或 /windows/<wid>
GET  /api/cases/<case>/fleets           或 /fleets/<fid>
GET  /api/cases/<case>/work/<wid-id>
```

业务策略拒绝（begin 被闸门拦下、陈旧 resume 等）统一返回 HTTP 200 且
`accepted:false`，响应体带机器可读的 `code` 与 `blockers[].code`；
JSON 格式错误或缺少必填字段返回 HTTP 400；未知 case/资源返回 404。

## 作为库使用

```python
from coordinator import Coordinator

c = Coordinator(".runtime/my-case")          # 不传目录则纯内存
c.submit("member", {"service": "s", "boot": 1,
                    "fleets": ["a"], "registers": ["R"],
                    "pause_groups": [["a"]]})
c.submit("open", {"window": "w", "fleets": ["a"]})
print(c.evaluate_begin("w")["can_begin"], c.evaluate_begin("w")["blockers"])
```

## 示例

- `handoff-does-not-drain.jsonl` —— 新代次、交接未排空、unknown + restart、版本化 resume。
- `scope-expansion-and-overlap.jsonl` —— `[a,b]` 最小静止组的扩张审批、无关 `c` 继续作业、共用 `R` 互斥、abort 回 v1。
- `shared-residual-work.jsonl` —— 双方都称 pending 为空仍不能清空共享残余工作，finish 后才放行。

资料读取器会列出各文件包含的消息和时间范围。演练记录只描述各参与方交给系统的输入；
协调服务对每条输入给出受理决定与阻塞项，并允许在演练之后继续接入新车队、新更账范围与状态查询。
