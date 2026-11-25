# 车队资料更账窗口的分布式静止屏障

更账控制端协调接单服务先静止、再按确定的资料版本恢复。资料为本地合成的范围、角色及工作交接。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **member**　服务 service 的生产代次 boot，覆盖负责的 fleets 和 registers，并声明可静止的最小 fleet 组 pause_groups。new_work 表示在该事件时仍接纳输入。
- **open**　发起者申请 window，引用 fleets 或 registers。expand_accepted 表示负责人确认窗口可以覆盖额外车队。
- **approve_expansion**　负责人确认一个尚未开始的 window 可覆盖所列的附加 fleets。
- **paused**　指定 boot 的服务已停止接纳目标范围的旧编号。pending 列所负责的旧工作及业务编号。
- **transfer**　实际交接 work，由来源服务及代次和接收服务及代次确认。接收方须能明确承担旧编号工作。
- **finished**　指定服务代次对指定 work 回报真正处理完成。
- **begin**　持有 window 的操作方申请进入更账。
- **result**　记录更账结果 committed、aborted 或 unknown，committed 携带 new_numbering。
- **resume**　指定 boot 的服务回报已按 numbering 恢复接单。
- **inspect**　按 window 或 fleet 查状态。cancel 在开始前撤回申请；restart 仿真协调端重启。
- **cancel**　在开始更账以前撤回 window。
- **restart**　模拟协调端重启，其余服务保留自身接单与暂停事实。

## 示例

- handoff-does-not-drain.jsonl
- scope-expansion-and-overlap.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会列出各文件包含的消息和时间范围。演练记录只描述各参与方交给系统的输入。
