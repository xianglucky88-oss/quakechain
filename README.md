# quakechain

A small offline data-processing service with a local HTTP fixture and SQLite storage. It is self-contained and does not require network access.

## Usage

```text
make run       # process the bundled source
make run_inc   # process using the incremental command-line mode
make test      # run the test suite
make mock      # start the local fixture service
```

The package exposes a Python entry point under `src/quakechain`. The fixture service supports the source formats used by the package, and the command-line entry point writes results to the local SQLite database. Re-running commands is safe for an existing database.

## Revisions and two time perspectives

震级和位置会在速报后修订。每次抓取都以**修订（revision）**为单位追加写入：
同一来源内容的重复抓取不会新增行，内容不同的修订各自保留一行。

每条修订都带有可说明的证据：

| 字段 | 含义 |
| --- | --- |
| `source_id` | 来源标识（本地夹具为 `local-fixture`，来源可自带） |
| `event_time` | 事件/报告时间（来源给出的 `ts`） |
| `revised_time` | 来源声明的修订时间（`revised_at` 等） |
| `revision_no` | 来源给出的修订序号（可选） |
| `basis` | 修订依据（`revision_reason` 等来源说明） |
| `receive_time` | 本进程首次接收该修订的时间（确定性逻辑时钟） |
| `rule_version` | 该行入库时所适用的本地规则版本 |

两个时间视角：

* `pipeline.latest()` —— 当前视角，取每个事件排序最高（最新）的有效修订。
* `pipeline.origin()` —— 来源时间视角，取每个事件最早发布的修订。
* `pipeline.initial()` —— 初报视角，等于该事件首次接收时刻的 `as_of` 快照
  （“初报时我们掌握的信息”）。
* `pipeline.as_of(t)` —— 还原 `t` 时刻已知的当前状态。
* `pipeline.history(id)` / `pipeline.explain(id)` —— 修订链、来源与修订依据。

排序只使用来源事实（`revision_no`、修订时间、事件时间，最后以内容哈希定序），
绝不使用到达顺序，因此迟到修订、同刻冲突和重复抓取都不会让较新的有效版本倒退。
同刻冲突会在 `explain()` 的 `conflicts` 中显式列出，并以内容哈希给出确定性胜者。
`version_no`/`prev_version_id` 是全局链证据；`is_latest` 相对于该视图可见的修订
（因此 `as_of(t)` 能精确重建当时 `latest()` 的返回结果）。

命令行对应 `--latest`、`--origin`、`--initial`、`--as-of <t>`、`--explain <id>`。

### 升级与规则更新

打开旧数据库时自动就地升级：保留原有 `content_hash` 主键与接收历史，
补齐来源/时间/依据字段，并把旧管道写入 payload 的派生字段移除、原文保存在
`legacy_payload` 以便追溯。旧的派生内容哈希通过 `canonical_hash` 去重，
升级后重新抓取不会产生重复修订。规则版本变化只重算派生列（`reindex()`），
不改写已存内容、修订身份或接收时间，旧记录仍带有其当时的 `rule_version`。
