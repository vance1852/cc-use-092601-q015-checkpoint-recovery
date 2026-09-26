# 建立训练数据加工任务断点恢复基础平台

本项目是一套可离线运行的 Python 服务端平台，用于管理数据中心、互联通道、加速卡资源批次、租户预约、容量分配、交付情景与硬件稳定性准入。业务状态、幂等结果和审计事件保存在 SQLite 中，适合调度、质量、风险和审计人员在单个 Linux 应用容器内协作。

## 目录

- `src/compute_fabric/`：站点、通道、资源库存、预约、容量分配和情景分析；
- `src/accelerator_lab/`：加速卡测点导入、排除复核、分析任务租约和准入决定；
- `src/silicon_qualification/`：AI 加速芯片批次、测量、分析与质量审批；
- `src/data_pipeline/`：大规模清洗/切分/特征统计任务的分片断点恢复流水线：
  输入清单摘要、处理规则版本、分片依赖与输出校验值持久化，工作进程以有期限
  租约领取分片（fencing 令牌防止迟到结果覆盖新持有者），完成与下游解锁原子
  提交，失败按可配置退避策略重试后进入人工处置，取消阻止新领取但保留已完成
  证据，重启后可重建待处理/运行中/失败/完成四态并解释每个分片的来源；
- `fixtures/`：离线验收使用的结构化协议与测点；
- `tests/`：核心规则、错误边界、事务、API 和命令行验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m compute_fabric.acceptance --workspace .
PYTHONPATH=src python3 -m accelerator_lab.acceptance --workspace .
PYTHONPATH=src python3 -m silicon_qualification.acceptance
PYTHONPATH=src python3 -m data_pipeline.acceptance --workspace .
```

这些命令使用临时 SQLite 数据库完成站点、资源、预约、分配、测点分析和芯片准入流程，不访问外部网络。

`data_pipeline` 验收额外演示：分片依赖原子解锁、租约过期后被其他进程接管、
旧持有者迟到结果被 fencing 拒绝、退避重试耗尽进入人工处置后重新排队、取消后
保留已完成输出证据，以及关闭进程后用新连接重建全部状态和分片来源解释。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m compute_fabric.api --database compute.sqlite3 --host 127.0.0.1 --port 8080
PYTHONPATH=src python3 -m accelerator_lab.api --database lab.sqlite3 --host 127.0.0.1 --port 8081
PYTHONPATH=src python3 -m silicon_qualification.api --database silicon.sqlite3 --host 127.0.0.1 --port 8082
PYTHONPATH=src python3 -m data_pipeline.api --database pipeline.sqlite3 --host 127.0.0.1 --port 8083
```

服务均提供 `GET /health`，其余接口使用 JSON。进程重启后可以继续查询 SQLite 中的业务状态和审计历史。

`data_pipeline` 的主要接口：

- `POST /jobs`：建任务（规则 `rule_id/version/definition`、`shards`（含 `input` 与 `depends_on`）、`manifest`、可配置 `retry` 策略）；
- `POST /shards/claim`、`POST /shards/renew`：领取/续租（带租约秒数，返回 `fence` 与冻结的输入、规则、清单摘要）；
- `POST /jobs/{id}/shards/{key}/complete`：完成（回报 `fence`、输出 `sha256`、输入与规则摘要，原子解锁下游）；
- `POST /jobs/{id}/shards/{key}/fail`：失败（按任务重试策略退避，耗尽转人工）；
- `POST /jobs/{id}/shards/{key}/requeue|discard`：人工处置（重新排队或放弃并级联取消下游）；
- `POST /jobs/{id}/cancel`、`POST /sweep`：取消任务与重启后收束过期租约；
- `GET /jobs/{id}/status?state=pending|running|failed|done|cancelled` 与
  `GET /jobs/{id}/shards/{key}/detail`：重建四态计数与每个分片的来源解释和尝试史。
