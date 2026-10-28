# Prometheus 指标与告警

Gateway 提供运行状态、排队、Agent 执行、模型调用与 Token、工具执行和渠道发送指标。指标默认关闭，独立的 `nanobot serve` 暂不接入。每个指标用于检测具体问题，或帮助定位已触发告警的原因；示例规则优先关注用户执行失败、等待过久和发送处理问题。

## 启用与采集

将以下字段合并到现有配置，重启 Gateway 后生效：

```json
{
  "gateway": {
    "metricsEnabled": true
  }
}
```

`GET /metrics` 使用 `/health` 所在的 `gateway.host` 和 `gateway.port`，默认地址为 `http://127.0.0.1:18790/metrics`。关闭时返回 404；启用后，只要采集成功就返回 200，即使 `/health` 返回 503。Prometheus 的 `up` 表示采集是否成功，`nanobot_gateway_ready` 完全复用健康检查的就绪判定。目前该判定检查必需的 WebSocket 渠道是否运行，其他渠道连接和模型调用需要分别观察。

```yaml
scrape_configs:
  - job_name: nanobot
    scrape_interval: 30s
    metrics_path: /metrics
    static_configs:
      - targets: ["127.0.0.1:18790"]
```

多实例分别配置 target，并确保 Prometheus 可以访问相应地址。`job`、`instance` 和 `up` 由 Prometheus 添加。每个 Gateway 使用独立 registry，Counter 和 Histogram 从本次启动开始累计，不从历史数据库恢复；查询使用 `rate` 和 `increase` 处理重启后的计数重置。

客户端根据请求的 `Accept` 协商 Prometheus 文本或 OpenMetrics，并生成匹配的 `Content-Type`。应用不附加业务样本时间戳。采集同步读取内存状态，不访问模型、渠道接口或历史数据库；关闭指标时不创建 registry，也不启动采集任务。

## 告警策略

[示例规则](../examples/prometheus/alerts.yml)包含 9 项告警和一个公共 recording rule。它们需要显式加载到 Prometheus；阈值是可调整的起点，`severity` 也需要结合实际值班流程设置。示例采集任务名为 `nanobot`，改名时同步修改规则中的 `job` 选择器。

| 告警 | 示例触发条件 | 恢复条件 | 排查动作 |
|---|---|---|---|
| `NanobotScrapeUnavailable` | `up=0` 持续 2 分钟 | 采集成功 | 检查 target 的采集错误、Gateway 进程、健康端口和网络路径 |
| `NanobotGatewayNotReady` | 健康就绪为 0 持续 2 分钟 | 就绪恢复为 1 | 查看 `/health` 和必需的 WebSocket 渠道启动错误 |
| `NanobotChannelStopped` | 适配器报告未运行，持续 2 分钟 | 重新报告运行 | 检查渠道配置及运行日志 |
| `NanobotChannelDisconnected` | 渠道任务运行，但传输连接断开持续 2 分钟 | 连接恢复 | 检查适配器重连日志和对端服务 |
| `NanobotAgentErrors` | 15 分钟内至少 3 次用户主执行错误 | 窗口内错误数降至阈值以下 | 从执行记录查找错误，再用 Provider 和工具指标定位 |
| `NanobotAgentRunOverdue` | 最老用户主执行超过 10 分钟，并持续观察到 1 分钟 | 对应执行结束，或剩余执行均未超时 | 检查活动执行、模型和工具耗时；允许长任务时调整阈值 |
| `NanobotQueueWaitTooLong` | 队首条目等待超过 2 分钟，并持续观察到 1 分钟 | 队列清空，或新的队首条目未超时 | 结合队列长度和执行数量定位积压阶段 |
| `NanobotDeliveryOverdue` | 最老已调度发送任务超过 2 分钟，并持续观察到 1 分钟 | 发送任务结束，或剩余任务均未超时 | 检查同一目标的前序任务、并发额度和适配器耗时 |
| `NanobotDeliveryProblem` | 15 分钟内至少一次普通消息处理结果为 `failed` 或 `unknown` | 相关结果移出窗口 | 查看 outbox 和适配器日志；`unknown` 需先核实结果再决定补发 |

示例采用错误次数作为低流量用户执行的触发条件，避免一次失败就产生 100% 错误率告警。Provider 尝试失败后重试成功、工具返回可恢复错误，都可能伴随 Agent 最终成功，因此这两层指标用于定位，不单独触发示例告警。`cancelled` 和 `limit_reached` 独立保留供排查。首期不设置未经工作负载验证的 P95、Token 预算、费用或缓存比例告警。

所有业务告警都要求 `up=1` 且 admission 未暂停；采集失败时改由 `NanobotScrapeUnavailable` 表示观测中断，维护期间抑制业务告警。维护结束后，Gauge 告警重新计算持续时间；Counter 窗口仍可能包含维护前的事件。告警表达式消失会结束该项告警，但只有新采集的健康状态才能证明对应功能恢复。若 target 从 Prometheus 配置中被移除，`up` 序列也会消失，需要由部署清单或服务发现监控负责发现 target 缺失。

渠道移除后停止导出它的状态，示例规则也随之结束对应状态告警。不支持连接状态观测的适配器不导出 `nanobot_channel_connected`；缺失该序列不能解释为已连接或已断开。NapCat 的连接指标观察 OneBot WebSocket 是否处于 OPEN 状态，QQ 账号在线状态应由能查询该状态的监控单独确认。

将规则文件复制到 Prometheus 的配置目录，并合并 `rule_files`：

```yaml
rule_files:
  - alerts.yml
```

在仓库根目录校验规则并运行行为测试：

```sh
promtool check rules examples/prometheus/alerts.yml
promtool test rules tests/metrics/alerts.test.yml
```

通知发送、Alertmanager 路由和告警阈值的生产配置由部署环境负责。规则遵循 [Prometheus 告警实践](https://prometheus.io/docs/practices/alerting/)中关注症状并提供排查依据的原则。

## 指标契约与用途

| 指标 | 类型 | 标签 | 统计口径与用途 |
|---|---|---|---|
| `nanobot_build_info` | Gauge，恒为 1 | `version` | 运行版本；排查发布后出现的错误或耗时变化 |
| `nanobot_gateway_ready` | Gauge，0/1 | 无 | `/health` 的就绪结果；触发未就绪告警 |
| `nanobot_gateway_maintenance` | Gauge，0/1 | 无 | admission 是否暂停；抑制维护期间的业务告警 |
| `nanobot_channel_running` | Gauge，0/1 | `channel` | 适配器报告的运行状态；检测渠道停止 |
| `nanobot_channel_connected` | Gauge，0/1 | `channel` | 支持观测的适配器报告的传输连接状态；检测运行中断连 |
| `nanobot_inbound_messages_total` | Counter | `channel` | admission 通过并进入 MessageBus 的消息数；结合队列等待告警判断入站流量变化 |
| `nanobot_queue_depth` | Gauge | `queue` | 各实际队列的条目数；判断等待过久时的积压规模 |
| `nanobot_queue_oldest_enqueued_timestamp_seconds` | Gauge | `queue` | 最老条目的入队 Unix 时间，空队列为 0；检测队列等待过久 |
| `nanobot_outbound_deliveries_pending` | Gauge | 无 | 已调度但尚未结束的发送任务数，含等待和执行；判断发送积压规模 |
| `nanobot_agent_runs_total` | Counter | `agent_type,source,outcome` | 已结束的 AgentRunner 执行次数；检测用户主执行反复失败，并区分取消与迭代上限 |
| `nanobot_agent_run_duration_seconds` | Histogram | `agent_type,source` | Runner 准备、执行和清理结束的耗时；排查长执行及性能退化 |
| `nanobot_agent_runs_in_progress` | Gauge | `agent_type,source` | 当前在 Runner 中执行的数量；判断长执行告警是否涉及并发积压 |
| `nanobot_agent_oldest_run_start_time_seconds` | Gauge | `agent_type,source` | 最老活动执行的开始 Unix 时间，空闲为 0；检测尚未结束的长执行 |
| `nanobot_llm_requests_total` | Counter | `provider,model,source,outcome` | 已结束的 nanobot Provider 调用尝试；定位执行错误是否伴随模型失败或重试 |
| `nanobot_llm_request_duration_seconds` | Histogram | `provider,model` | 每次调用尝试的耗时，流式计至结束；定位长执行中的模型耗时 |
| `nanobot_llm_tokens_total` | Counter | `provider,model,usage_source` | 每次调用的规范化总 Token；供明确的消耗预算策略使用 |
| `nanobot_llm_input_tokens_total` | Counter | `provider,model,usage_source` | 输入 Token，包含缓存读取和写入；区分预算消耗中的输入部分 |
| `nanobot_llm_output_tokens_total` | Counter | `provider,model,usage_source` | 输出 Token；区分预算消耗中的输出部分 |
| `nanobot_tool_calls_total` | Counter | `tool,outcome` | 本地和 MCP 工具处理结果，包含参数拒绝；定位执行错误与工具错误的关系 |
| `nanobot_tool_call_duration_seconds` | Histogram | `tool` | 单次工具处理耗时，包含校验；定位长执行中的工具耗时 |
| `nanobot_channel_send_attempts_total` | Counter | `channel,kind,outcome` | 单次适配器操作结果，每次重试分别记录；定位发送问题中的尝试与重试 |
| `nanobot_channel_send_duration_seconds` | Histogram | `channel,kind` | 单次适配器操作耗时，不含排队和重试等待；区分发送与调度等待 |
| `nanobot_channel_delivery_results_total` | Counter | `channel,kind,outcome` | 重试结束或 outbox 分类后的发送处理结果；检测失败和结果未知，避免成功重试误报 |
| `nanobot_channel_oldest_pending_delivery_start_time_seconds` | Gauge | `channel` | 最老已调度发送任务的开始 Unix 时间，空闲为 0；检测含目标串行和并发额度等待的长发送 |

独立 registry 还注册官方 Python 版本和进程 collectors，用于排查运行环境、CPU 和内存资源问题。进程指标按客户端在当前操作系统上的支持导出，缺失项目不补零。首期不导出通用 GC、缓存 Token、usage 覆盖率和首输出时延指标。

### 标签和边界

- `agent_type`：`main`、`subagent`。Runner 之外的会话持久化、队列等待和渠道发送单独观测。
- `source`：沿用 `user/api/cron/dream/system`。
- Agent `outcome`：`success/error/cancelled/limit_reached`。正常完成和工具主动结束响应为成功；空最终响应、现有错误结果及异常为错误；`max_iterations` 为达到上限。
- Provider 和工具 `outcome`：`success/error/cancelled`。工具错误依据结构化结果，不解析错误文本；未注册工具的拒绝使用 `tool="unregistered"`，其他工具使用注册名称。
- `usage_source`：沿用 `LLMUsage.source` 的 `reported/estimated/mixed`。直接累计每次 `LLMCallRecord` 的 usage，不重新估算，也不从整轮汇总反拆。缺失 usage 不表示零消耗；估算量不等同供应商账单。供应商总量可能包含输入、输出以外的 Token，因此保留原总量。
- `queue`：`bus_inbound/session_inbox/bus_outbound/outbound_buffer`。会话收件箱长度为所有实际会话队列之和，最老时间取各队列最老条目的最小值；`outbound_buffer` 是流式合并时暂存的条目。时间表示进入该阶段的时间，不累计其他阶段等待。
- `channel`：渠道运行实例名，区分同一渠道的多个配置实例。Counter 保留本次进程已观察的历史，移除渠道后不继续导出它的运行、连接和最老待发送状态。
- 发送 `kind`：`message/stream/reasoning/event`。完成占位事件没有适配器操作，不计入发送指标。示例结果告警只选择 `kind="message"`；流式和其他事件可按渠道的实际发送语义制定策略。
- 发送尝试 `outcome`：`returned/error/cancelled`，分别表示适配器正常返回、抛异常和调用取消。
- 发送处理结果 `outcome`：`returned` 表示最终适配器正常返回；`failed` 表示发送前拒绝或不可发送；`unknown` 表示可能已提交但结果无法确认；`deferred` 表示 outbox 因渠道未就绪保留待重试；`cancelled` 表示尚未开始操作的等待阶段取消。一次 outbox 延后后仍可能再次处理，因此它记录的是处理结果，不是唯一消息数。操作中取消也可能属于 `unknown`。

Provider 次数包含 nanobot 层重试和模型回退尝试，不代表 SDK 内部 HTTP 请求次数。`provider/model` 表示实际选择的 Provider 和请求模型，不推测代理背后的真实上游。工具直接发起的 NapCat 操作归入工具指标，供应商托管工具归入模型调用；它们不计入 ChannelManager 发送次数。

`running` 表示适配器报告运行，`connected` 表示观察到传输连接，`returned` 表示适配器正常返回，均不能证明用户已经收到消息。已调度发送时间只覆盖当前内存任务，不扫描等待恢复的历史 outbox；渠道未就绪时还应结合连接告警和 outbox 状态检查。队列等待和 Runner 耗时同样各有边界，群批处理、steer 和流式发送存在多对多关系，首期不提供单条平台消息端到端时延或投递成功率。

已知 Agent 结果和已注册渠道发送结果 Counter 在第一次处理前导出 0，配置的主 Provider 请求 Counter 同样初始化为 0；再次注册不重置已有值。其他诊断指标在观察到操作后出现。Prometheus 必须先采到基线才能可靠观察后续增量，启动到首次采集之间发生的事件仍可能缺少基线；采集间隙和重启期间的事件也无法完整恢复。Counter 告警是已采样事件的窗口统计，需结合执行记录排查。

标签不包含会话、用户、群号、请求 ID、内容、路径、URL 或错误文本。Histogram 不附加 outcome，模型 Histogram 不附加 source，以控制时序数量。

### Histogram 分桶

各分桶单位为秒，客户端自动追加 `+Inf`。分位数在 Prometheus 查询时计算，不在应用内计算 P95 或累计平均值。

| Histogram | 分桶上界 |
|---|---|
| Agent 执行 | `0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1200, 3600` |
| Provider、工具 | `0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600` |
| 渠道发送 | `0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120` |

## 排查查询

查看各队列最老条目的等待秒数，使用非空时间戳排除空队列：

```promql
(time() - nanobot_queue_oldest_enqueued_timestamp_seconds{job="nanobot"})
and (nanobot_queue_oldest_enqueued_timestamp_seconds{job="nanobot"} > 0)
```

Provider 调用速率和错误比例用于定位执行失败，比例排除取消：

```promql
sum by (job, instance, provider, model) (rate(nanobot_llm_requests_total{job="nanobot"}[5m]))
```

```promql
(
  sum by (job, instance, provider, model) (rate(nanobot_llm_requests_total{job="nanobot",outcome="error"}[5m]))
  or
  0 * sum by (job, instance, provider, model) (rate(nanobot_llm_requests_total{job="nanobot",outcome=~"success|error"}[5m]))
)
/
sum by (job, instance, provider, model) (rate(nanobot_llm_requests_total{job="nanobot",outcome=~"success|error"}[5m]))
```

Provider 已完成调用的 P95 耗时：

```promql
histogram_quantile(0.95,
  sum by (job, instance, le, provider, model) (
    rate(nanobot_llm_request_duration_seconds_bucket{job="nanobot"}[5m])
  )
)
```

按来源查看一小时 Token 消耗，预算策略应根据明确的容量或额度决定阈值，并分别对待上报量和估算量：

```promql
sum by (job, instance, provider, model, usage_source) (
  increase(nanobot_llm_tokens_total{job="nanobot"}[1h])
)
```

输入、输出预算可以分别查询 `nanobot_llm_input_tokens_total` 和 `nanobot_llm_output_tokens_total`。空闲窗口、缺失观测或零分母不应解释为零错误率或零延迟；查询窗口应容纳足够的采集样本。逐次模型调用仍可通过 [Langfuse](./configuration.md#langfuse-observability) 排查。

指标名称、单位和 Counter 语义遵循 [Prometheus 命名规范](https://prometheus.io/docs/practices/naming/)和[埋点实践](https://prometheus.io/docs/practices/instrumentation/)，classic Histogram 使用[官方 Python 客户端](https://prometheus.github.io/client_python/instrumenting/histogram/)。
