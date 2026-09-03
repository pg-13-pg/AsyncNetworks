# Proactor 与网关代理测试总结

## 1. 测试对象与最佳结果

| 程序 | 测试地址 | 测试参数 | 吞吐 | 平均延迟 | P99 |
|---|---|---|---:|---:|---:|
| `proactor_test` | `10.0.0.171:6666/` | `-t8 -c4096 -d60s` | `900,577 req/s` | `4.13ms` | `7.20ms` |
| `gateway_mock_upstream` | `10.0.0.171:9001/bytes/4096` | `-t8 -c4096 -d60s` | `540,790 req/s` | 约 `7.45ms` | `10.83ms` |
| `ucp_gateway` | `10.0.0.171:8080/api/bytes/4096` | `-t8 -c4096 -d60s` | `97,452--97,586 req/s` | `41.9ms` | `44.12ms` |

网关最佳测试的传输速率约为 `390MB/s`，即约 `3.1Gbit/s`，低于 `12Gbit/s` 内网带宽上限。

## 2. 参数配置

### proactor_test

配置文件为 `config/ucp.conf`：

```ini
[server]
ip = 10.0.0.171
port = 6666
thread_num = 8
read_timeout_ms = 5000

[event_loop]
ring_entries = 32768
sqpoll = true
sqpoll_idle_ms = 50
registered_buffers_count = 16384
registered_buffer_size = 4096
pending_queue_capacity = 65536
```

### gateway_mock_upstream

直接 upstream 测试使用 8 个 worker，关键 EventLoop 参数为：

```ini
workers = 8
ring_entries = 32768
sqpoll = true
sqpoll_idle_ms = 50
registered_buffers_count = 16384
registered_buffer_size = 4096
pending_queue_capacity = 65536
pending_submission_capacity = 4096
```

### ucp_gateway

网关配置文件为 `config/gateway.conf`：

```ini
[gateway]
listen_ip = 10.0.0.171
listen_port = 8080
workers = 8

[event_loop]
ring_entries = 32768
sqpoll = false
sqpoll_idle_ms = 50
registered_buffers_count = 0
registered_buffer_size = 4096
pending_queue_capacity = 131072
pending_submission_capacity = 8192

[route.api]
prefix = /api/
upstreams = 127.0.0.1:9001,127.0.0.1:9002
connect_timeout_ms = 500
response_timeout_ms = 3000
max_connections_per_worker = 512
max_idle_per_worker = 128
idle_timeout_ms = 30000
```

两个模拟 upstream 分别使用：

```text
config/mock_upstream_9001.conf  -> 127.0.0.1:9001, workers=1
config/mock_upstream_9002.conf  -> 127.0.0.1:9002, workers=1
```

两个 upstream 的 `sqpoll` 也设置为 `false`，以避免和网关争抢 CPU。

所有服务和压测客户端在启动前设置：

```bash
ulimit -n 1048576
```

## 3. SQPOLL 对比

网关使用 8 个 worker、两个单 worker upstream、`c4096` 时的结果：

| 配置 | 吞吐 | 平均延迟 | P99 | CPU 特征 |
|---|---:|---:|---:|---|
| `sqpoll=true` | `30,992 req/s` | `129.92ms` | `392.96ms` | `%sys` 约 `85%`，SQPOLL 线程占用明显 |
| `sqpoll=false` | `97,452--97,586 req/s` | `41.9ms` | `44.12ms` | `%sys` 约 `27%`，性能明显更好 |

当前服务器和代理场景应使用：

```ini
sqpoll = false
```

SQPOLL 会创建内核轮询线程持续检查提交队列。在本测试中，网关、两个 upstream 和 SQPOLL 线程共同竞争 8 个 CPU 核，导致系统态 CPU 和网络软中断开销过高。关闭 SQPOLL 后，线程等待 I/O 完成，CPU 竞争下降，吞吐和延迟反而改善。

## 4. 结果与瓶颈分析

### proactor_test

链路只有：

```text
客户端 -> proactor_test
```

它主要反映单个异步服务端和 EventLoop 的基础收发能力，不包含网关代理、HTTP 转发和上下游连接管理开销，因此吞吐最高、延迟最低，不代表真实网关业务性能。

### gateway_mock_upstream

链路为：

```text
客户端 -> gateway_mock_upstream
```

该测试直接访问模拟上游，省去了网关代理会话和双连接数据搬运，所以性能明显高于完整网关代理。

### ucp_gateway

当前完整链路为：

```text
客户端 -> 网关 8080 -> 127.0.0.1:9001/9002 -> 网关 -> 客户端
```

一次请求涉及两条 TCP 连接、HTTP 解析、socket 缓冲区复制、上下游状态机和多次异步事件处理。应用采用异步 I/O 只避免了用户线程阻塞等待，并不能消除 Linux 内核对每个 TCP 数据包的处理。

监控中观察到：

```text
%soft 约 59%
%idle 接近 0%
lo rx/tx 约 419MB/s
```

这说明大量 CPU 时间消耗在网络 softirq 和本机 loopback TCP 流量上，而不是磁盘或物理网卡带宽上。`iostat` 中 NVMe `%util` 约 `0.1%`，可以排除磁盘瓶颈。

网关日志曾确认最佳测试无错误：

```text
status_2xx = 5,862,408
status_4xx = 0
status_5xx = 0
connect_errors = 0
timeouts = 0
queue_rejections = 0
pool_reuses = 5,857,962
```

因此当前网关的限制主要是同机双向代理带来的 CPU、TCP 协议栈、softirq 和 loopback 开销，而不是连接池故障、队列溢出或带宽耗尽。

## 5. 测试结论

1. `proactor_test` 适合测量基础异步 I/O 性能，最佳结果约 `900k req/s`。
2. `gateway_mock_upstream` 适合测量模拟上游直接响应性能，最佳结果约 `540k req/s`。
3. `ucp_gateway` 应使用 `sqpoll=false`；当前最佳结果约 `97.5k req/s`，P99 约 `44ms`。
4. `c2048` 更适合作为稳定工作点，`c4096` 可用于观察过载后的延迟变化。
5. 如果要测试网关本身的能力，应将 upstream 放到另一台服务器；同机测试则反映网关、upstream 和 loopback 转发的综合能力。
