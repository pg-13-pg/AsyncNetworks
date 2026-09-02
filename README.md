# UCP Proxy Core

UCP 是一个面向学习与秋招展示的 Linux 高并发网络项目。当前主线交付物不是完整
API 网关，而是一个建立在 `io_uring`、C++20 协程和多 EventLoop 之上的
HTTP/1.1 反向代理数据面核心。项目重点是可解释的 I/O 生命周期、连接归属、流式
背压、上游连接复用、错误传播和可复现验证，而不是鉴权、管理后台等外围功能。

仓库中原有的 TCP 示例和 legacy I/O API 仍可构建；代理使用 `include/ucp` 下的
类型化安全路径。

## 核心能力

- 每个已接受的 I/O 由 `IoOperation` 独立描述，并由所属 `EventLoop` 持有到所有
  相关 CQE 排空；超时、取消和 I/O 完成竞态只对外完成一次。
- 每个 worker 持有独立的 `io_uring`、路由选择器、指标分片和上游连接池。
  下游连接、`ProxySession` 与上游连接固定在同一 worker，不跨线程共享热路径状态。
- `asyncReadSome`、`asyncWriteSome`、`asyncWriteAll` 和 `asyncConnect` 返回类型化
  结果，显式区分 EOF、超时、取消、连接重置和资源耗尽。
- 请求与响应正文按固定大小分块转发。当前块写完后才读取下一块，将慢消费者的
  压力传回生产端，限制单会话内存增长。
- 静态路由采用最长前缀匹配，上游选择为 worker-local round robin；连接池通过
  move-only RAII lease 决定复用或丢弃连接。
- 普通跨线程任务队列有容量限制，生命周期控制有独立入口；关闭时按停止接收、
  会话 drain、取消未完成 I/O、消费 CQE、释放 ring 资源的顺序执行。

## 架构

```text
Client
  |
  v
TcpServer -> downstream TcpConnection
                    |
                    v
              ProxySession
              /    |     \
     HttpFramer  RouteTable  GatewayMetricShard
                    |
                    v
       worker-local UpstreamPool
                    |
                    v
           AsyncConnect / TcpConnection
                    |
                    v
                 Upstream
```

主 EventLoop 负责 accept 和进程级生命周期；连接由 `TcpServer` 分派到 worker。
业务协程只操作本 worker 的连接和状态，因此热路径不需要全局连接池锁。跨 worker
指标只在快照阶段聚合。

## HTTP 边界

当前支持：

- HTTP/1.1 `GET` 和 `POST`；
- 显式 `Content-Length`；
- 下游与上游 keep-alive；
- 每条下游连接同一时刻一个请求；
- 最大 16 KiB 请求头和最大 1 MiB 请求体，均可通过配置收紧；
- hop-by-hop `Connection` 头规范化；
- 静态最长前缀路由和无重试的上游选择。

当前会拒绝或不实现：

- HTTP/1.0、`PUT` 等非 GET/POST 方法和 `CONNECT`；
- chunked transfer encoding、Upgrade、WebSocket 和并发流水线请求；
- TLS、HTTP/2、HTTP/3；
- 路径重写、服务发现、动态配置、自动重试和主动健康检查；
- 鉴权、限流、熔断、管理 API 和完整的生产级 HTTP 兼容性。

这些限制是当前学习项目的明确边界。尤其是请求一旦部分发送到上游，自动重试可能
重复执行 POST，因此当前实现选择返回错误而不是隐式重放。

## 环境

- Linux，内核需要支持仓库所用的 `io_uring` 操作；
- CMake 3.16 或更高版本；
- 支持 C++20 协程的 GCC 或 Clang；
- `liburing`、`fmt` 和 pthread；
- 可选：`wrk`，用于 direct-vs-proxy 基准。

Ubuntu/Debian 可安装基础依赖：

```bash
sudo apt install cmake g++ liburing-dev libfmt-dev wrk
```

## 构建与测试

Debug 构建和完整 CTest：

```bash
cmake -S . -B build-gateway \
  -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTING=ON
cmake --build build-gateway -j2
ctest --test-dir build-gateway --output-on-failure
```

ASan/UBSan：

```bash
cmake -S . -B build-gateway-asan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTING=ON \
  -DUCP_ENABLE_SANITIZERS=ON
cmake --build build-gateway-asan -j2
ctest --test-dir build-gateway-asan --output-on-failure
```

TSan 必须单独构建：

```bash
cmake -S . -B build-gateway-tsan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTING=ON \
  -DUCP_ENABLE_TSAN=ON
cmake --build build-gateway-tsan -j2
ctest --test-dir build-gateway-tsan --output-on-failure
```

当前 WSL2 环境的 GCC TSan 会随机因 shadow-memory mapping 冲突而无法启动进程，
不能把未执行的用例表述为通过。完整观察记录见
[TSan notes](docs/tsan-notes.md)。

## 启动代理

先启动两个确定性 benchmark upstream：

```bash
build-gateway/bin/gateway_mock_upstream --port 9001
build-gateway/bin/gateway_mock_upstream --port 9002
```

再启动网关：

```bash
build-gateway/bin/ucp_gateway config/gateway.conf
```

默认监听 `127.0.0.1:8080`，`/api/` 路由轮询访问
`127.0.0.1:9001` 与 `127.0.0.1:9002`。进程通过 SIGINT/SIGTERM 进入优雅关闭。

连通性检查：

```bash
curl --fail --output /dev/null http://127.0.0.1:9001/bytes/1024
curl --fail --output /dev/null http://127.0.0.1:8080/api/bytes/1024
```

## 性能验证

固定矩阵会对 1 KiB、4 KiB、16 KiB 响应分别执行 direct 和 proxy 的 warm-up，
随后在 64、256、1024 连接下运行 30 秒。脚本保存原始 stdout/stderr，并在 wrk
报告 socket 或 HTTP 错误时返回非零状态。

```bash
bench/run_gateway_bench.sh \
  --gateway http://127.0.0.1:8080 \
  --direct http://127.0.0.1:9001 \
  --output benchmark-results \
  --threads "$(nproc)"
```

30 分钟 soak 增加 `--soak`。仓库不预填吞吐数字；环境、完整命令、结果表与解释
规则见 [benchmark report](docs/gateway-benchmark.md)。只有无 socket/HTTP 错误的
同环境 direct/proxy 数据才适合比较。

## 文档入口

- [架构与边界设计](docs/superpowers/specs/2026-08-31-ucp-proxy-core-design.md)
- [逐任务实现计划](docs/superpowers/plans/2026-08-31-ucp-proxy-core.md)
- [TSan 实测与限制](docs/tsan-notes.md)
- [基准命令与结果模板](docs/gateway-benchmark.md)

## 已知限制与后续方向

当前代码仍应在原生 Linux 上完成全套 TSan，并通过真实硬件的 long-running soak
建立性能基线。后续优先级应保持在故障注入、资源上限、观测导出和协议正确性，
而不是在缺少证据时扩展功能或宣称峰值吞吐。
