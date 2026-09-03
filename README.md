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

## 环境与原生构建

- Linux，内核需要支持仓库所用的 `io_uring` 操作；
- CMake 3.16 或更高版本；
- 支持 C++20 协程的 GCC 或 Clang；
- `liburing`、`fmt` 和 pthread；
- 可选：`wrk`，用于 direct-vs-proxy 基准。

在 Ubuntu 上进入仓库目录后，运行安装脚本：

```bash
sudo scripts/install_dependencies.sh
```

仅构建和运行测试时，可使用精简安装：

```bash
sudo scripts/install_dependencies.sh --build-only
```

先克隆仓库并进入项目目录：

```bash
git clone <repository-url>
cd uring_coroutine_proactor
```

### Debug 构建与单元测试

Debug 构建和完整 CTest：

```bash
cmake -S . -B build-gateway \
  -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTING=ON
cmake --build build-gateway -j2
ctest --test-dir build-gateway --output-on-failure
```

### Release 构建

性能测试应使用 Release 构建，而不是 Debug 或 sanitizer 构建：

```bash
cmake -S . -B build-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF
cmake --build build-release --target ucp_gateway gateway_mock_upstream -j"$(nproc)"
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

## 本地启动与验证

`gateway_mock_upstream` 是仓库自带的确定性 HTTP 上游模拟器，支持
`/bytes/1024`、`/bytes/4096` 和 `/bytes/16384`。在三个终端中分别启动两个
模拟上游和网关：

```bash
build-release/bin/gateway_mock_upstream --port 9001
```

```bash
build-release/bin/gateway_mock_upstream --port 9002
```

```bash
build-release/bin/ucp_gateway config/gateway.conf
```

默认监听 `127.0.0.1:8080`，`/api/` 路由轮询访问
`127.0.0.1:9001` 与 `127.0.0.1:9002`。进程通过 SIGINT/SIGTERM 进入优雅关闭。

连通性检查：

```bash
curl --fail --output /dev/null http://127.0.0.1:9001/bytes/1024
curl --fail --output /dev/null http://127.0.0.1:8080/api/bytes/1024
```

使用 `Ctrl-C` 或向进程发送 `SIGTERM` 可触发优雅关闭。

## Ubuntu 原生部署与双服务器压测

网关服务器 A 与压测服务器 B 应位于同一云厂商的同一 VPC/私网内，避免公网链路、NAT
或公网带宽成为测试瓶颈。整个流程直接运行 Ubuntu 进程，不需要容器运行时。

```text
压测服务器 B (wrk) -> 网关服务器 A:8080 -> A 上的 mock upstream:9001/9002
```

### 1. 准备两台服务器

建议使用 Ubuntu 22.04 或 24.04。两台服务器都执行：

```bash
git clone <repository-url>
cd uring_coroutine_proactor
sudo scripts/install_dependencies.sh
uname -r
nproc
```

只构建网关时可在压测机使用 `--build-only`；压测机仍需 `wrk`、`sysstat` 和
`iproute2` 来采集结果。云安全组和主机防火墙只允许服务器 B 的私网 IP 访问服务器 A
的 `8080/tcp`，不要对外开放 mock upstream 端口。

### 2. 设置资源上限与内核队列

`ulimit -n` 只影响执行它的 shell 及其子进程。网关、两个 mock upstream 和压测工具若
在不同 SSH 会话或 tmux pane 中启动，必须各自在启动前设置所需的文件描述符上限；不能在
一个终端设置后假定其他终端会继承。

在网关服务器 A 与压测服务器 B 的实际启动终端中执行：

```bash
ulimit -Hn
ulimit -Sn 1048576
ulimit -Sn
```

网关的每个活跃代理会话通常同时持有下游和上游 socket，因此 `LimitNOFILE` 必须覆盖目标
连接数、监听 socket、日志、epoll/io_uring 内部 fd 与余量。`1048576` 是高连接数压测的
起点，不是吞吐优化项；目标并发较低时应按实际连接预算降低它。

若第一条命令显示的 hard limit 小于目标值，交互式 SSH/tmux 启动方式应先为运行用户创建
PAM limits 配置，重新登录后再执行 `ulimit`：

```text
# /etc/security/limits.d/99-ucp-benchmark.conf
<run-user> soft nofile 1048576
<run-user> hard nofile 1048576
```

若通过 systemd 启动网关，应为实际使用的服务单元创建 override，而不是依赖登录 shell：

```bash
sudo systemctl edit <your-ucp-gateway-service>.service
```

写入以下内容后重载并重启服务：

```ini
[Service]
LimitNOFILE=1048576
```

```bash
sudo systemctl daemon-reload
sudo systemctl restart <your-ucp-gateway-service>.service
gateway_pid="$(pgrep -n -x ucp_gateway)"
grep -E 'Max open files|Max locked memory' "/proc/${gateway_pid}/limits"
```

默认配置的 `registered_buffers_count = 0`，不需要提高 locked-memory 上限。只有启用注册
缓冲区时才需要为 `LimitMEMLOCK` 预留内存：最小预算为
`workers * registered_buffers_count * registered_buffer_size`，再加 io_uring 元数据余量。
例如 `4 * 4096 * 4096 = 64 MiB`，可将服务 override 中的 `LimitMEMLOCK` 设为至少 `128M`，
并通过上面的 `/proc/<pid>/limits` 检查实际生效值。

连接风暴还会受内核文件表、accept 队列和 SYN 队列限制。先记录现值：

```bash
# 服务器 A
sysctl fs.file-max net.core.somaxconn net.ipv4.tcp_max_syn_backlog

# 服务器 B
sysctl net.ipv4.ip_local_port_range
```

仅在压测确认这些队列成为瓶颈时，才在专用压测主机上使用如下起点，并将改动与结果一起
记录。服务器 A 的 `/etc/sysctl.d/99-ucp-benchmark.conf` 只设置服务端项：

```ini
fs.file-max = 2097152
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
```

服务器 B 的同名文件只设置压测端项：

```ini
net.ipv4.ip_local_port_range = 1024 65535
```

```bash
sudo sysctl --system
```

单一压测源 IP 对同一目标 IP/端口的可用临时端口数量约等于 `ip_local_port_range` 的范围，
约 6.4 万个，而不是无限。若探索超过该数量的并发连接，应增加压测机、源 IP 或目标端口，
不能仅继续提高 `wrk -c`。

### 3. 配置并启动网关服务器 A

本地配置默认只监听回环地址。双服务器测试前复制配置并将 `listen_ip` 改为服务器 A
的私网地址或 `0.0.0.0`，同时按 CPU 核数调整 `workers`：

```bash
cp config/gateway.conf config/gateway.server.conf
$EDITOR config/gateway.server.conf
```

配置中的上游可继续使用 `127.0.0.1:9001,127.0.0.1:9002`，因为 mock 服务与网关在同一
台服务器上。推荐先以 `workers=1`、CPU 一半和 CPU 全核分别测试；`ring_entries`、
`sqpoll`、注册缓冲区和 `max_connections_per_worker` 也应一次只调整一个变量。

在服务器 A 的三个终端启动两个上游和网关：

```bash
build-release/bin/gateway_mock_upstream --port 9001
build-release/bin/gateway_mock_upstream --port 9002
build-release/bin/ucp_gateway config/gateway.server.conf
```

```bash
ss -ltn '( sport = :8080 or sport = :9001 or sport = :9002 )'
curl --fail --output /dev/null http://127.0.0.1:8080/api/bytes/1024
```

### 4. 在服务器 B 逐级压测

先验证低并发，再逐级提高连接数；每一组结束后确认没有错误再继续：

```bash
export GATEWAY_IP=10.0.1.10
wrk --latency -t4 -c64 -d30s "http://${GATEWAY_IP}:8080/api/bytes/1024"
wrk --latency -t8 -c256 -d30s "http://${GATEWAY_IP}:8080/api/bytes/4096"
wrk --latency -t8 -c1024 -d60s "http://${GATEWAY_IP}:8080/api/bytes/4096"
wrk --latency -t16 -c2048 -d60s "http://${GATEWAY_IP}:8080/api/bytes/16384"
```

服务器 A 观察网关 CPU、内存、监听连接和日志；服务器 B 观察压测机 CPU、文件描述符、
临时端口和带宽：

```bash
# 服务器 A
pidstat -dur -p "$(pgrep -n -x ucp_gateway)" 1
ss -s

# 服务器 B
mpstat -P ALL 1
sar -n DEV 1
ss -s
```

只有 `wrk` 无 `Socket errors`、HTTP 状态正确、两台机器仍有资源余量且网关无协议/超时/
资源耗尽错误时，结果才可用于比较。记录 Requests/sec、平均延迟、P99、CPU/内存、网络
吞吐、完整命令和配置；错误率或 P99 持续上升通常表示已经接近极限。

### 5. direct-vs-proxy 与原生 io_uring 故障排查

`bench/run_gateway_bench.sh` 的 fixed matrix 用于在相同环境下比较 direct upstream 与
gateway，最高只测试 `1024` 个连接。它不是自动寻找网络或框架极限的工具；极限测试应在
每组无错误后，以本节的 `wrk` 命令逐级增加连接数，并记录 P99、错误率、FD、CPU、队列和
网络指标。

`gateway_mock_upstream` 默认绑定 `127.0.0.1`。如需从服务器 B 直接测试异步 HTTP
服务本身，可在服务器 A 上显式绑定私网地址；该路径不会经过网关：

```bash
build-release/bin/gateway_mock_upstream --host 10.0.0.171 --port 9001
```

直连服务也支持配置 worker EventLoop 数量。例如使用 8 个 worker：

```bash
build-release/bin/gateway_mock_upstream \
  --host 10.0.0.171 --port 9001 --workers 8
```

然后在服务器 B 上直接压测：

```bash
wrk --latency -t4 -c1024 -d60s \
  http://10.0.0.171:9001/bytes/4096
```

建议将直连服务的 `--workers` 依次设置为 `1`、`4`、`8`，每次重启服务后，
在相同的客户端线程数、连接数和持续时间下比较 RPS、P99、错误数以及服务器 CPU。
`/bytes/1024`、`/bytes/4096` 和 `/bytes/16384` 分别用于小响应、中等响应和吞吐压力测试。

这组数据代表异步 HTTP 服务直连吞吐；访问 `:8080/api/...` 才是网关代理链路。
跨机测试时仅在安全组中向服务器 B 放行对应端口，不要向公网开放。

```bash
# 0 表示内核没有全局禁用 io_uring
cat /proc/sys/kernel/io_uring_disabled
ulimit -n
ulimit -l
pgrep -a ucp_gateway
ss -ltn '( sport = :8080 or sport = :9001 or sport = :9002 )'
cat /proc/net/sockstat
```

若出现 `Operation not permitted`，先确认云主机内核和安全策略允许 io_uring；若出现连接
错误，优先检查安全组、文件描述符、临时端口、网卡带宽和网关日志，不要直接把并发数继续
提高。

## 性能验证

固定矩阵会对 1 KiB、4 KiB、16 KiB 响应分别执行 direct 和 proxy 的 warm-up，
随后在 64、256、1024 连接下运行 30 秒。脚本保存原始 stdout/stderr，并在 wrk
报告 socket 或 HTTP 错误时返回非零状态。下列 `127.0.0.1` 命令仅适用于网关和 upstream
均运行在同一主机的本地对照；两服务器部署请使用前述服务器 A 的私网 IP，并遵守 direct
upstream 对服务器 B 的可达性要求。

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
