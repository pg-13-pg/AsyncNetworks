# 高并发异步网络框架调研与 UCP 优化建议

> 调研日期：2026-09-03
> 范围：GitHub 上获得较广泛认可的高并发异步网络框架，以及 `io_uring` 官方资料。
> 注意：本文的“落地建议”是待验证的设计建议，不代表已经实现或已经获得性能收益。

## 1. 当前项目背景

UCP Proxy Core 是一个基于 Linux `io_uring`、C++20 协程和多 EventLoop 的
HTTP/1.1 反向代理数据面。当前关键结构为：

- 主 EventLoop 负责接受连接，再将连接分配给 worker EventLoop；
- 每个 worker 持有独立的 `io_uring`、路由选择器、指标分片和上游连接池；
- 下游连接、`ProxySession` 和上游连接保持 worker-local，热路径不跨线程共享；
- `IoOperation` 管理单次 I/O、超时、取消与 CQE 排空；
- 跨线程任务队列有容量限制，并有独立控制队列处理生命周期工作；
- 已有网关直连/代理对照基准和 soak 测试脚本。

这套架构已经具备正确的方向：连接亲和性和 worker-local 热路径优先于共享全局状态。
后续优化应先保证 I/O 生命周期、背压和关闭语义，再追求吞吐数字。

## 2. 调研项目

| 项目 | GitHub Stars（调研时） | 借鉴价值 |
| --- | ---: | --- |
| [Tokio](https://github.com/tokio-rs/tokio) | 32.1k | 多线程 work-stealing 调度、公平性与取消/背压语义 |
| [NGINX](https://github.com/nginx/nginx) | 31.3k | 多 worker 事件驱动、CPU 亲和、`reuseport` 与优雅关闭 |
| [libuv](https://github.com/libuv/libuv) | 26.9k | 稳定事件抽象、异步网络和受限线程池 |
| [muduo](https://github.com/chenshuo/muduo) | 16.2k | C++ one-loop-per-thread Reactor 实践 |
| [libevent](https://github.com/libevent/libevent) | 11.9k | 后端能力抽象、测试与安全工程实践 |
| [Cloudflare quiche](https://github.com/cloudflare/quiche) | 11.8k | 协议状态、I/O 和定时器解耦；流控与超时配置 |
| [Seastar](https://github.com/scylladb/seastar) | 9.3k | 每核独立 shard、NUMA 和 share-nothing 架构 |
| [liburing](https://github.com/axboe/liburing) | 3.8k | `io_uring` 官方接口、回归测试与网络优化资料 |
| [tokio-uring](https://github.com/tokio-rs/tokio-uring) | 1.5k | `io_uring` 下 buffer 所有权与内核版本约束 |

Stars 为调研当天 GitHub 页面所示近似值，会随时间变化；它们仅反映社区关注度，不能单独证明设计适合本项目。

## 3. 关键技术与启示

### 3.1 Seastar：每核独立、无共享热路径

[Seastar 文档](https://docs.seastar.io/master/group__smp-module.html) 描述了每逻辑核独立
event loop、分配器和状态，并以显式消息传递替代跨核锁竞争。其教程也强调单线程
per-core 协作式任务调度。

对 UCP 的启示：

- 保持一个 worker 只由所属线程驱动，并继续让连接、会话、上游连接池和缓冲资源
  worker-local；
- CPU 亲和性和 NUMA 策略应优先于跨 worker 迁移已建立连接；
- 跨 worker 只传递低频控制消息或新连接，不共享上游连接池；
- 后续可为后台工作和 I/O 增加配额，避免单一任务占用事件循环。

### 3.2 liburing：批量化、multishot 与 provided buffers

[io_uring and networking in 2023](https://github.com/axboe/liburing/wiki/io-uring-and-networking-in-2023)
给出以下网络优化方向：

- 以 `io_uring_submit_and_wait` 合并提交和等待，减少不必要的系统调用；
- `IORING_ACCEPT_MULTISHOT`（Linux 5.19+）减少 accept 的重复 re-arm；
- `IORING_RECV_MULTISHOT`（Linux 6.0+）配合 provided buffers 降低高频接收的提交开销；
- buffer ring（Linux 5.19+）让内核按需选择接收缓冲区，避免对大量空闲连接预分配；
- 正确处理 `IORING_CQE_F_MORE`、`IORING_CQE_F_SOCK_NONEMPTY` 与缓冲耗尽 `-ENOBUFS`；
- 每线程独占 ring；具备条件时用 `IORING_SETUP_COOP_TASKRUN`、
  `IORING_SETUP_DEFER_TASKRUN` 和 `IORING_SETUP_SINGLE_ISSUER` 控制任务运行与 IPI。

[liburing README](https://github.com/axboe/liburing) 还提醒：大 ring 和注册缓冲区会消耗
locked memory，默认 `RLIMIT_MEMLOCK` 很可能不足。因此注册缓冲区数量必须按
`worker_count * buffer_count * buffer_size` 计算，而不是只为提高并发盲目增大。

### 3.3 Tokio：调度公平性与有界队列

[Tokio runtime 文档](https://docs.rs/tokio/latest/tokio/runtime/) 的多线程调度器使用
worker-local 队列、全局注入队列、批量窃取和定期 I/O 检查。其核心价值不只是窃取，
而是避免单个持续就绪任务导致 I/O、定时器或其他连接饥饿。

对 UCP 的启示：

- 继续优先执行 worker-local 就绪工作；
- 为单轮 CQE/回调处理设定预算，在预算耗尽时重新检查 I/O、定时器和控制队列；
- 保留有界跨线程队列及其背压指标；
- 当前阶段不建议为网络会话引入 work-stealing，因为会破坏缓存/连接亲和性，并显著
  增加生命周期和取消复杂度。

### 3.4 NGINX：每核 worker、监听分发与运维边界

[NGINX 核心配置文档](https://nginx.org/en/docs/ngx_core_module.html) 支持
`worker_processes auto`、CPU 亲和性、fd 上限和优雅关闭。
[NGINX HTTP 核心文档](https://nginx.org/en/docs/http/ngx_http_core_module.html) 描述了
`reuseport` 的独立监听 socket 和内核级连接分发。

对 UCP 的启示：

- 将 worker 数、CPU 亲和性与 `SO_REUSEPORT` 作为可配置且可测量的部署模式；
- 对连接风暴进行批量 accept，但必须保持队列满时的明确拒绝/关闭策略；
- 启动时检查并记录 `RLIMIT_NOFILE`，部署文档提供可复现的 fd 上限建议；
- 保持 stop accept -> session drain -> cancel I/O -> drain CQE -> release ring 的关闭顺序。

### 3.5 libuv、libevent 与 quiche：稳定性优先

[libuv](https://github.com/libuv/libuv) 与 [libevent](https://github.com/libevent/libevent)
体现了后端能力抽象、广泛测试和稳定接口的重要性。
[quiche](https://github.com/cloudflare/quiche) 将 socket I/O、事件循环和定时器交由宿主，
并将流控、超时等行为显式配置。

对 UCP 的启示：

- 引入 `io_uring` 优化前必须有运行时 feature probing 和可靠的旧内核回退；
- 将协议状态、I/O 生命周期与定时器职责保持分离；
- 为连接/请求限制、超时、队列水位和资源耗尽提供可观察指标；
- 示例程序、性能演示和生产路径要分开验证，不能以示例吞吐代替稳定性证据。

## 4. 面向 UCP 的实施优先级

### P0：先提高可验证性与基础效率

1. 调整 EventLoop 为“批量提交 + 批量 CQE 消费 + 等待”的循环，减少空提交、空等待和
   无效 wakeup。
2. 启动时探测内核能力和资源限制，记录内核版本、ring flags、`RLIMIT_NOFILE`、
   `RLIMIT_MEMLOCK`、worker 数、ring 深度与注册缓冲区内存预算。
3. 增加指标：SQE 暂存队列深度/拒绝数、CQ overflow、`-ENOBUFS`、取消重试、每轮 CQE
   数量、背压转换、队列延迟和请求 P50/P95/P99。
4. 扩展基准矩阵：单 worker 与多 worker、SQPOLL 开关、不同 ring 深度、不同并发、
   连接风暴和长时间 soak。

### P1：内核能力可用时的 I/O 降开销

1. 实现 multishot accept（Linux 5.19+），在 CQE 不带 `IORING_CQE_F_MORE` 或出现错误时
   安全 re-arm；保留单次 accept 回退。
2. 设计 provided-buffer/buffer-ring 接收路径（Linux 5.19+），以 RAII 管理 buffer 的
   借出、归还和会话取消，显式处理 `-ENOBUFS`。
3. 在 Linux 6.0+ 且 provided-buffer 路径稳定后评估 multishot recv；它会改变操作的
   CQE 生命周期，必须先补齐单元测试、取消测试和压力测试。
4. 在 Linux 6.1+ 经过基准确认后，尝试 `COOP_TASKRUN`、`DEFER_TASKRUN` 与
   `SINGLE_ISSUER`；若延迟、兼容性或稳定性不佳，回退到已有 ring 配置。

### P2：按收益决定的进阶项目

1. 为大响应评估 `SEND_ZC`、固定文件描述符和 vectored send；必须处理零拷贝通知 CQE，
   不能在通知前释放源缓冲区。
2. 评估每 worker 独立监听 socket 与 `SO_REUSEPORT`，避免 base loop 成为接入瓶颈。
3. 针对 CPU 密集型扩展单独建立有界计算执行器；网络 worker 只负责 I/O 和短回调。
4. 只有证明确有不均衡时，才研究任务级 work-stealing；已建立连接不应随意跨 worker
   迁移。

## 5. 不建议立即做的事情

- 不要因为“高并发”就无限增加 ring 深度、注册缓冲区或跨线程队列容量；这会将资源耗尽
  从可控拒绝变成不可控 OOM 或 locked-memory 失败。
- 不要在没有内核能力探测和回退路径的情况下把 multishot/DEFER_TASKRUN 作为唯一实现。
- 不要将连接池、会话状态或读写缓冲区改成跨 worker 全局共享；锁竞争和缓存失效通常会
  抵消理论吞吐收益。
- 不要用 Debug、sanitizer 构建或单机 loopback 的峰值数字宣称生产性能。
- 不要在请求已部分发送到上游后自动重试非幂等请求。

## 6. 本次本地基线观察

在本机执行了 Release 构建：

```bash
cmake -S . -B /tmp/ucp-baseline -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build /tmp/ucp-baseline --parallel 2
ctest --test-dir /tmp/ucp-baseline --output-on-failure
```

构建完成，但 12 个 CTest 中有 7 个网络相关用例失败。可见错误包括监听 socket 创建
失败和 `setsockopt` 失败；这可能是当前执行环境的网络限制，也可能暴露测试可移植性
问题。由于尚未在原生 Linux 压测环境复验，不能将其归因为项目缺陷，也不能把本次结果
作为稳定性通过证据。

本仓库已有的 [gateway benchmark](gateway-benchmark.md) 应作为后续对比的固定基线：
相同硬件、内核、编译选项、worker 数、并发数和负载下，只有无 socket/HTTP 错误的结果
才可比较吞吐和延迟。

## 7. 内核版本建议

建议将默认兼容目标定为 Linux 5.15+：保持当前单次 I/O 实现，并在运行时探测后启用
Linux 5.19+ 的 multishot accept 和 buffer ring。若部署环境统一为 Linux 6.1+，可以将
multishot recv、`DEFER_TASKRUN` 和 `SINGLE_ISSUER` 纳入实验路径。更高版本的新能力只能
作为额外优化，不应成为框架正确运行的前提。

## 8. 参考资料

- [Seastar GitHub](https://github.com/scylladb/seastar)
- [Seastar SMP documentation](https://docs.seastar.io/master/group__smp-module.html)
- [liburing GitHub](https://github.com/axboe/liburing)
- [io_uring and networking in 2023](https://github.com/axboe/liburing/wiki/io-uring-and-networking-in-2023)
- [Tokio GitHub](https://github.com/tokio-rs/tokio)
- [Tokio runtime documentation](https://docs.rs/tokio/latest/tokio/runtime/)
- [NGINX GitHub](https://github.com/nginx/nginx)
- [NGINX core module](https://nginx.org/en/docs/ngx_core_module.html)
- [NGINX HTTP core module](https://nginx.org/en/docs/http/ngx_http_core_module.html)
- [libuv GitHub](https://github.com/libuv/libuv)
- [libevent GitHub](https://github.com/libevent/libevent)
- [Cloudflare quiche GitHub](https://github.com/cloudflare/quiche)
- [muduo GitHub](https://github.com/chenshuo/muduo)
- [tokio-uring GitHub](https://github.com/tokio-rs/tokio-uring)
