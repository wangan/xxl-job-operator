# XXL-JOB Kubernetes Operator (`xxl-job-operator`)

一套面向云原生与 GitOps 场景设计的 **XXL-JOB Kubernetes Operator**（基于 Go 1.24 + `controller-runtime` 构建，适配 XXL-JOB `v2.4.1`）。

本项目将社区在 Kubernetes 中落地 XXL-JOB 的**三大主流技术流派（常驻容器流派、Sidecar 无侵入流派、K8s 原生一次性 Job 代理流派）**统一收敛到一套声明式 CRD 体系中，并同时提供无 Operator 的标准 YAML 对照模板。

---

## 1. 为什么需要 `xxl-job-operator`？（流派对比与选型）

| 落地流派 | 适用场景 | 传统方案痛点 | `xxl-job-operator` 解决方案 |
| :--- | :--- | :--- | :--- |
| **流派 1：容器常驻流派 (`mode: Resident`)** | 高频调度、分片广播、Java Spring Boot 微服务内置执行器 | 1. 容易拿到容器虚拟网桥错误 IP<br/>2. Pod 重建/漂移后旧 IP 残留 90s 导致调度报错<br/>3. 滚动升级时在途任务被强杀 | 1. **Webhook 自动注入** Downward API (`POD_IP`、`XXL_JOB_EXECUTOR_IP`、通过 `SecretKeyRef` 注入 `XXL_JOB_ACCESSTOKEN`) 与 `PreStop` 无损停机钩子（默认先排空等待再摘除注册）<br/>2. **Controller 以 `addressType=1` 实时托管 `address_list` + 独立后台 `RegistryHeartbeatRunner` 并发保活 `xxl_job_registry`**：一旦 Pod 进入 `Terminating` 或 `NotReady`，**立即从 `xxl_job_group.address_list` 剔除**（秒级切断新流量），同时在有界排空窗口内通过 `status.registryKeepAliveAddresses` 维持 `xxl_job_registry` 心跳（防止 >10 分钟在途长任务被 `JobCompleteHelper` 误判为结果丢失触发重复重跑），待窗口结束或退出 `Running` 后再调用 `/api/registryRemove`；当全部实例下线时自动写入 `/proxy/_unavailable/` 哨兵地址防止回退 |
| **流派 2：Sidecar 无侵入流派 (`mode: Sidecar`)** | Python / Go / Node.js / C++ 多语言服务，或不改代码的存量应用 | 需要在每个业务项目中引入不同语言的非官方 SDK，维护成本高 | 开启 Webhook（`--enable-webhooks=true`）并为 Pod 添加标签 `xxljob.io/inject: "true"` 与注解 `xxljob.io/mode: "sidecar"` 后，**Webhook 自动解析关联 `XxlJobExecutor` -> `XxlJobAdmin` 的 Secret 凭证并注入满足 Restricted PSS 的 `xxl-job-sidecar` 容器**（缺少凭证时 `admission.Denied` 拒绝创建，默认关闭 GLUE 脚本远程执行，注入与业务容器共享的 `XXL_JOB_SIDECAR_SHARED_SECRET`，回调缓冲使用仅挂载至 Sidecar 的私有卷 `/xxl-job/spool/callbacks`，`POST /preStop` 携带 `XXL-JOB-ACCESS-TOKEN` 鉴权并等待在途任务排空），支持阻塞策略、协同 `/kill`、回调磁盘持久化与多 Admin 故障转移 |
| **流派 3：K8s 原生 Job 代理流派 (`mode: K8sJobProxy`)** | 低频批处理、日终对账、ETL/AI 训练等高算力弹性任务 | 常驻执行器 Pod 99% 时间空闲浪费资源；K8s CronJob 又缺少可视化的依赖编排、分片、告警和手动重跑能力 | 内置无状态 **`ProxyExecutorServer`**（多租户隔离路径 `/proxy/<namespace>/<executor>/`）：收到 `/run` 请求后按 `podSecurityLevel`（默认 `restricted`）校验模板并动态创建带 `xxljob.io/callback-guard` Finalizer 的一次性 `batch/v1.Job`（统一先以 `suspend: true` 创建，在通过 `claimDiscardLaterWinner` 跨副本乐观锁去重或 `SERIAL_EXECUTION` 队头轮转后再解除挂起，由单实例 Leader **`K8sJobCallbackReconciler`** 基于 `APIReader` 强一致出队并监控 `FailedCreate` 事件，超时返回 `502`，保障跨重启/被删/配额受限场景可靠回调 `/api/callback`），支持控制台查看日志 (`/log`) 与终止在途任务 (`/kill`) |
| **基准对照：无 Operator 标准方案** | 轻量集群、暂不引入 CRD 的团队 | 手写 YAML 容易遗漏网卡 IP 注入和 PreStop 摘流 | 在 [`config/samples/05-standalone-without-operator.yaml`](config/samples/05-standalone-without-operator.yaml) 中提供开箱即用的标准高可用模板 |

---

## 2. 核心架构与 CRD 设计

### 2.1 三大核心 CRD (`xxljob.io/v1alpha1`)
1. **`XxlJobAdmin` (`xja`)**：声明式管理 XXL-JOB Admin 调度中心（多副本高可用 Deployment、Service、MySQL Secret 按需读取与环境变量 `SPRING_DATASOURCE_PASSWORD` / `XXL_JOB_ACCESSTOKEN` 安全注入、Secret 凭据轮换自动计算 `xxljob.io/credentials-hash` 触发滚动升级并保留 10 分钟旧 Token 宽限期、省略 `spec.replicas` 时自动保留 HPA 扩缩容副本数）。
2. **`XxlJobExecutor` (`xje`)**：声明式管理执行器组（对应 `xxl_job_group`，`spec.appName` 不可变，`spec.title` 最大长度 12），支持通过 `spec.mode` 在 `Resident`、`Sidecar`、`K8sJobProxy` 三种模式间切换，支持 `spec.allowedServiceAccounts` 限制 `K8sJobProxy` 任务可绑定的 ServiceAccount、`spec.podSecurityLevel` 配置 Pod 安全标准（`restricted` / `baseline` / `privileged`）、`spec.sidecarEnableGlue` 控制 Sidecar GLUE 开关，自动同步注册地址并汇报 `status.jobGroupId`、`status.activePods`、`status.terminatingPodsDrained`、`status.registryKeepAliveAddresses` 与 `RegistryHealthy` Condition。
3. **`XxlJobTask` (`xjt`)**：声明式管理具体调度任务（对应 `xxl_job_info`），实现定时任务的 **GitOps 化版本管理**（通过 `[xjt:<namespace>/<name>]` 归属标记防止跨任务误认领，按 `status.jobId` 原地更新与同 Admin / 跨 Admin 执行器组自动迁移清理，支持深拷贝快照缓存与写失败自动失效、停用任务时先 `stop` 再 `update`、多语言 GLUE 脚本 `$1/$2/$3` 分片参数透传以及生产级 `k8sJobTemplate` 容器模板）。

### 2.2 关键运维说明与注解

- **`auth.password` / `auth.passwordSecretRef` 语义说明**：
  XXL-JOB 官方镜像未提供通过环境变量修改数据库初始管理员密码的能力（初始密码由 MySQL 初始化脚本 `tables_xxl_job.sql` 中的 `xxl_job_user` 表决定，默认为 `admin` / `123456`）。`XxlJobAdmin.spec.auth.password`（或 `passwordSecretRef`）用于告知 **Operator** 登录 Admin Web 控制台 (`/login`) 所使用的密码；若您在控制台修改了 `admin` 密码，请同步更新该字段或对应 Secret。
- **级联删除保护与异常逃生 (`xxljob.io/force-delete: "true"`)**：
  正常删除时严格遵循 `XxlJobTask -> XxlJobExecutor -> XxlJobAdmin` 顺序保护（10 分钟删除超时仅跳过远端 Admin 网络不可达错误，不会绕过本地子 CR 依赖检查或 Admin 端业务拒绝；若 `RemoveJobGroup` 返回业务错误但复查确认该执行器组已不存在，则幂等放行 Finalizer）；当整个 Kubernetes Namespace 被删除或显式打上注解 `xxljob.io/force-delete: "true"` 时，控制器才会同时放行依赖检查与远端清理。
- **修改 `glueType` 保护 (`xxljob.io/allow-gluetype-recreate: "true"`)**：
  XXL-JOB `/jobinfo/update` 不支持原地修改 `glueType`，变更 `glueType` 需先删后建并分配新的 `jobId`（会影响 `childJobId` 引用与历史日志关联）。Operator 默认拒绝此类修改，需在 `XxlJobTask` 上显式添加注解 `xxljob.io/allow-gluetype-recreate: "true"` 确认后方可执行。
- **`K8sJobProxy` / `Sidecar` 访问令牌要求、业务容器协议契约与多租户安全边界**：
  - 启用 `K8sJobProxy` 或 `Sidecar` 模式时，关联的 `XxlJobAdmin` **必须配置非空且非 `default_token` 的 `accessToken` 或 `accessTokenSecretRef`**；Webhook 仅从 `XxlJobAdmin.spec.auth.accessTokenSecretRef` 或 Pod 注解 `xxljob.io/access-token-secret-name` 注入 `SecretKeyRef`（绝不将 `XxlJobAdmin` 中的明文 Token 复制到业务 Pod Spec 中）。
  - **Secret 轮换运维说明**：当旋转 `XxlJobAdmin` 关联的 `accessTokenSecretRef` / `passwordSecretRef` 时，Operator 会自动计算 `xxljob.io/credentials-hash` 触发 `XxlJobAdmin` Deployment 滚动重启，并在出站/入站双向保留 **10 分钟旧 Token 宽限期**；由于 Kubernetes 环境变量 `SecretKeyRef` 仅在 Pod 启动时注入，若使用 `Resident` 或 `Sidecar` 模式，请在旋转 Secret 后的 10 分钟宽限期内触发业务 Deployment 滚动更新（如 `kubectl rollout restart deployment/<name>`）以加载新 Token。
  - **Sidecar 与业务容器本地 HTTP 契约**：在 `Sidecar` 模式下（`BEAN` 任务），`xxl-job-sidecar` 通过本地回环向业务容器发起 `POST http://127.0.0.1:<port>/job/<executorHandler>`（请求头携带 `X-XXL-JOB-SIDECAR-SECRET: <shared-secret>`、请求体为 JSON 格式的 `RunRequest`）；当收到调度中心 `/kill` 或 `COVER_EARLY` 抢占终止指令时，Sidecar 除了取消在途 HTTP 请求 Context 外，还会向业务容器发起最大努力的协同通知 `POST http://127.0.0.1:<port>/job/<executorHandler>/kill`（携带相同 `X-XXL-JOB-SIDECAR-SECRET` 头与 `{"jobId":...,"logId":...}` 请求体）。
  - Sidecar 默认禁用 GLUE 脚本执行（`XXL_JOB_SIDECAR_ENABLE_GLUE=false`），仅在 `XxlJobExecutor.spec.sidecarEnableGlue: true` 或 Pod 显式标注 `xxljob.io/enable-glue: "true"`（或 `xxljob.io/sidecar-enable-glue: "true"`）时开启。
  - 在 `K8sJobProxy` 模式下，默认执行 `restricted` Pod Security 校验（覆盖 `containers`、`initContainers` 与 `ephemeralContainers`，禁止 `privileged`、`hostNetwork`/`hostPID`/`hostIPC`、`hostPort`、`windowsOptions.hostProcess`、`hostPath`/`gitRepo` Volume、非法的 `seLinuxOptions`/`appArmorProfile`/旧版 seccomp 注解、挂载 `kubernetes.io/service-account-token` 类型 Secret 及未授权的 `projected.serviceAccountToken`，注入非特权 `securityContext`、`capabilities.drop: ["ALL"]` 且默认 `automountServiceAccountToken: false`）；若 `XxlJobTask.spec.k8sJobTemplate` 指定了非 `default` 的 ServiceAccount，或为 `default` ServiceAccount 显式开启 `automountServiceAccountToken: true`，则该 ServiceAccount 必须列入所属 `XxlJobExecutor.spec.allowedServiceAccounts` 白名单。
- **Webhook 依赖 (`cert-manager`) 与启用方式**：
  为兼容未安装 [`cert-manager`](https://cert-manager.io/) 的集群，`config/manager/manager.yaml` 默认设置 `--enable-webhooks=false`。如需使用 `Resident` / `Sidecar` 模式的 Pod 自动注入（[`config/webhook/manifests.yaml`](config/webhook/manifests.yaml) 对匹配 `xxljob.io/inject: "true"` 的 Pod 采用 `failurePolicy: Fail` 强校验）：
  1. 部署 `kubectl apply -f config/webhook/manifests.yaml`
  2. 将 `config/manager/manager.yaml` 中的 `--enable-webhooks=true` 开启并挂载 `webhook-server-cert` Secret
  3. 在业务 Pod 模板上添加 label `xxljob.io/inject: "true"` 及 annotation `xxljob.io/mode: "resident"`（或 `"sidecar"`）
- **可观测性指标 (Prometheus Metrics)**：
  Operator 在 `:8080/metrics` 暴露以下 3 项自定义指标（定义于 [`internal/controller/metrics.go`](internal/controller/metrics.go)）：
  - `xxljob_registry_heartbeat_errors_total`（Counter，标签 `namespace`、`executor`）：执行器向 XXL-JOB Admin 发送 `/api/registry` 或 `/api/registryRemove` 失败次数
  - `xxljob_callback_pending_jobs`（Gauge，标签 `namespace`、`executor`）：当前等待向 XXL-JOB Admin 投递 `/api/callback` 的 K8s Job 数量
  - `xxljob_serial_queue_depth`（Gauge，标签 `namespace`、`executor`、`job_id`）：当前处于 `SERIAL_EXECUTION` 挂起排队状态的 K8s Job 队列深度

### 2.3 仓库目录结构

```text
xxl-job-operator/
├── api/v1alpha1/                  # CRD Schema 定义 (XxlJobAdmin, XxlJobExecutor, XxlJobTask)
├── cmd/
│   ├── main.go                    # Operator Manager + 内置无状态 K8sJobProxy Server + HeartbeatRunner + Webhook 入口
│   └── sidecar/main.go            # 轻量级 Sidecar 代理容器入口
├── internal/
│   ├── controller/                # 三大 CRD 控制器 + K8sJobCallbackReconciler + RegistryHeartbeatRunner + Metrics
│   └── webhook/                   # Pod MutatingWebhook (Resident DownwardAPI/PreStop & Sidecar Secret 注入)
├── pkg/
│   ├── xxlclient/                 # XXL-JOB OpenAPI (/api/*) 与管理端 API (/jobgroup/*, /jobinfo/*) 客户端（含深拷贝快照缓存）
│   ├── proxyexecutor/             # K8s 原生一次性 batch/v1.Job 代理执行器服务 (/proxy/<ns>/<exec>/*) 与 PodSecurity 校验
│   └── sidecar/                   # Sidecar 模式核心代理引擎（含 FIFO 串行队列、协同 /kill 与回调磁盘持久化）
└── config/
    ├── crd/bases/                 # CRD OpenAPI v3 YAML 清单（含 CEL 校验规则）
    ├── manager/manager.yaml       # Operator RBAC、Service 与 Deployment 部署清单
    ├── webhook/manifests.yaml     # MutatingWebhookConfiguration (failurePolicy: Fail) 与 cert-manager Certificate 清单
    └── samples/                   # 5 套完整场景 YAML 示例模板
```

---

## 3. 快速开始

### 3.1 本地编译与单元测试

```bash
# 1. 运行全部单元测试（含 Race 检测）
make test

# 2. 编译 Operator Manager 与 Sidecar 二进制文件
make build
```

### 3.2 部署到 Kubernetes 集群

```bash
# 1. 安装 CRD
make install

# 2. 部署 Operator 控制器与内置 K8sJobProxy Service
make deploy

# 3. (可选) 如需启用 Resident / Sidecar Pod 自动注入 Webhook（需集群已安装 cert-manager 并开启 --enable-webhooks=true）：
kubectl apply -f config/webhook/manifests.yaml

# 4. 部署高可用 XXL-JOB Admin 实例
kubectl apply -f config/samples/01-xxljob-admin.yaml

# 5. 按需选择以下任意流派示例体验：
# 流派 1：常驻容器模式 + 滚动升级 addressType=1 秒级无损摘流
kubectl apply -f config/samples/02-executor-resident.yaml

# 流派 3：K8s 原生一次性 Job 代理模式（无需常驻业务 Pod）
kubectl apply -f config/samples/03-executor-k8sjob-proxy.yaml

# 流派 2：Sidecar 无侵入自动注入模式
kubectl apply -f config/samples/04-executor-sidecar.yaml
```

### 3.3 查看资源状态

```bash
kubectl get xja,xje,xjt
```
