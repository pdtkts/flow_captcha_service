# flow_captcha_service

`flow_captcha_service` 是给 `flow2api` 使用的独立打码服务，采用 HTTP 透传方式调用。

它的核心定位不是“接第三方打码平台”，而是“自己托管有头浏览器打码能力”，并支持主从集群。

---

## 项目介绍与能力范围

### 1. 能力范围

- 仅支持有头浏览器打码（Playwright + Chromium）
- 不接 yescaptcha/capsolver 等外部平台
- 支持会话化流程：`solve -> finish/error`
- 支持 `standalone / master / subnode` 三种角色
- 支持独立用户门户（`/`）做接入说明、在线调试、自助日志查询
- 支持管理面板（`/admin`）做常用运维操作
- 支持 API Key、额度、日志、集群节点状态管理

### 2. 角色说明

- `standalone`：单机直接打码
- `master`：只调度子节点，不执行本地浏览器打码
- `subnode`：执行本地浏览器打码，并向 master 注册/心跳

---

## 项目架构

### 1. 逻辑架构

```text
flow2api
   |
   | HTTP
   v
[master]  (调度，不打码)
   |
   | 路由转发（nodeId:childSessionId）
   v
[subnode] (有头浏览器打码)
```

### 2. 关闭链路（主节点如何通知子节点关闭）

本项目没有单独 `/close` 业务接口，关闭语义通过会话协议完成：

1. 上游先调用 `solve`，master 返回 `nodeId:childSessionId`
2. 业务成功后调用 `finish`
3. master 按路由 session 转发到对应 subnode
4. subnode 执行本地 `runtime.finish()`，标记业务会话结束，浏览器通常保持常驻复用
5. 业务失败则调用 `error`，仅在明确验证码评估失败时回收对应浏览器槽位

---

## 详细流程图

### 1. `solve -> finish/error` 主链路时序图

```mermaid
sequenceDiagram
    autonumber
    participant F as flow2api
    participant A as flow_captcha_service API
    participant C as ClusterManager(master)
    participant R as CaptchaRuntime
    participant S as SessionRegistry
    participant B as BrowserCaptchaService
    participant T as TokenBrowser(slot)
    participant G as Google Flow / reCAPTCHA

    F->>A: POST /api/v1/solve<br/>(project_id, action, token_id)
    A->>A: 校验 service API key / quota
    alt master 角色
        A->>C: dispatch_solve(payload)
        C->>C: 选择候选 subnode<br/>预留 dispatch slot
        C->>A: 转发到子节点 /api/v1/solve
        A->>R: solve(...)
    else standalone / subnode
        A->>R: solve(...)
    end
    R->>B: get_token(project_id, action, token_id)
    B->>B: 依据 project_id 选择亲和槽位<br/>空闲优先 + 轮询兜底
    B->>T: get_or_create_shared_browser()
    alt 槽位已有共享浏览器
        T->>T: 复用 shared browser/context/keepalive
    else 首次启动或已回收
        T->>T: 创建 playwright/browser/context
        T->>T: 创建 keepalive page(about:blank)
    end
    T->>G: 打开 Flow 页面并执行 reCAPTCHA
    G-->>T: reCAPTCHA token
    T-->>B: token + browser_id
    B-->>R: token + browser_id + fingerprint
    R->>S: create(session_id, browser_id, project_id, action)
    R-->>A: session_id + token + fingerprint + expires
    alt master 角色
        C->>C: childSessionId -> nodeId:childSessionId
        C-->>F: 返回 routed session_id
    else standalone / subnode
        A-->>F: 返回普通 session_id
    end

    Note over F,A: 上游拿到 token 后，图片/视频请求继续直接走官网

    F->>A: POST /api/v1/sessions/{session_id}/finish
    A->>R: finish(session_id)
    R->>B: report_request_finished(browser_ref)
    B->>B: 仅标记业务请求完成<br/>共享浏览器继续常驻复用
    R->>S: finish(session_id)
    A-->>F: success

    F->>A: POST /api/v1/sessions/{session_id}/error
    A->>R: mark_error(session_id, error_reason)
    R->>B: report_error(browser_ref, error_reason)
    alt 明确命中 reCAPTCHA evaluation/verification failed
        B->>T: recycle_browser(rotate_profile=true)
    else 普通业务失败 / 非明确验证码失败
        B->>B: 浏览器保持常驻，等待后续复用
    end
    R->>S: mark_error(session_id)
    A-->>F: success
```

**这张图对应的真实语义：**

- `solve` 只负责拿到 reCAPTCHA token，并把 `session_id -> browser_id` 关系登记到 `SessionRegistry`
- `finish` / `error` 是业务会话的回收协议，不是“浏览器一定关闭”的意思
- 对于常规 `solve` 主路径，浏览器是 **常驻复用** 的；成功后不会主动关闭共享浏览器
- 只有明确命中 `reCAPTCHA evaluation failed / verification failed` 这类错误时，才会回收该槽位浏览器

### 2. 浏览器槽位复用与回收流程图

```mermaid
flowchart TD
    A[收到 get_token 请求] --> B[根据 project_id 选择槽位]
    B --> C{该槽位已有 shared browser/context 吗}
    C -- 是 --> D[复用现有 shared browser]
    C -- 否 --> E[新建 browser/context]
    E --> F[创建 keepalive page]
    D --> G[执行 reCAPTCHA]
    F --> G
    G --> H{拿到 token 吗}
    H -- 是 --> I[返回 token + browser_id]
    I --> J[业务请求继续生成图片或视频]
    J --> K{上游回调 finish 还是 error}
    K -- finish --> L[report_request_finished]
    L --> M[共享浏览器保持常驻]
    K -- error --> N[report_error]
    N --> O{错误是否明确属于 reCAPTCHA evaluation/verification failed}
    O -- 否 --> M
    O -- 是 --> P[recycle_browser + rotate_profile]
    P --> Q[下次请求重新拉起浏览器]
    M --> R{空闲超过 idle TTL 吗}
    R -- 否 --> S[继续待命复用]
    R -- 是 --> T[idle reaper 回收该槽位浏览器]
    T --> U[槽位仍保留, 仅浏览器实例被释放]
    Q --> U
```

**关键点：**

- 槽位数量由 `captcha.browser_count` 决定；一个槽位同一时刻只跑一个 solve
- `finish` 不会关闭共享浏览器，只是让该次业务会话结束
- `idle TTL` 是自动回收空闲浏览器，不是回收槽位；下次有请求会重新拉起浏览器
- 如果代理配置变化、浏览器断连、keepalive page 损坏，也会触发槽位级浏览器重建

### 3. master 调度与 routed session 流程图

```mermaid
flowchart TD
    A[master 收到 /solve] --> B[拉取可用 subnode 列表]
    B --> C[结合 heartbeat/runtime stats 计算 effective_capacity]
    C --> D[按 busy_browser_count 剩余容量 权重 排序候选节点]
    D --> E{有可派发节点吗}
    E -- 否 --> F[等待 0.35s 后重试]
    F --> B
    E -- 是 --> G[尝试预留 dispatch reservation]
    G --> H[POST 到子节点 /api/v1/solve]
    H --> I{子节点成功返回 token 和 childSessionId 吗}
    I -- 否 --> J[释放 reservation<br/>记录节点错误<br/>换下一个节点]
    J --> D
    I -- 是 --> K[释放临时 reservation]
    K --> L[记录 active routed session]
    L --> M[包装 session_id 为 nodeId:childSessionId]
    M --> N[返回给 flow2api]

    N --> O[后续 finish/error 带着 routed session_id 回来]
    O --> P[master 解析 nodeId + childSessionId]
    P --> Q[转发到对应 subnode /finish 或 /error]
    Q --> R[master 本地 active_sessions -1]
```

**这张图对应的真实语义：**

- master 本身不执行本地有头浏览器打码，只做调度、转发、会话路由
- `nodeId:childSessionId` 是主节点路由会话格式，保证 `finish/error` 能准确回到原子节点
- master 额外维护了短时 `dispatch reservation`，用于覆盖 heartbeat 上报延迟，避免瞬时超发
- 节点容量依据 **busy browser slots** 统计，而不是简单把 `pending session` 当成活跃线程

### 4. `custom-score` 链路流程图

```mermaid
flowchart TD
    A[客户端调用 /api/v1/custom-score] --> B{当前角色是 master 吗}
    B -- 是 --> C[master 按调度逻辑转发到 subnode]
    B -- 否 --> D[本地 runtime.custom_score]
    C --> D
    D --> E[BrowserCaptchaService.get_custom_score]
    E --> F[使用临时浏览器执行验证码与页面内 verify]
    F --> G[返回 token / verify_result / fingerprint]
    G --> H[临时浏览器关闭]
    H --> I[API 返回 custom score 结果]
```

**注意：**

- `custom-score` 当前不是常驻共享浏览器链路，而是 **临时浏览器链路**
- 也就是说，主业务 `solve` 和 `custom-score` 的浏览器生命周期策略并不完全相同

### 5. 关键实现原则

1. **浏览器常驻复用，不等于业务会话常驻占槽**
   - `solve` 期间槽位忙
   - token 一旦拿到，槽位的 solve 忙碌态就会释放
   - 后续图片/视频生成继续跑，但不会把该槽位一直算成 `thread_active`

2. **`pending_sessions` 与 `busy_browser_count` 不是同一个概念**
   - `pending_sessions`：还没收到 `finish/error` 的业务会话数
   - `busy_browser_count`：当前真正正在执行 solve 的浏览器槽位数
   - 调度基于后者，避免“线程看起来一直满”的错觉

3. **同一个 `project_id` 会优先复用历史槽位**
   - 如果亲和槽位空闲，优先命中原槽位
   - 如果亲和槽位忙，才扩展到其他空闲槽位
   - 没有空闲槽位时，再走轮询兜底

4. **成功不关浏览器，明确验证码失败才回收浏览器**
   - `finish`：保留共享浏览器
   - `error`：只有在错误文本明确命中 `reCAPTCHA evaluation failed` / `verification failed` 等条件时，才回收浏览器并切换指纹
   - 普通上游业务失败不会把浏览器全部打掉

5. **空闲自动回收是为了控资源，不是为了打断复用**
   - 后台 `idle reaper` 每 15 秒巡检一次
   - 槽位空闲超过 `browser_idle_ttl_seconds` 时，回收该槽位浏览器实例
   - 新请求进来后，会按同样的槽位选择逻辑重新拉起浏览器

6. **两层句柄不要混淆**
   - 子节点本地：`browser_ref = browser_id[:request_ref]`
   - master 对外：`session_id = nodeId:childSessionId`
   - 前者用于浏览器槽位回调，后者用于跨节点会话路由


---

## 配置文件与修改方式

### 1. 配置文件位置

- 模板：`config/setting_example.toml`
- Persisted runtime config: `data/setting.toml`
- Migration: if legacy `config/setting.toml` exists, it is copied to `data/setting.toml` on startup

首次使用：

```bash
cp config/setting_example.toml data/setting.toml
```

### 2. 修改配置的两种方式

#### A. 通过管理面板（推荐）

- 入口：`http://<host>:<port>/admin`
- 可在线修改运行配置与系统配置
- 会提示哪些改动需要重启服务

#### B. Edit `data/setting.toml` directly

- 修改后重启服务生效（部分配置可热生效，但建议按重启策略执行）

### 3. 配置优先级

环境变量优先级高于 `setting.toml`。  
如果某项被环境变量覆盖，面板会显示提示。

---

## 本地部署

默认端口为 `8060`。

### 1. standalone（本地单机）

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/Mac
source .venv/bin/activate

pip install -r requirements.txt
python main.py
```

访问：

- 用户门户：`http://127.0.0.1:8060/`
- 服务健康检查：`http://127.0.0.1:8060/api/v1/health`
- 管理面板：`http://127.0.0.1:8060/admin`

### 2. 本地主从（不走 Docker）

- 启动 master：`FCS_CLUSTER_ROLE=master`
- 启动 subnode：`FCS_CLUSTER_ROLE=subnode` 并配置：
  - `FCS_CLUSTER_MASTER_BASE_URL`
  - `FCS_CLUSTER_MASTER_CLUSTER_KEY`
  - `FCS_CLUSTER_NODE_PUBLIC_BASE_URL`
  - `FCS_CLUSTER_NODE_API_KEY`

---

## Docker 部署（含持久化）

> 推荐始终保留持久化挂载，否则重启后会丢失数据库、API Key、cluster key、日志等状态。

### 1. 持久化目录建议

- `./data`：数据库与运行状态（必须持久化）
- `./config`：配置文件（建议持久化）

### 2. standalone（有头）

```bash
docker compose -f docker-compose.headed.yml up -d --build
```

默认已挂载：

- `./data:/app/data`
- `./config:/app/config`

### 3. master（轻量镜像）

```bash
docker compose -f docker-compose.cluster.master.yml up -d --build
```

使用 `Dockerfile.master`（不安装 Playwright/Chromium，镜像更小）。

### 4. subnode（有头镜像）

```bash
docker compose -f docker-compose.cluster.subnode.yml up -d --build
```

启动前要替换：

- `FCS_CLUSTER_MASTER_CLUSTER_KEY`
- `FCS_CLUSTER_NODE_API_KEY`

---

## 一键部署（master + subnode）

```bash
docker compose -f docker-compose.cluster.stack.yml up -d --build
```

该方案同时拉起：

- `flow-captcha-master`（轻量镜像）
- `flow-captcha-subnode`（有头镜像）

默认持久化路径：

- `./data/master:/app/data`
- `./data/subnode:/app/data`
- `./config:/app/config`

启动前至少替换：

- `FCS_CLUSTER_MASTER_CLUSTER_KEY`
- `FCS_CLUSTER_NODE_API_KEY`

---

## GHCR 镜像

项目通过 GitHub Actions 自动发布到 GHCR：

- `ghcr.io/<owner>/flow_captcha_service-master`（轻量 master）
- `ghcr.io/<owner>/flow_captcha_service-headed`（有头 standalone/subnode）
- 发布架构：`linux/amd64`、`linux/arm64`

拉取示例：

```bash
docker pull ghcr.io/genz27/flow_captcha_service-master:latest
docker pull ghcr.io/genz27/flow_captcha_service-headed:latest
```

### 拉取镜像是否需要环境变量？

- `docker pull`：不需要环境变量
- `docker run / docker compose up`：需要按角色配置环境变量

如果仓库/包是私有，请先使用带 `read:packages` 权限的 PAT 登录 GHCR。

---

## 常见问题

### `exec /usr/local/bin/entrypoint.headed.sh: exec format error`

排查顺序：

1. 确认机器架构（你是 `x86_64/amd64`）
2. 确认拉到的是新镜像（重新 `docker pull`）
3. 删除旧 tag 本地缓存后再拉取并重启容器

本仓库已将有头镜像启动方式改为内联 `bash` 启动流程，不再依赖脚本文件执行，可避免该错误。


- `cluster.node_max_concurrency = 0` means the dispatcher follows `captcha.browser_count`.
