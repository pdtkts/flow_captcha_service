const STORAGE_KEY = "fcs.portal.apiKey";
const REMEMBER_KEY = "fcs.portal.remember";

const state = {
  apiKey: "",
  remember: false,
  summary: null,
  user: null,
  isAuthenticated: false,
  userOverview: null,
  latestRedeem: null,
  lastSessionId: "",
};

const dom = {
  byId(id) {
    return document.getElementById(id);
  },
};

function setText(id, value) {
  const element = dom.byId(id);
  if (element) {
    element.textContent = value;
  }
}

function showNotice(id, message, kind = "info") {
  const element = dom.byId(id);
  if (!element) {
    return;
  }
  element.className = `notice ${kind}`;
  element.textContent = message;
}

function showBlock(id, visible) {
  const element = dom.byId(id);
  if (!element) {
    return;
  }
  element.classList.toggle("hidden-block", !visible);
}

function formatDateTime(value) {
  if (!value) {
    return "--";
  }
  const normalized = String(value).replace(" ", "T");
  const date = new Date(normalized.endsWith("Z") ? normalized : `${normalized}Z`);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return date.toLocaleString("zh-CN", { hour12: false });
}

function formatDuration(value) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const milliseconds = Number(value);
  if (!Number.isFinite(milliseconds)) {
    return "--";
  }
  if (milliseconds < 1000) {
    return `${milliseconds} ms`;
  }
  return `${(milliseconds / 1000).toFixed(2)} s`;
}

function formatAgeSeconds(value) {
  if (value === null || value === undefined || value === "") {
    return "--";
  }
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds < 0) {
    return "--";
  }
  if (seconds < 60) {
    return `${seconds}s`;
  }
  if (seconds < 3600) {
    return `${(seconds / 60).toFixed(1)}m`;
  }
  return `${(seconds / 3600).toFixed(1)}h`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function roleLabel(role) {
  if (role === "master") {
    return "主节点 / Master";
  }
  if (role === "subnode") {
    return "子节点 / Subnode";
  }
  return "单机 / Standalone";
}

function statusClass(status) {
  const text = String(status || "").toLowerCase();
  if (text.includes("success")) {
    return "success";
  }
  if (text.includes("fail") || text.includes("error")) {
    return "error";
  }
  if (text.includes("cancel") || text.includes("timeout")) {
    return "warning";
  }
  return "info";
}

function firstDefined(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }
  return null;
}

function asNumber(...values) {
  const value = firstDefined(...values);
  if (value === null) {
    return null;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function hasUserSession() {
  return !!state.isAuthenticated && !!state.user;
}

function activeMode() {
  if (hasUserSession()) {
    return "user";
  }
  if (state.apiKey) {
    return "apiKey";
  }
  return "guest";
}

function modeLabel() {
  if (activeMode() === "user") {
    return "用户态";
  }
  if (activeMode() === "apiKey") {
    return "API Key 兼容态";
  }
  return "访客";
}

function renderResponse(title, payload, kind = "info") {
  const meta = dom.byId("responseMeta");
  const viewer = dom.byId("responseViewer");
  if (meta) {
    meta.className = `tag ${kind === "error" ? "muted" : ""}`.trim();
    meta.textContent = `${title} · ${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`;
  }
  if (viewer) {
    viewer.textContent = JSON.stringify(payload, null, 2);
  }
}

// 统一请求入口：用户态接口默认依赖浏览器会话；兼容模式再附加 API Key。
async function requestJson(url, options = {}) {
  const method = options.method || "GET";
  const headers = {
    Accept: "application/json",
    ...(options.headers || {}),
  };

  if (options.apiKeyAuth) {
    if (!state.apiKey) {
      throw new Error("请先连接 API Key 或登录用户账号");
    }
    headers.Authorization = `Bearer ${state.apiKey}`;
  }

  let body;
  if (options.body !== undefined) {
    headers["Content-Type"] = "application/json";
    body = JSON.stringify(options.body);
  }

  const response = await fetch(url, {
    method,
    headers,
    body,
    credentials: "same-origin",
  });

  const rawText = await response.text();
  let payload = {};
  if (rawText) {
    try {
      payload = JSON.parse(rawText);
    } catch (error) {
      payload = { raw: rawText };
    }
  }

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || rawText || `HTTP ${response.status}`);
  }
  return payload;
}

function buildCurl(method, path, payload, authMode = "none") {
  const lines = [`curl -X ${method.toUpperCase()} "${window.location.origin}${path}"`];
  lines.push(`  -H "Accept: application/json"`);

  if (authMode === "cookie") {
    lines.push(`  -b "portal_session=YOUR_SESSION_COOKIE"`);
  }
  if (authMode === "apiKey") {
    lines.push(`  -H "Authorization: Bearer ${state.apiKey || "YOUR_API_KEY"}"`);
  }
  if (payload !== undefined) {
    lines.push(`  -H "Content-Type: application/json"`);
    lines.push(`  -d '${JSON.stringify(payload, null, 2).replace(/'/g, "\\'")}'`);
  }
  return lines.join(" \\\n");
}

function persistApiKey() {
  if (state.remember) {
    localStorage.setItem(STORAGE_KEY, state.apiKey);
    localStorage.setItem(REMEMBER_KEY, "1");
    sessionStorage.removeItem(STORAGE_KEY);
    return;
  }
  localStorage.removeItem(STORAGE_KEY);
  localStorage.setItem(REMEMBER_KEY, "0");
  if (state.apiKey) {
    sessionStorage.setItem(STORAGE_KEY, state.apiKey);
  } else {
    sessionStorage.removeItem(STORAGE_KEY);
  }
}

function restoreApiKey() {
  state.remember = localStorage.getItem(REMEMBER_KEY) === "1";
  state.apiKey = (state.remember ? localStorage.getItem(STORAGE_KEY) : sessionStorage.getItem(STORAGE_KEY)) || "";
  const input = dom.byId("apiKeyInput");
  const remember = dom.byId("rememberKey");
  if (input) {
    input.value = state.apiKey;
  }
  if (remember) {
    remember.checked = state.remember;
  }
}

function clearStoredApiKey() {
  state.apiKey = "";
  localStorage.removeItem(STORAGE_KEY);
  localStorage.removeItem(REMEMBER_KEY);
  sessionStorage.removeItem(STORAGE_KEY);
}

function updateModeBadges() {
  setText("playgroundModeBadge", `当前模式：${modeLabel()}`);
  setText("logsModeBadge", `当前来源：${modeLabel()}`);
  setText("solveModeTag", activeMode() === "user" ? "POST /api/portal/user/solve" : "POST /api/v1/solve");
  setText("scoreModeTag", activeMode() === "user" ? "POST /api/portal/user/custom-score" : "POST /api/v1/custom-score");
  setText("sessionModeTag", activeMode() === "user" ? "用户态 finish / error" : "兼容态 finish / error");
}

function updateCodeExamples() {
  const sessionId = state.lastSessionId || "session_id_here";
  setText("codeHealth", buildCurl("GET", "/api/v1/health"));
  setText(
    "codeSolve",
    buildCurl(
      "POST",
      hasUserSession() ? "/api/portal/user/solve" : "/api/v1/solve",
      { project_id: "demo-project", action: "IMAGE_GENERATION" },
      hasUserSession() ? "cookie" : "apiKey",
    ),
  );
  setText(
    "codeFinish",
    buildCurl(
      "POST",
      hasUserSession() ? `/api/portal/user/sessions/${sessionId}/finish` : `/api/v1/sessions/${sessionId}/finish`,
      { status: "success" },
      hasUserSession() ? "cookie" : "apiKey",
    ),
  );
  setText(
    "codeScore",
    buildCurl(
      "POST",
      hasUserSession() ? "/api/portal/user/custom-score" : "/api/v1/custom-score",
      {
        website_url: "https://antcpt.com/score_detector/",
        website_key: "6LcR_okUAAAAAPYrPe-HK_0RULO1aZM15ENyM-Mf",
        verify_url: "https://antcpt.com/score_detector/verify.php",
        action: "homepage",
        enterprise: false,
      },
      hasUserSession() ? "cookie" : "apiKey",
    ),
  );
}

function renderSummary(summary) {
  state.summary = summary;
  const role = summary?.service?.role || summary?.meta?.role || "standalone";
  const nodeName = summary?.service?.node_name || summary?.meta?.node_name || "--";
  const idleCapacity = Number(summary?.cluster?.total_idle_capacity || 0);
  const healthyNodes = Number(summary?.cluster?.healthy_node_count || 0);
  const nodeCount = Number(summary?.cluster?.node_count || 0);
  const activeSessions = Number(summary?.runtime?.active_sessions || 0);
  const pendingSessions = Number(summary?.runtime?.pending_sessions || 0);

  setText("serviceRoleChip", `当前角色：${roleLabel(role)}`);
  setText("serviceNodeChip", `服务节点：${nodeName}`);
  setText("serviceDispatchChip", role === "master" ? `调度容量：可用 ${idleCapacity}` : `本地容量：${summary?.runtime?.browser?.thread_idle || 0} 空闲`);
  setText("serviceModeChip", hasUserSession() ? "已登录用户，主按钮走用户态接口" : "未登录用户时，可切换到 API Key 兼容模式");

  setText("metricRole", roleLabel(role));
  setText("metricRoleNote", role === "master" ? "适合作为统一注册与接入入口" : "当前不是主节点，注册位置应由后端继续校验");
  setText("metricNode", nodeName);
  setText("metricCapacity", role === "master" ? String(idleCapacity) : String(summary?.runtime?.browser?.thread_idle || 0));
  setText("metricCapacityNote", role === "master" ? `健康子节点 ${healthyNodes}/${nodeCount || 0}` : `本地总线程 ${summary?.runtime?.browser?.thread_total || 0}`);
  setText("metricSessions", String(activeSessions));
  setText("metricSessionsNote", `待完成 ${pendingSessions} · 缓存 ${summary?.runtime?.cached_sessions || 0}`);

  renderClusterNodes(summary);
}

function renderClusterNodes(summary) {
  const tbody = dom.byId("clusterNodesBody");
  const note = dom.byId("clusterBoardNote");
  if (!tbody) {
    return;
  }
  const cluster = summary?.cluster || {};
  if (!cluster.enabled) {
    if (note) {
      note.textContent = cluster.message || "当前角色不是 master，暂无子节点看板。";
    }
    tbody.innerHTML = '<tr><td colspan="8" class="empty-cell">当前角色不是 master，暂无子节点可展示。</td></tr>';
    return;
  }

  const nodes = Array.isArray(cluster.nodes) ? cluster.nodes : [];
  if (note) {
    note.textContent = `已注册 ${cluster.node_count || nodes.length} 个子节点，健康 ${cluster.healthy_node_count || 0} 个，当前空闲容量 ${cluster.total_idle_capacity || 0}。`;
  }
  if (!nodes.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="empty-cell">主节点暂未发现可展示的子节点摘要。</td></tr>';
    return;
  }

  tbody.innerHTML = nodes.map((node) => `
    <tr>
      <td>${escapeHtml(node.node_name || "--")}</td>
      <td><span class="status-chip ${node.healthy ? "success" : "error"}">${node.healthy ? "健康" : "异常"}</span></td>
      <td>${escapeHtml(String(node.thread_total ?? "--"))}</td>
      <td>${escapeHtml(String(node.thread_active ?? "--"))}</td>
      <td>${escapeHtml(String(node.thread_idle ?? "--"))}</td>
      <td>${escapeHtml(`${node.active_sessions ?? 0} / ${node.cached_sessions ?? 0}`)}</td>
      <td>${escapeHtml(formatAgeSeconds(node.heartbeat_age_seconds))}</td>
      <td>${escapeHtml(node.health_reason || "--")}</td>
    </tr>
  `).join("");
}

function normalizeAuthPayload(payload) {
  const user = payload?.user || payload?.profile || payload?.item || null;
  const authenticated = Boolean(payload?.authenticated ?? user);
  return {
    authenticated,
    user,
  };
}

function renderUserAuth() {
  const user = state.user;
  const registered = hasUserSession();
  setText("accountRegisteredState", registered ? "已注册" : "未注册");
  setText("accountUsername", user?.username || user?.name || "--");
  setText("accountLocation", user?.register_location || user?.location || "--");
  setText("accountLastLogin", formatDateTime(user?.last_login_at || user?.last_login || null));

  if (registered) {
    showNotice("accountNotice", `已登录用户：${user?.username || "unknown"}，主按钮将优先走用户态接口。`, "success");
  } else {
    showNotice("accountNotice", "请先注册或登录终端用户账号，主按钮将优先走用户态接口。", "info");
  }
  updateModeBadges();
}

function renderUserOverview() {
  const payload = state.userOverview;
  const user = payload?.user || state.user || {};
  const usage = payload?.usage || payload?.stats || {};

  const remaining = asNumber(
    payload?.quota_remaining,
    payload?.quota?.remaining,
    user?.quota_remaining,
    usage?.quota_remaining,
  );
  const used = asNumber(
    payload?.quota_used,
    payload?.quota?.used,
    user?.quota_used,
    usage?.quota_used,
  );
  const successCount = asNumber(usage?.solve_success_total, usage?.success_total, usage?.success_count, payload?.success_count);
  const failedCount = asNumber(usage?.solve_failed_total, usage?.failed_total, usage?.failed_count, payload?.failed_count);
  const successRate = firstDefined(usage?.success_rate, payload?.success_rate);
  const lastRequest = firstDefined(usage?.last_request_at, payload?.last_request_at, user?.last_request_at);

  setText("meQuotaRemaining", remaining === null ? "--" : String(remaining));
  setText("meQuotaUsed", used === null ? "--" : String(used));
  setText("meSolveSuccess", successCount === null ? "--" : String(successCount));
  setText("meSolveFailed", failedCount === null ? "--" : String(failedCount));
  setText("meSuccessRate", successRate === null ? "--" : `${Number(successRate).toFixed(2)}%`);
  setText("meLastRequest", formatDateTime(lastRequest));

  setText(
    "usageRuleText",
    payload?.usage_rule || "成功 solve 才扣减 1 次，失败 / error 上报不额外扣次。",
  );
}

function renderRedeemResult() {
  const payload = state.latestRedeem;
  if (!payload) {
    showBlock("redeemSuccessCard", false);
    return;
  }
  const redeemed = payload?.redeemed_code || payload?.item || payload;
  showBlock("redeemSuccessCard", true);
  setText("redeemCodeResult", redeemed?.code || redeemed?.cdk_code || payload?.code || "--");
  setText("redeemQuotaResult", String(firstDefined(redeemed?.quota_times, redeemed?.quota_value, payload?.quota_times, payload?.amount) || "--"));
  setText("redeemTimeResult", formatDateTime(firstDefined(redeemed?.redeemed_at, payload?.redeemed_at, payload?.created_at)));
}

function renderCompatibilityState() {
  setText("tinyConnectState", state.apiKey ? "已连接" : "未连接");
  setText("tinyKeyPrefix", state.apiKey ? `${state.apiKey.slice(0, 8)}...` : "--");
  setText("tinyQuotaMode", state.apiKey ? "兼容模式" : "--");
  setText("tinyLastRequest", formatDateTime(state.userOverview?.usage?.last_request_at || state.userOverview?.last_request_at || null));
  showNotice("compatNotice", state.apiKey ? "API Key 兼容模式已启用，但主按钮仍会优先判断用户登录态。" : "仅当你需要兼容旧 API Key 工作流时，再使用这里。", state.apiKey ? "warning" : "info");
}

function renderLogs(items) {
  const tbody = dom.byId("logsTableBody");
  if (!tbody) {
    return;
  }
  if (!items || !items.length) {
    tbody.innerHTML = '<tr><td colspan="6" class="empty-cell">当前还没有可显示的日志记录。</td></tr>';
    return;
  }
  tbody.innerHTML = items.map((item) => {
    const sessionText = escapeHtml(item.session_id || "--");
    return `
      <tr>
        <td>${escapeHtml(formatDateTime(item.created_at))}</td>
        <td><span class="status-chip ${statusClass(item.status)}">${escapeHtml(item.status || "--")}</span></td>
        <td>${escapeHtml(item.project_id || "--")}</td>
        <td>${escapeHtml(item.action || "--")}</td>
        <td title="${sessionText}">${sessionText}</td>
        <td>${escapeHtml(formatDuration(item.duration_ms))}</td>
      </tr>
    `;
  }).join("");
}

async function loadSummary() {
  const summary = await requestJson("/api/portal/summary");
  renderSummary(summary);
  const role = state.summary?.service?.role || state.summary?.meta?.role || "unknown";
  const locationHint = dom.byId("registerLocationHint");
  if (locationHint) {
    locationHint.textContent = role === "master" ? "???????????????????? master-portal ? /portal?" : "???? master ????????????????";
  }
  updateCodeExamples();
}

async function loadAuthMe() {
  try {
    const payload = await requestJson("/api/portal/auth/me");
    const auth = normalizeAuthPayload(payload);
    state.isAuthenticated = auth.authenticated;
    state.user = auth.user;
  } catch (error) {
    state.isAuthenticated = false;
    state.user = null;
  }
  renderUserAuth();
  updateCodeExamples();
}

async function loadUserData(showSuccess = false) {
  if (!hasUserSession()) {
    state.userOverview = null;
    renderUserOverview();
    renderLogs([]);
    updateModeBadges();
    return;
  }

  const overview = await requestJson("/api/portal/user/overview");
  state.userOverview = overview;
  state.user = overview?.user || state.user;
  renderUserAuth();
  renderUserOverview();
  await loadLogs(false);

  if (showSuccess) {
    showNotice("accountNotice", `欢迎回来，${state.user?.username || "用户"}。`, "success");
  }
}

async function loadCompatData(showSuccess = false) {
  if (!state.apiKey) {
    renderCompatibilityState();
    return;
  }
  try {
    const overview = await requestJson("/api/portal/me/overview", { apiKeyAuth: true });
    state.userOverview = overview;
    renderCompatibilityState();
    if (!hasUserSession()) {
      renderUserOverview();
      await loadLogs(false);
    }
    if (showSuccess) {
      showNotice("compatNotice", `API Key 已连接：${overview?.api_key?.name || "兼容模式"}`, "success");
    }
  } catch (error) {
    renderCompatibilityState();
    throw error;
  }
}

async function loadLogs(showSuccess = false) {
  const status = String(dom.byId("logStatusFilter")?.value || "").trim();
  const projectId = String(dom.byId("logProjectFilter")?.value || "").trim();
  const params = new URLSearchParams({ limit: "12", offset: "0" });
  if (status) {
    params.set("status", status);
  }
  if (projectId) {
    params.set("project_id", projectId);
  }

  let payload;
  if (hasUserSession()) {
    payload = await requestJson(`/api/portal/user/logs?${params.toString()}`);
  } else if (state.apiKey) {
    payload = await requestJson(`/api/portal/sessions?${params.toString()}`, { apiKeyAuth: true });
  } else {
    renderLogs([]);
    return;
  }

  renderLogs(payload?.items || []);
  if (showSuccess) {
    renderResponse("日志已刷新", payload, "success");
  }
}

async function checkRegisterStatus(showSuccess = false) {
  const username = String(dom.byId("registerUsername")?.value || "").trim();
  if (!username) {
    showNotice("registerCheckHint", "输入用户名后会自动检查是否已注册。", "info");
    return null;
  }
  const result = await requestJson(`/api/portal/auth/check?username=${encodeURIComponent(username)}`);
  if (result.registered) {
    showNotice("registerCheckHint", result.message || "该用户名已注册，请直接登录。", "warning");
  } else if (showSuccess) {
    showNotice("registerCheckHint", result.message || "该用户名尚未注册，可以继续创建账号。", "success");
  } else {
    showNotice("registerCheckHint", result.message || "该用户名尚未注册。", "info");
  }
  return result;
}

function ensureUserReady() {
  if (hasUserSession()) {
    return "user";
  }
  if (state.apiKey) {
    return "apiKey";
  }
  throw new Error("请先登录用户账号，或在下方高级模式连接 API Key");
}

async function handleRegister(event) {
  event.preventDefault();
  const username = String(dom.byId("registerUsername")?.value || "").trim();
  const password = String(dom.byId("registerPassword")?.value || "");
  const registerLocation = String(dom.byId("registerLocation")?.value || "").trim();

  if (!username || !password || !registerLocation) {
    throw new Error("用户名、密码、注册位置都不能为空");
  }
  if ((state.summary?.service?.role || state.summary?.meta?.role) && (state.summary?.service?.role || state.summary?.meta?.role) !== "master") {
    throw new Error("当前入口不是 master 主节点，注册位置校验未通过");
  }

  const check = await checkRegisterStatus(false);
  if (check?.registered) {
    throw new Error(check.message || "该用户名已注册，请直接登录");
  }

  const result = await requestJson("/api/portal/auth/register", {
    method: "POST",
    body: { username, password, register_location: registerLocation },
  });

  state.user = result?.user || state.user;
  state.isAuthenticated = true;
  await loadAuthMe();
  await loadUserData(true);
  showNotice("accountNotice", `注册成功，当前账号 ${username} 已登录。`, "success");
  renderResponse("注册成功", result, "success");
}

async function handleLogin(event) {
  event.preventDefault();
  const username = String(dom.byId("loginUsername")?.value || "").trim();
  const password = String(dom.byId("loginPassword")?.value || "");
  if (!username || !password) {
    throw new Error("请输入用户名和密码");
  }
  const result = await requestJson("/api/portal/auth/login", {
    method: "POST",
    body: { username, password },
  });
  await loadAuthMe();
  await loadUserData(true);
  renderResponse("登录成功", result, "success");
}

async function handleLogout() {
  await requestJson("/api/portal/auth/logout", { method: "POST" });
  state.user = null;
  state.isAuthenticated = false;
  state.userOverview = null;
  state.latestRedeem = null;
  renderUserAuth();
  renderUserOverview();
  renderRedeemResult();
  renderLogs([]);
  updateCodeExamples();
  showNotice("accountNotice", "你已退出用户登录。", "info");
}

async function handleRedeem(event) {
  event.preventDefault();
  if (!hasUserSession()) {
    throw new Error("请先登录用户账号，再兑换 CDK");
  }
  const code = String(dom.byId("redeemCodeInput")?.value || "").trim();
  if (!code) {
    throw new Error("请输入兑换码");
  }
  const result = await requestJson("/api/portal/redeem", {
    method: "POST",
    body: { code },
  });
  state.latestRedeem = result;
  renderRedeemResult();
  await loadUserData();
  showNotice("redeemNotice", result?.message || "兑换成功，次数已刷新。", "success");
  renderResponse("兑换成功", result, "success");
}

async function handleSolveSubmit(event) {
  event.preventDefault();
  const mode = ensureUserReady();
  const tokenIdRaw = String(dom.byId("solveTokenId")?.value || "").trim();
  const payload = {
    project_id: String(dom.byId("solveProjectId")?.value || "demo-project").trim() || "demo-project",
    action: String(dom.byId("solveAction")?.value || "IMAGE_GENERATION"),
  };
  if (tokenIdRaw) {
    payload.token_id = Number(tokenIdRaw);
  }

  const result = mode === "user"
    ? await requestJson("/api/portal/user/solve", { method: "POST", body: payload })
    : await requestJson("/api/v1/solve", { method: "POST", body: payload, apiKeyAuth: true });

  state.lastSessionId = String(result.session_id || "").trim();
  const sessionInput = dom.byId("sessionIdInput");
  if (sessionInput && state.lastSessionId) {
    sessionInput.value = state.lastSessionId;
  }
  updateCodeExamples();
  renderResponse("Solve 成功", result, "success");
  if (mode === "user") {
    await loadUserData();
  } else {
    await loadCompatData();
  }
}

async function handleScoreSubmit(event) {
  event.preventDefault();
  const mode = ensureUserReady();
  const payload = {
    website_url: String(dom.byId("scoreWebsiteUrl")?.value || "").trim(),
    website_key: String(dom.byId("scoreWebsiteKey")?.value || "").trim(),
    verify_url: String(dom.byId("scoreVerifyUrl")?.value || "").trim(),
    action: String(dom.byId("scoreAction")?.value || "homepage").trim() || "homepage",
    enterprise: !!dom.byId("scoreEnterprise")?.checked,
  };
  const result = mode === "user"
    ? await requestJson("/api/portal/user/custom-score", { method: "POST", body: payload })
    : await requestJson("/api/v1/custom-score", { method: "POST", body: payload, apiKeyAuth: true });

  renderResponse("Custom Score 成功", result, "success");
}

async function handleFinish() {
  const mode = ensureUserReady();
  const sessionId = String(dom.byId("sessionIdInput")?.value || "").trim();
  if (!sessionId) {
    throw new Error("请先填写 session_id");
  }
  const payload = { status: String(dom.byId("finishStatus")?.value || "success") };
  const result = mode === "user"
    ? await requestJson(`/api/portal/user/sessions/${encodeURIComponent(sessionId)}/finish`, { method: "POST", body: payload })
    : await requestJson(`/api/v1/sessions/${encodeURIComponent(sessionId)}/finish`, { method: "POST", body: payload, apiKeyAuth: true });

  renderResponse("Finish 成功", result, "success");
  if (mode === "user") {
    await loadUserData();
  } else {
    await loadCompatData();
  }
}

async function handleErrorReport() {
  const mode = ensureUserReady();
  const sessionId = String(dom.byId("sessionIdInput")?.value || "").trim();
  if (!sessionId) {
    throw new Error("请先填写 session_id");
  }
  const payload = { error_reason: String(dom.byId("errorReasonInput")?.value || "upstream_error").trim() || "upstream_error" };
  const result = mode === "user"
    ? await requestJson(`/api/portal/user/sessions/${encodeURIComponent(sessionId)}/error`, { method: "POST", body: payload })
    : await requestJson(`/api/v1/sessions/${encodeURIComponent(sessionId)}/error`, { method: "POST", body: payload, apiKeyAuth: true });

  renderResponse("Error 上报成功", result, "success");
  if (mode === "user") {
    await loadUserData();
  } else {
    await loadCompatData();
  }
}

async function handleConnect(event) {
  event.preventDefault();
  const input = dom.byId("apiKeyInput");
  const remember = dom.byId("rememberKey");
  state.apiKey = String(input?.value || "").trim();
  state.remember = !!remember?.checked;
  if (!state.apiKey) {
    throw new Error("请输入 API Key 后再连接");
  }
  persistApiKey();
  await loadCompatData(true);
  await loadLogs(false);
  updateModeBadges();
  updateCodeExamples();
}

function handleDisconnect() {
  clearStoredApiKey();
  state.remember = false;
  const input = dom.byId("apiKeyInput");
  const remember = dom.byId("rememberKey");
  if (input) {
    input.value = "";
  }
  if (remember) {
    remember.checked = false;
  }
  renderCompatibilityState();
  updateModeBadges();
  updateCodeExamples();
}

function bindCopyButtons() {
  document.querySelectorAll("[data-copy-target]").forEach((button) => {
    button.addEventListener("click", async () => {
      const target = dom.byId(button.getAttribute("data-copy-target"));
      if (!target) {
        return;
      }
      const raw = target.textContent || "";
      try {
        await navigator.clipboard.writeText(raw);
        const previousText = button.textContent;
        button.textContent = "已复制";
        setTimeout(() => {
          button.textContent = previousText || "复制";
        }, 1200);
      } catch (error) {
        renderResponse("复制失败", { detail: error.message || String(error) }, "error");
      }
    });
  });
}

function wireEvents() {
  dom.byId("refreshSummaryBtn")?.addEventListener("click", async () => {
    try {
      await loadSummary();
      renderResponse("概览已刷新", state.summary || {}, "success");
    } catch (error) {
      renderResponse("概览刷新失败", { detail: error.message || String(error) }, "error");
    }
  });

  dom.byId("loginForm")?.addEventListener("submit", async (event) => {
    try {
      await handleLogin(event);
    } catch (error) {
      showNotice("accountNotice", error.message || "登录失败", "error");
      renderResponse("登录失败", { detail: error.message || String(error) }, "error");
    }
  });

  dom.byId("registerUsername")?.addEventListener("blur", async () => {
    try {
      await checkRegisterStatus(false);
    } catch (_) {}
  });

  dom.byId("registerForm")?.addEventListener("submit", async (event) => {
    try {
      await handleRegister(event);
    } catch (error) {
      showNotice("accountNotice", error.message || "注册失败", "error");
      renderResponse("注册失败", { detail: error.message || String(error) }, "error");
    }
  });

  dom.byId("logoutBtn")?.addEventListener("click", async () => {
    try {
      await handleLogout();
      renderResponse("已退出登录", { success: true }, "success");
    } catch (error) {
      showNotice("accountNotice", error.message || "退出失败", "error");
      renderResponse("退出失败", { detail: error.message || String(error) }, "error");
    }
  });

  dom.byId("redeemForm")?.addEventListener("submit", async (event) => {
    try {
      await handleRedeem(event);
    } catch (error) {
      showNotice("redeemNotice", error.message || "兑换失败", "error");
      renderResponse("兑换失败", { detail: error.message || String(error) }, "error");
    }
  });

  dom.byId("refreshUserBtn")?.addEventListener("click", async () => {
    try {
      if (hasUserSession()) {
        await loadUserData();
      } else if (state.apiKey) {
        await loadCompatData();
      }
      renderResponse("用户数据已刷新", state.userOverview || {}, "success");
    } catch (error) {
      renderResponse("用户数据刷新失败", { detail: error.message || String(error) }, "error");
    }
  });

  const refreshLogs = async () => {
    try {
      await loadLogs(true);
    } catch (error) {
      renderResponse("日志刷新失败", { detail: error.message || String(error) }, "error");
    }
  };

  dom.byId("refreshLogsBtn")?.addEventListener("click", refreshLogs);
  dom.byId("logStatusFilter")?.addEventListener("change", refreshLogs);
  dom.byId("logProjectFilter")?.addEventListener("keydown", async (event) => {
    if (event.key !== "Enter") {
      return;
    }
    event.preventDefault();
    await refreshLogs();
  });

  dom.byId("solveForm")?.addEventListener("submit", async (event) => {
    try {
      await handleSolveSubmit(event);
    } catch (error) {
      renderResponse("Solve 失败", { detail: error.message || String(error) }, "error");
    }
  });

  dom.byId("scoreForm")?.addEventListener("submit", async (event) => {
    try {
      await handleScoreSubmit(event);
    } catch (error) {
      renderResponse("Custom Score 失败", { detail: error.message || String(error) }, "error");
    }
  });

  dom.byId("finishBtn")?.addEventListener("click", async () => {
    try {
      await handleFinish();
    } catch (error) {
      renderResponse("Finish 失败", { detail: error.message || String(error) }, "error");
    }
  });

  dom.byId("errorBtn")?.addEventListener("click", async () => {
    try {
      await handleErrorReport();
    } catch (error) {
      renderResponse("Error 上报失败", { detail: error.message || String(error) }, "error");
    }
  });

  dom.byId("useLastSessionBtn")?.addEventListener("click", () => {
    if (!state.lastSessionId) {
      renderResponse("最近 Session 不存在", { detail: "请先成功执行一次 solve。" }, "error");
      return;
    }
    const input = dom.byId("sessionIdInput");
    if (input) {
      input.value = state.lastSessionId;
    }
    renderResponse("已回填最近 Session", { session_id: state.lastSessionId }, "success");
  });

  dom.byId("connectForm")?.addEventListener("submit", async (event) => {
    try {
      await handleConnect(event);
      renderResponse("API Key 已连接", { success: true }, "success");
    } catch (error) {
      showNotice("compatNotice", error.message || "API Key 连接失败", "error");
      renderResponse("API Key 连接失败", { detail: error.message || String(error) }, "error");
    }
  });

  dom.byId("disconnectBtn")?.addEventListener("click", handleDisconnect);

  bindCopyButtons();
}

async function bootstrap() {
  restoreApiKey();
  renderUserAuth();
  renderUserOverview();
  renderCompatibilityState();
  renderRedeemResult();
  updateModeBadges();
  updateCodeExamples();
  wireEvents();

  try {
    await loadSummary();
  } catch (error) {
    renderResponse("概览加载失败", { detail: error.message || String(error) }, "error");
  }

  await loadAuthMe();
  if (hasUserSession()) {
    try {
      await loadUserData(true);
    } catch (error) {
      showNotice("accountNotice", error.message || "用户数据加载失败", "error");
    }
  } else if (state.apiKey) {
    try {
      await loadCompatData(true);
      await loadLogs(false);
    } catch (error) {
      showNotice("compatNotice", error.message || "API Key 自动恢复失败", "error");
    }
  }
}

window.addEventListener("DOMContentLoaded", bootstrap);
