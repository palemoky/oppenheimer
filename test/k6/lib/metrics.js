// 自定义指标、公共阈值、汇总输出。
import { Counter, Trend } from 'k6/metrics';
import { cfg } from './config.js';

// 受理耗时：下单请求本身（只记被受理的 202）。
export const acceptMs = new Trend('lt_accept_ms', true);
// 落库耗时：从下单返回到轮询看到订单离开 creating。
export const landingMs = new Trend('lt_landing_ms', true);
// 下单请求总数（不论结果），用来算比例。
export const createCnt = new Counter('lt_create');
// 各种非 2xx 或失败的原因码计数，tag reason 取值有限（见 REASONS）。
export const reasonCnt = new Counter('lt_reason');
// 业务结果。
export const accepted = new Counter('lt_accepted');
export const landed = new Counter('lt_landed'); // 落库成功：离开 creating 进入 pending_payment
export const orderFailed = new Counter('lt_order_failed'); // 落库失败：状态 failed
export const paid = new Counter('lt_paid'); // 支付被受理（202）
export const cancelled = new Counter('lt_cancelled'); // 取消成功（200）
export const pollTimeout = new Counter('lt_poll_timeout'); // 轮询超时仍是 creating

// 原因码的有限集合。不在集合里的一律记成 OTHER，避免标签基数失控。
export const REASONS = [
  'RATE_LIMITED',
  'BACKLOGGED',
  'READ_BUSY',
  'INSUFFICIENT_STOCK',
  'PURCHASE_LIMIT_EXCEEDED',
  'IDEMPOTENCY_KEY_REUSED',
  'UNAVAILABLE',
  'DEADLINE_EXCEEDED',
  'STOCK_GATE_UNAVAILABLE',
  'ORDER_LANDING_UNAVAILABLE',
  'INVENTORY_UNAVAILABLE',
  'ORDER_QUEUE_UNAVAILABLE',
  'ORDER_UNAVAILABLE',
  'ARCHIVE_UNAVAILABLE',
  'FEATURE_DISABLED',
  'INVALID_ARGUMENT',
  'UNAUTHENTICATED',
  'NOT_FOUND',
  'FAILED_PRECONDITION',
  'TIMEOUT',
  'CONN_ERROR',
  'HTTP_5XX',
  'OTHER',
];
const REASON_SET = {};
for (const r of REASONS) REASON_SET[r] = true;

export function normReason(code) {
  return REASON_SET[code] ? code : 'OTHER';
}

export const OPS = [
  'listOrders',
  'createOrder',
  'getOrder',
  'cancelOrder',
  'payOrder',
  'getPayment',
  'getStock',
  'listMerchantOrders',
];

function opKeys() {
  const out = {};
  for (const o of OPS) out[`http_reqs{name:${o}}`] = ['count>=0'];
  return out;
}

function reasonKeys(extra) {
  const out = {};
  for (const r of REASONS) out[`lt_reason{reason:${r}${extra}}`] = ['count>=0'];
  return out;
}

// 公共阈值：下单请求 p99 < 200ms；失败（5xx、超时、连接错误）比例 < 0.1%。
// 409 和 429 在 api.js 里登记为预期状态码，不进 http_req_failed。
// 带 count>=0 的条目是为了让带标签的子指标出现在汇总里（k6 只为有阈值的子指标统计），它们永远通过。
export function thresholds(extra = {}) {
  return Object.assign(
    {
      'http_req_duration{name:createOrder}': ['p(99)<200'],
      http_req_failed: ['rate<0.001'],
    },
    reasonKeys(''),
    opKeys(),
    extra,
  );
}

// 按组（A/B）拆开的子指标，用于 ratelimit。
export function groupThresholds(groups) {
  const out = {};
  for (const g of groups) {
    out[`lt_create{load_group:${g}}`] = ['count>=0'];
    out[`lt_accepted{load_group:${g}}`] = ['count>=0'];
    out[`lt_accept_ms{load_group:${g}}`] = ['p(99)>=0'];
    out[`http_req_duration{name:createOrder,load_group:${g}}`] = ['p(99)>=0'];
    out[`lt_reason{reason:RATE_LIMITED,load_group:${g}}`] = ['count>=0'];
    out[`lt_reason{reason:BACKLOGGED,load_group:${g}}`] = ['count>=0'];
  }
  return out;
}

// 所有场景共用的 options 片段。url 标签基数高，去掉；请求用 name 标签区分接口。
export function baseOptions(scenarios, extraThresholds = {}) {
  return {
    scenarios,
    thresholds: thresholds(extraThresholds),
    summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
    systemTags: [
      'proto',
      'status',
      'method',
      'name',
      'scenario',
      'expected_response',
      'error',
      'error_code',
      'check',
    ],
  };
}

// ---- 汇总 ----

function vals(data, key) {
  const m = data.metrics[key];
  if (!m) return null;
  return m.values || m;
}

function cnt(data, key) {
  const v = vals(data, key);
  return v && v.count !== undefined ? v.count : 0;
}

function ms(v) {
  return v === undefined || v === null ? '-' : `${v.toFixed(1)}ms`;
}

function trendLine(data, key) {
  const v = vals(data, key);
  if (!v) return '-';
  return `avg=${ms(v.avg)} med=${ms(v.med)} p90=${ms(v['p(90)'])} p95=${ms(v['p(95)'])} p99=${ms(v['p(99)'])} max=${ms(v.max)}`;
}

// 返回 handleSummary 要的 map：stdout 一份简短文本，summary.json 一份完整数据。
// opts.groups：按组拆分的场景（ratelimit）。
export function makeSummary(data, scenario, opts = {}) {
  const L = [];
  const iters = cnt(data, 'iterations');
  const dropped = cnt(data, 'dropped_iterations');
  const secs = data.state && data.state.testRunDurationMs ? data.state.testRunDurationMs / 1000 : 0;
  L.push(`== k6 场景 ${scenario}  时长 ${secs.toFixed(1)}s ==`);
  L.push(`迭代数 ${iters}，因虚拟用户不够未发出的迭代 dropped_iterations=${dropped}${dropped > 0 ? '  [!] 实际速率低于目标，结果不可当作目标速率下的结果' : ''}`);
  const vu = vals(data, 'vus_max');
  if (vu) L.push(`vus_max=${vu.value !== undefined ? vu.value : vu.max}`);
  const reqs = vals(data, 'http_reqs');
  const failed = vals(data, 'http_req_failed');
  if (reqs) L.push(`http 请求 ${reqs.count}（${reqs.rate.toFixed(1)}/s），失败比例 ${failed ? (100 * (failed.rate !== undefined ? failed.rate : 0)).toFixed(3) : '-'}%（5xx、超时、连接错误及意外的 4xx；409/429 不算）`);
  L.push(`下单请求 http_req_duration{createOrder}: ${trendLine(data, 'http_req_duration{name:createOrder}')}`);
  L.push(`受理耗时 lt_accept_ms: ${trendLine(data, 'lt_accept_ms')}`);
  L.push(`落库耗时 lt_landing_ms: ${trendLine(data, 'lt_landing_ms')}`);
  L.push(
    `业务结果: 下单请求 ${cnt(data, 'lt_create')}  受理 ${cnt(data, 'lt_accepted')}  落库成功 ${cnt(data, 'lt_landed')}  落库失败 ${cnt(data, 'lt_order_failed')}  ` +
      `已支付(受理) ${cnt(data, 'lt_paid')}  已取消 ${cnt(data, 'lt_cancelled')}  轮询超时 ${cnt(data, 'lt_poll_timeout')}`,
  );
  const ops = [];
  for (const o of OPS) {
    const c = cnt(data, `http_reqs{name:${o}}`);
    if (c > 0) ops.push(`${o}=${c}`);
  }
  L.push(`各接口请求数: ${ops.join(' ')}`);
  const rs = [];
  for (const r of REASONS) {
    const c = cnt(data, `lt_reason{reason:${r}}`);
    if (c > 0) rs.push(`${r}=${c}`);
  }
  L.push(`原因码计数: ${rs.length > 0 ? rs.join(' ') : '无'}`);
  for (const g of opts.groups || []) {
    const cr = cnt(data, `lt_create{load_group:${g}}`);
    const ac = cnt(data, `lt_accepted{load_group:${g}}`);
    const rl = cnt(data, `lt_reason{reason:RATE_LIMITED,load_group:${g}}`);
    const bl = cnt(data, `lt_reason{reason:BACKLOGGED,load_group:${g}}`);
    const pct = (x) => (cr > 0 ? `${((100 * x) / cr).toFixed(1)}%` : '-');
    L.push(`组 ${g}: 下单请求 ${cr}  受理 ${ac}(${pct(ac)})  RATE_LIMITED ${rl}(${pct(rl)})  BACKLOGGED ${bl}(${pct(bl)})`);
    L.push(`组 ${g}: 下单请求耗时 ${trendLine(data, `http_req_duration{name:createOrder,load_group:${g}}`)}`);
    L.push(`组 ${g}: 受理耗时 ${trendLine(data, `lt_accept_ms{load_group:${g}}`)}`);
  }
  const th = [];
  for (const k of Object.keys(data.metrics)) {
    const t = data.metrics[k].thresholds;
    if (!t) continue;
    for (const e of Object.keys(t)) {
      if (e.indexOf('>=0') >= 0) continue; // 仅为产生子指标的占位阈值
      th.push(`${t[e].ok ? 'PASS' : 'FAIL'} ${k} ${e}`);
    }
  }
  L.push('阈值:');
  for (const x of th) L.push(`  ${x}`);
  L.push('');
  const full = Object.assign({}, data, { load_config: cfg });
  const out = { stdout: L.join('\n') };
  out[`${cfg.outDir}/summary.json`] = JSON.stringify(full);
  return out;
}
