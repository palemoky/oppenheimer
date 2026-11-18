// 8 个接口各一个函数，外加下单和轮询两个组合动作。
// 每个请求打 tag name=<operationId>，不让带 ID 的 URL 成为指标标签。
import http from 'k6/http';
import exec from 'k6/execution';
import { sleep } from 'k6';
import { cfg } from './config.js';
import * as m from './metrics.js';

// 预期的状态码：2xx、409（库存不足、限购、状态不允许）、429（限流、背压、读名额满）。
// 其余（5xx、超时、连接错误 status=0，以及 400/401/404）算失败，进 http_req_failed。
http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }, 409, 429));

function headers(user, extra) {
  const h = { 'X-User-Id': String(user), 'X-Load-Run': cfg.runId };
  if (cfg.hostHeader) h.Host = cfg.hostHeader;
  if (extra) Object.assign(h, extra);
  return h;
}

// 登记失败/非 2xx 的原因码。body 只在 status >= 400 时才解析。
function noteReason(res, op, group) {
  if (res.status >= 200 && res.status < 300) return;
  let reason;
  if (res.status === 0) {
    reason = res.error_code === 1050 ? 'TIMEOUT' : 'CONN_ERROR';
  } else {
    let code = '';
    if (res.body) {
      try {
        code = JSON.parse(res.body).error.code || '';
      } catch (e) {
        code = '';
      }
    }
    reason = m.normReason(code);
    if (reason === 'OTHER' && res.status >= 500) reason = 'HTTP_5XX';
  }
  const tags = { reason, op };
  if (group) tags.load_group = group;
  m.reasonCnt.add(1, tags);
}

// opts: {body, discard, key, group}
function call(op, method, path, user, opts = {}) {
  const tags = { name: op };
  if (opts.group) tags.load_group = opts.group;
  const params = {
    headers: headers(user, opts.key ? { 'Idempotency-Key': opts.key, 'Content-Type': 'application/json' } : null),
    tags,
    timeout: cfg.reqTimeout,
    responseType: opts.discard ? 'none' : 'text',
  };
  const res = http.request(method, `${cfg.baseUrl}${path}`, opts.body === undefined ? null : opts.body, params);
  noteReason(res, op, opts.group);
  return res;
}

function parse(res) {
  if (res.status < 200 || res.status >= 300 || !res.body) return null;
  try {
    return JSON.parse(res.body);
  } catch (e) {
    return null;
  }
}

export function listOrders(user, group) {
  return call('listOrders', 'GET', '/api/v1/orders?limit=20', user, { discard: true, group });
}

// 返回 {res, order}；202 时 order 是响应里的订单（k6 的响应对象不能加属性，所以另包一层）。
export function createOrder(user, skuId, quantity, key, group) {
  const body = JSON.stringify({ items: [{ sku_id: String(skuId), quantity }] });
  const res = call('createOrder', 'POST', '/api/v1/orders', user, { body, key, group });
  const tags = group ? { load_group: group } : {};
  m.createCnt.add(1, tags);
  let order = null;
  if (res.status === 202) {
    order = parse(res);
    m.acceptMs.add(res.timings.duration, tags);
  }
  return { res, order };
}

export function getOrder(user, orderId, group) {
  const res = call('getOrder', 'GET', `/api/v1/orders/${orderId}`, user, { group });
  return { res, order: parse(res) };
}

export function cancelOrder(user, orderId, group) {
  return call('cancelOrder', 'POST', `/api/v1/orders/${orderId}/cancel`, user, { discard: true, group });
}

export function payOrder(user, orderId, group) {
  return call('payOrder', 'POST', `/api/v1/orders/${orderId}/pay`, user, { discard: true, group });
}

export function getPayment(user, orderId, group) {
  return call('getPayment', 'GET', `/api/v1/orders/${orderId}/payment`, user, { discard: true, group });
}

export function getStock(user, skuId, group) {
  return call('getStock', 'GET', `/api/v1/skus/${skuId}/stock`, user, { discard: true, group });
}

export function listMerchantOrders(user, merchantId, group) {
  return call('listMerchantOrders', 'GET', `/api/v1/merchants/${merchantId}/orders?limit=20`, user, {
    discard: true,
    group,
  });
}

// ---- 组合动作 ----

// 幂等键：lt-<RUN_ID>-<虚拟用户号>-<该虚拟用户内的序号>。虚拟用户号让它在全局唯一。
let seq = 0;
function nextKey() {
  seq++;
  return `lt-${cfg.runId}-${exec.vu.idInTest}-${seq}`;
}

// 下单。被受理（202）时输出一行订单号清单（console.log，由包装脚本提取成 orders.jsonl）。
// 返回 {res, orderId, key, acceptedAtMs}；orderId 为 null 表示没被受理。
export function placeOrder(user, skuId, quantity, group) {
  const key = nextKey();
  const { res, order } = createOrder(user, skuId, quantity, key, group);
  if (res.status !== 202 || !order || !order.order_id) {
    return { res, orderId: null, key, acceptedAtMs: 0 };
  }
  const now = Date.now();
  const orderId = String(order.order_id); // ID 一律当字符串
  const tags = group ? { load_group: group } : {};
  m.accepted.add(1, tags);
  console.log(
    JSON.stringify({
      run: cfg.runId,
      user_id: String(user),
      order_id: orderId,
      idempotency_key: key,
      sku_id: String(skuId),
      quantity,
      accepted_at: new Date(now).toISOString(),
    }),
  );
  return { res, orderId, key, acceptedAtMs: now };
}

// 轮询到订单离开 creating。返回最终状态字符串，超时返回 'timeout'。
export function pollLanded(user, orderId, acceptedAtMs, group) {
  const deadline = acceptedAtMs + cfg.pollTimeoutMs;
  const tags = group ? { load_group: group } : {};
  for (;;) {
    sleep(cfg.pollIntervalMs / 1000);
    const { res, order } = getOrder(user, orderId, group);
    if (res.status === 200 && order && order.status !== 'creating') {
      const st = order.status;
      m.landingMs.add(Date.now() - acceptedAtMs, tags);
      if (st === 'failed') m.orderFailed.add(1, tags);
      else m.landed.add(1, tags);
      return st;
    }
    // 429（READ_BUSY、限流）、404、5xx：继续轮询到超时，请求本身的结果已记入原因码和失败率。
    if (Date.now() >= deadline) {
      m.pollTimeout.add(1, tags);
      return 'timeout';
    }
  }
}
