// 场景“混合读写”：一次迭代按权重选一种操作；用户和 SKU 用 Zipf 分布选，少数用户和商品被反复访问。
import { arrival, bindRun, cfg, setupRun } from './lib/config.js';
import * as api from './lib/api.js';
import * as m from './lib/metrics.js';
import { uniformMerchant, uniformUser, zipfSku, zipfUser } from './lib/pick.js';

// 操作权重
const WEIGHTS = [
  ['getStock', 50],
  ['getOrder', 25],
  ['listOrders', 10],
  ['listMerchantOrders', 5],
  ['createOrder', 8],
  ['payOrder', 2],
];
const TOTAL = WEIGHTS.reduce((a, w) => a + w[1], 0);

const ZIPF_S = Number(__ENV.ZIPF_S || 1.1); // Zipf 指数
const REMEMBER = 20; // 每个虚拟用户记住最近受理的订单数
const PAY_MIN_AGE_MS = 1000; // 受理不到这么久的订单还没落库，不拿来支付

export const options = m.baseOptions({ mixed: arrival() });

export function setup() {
  return setupRun('mixed');
}

// 本虚拟用户最近受理的订单：{user, id, at, paid}
const recent = [];

function remember(user, id, at) {
  recent.push({ user, id, at, paid: false });
  if (recent.length > REMEMBER) recent.shift();
}

function pickOp() {
  let r = Math.random() * TOTAL;
  for (const w of WEIGHTS) {
    r -= w[1];
    if (r < 0) return w[0];
  }
  return WEIGHTS[0][0];
}

function doCreate() {
  const user = zipfUser(ZIPF_S);
  const o = api.placeOrder(user, zipfSku(ZIPF_S), 1);
  if (o.orderId !== null) remember(user, o.orderId, o.acceptedAtMs);
}

export default function (data) {
  bindRun(data);
  let op = pickOp();
  if (op === 'getOrder' && recent.length === 0) op = 'createOrder';
  let target = null;
  if (op === 'payOrder') {
    const now = Date.now();
    const c = recent.filter((x) => !x.paid && now - x.at >= PAY_MIN_AGE_MS);
    if (c.length === 0) op = 'createOrder';
    else target = c[Math.floor(Math.random() * c.length)];
  }
  switch (op) {
    case 'getStock':
      api.getStock(zipfUser(ZIPF_S), zipfSku(ZIPF_S));
      break;
    case 'getOrder': {
      const t = recent[Math.floor(Math.random() * recent.length)];
      api.getOrder(t.user, t.id);
      break;
    }
    case 'listOrders':
      api.listOrders(zipfUser(ZIPF_S));
      break;
    case 'listMerchantOrders':
      api.listMerchantOrders(uniformUser(), uniformMerchant());
      break;
    case 'payOrder': {
      target.paid = true;
      const res = api.payOrder(target.user, target.id);
      if (res.status === 202) m.paid.add(1);
      break;
    }
    default:
      doCreate();
  }
}

export function handleSummary(data) {
  const out = m.makeSummary(data, 'mixed');
  out.stdout += `混合读写: Zipf 指数 ${ZIPF_S}，用户 ${cfg.userCount} 个，SKU ${cfg.skus.length} 个\n`;
  return out;
}
