// 场景“普通下单”：一次迭代是一个用户的一次购买。
// 下单 -> 轮询到离开 creating -> 按 PAY_RATIO 支付、按 CANCEL_RATIO 取消、其余不动（等系统超时关单）。
import { arrival, bindRun, setupRun } from './lib/config.js';
import * as api from './lib/api.js';
import * as m from './lib/metrics.js';
import { uniformSku, uniformUser } from './lib/pick.js';
import { cfg } from './lib/config.js';

export const options = m.baseOptions({ order: arrival() });

export function setup() {
  return setupRun('order');
}

export default function (data) {
  bindRun(data);
  const user = uniformUser();
  const sku = uniformSku();
  const o = api.placeOrder(user, sku, 1);
  if (o.orderId === null) return;
  const st = api.pollLanded(user, o.orderId, o.acceptedAtMs);
  if (st !== 'pending_payment') return;
  const r = Math.random();
  if (r < cfg.payRatio) {
    const res = api.payOrder(user, o.orderId);
    if (res.status === 202) {
      m.paid.add(1);
      api.getPayment(user, o.orderId); // 只查一次，不等终态
    }
  } else if (r < cfg.payRatio + cfg.cancelRatio) {
    const res = api.cancelOrder(user, o.orderId);
    if (res.status === 200) m.cancelled.add(1);
  }
}

export function handleSummary(data) {
  return m.makeSummary(data, 'order');
}
