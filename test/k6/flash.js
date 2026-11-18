// 场景“秒杀”：所有人抢同一个 SKU（FLASH_SKU，必填）。
// 到达速率在 RAMP（默认 5s）内从 0 升到 RATE，保持 DURATION。
// 一次迭代：查一次库存 -> 下单；被受理的轮询到落库，不支付。库存不足（409）是预期结果，单独计数。
import { arrival, bindRun, cfg, setupRun } from './lib/config.js';
import * as api from './lib/api.js';
import * as m from './lib/metrics.js';
import { uniformUser } from './lib/pick.js';

const FLASH_SKU = __ENV.FLASH_SKU;
if (!FLASH_SKU) throw new Error('FLASH_SKU 必填，例如 FLASH_SKU=1001');
const RAMP = __ENV.RAMP || '5s';

export const options = m.baseOptions({
  flash: arrival({
    stages: [
      { target: cfg.rate, duration: RAMP },
      { target: cfg.rate, duration: cfg.duration },
    ],
    startRate: 0,
  }),
});

export function setup() {
  return setupRun('flash');
}

export default function (data) {
  bindRun(data);
  const user = uniformUser();
  api.getStock(user, FLASH_SKU);
  const o = api.placeOrder(user, FLASH_SKU, 1);
  if (o.orderId === null) return;
  api.pollLanded(user, o.orderId, o.acceptedAtMs);
}

export function handleSummary(data) {
  const out = m.makeSummary(data, 'flash');
  const get = (k) => {
    const x = data.metrics[k];
    return x ? (x.values || x).count || 0 : 0;
  };
  out.stdout += `秒杀: 受理 ${get('lt_accepted')}  落库成功 ${get('lt_landed')}  库存不足(409 INSUFFICIENT_STOCK) ${get('lt_reason{reason:INSUFFICIENT_STOCK}')}\n`;
  return out;
}
