// 场景“限流验证”：两组并行的下单流量，指标按组（load_group=A/B）拆开。
// A 组：少数几个用户以远超限额的速率下单。B 组：大量正常用户以低速率下单。
// 要回答：A 组被限流（429 RATE_LIMITED）的比例；B 组的受理耗时和成功率有没有被 A 组拖累。
// 两组都只下单、不轮询（只量受理这一步）。
import { arrival, bindRun, cfg, setupRun } from './lib/config.js';
import * as api from './lib/api.js';
import * as m from './lib/metrics.js';
import { uniformSku } from './lib/pick.js';

const A_USERS = Number(__ENV.A_USERS || 10);
const A_RATE = Number(__ENV.A_RATE || 500); // A 组合计每秒下单数
const B_RATE = Number(__ENV.B_RATE || 50);
if (A_USERS < 1 || A_USERS >= cfg.userCount) throw new Error('A_USERS 必须在 [1, USER_COUNT) 内');

function noStages(rate) {
  return arrival({ stages: [], rate, maxVUs: Number(__ENV.MAX_VUS || Math.max(50, rate * 2)),preVUs: Math.ceil(rate / 5) + 10 });
}

export const options = m.baseOptions(
  {
    groupA: Object.assign(noStages(A_RATE), { exec: 'groupA' }),
    groupB: Object.assign(noStages(B_RATE), { exec: 'groupB' }),
  },
  m.groupThresholds(['A', 'B']),
);

export function setup() {
  return setupRun('ratelimit');
}

export function groupA(data) {
  bindRun(data);
  const user = cfg.userBase + Math.floor(Math.random() * A_USERS);
  api.placeOrder(user, uniformSku(), 1, 'A');
}

export function groupB(data) {
  bindRun(data);
  // B 组用户取 A 组用户之后的范围，两组不重叠。
  const user = cfg.userBase + A_USERS + Math.floor(Math.random() * (cfg.userCount - A_USERS));
  api.placeOrder(user, uniformSku(), 1, 'B');
}

export function handleSummary(data) {
  const out = m.makeSummary(data, 'ratelimit', { groups: ['A', 'B'] });
  out.stdout += `限流验证: A 组 ${A_USERS} 个用户合计 ${A_RATE}/s，B 组 ${B_RATE}/s\n`;
  return out;
}
