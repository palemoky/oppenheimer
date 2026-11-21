// 集中读取环境变量。所有场景和 lib 只从这里拿配置。
// k6 的每个虚拟用户各有一份模块状态；这里的值在初始化阶段算好，运行中不再改（runId 除外，见 bindRun）。

const env = __ENV;

const USER_LIMIT = 1099511627776; // 2^40：对账工具把 >= 2^40 的用户号当作造数排除

function str(name, def) {
  const v = env[name];
  return v === undefined || v === '' ? def : v;
}

function num(name, def) {
  const v = env[name];
  if (v === undefined || v === '') return def;
  const n = Number(v);
  if (!Number.isFinite(n)) throw new Error(`${name}=${v} 不是数字`);
  return n;
}

function list(name, def) {
  return str(name, def)
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s !== '');
}

// "100:3m,125:3m" -> [{target:100,duration:'3m'},...]
function parseStages(s) {
  if (!s) return [];
  return s.split(',').map((part) => {
    const [t, d] = part.trim().split(':');
    const target = Number(t);
    if (!Number.isFinite(target) || !d) throw new Error(`STAGES 格式错误: ${part}（应为 速率:时长）`);
    return { target, duration: d };
  });
}

const stages = parseStages(env.STAGES);
const rate = num('RATE', 20);
const peak = stages.length > 0 ? Math.max(...stages.map((x) => x.target)) : rate;

export const cfg = {
  baseUrl: str('BASE_URL', 'http://127.0.0.1:8080').replace(/\/+$/, ''),
  hostHeader: str('HOST_HEADER', ''),
  runIdEnv: str('RUN_ID', ''),
  runId: str('RUN_ID', ''), // 默认值在 setup 里生成，见 setupRun / bindRun
  userBase: num('USER_BASE', 549755813888), // 2^39
  userCount: num('USER_COUNT', 1000000),
  skus: list('SKUS', '1001,1002'),
  merchants: list('MERCHANTS', '1,2'),
  rate,
  duration: str('DURATION', '30s'),
  stages,
  peakRate: peak,
  maxVUs: num('MAX_VUS', Math.max(50, Math.ceil(peak * 2))),
  preVUs: num('PRE_VUS', 0), // 0 表示按峰值速率估算
  pollIntervalMs: num('POLL_INTERVAL_MS', 200),
  pollMaxIntervalMs: num('POLL_MAX_INTERVAL_MS', 2000),
  pollBackoff: Math.max(num('POLL_BACKOFF', 2), 1),
  pollTimeoutMs: num('POLL_TIMEOUT_MS', 10000),
  payRatio: num('PAY_RATIO', 0.5),
  cancelRatio: num('CANCEL_RATIO', 0.2),
  reqTimeout: str('REQ_TIMEOUT', '10s'),
  outDir: str('OUT_DIR', '.'),
};

// 用户号必须落在对账范围内：超出 2^40 的订单会被对账工具当作造数排除。
if (cfg.userBase + cfg.userCount > USER_LIMIT) {
  throw new Error(
    `USER_BASE + USER_COUNT = ${cfg.userBase + cfg.userCount} 超过 2^40 (${USER_LIMIT})：` +
      `用户号 >= 2^40 的订单不会被对账。请调小 USER_BASE 或 USER_COUNT`,
  );
}
if (cfg.skus.length === 0) throw new Error('SKUS 不能为空');
if (cfg.payRatio + cfg.cancelRatio > 1) throw new Error('PAY_RATIO + CANCEL_RATIO 不能超过 1');

function pad(n) {
  return n < 10 ? `0${n}` : `${n}`;
}

// 形如 20261117T150600-order
export function newRunId(scenario) {
  const d = new Date();
  const ts =
    `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}T` +
    `${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}`;
  return `${ts}-${scenario}`;
}

// RUN_ID 没设时，所有虚拟用户必须用同一个值，所以只在 setup（全局只跑一次）里生成，
// 通过 setup 的返回值传给每个虚拟用户，在 default 函数开头用 bindRun 绑定。
export function setupRun(scenario) {
  return { runId: cfg.runIdEnv || newRunId(scenario) };
}

export function bindRun(data) {
  cfg.runId = data.runId;
}

// 开环 executor：给了 STAGES 用 ramping-arrival-rate，否则用 constant-arrival-rate。
// o: {rate, duration, stages, startRate, exec, maxVUs, preVUs}，缺省取 cfg。
export function arrival(o = {}) {
  const st = o.stages !== undefined ? o.stages : cfg.stages;
  const r = o.rate !== undefined ? o.rate : cfg.rate;
  const peakRate = st.length > 0 ? Math.max(...st.map((x) => x.target)) : r;
  const maxVUs = o.maxVUs !== undefined ? o.maxVUs : cfg.maxVUs;
  const pre = Math.min(maxVUs, o.preVUs || cfg.preVUs || Math.max(20, Math.ceil(peakRate / 2)));
  const s =
    st.length > 0
      ? {
          executor: 'ramping-arrival-rate',
          startRate: o.startRate !== undefined ? o.startRate : st[0].target,
          timeUnit: '1s',
          stages: st,
          preAllocatedVUs: pre,
          maxVUs,
        }
      : {
          executor: 'constant-arrival-rate',
          rate: r,
          timeUnit: '1s',
          duration: o.duration || cfg.duration,
          preAllocatedVUs: pre,
          maxVUs,
        };
  if (o.exec) s.exec = o.exec;
  return s;
}
