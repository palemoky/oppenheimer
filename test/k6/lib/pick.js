// 选用户、选 SKU：均匀分布和 Zipf 分布。
import { cfg } from './config.js';

// Zipf：第 k 名（k 从 1 起）的概率正比于 k^-s。
// n 不大（<= 10000）时预先算好累积分布，抽样时二分查找；
// n 很大（比如 100 万个用户）时一张表每个虚拟用户要几 MB，改用连续分布的反函数，
// 常数在构造时算好，抽样 O(1)、不占内存。后者是近似（n 很大时误差可忽略）。
export class Zipf {
  constructor(n, s) {
    this.n = n;
    this.s = s;
    if (n <= 10000) {
      const cdf = new Float64Array(n);
      let sum = 0;
      for (let k = 1; k <= n; k++) {
        sum += Math.pow(k, -s);
        cdf[k - 1] = sum;
      }
      for (let i = 0; i < n; i++) cdf[i] /= sum;
      this.cdf = cdf;
    } else {
      this.cdf = null;
      this.one = Math.abs(s - 1) < 1e-9;
      this.a = this.one ? 0 : Math.pow(n, 1 - s) - 1;
    }
  }

  // 返回 [0, n) 的名次（0 最热）
  sample() {
    const u = Math.random();
    if (this.cdf) {
      let lo = 0;
      let hi = this.n - 1;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (this.cdf[mid] < u) lo = mid + 1;
        else hi = mid;
      }
      return lo;
    }
    const x = this.one ? Math.pow(this.n, u) : Math.pow(1 + u * this.a, 1 / (1 - this.s));
    return Math.min(this.n - 1, Math.max(0, Math.floor(x) - 1));
  }
}

// 用户号：USER_BASE + [0, USER_COUNT)。返回数字（2^53 以内精确），发请求时再转十进制字符串。
export function uniformUser() {
  return cfg.userBase + Math.floor(Math.random() * cfg.userCount);
}

export function uniformSku() {
  return cfg.skus[Math.floor(Math.random() * cfg.skus.length)];
}

export function uniformMerchant() {
  return cfg.merchants[Math.floor(Math.random() * cfg.merchants.length)];
}

// 惰性构造（每个虚拟用户各一份）。
let zUser = null;
let zSku = null;

export function zipfUser(s) {
  if (zUser === null) zUser = new Zipf(cfg.userCount, s);
  return cfg.userBase + zUser.sample();
}

export function zipfSku(s) {
  if (zSku === null) zSku = new Zipf(cfg.skus.length, s);
  return cfg.skus[zSku.sample()];
}
