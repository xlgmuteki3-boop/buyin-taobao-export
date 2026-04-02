// ==UserScript==
// @name         百应+淘宝 商品一键导出
// @namespace    https://github.com/xlgmuteki3-boop/buyin-taobao-export
// @version      3.13.28
// @description  抖音百应、淘宝/天猫商品导出 CSV 或写入飞书多维表格，支持列名映射与自动建列
// @author       xlgmuteki3-boop
// @license      MIT
// @match        https://buyin.jinritemai.com/*
// @match        https://*.buyin.jinritemai.com/*
// @match        https://*.taobao.com/*
// @match        https://*.world.taobao.com/*
// @match        https://*.tmall.com/*
// @match        https://*.tmall.hk/*
// @run-at       document-start
// @grant        GM_xmlhttpRequest
// @grant        GM_setValue
// @grant        GM_getValue
// @grant        GM_registerMenuCommand
// @connect      open.feishu.cn
// @downloadURL  https://raw.githubusercontent.com/xlgmuteki3-boop/buyin-taobao-export/main/buyin_taobao_export.user.js
// @updateURL    https://raw.githubusercontent.com/xlgmuteki3-boop/buyin-taobao-export/main/buyin_taobao_export.user.js
// ==/UserScript==

(function () {
  'use strict';

  const isBuyin = () => /buyin\.jinritemai\.com/.test(location.hostname);
  const isTaobao = () => /taobao\.com|tmall\.com|world\.taobao/.test(location.hostname);

  /** 内部字段 key → 飞书多维表格列标题（与表中列名一致）；可在「飞书配置」里用 JSON 覆盖 */
  const FEISHU_DEFAULT_FIELD_MAP = {
    title: '标题',
    price: '价格',
    commission: '佣金',
    sales: '销量',
    link: '商品链接',
    imgSrc: '图片',
    shopName: '店铺名称',
    shopLink: '店铺链接',
    platform: '平台',
    shopTag: '店铺标记',
    monitorTimes: '监控次数',
    monitorIndex: '监控轮次',
    inspectCycle: '巡检周期',
    shelfTime: '上架时间',
    recordType: '录入类型',
    guarantee: '保障',
    deliveryTime: '发货时效',
  };
  /** 写入飞书时参与映射的字段顺序（与 rowToFeishuFields 一致）；内部键 _ge* 不传飞书 */
  const FEISHU_UPLOAD_FIELD_KEYS = [
    'title',
    'price',
    'commission',
    'sales',
    'link',
    'imgSrc',
    'shopName',
    'shopLink',
    'platform',
    'shopTag',
    'monitorTimes',
    'monitorIndex',
    'inspectCycle',
    'shelfTime',
    'recordType',
    'guarantee',
    'deliveryTime',
  ];
  const FEISHU_CFG_KEY = 'ge_feishu_bitable_v1';
  const GE_BATCH_SHOPS_KEY = 'ge_batch_shops_json_v1';
  const GE_BATCH_STATE_KEY = 'ge_batch_state_v1';
  const GE_BATCH_ACCUM_KEY = 'ge_batch_accum_rows_v1';
  /** 一次「开始批量监控」时的启用店铺快照，跳转后续跑只用此列表，避免运行中途改配置打乱下标 */
  const GE_BATCH_RUN_SNAPSHOT_KEY = 'ge_batch_run_snapshot_v1';
  /** 浮动面板：折叠状态、批量结束是否自动写飞书 */
  const GE_EXPORT_PANEL_PREFS_KEY = 'ge_export_panel_prefs_v1';
  /** 浮动面板：fixed 坐标 left/top（px），跨标签页由 GM 持久化 */
  const GE_EXPORT_PANEL_POS_KEY = 'ge_export_panel_pos_v1';
  /** 百应：最近一次可解析的列表 JSON，导出前重放 merge，减轻拦截漏包导致无商品链 */
  const GE_BUYIN_LAST_API_KEY = 'ge_buyin_last_api_v1';
  const GE_BUYIN_LAST_API_MAX = 2 * 1024 * 1024;
  /** 百应：后台补拉 — 记录近期数据请求，定时 no-store 重放（无法读取 DevTools Network UI，仅等价多拉接口） */
  const GE_BUYIN_REPLAY_MAX = 16;
  const GE_BUYIN_BG_INTERVAL_MS = 75000;
  const GE_BUYIN_BG_MIN_GAP_MS = 28000;
  const geBuyinReplayBuffer = [];
  let geBuyinLastBgRefresh = 0;
  let geBuyinBgBusy = false;
  /** 上架时间调试日志：改为 true，或 GM/ localStorage 键 ge_shelf_debug_v1 = 1 */
  const GE_SHELF_DEBUG = false;

  function geGm() {
    return typeof GM !== 'undefined' ? GM : null;
  }
  function gmGet(key, def) {
    try {
      if (typeof GM_getValue === 'function') return GM_getValue(key, def);
      const g = geGm();
      if (g && typeof g.getValue === 'function') return g.getValue(key, def);
    } catch (_) {}
    try {
      const s = localStorage.getItem(key);
      return s != null ? s : def;
    } catch (_) {
      return def;
    }
  }
  function gmSet(key, val) {
    try {
      if (typeof GM_setValue === 'function') return void GM_setValue(key, val);
      const g = geGm();
      if (g && typeof g.setValue === 'function') return void g.setValue(key, val);
    } catch (_) {}
    try {
      localStorage.setItem(key, val);
    } catch (_) {}
  }
  function gmXhr(opts) {
    return new Promise((resolve, reject) => {
      const done = (r) => {
        try {
          resolve({ status: r.status, responseText: r.responseText || '' });
        } catch (e) {
          reject(e);
        }
      };
      const req = {
        method: opts.method || 'GET',
        url: opts.url,
        headers: opts.headers || {},
        data: opts.body,
        onload: done,
        onerror: () => reject(new Error('网络错误')),
        ontimeout: () => reject(new Error('请求超时')),
      };
      if (typeof GM_xmlhttpRequest === 'function') {
        GM_xmlhttpRequest(req);
        return;
      }
      const g = geGm();
      if (g && typeof g.xmlHttpRequest === 'function') {
        g.xmlHttpRequest(req);
        return;
      }
      reject(new Error('需要 Tampermonkey / Violentmonkey 并允许访问 open.feishu.cn'));
    });
  }

  function loadFeishuConfig() {
    const raw = gmGet(FEISHU_CFG_KEY, '');
    if (!raw)
      return {
        wikiNodeToken: '',
        appToken: '',
        tableId: '',
        accessToken: '',
        feishuAppId: '',
        feishuAppSecret: '',
        fieldMapJson: '',
        useHyperlink: false,
        feishuCoerceNumberFields: false,
        feishuAutoCreateFields: true,
      };
    try {
      const j = JSON.parse(raw);
      return {
        wikiNodeToken: (j.wikiNodeToken || '').trim(),
        appToken: (j.appToken || '').trim(),
        tableId: (j.tableId || '').trim(),
        accessToken: (j.accessToken || '').trim(),
        feishuAppId: (j.feishuAppId || '').trim(),
        feishuAppSecret: (j.feishuAppSecret || '').trim(),
        fieldMapJson: typeof j.fieldMapJson === 'string' ? j.fieldMapJson : '',
        useHyperlink: !!j.useHyperlink,
        feishuCoerceNumberFields: !!j.feishuCoerceNumberFields,
        feishuAutoCreateFields: j.feishuAutoCreateFields !== false,
      };
    } catch (_) {
      return {
        wikiNodeToken: '',
        appToken: '',
        tableId: '',
        accessToken: '',
        feishuAppId: '',
        feishuAppSecret: '',
        fieldMapJson: '',
        useHyperlink: false,
        feishuCoerceNumberFields: false,
        feishuAutoCreateFields: true,
      };
    }
  }

  function saveFeishuConfig(cfg) {
    gmSet(
      FEISHU_CFG_KEY,
      JSON.stringify({
        wikiNodeToken: (cfg.wikiNodeToken || '').trim(),
        appToken: (cfg.appToken || '').trim(),
        tableId: (cfg.tableId || '').trim(),
        accessToken: (cfg.accessToken || '').trim(),
        feishuAppId: (cfg.feishuAppId || '').trim(),
        feishuAppSecret: (cfg.feishuAppSecret || '').trim(),
        fieldMapJson: typeof cfg.fieldMapJson === 'string' ? cfg.fieldMapJson : '',
        useHyperlink: !!cfg.useHyperlink,
        feishuCoerceNumberFields: !!cfg.feishuCoerceNumberFields,
        feishuAutoCreateFields: cfg.feishuAutoCreateFields !== false,
      })
    );
  }

  function parseFieldMapJson(str) {
    const def = { ...FEISHU_DEFAULT_FIELD_MAP };
    if (!str || !String(str).trim()) return def;
    try {
      const o = JSON.parse(str);
      if (!o || typeof o !== 'object') return def;
      const out = { ...def };
      for (const k of Object.keys(FEISHU_DEFAULT_FIELD_MAP)) {
        if (typeof o[k] === 'string' && o[k].trim()) out[k] = o[k].trim();
      }
      return out;
    } catch (_) {
      return def;
    }
  }

  function mergeFieldMap(cfg) {
    return parseFieldMapJson(cfg.fieldMapJson);
  }

  /** 飞书「数字」列不能接受「¥211」，需传 JSON number */
  function parsePriceForFeishuNumber(s) {
    const t = String(s || '')
      .replace(/[¥￥,\s]/g, '')
      .replace(/^起/, '')
      .trim();
    if (!t) return null;
    const m = t.match(/(\d+(?:\.\d+)?)/);
    if (!m) return null;
    const n = parseFloat(m[1]);
    return Number.isFinite(n) ? n : null;
  }

  function parseSalesForFeishuNumber(s) {
    const t = String(s || '').trim().replace(/,/g, '');
    if (!t) return null;
    let m = t.match(/^(\d+(?:\.\d+)?)万\+$/);
    if (m) return Math.round(parseFloat(m[1]) * 10000);
    m = t.match(/^(\d+(?:\.\d+)?)万$/);
    if (m) return Math.round(parseFloat(m[1]) * 10000);
    m = t.match(/^(\d+)万\+$/);
    if (m) return parseInt(m[1], 10) * 10000;
    m = t.match(/^(\d+(?:\.\d+)?)\+$/);
    if (m) return Math.round(parseFloat(m[1]));
    m = t.match(/^(\d+)$/);
    if (m) return parseInt(m[1], 10);
    return null;
  }

  /**
   * 将销量文案转为可比较整数（供「只录高销」筛选；飞书列仍走 parseSalesForFeishuNumber）。
   * 支持接口里的「已售6007」「6,007件」等：先抽数字段再解析；纯规则同万/千/+。
   */
  function geParseSalesToNumber(salesText) {
    let t = String(salesText || '')
      .trim()
      .replace(/,/g, '');
    if (!t) return null;
    let n = parseSalesForFeishuNumber(t);
    if (n !== null) return n;
    const m = t.match(/(\d+(?:\.\d+)?[万千]?\+?)/);
    if (m) {
      n = parseSalesForFeishuNumber(m[1]);
      if (n !== null) return n;
    }
    return null;
  }

  function feishuCell(internalKey, raw, mergedMap, cfg) {
    const col = mergedMap[internalKey];
    if (!col) return null;
    const v = raw == null ? '' : String(raw).trim();
    // 空值不传该字段，飞书单元格留空；避免「数字」列收到 "" 触发 NumberFieldConvFail
    if (!v) return null;

    if (cfg.feishuCoerceNumberFields) {
      if (internalKey === 'price') {
        const n = parsePriceForFeishuNumber(v);
        if (n !== null) return n;
        return null;
      }
      if (internalKey === 'sales') {
        const n = parseSalesForFeishuNumber(v);
        if (n !== null) return n;
        return null;
      }
    }

    const hyp = cfg.useHyperlink && (internalKey === 'link' || internalKey === 'shopLink') && /^https?:\/\//i.test(v);
    if (hyp) return { text: v.length > 120 ? v.slice(0, 117) + '...' : v, link: v };
    return v;
  }

  function rowToFeishuFields(row, mergedMap, cfg) {
    const fields = {};
    // 按 FEISHU_UPLOAD_FIELD_KEYS 写入飞书；映射列为空则跳过该字段（关自动建列时由飞书接口报错提示）
    for (const k of FEISHU_UPLOAD_FIELD_KEYS) {
      let raw = row[k];
      if (k === 'platform' && (raw == null || !String(raw).trim())) {
        if (isBuyin()) raw = '抖音';
        else if (isTaobao()) raw = '淘宝';
      }
      const cell = feishuCell(k, raw, mergedMap, cfg);
      if (cell === null || cell === '') continue;
      fields[mergedMap[k]] = cell;
    }
    return fields;
  }

  function chunk(arr, n) {
    const o = [];
    for (let i = 0; i < arr.length; i += n) o.push(arr.slice(i, i + n));
    return o;
  }
  function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
  }

  /** 批量监控单次运行令牌：开始监控时生成、停止时清空；异步链比对 GM 中 execToken 与本轮 token 即可立即退出。 */
  function geGenBatchExecToken() {
    return 'bx_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 12);
  }

  function geBatchIsCancelled(execToken) {
    const s = loadBatchState();
    if (!s.running) return true;
    if (execToken == null || String(execToken) === '') return false;
    return String(s.execToken || '') !== String(execToken);
  }

  function geBatchAbortIfCancelled(execToken) {
    if (execToken == null || String(execToken) === '') return;
    if (geBatchIsCancelled(execToken)) {
      const e = new Error('GE_BATCH_CANCELLED');
      e.code = 'GE_BATCH_CANCELLED';
      throw e;
    }
  }

  async function geBatchSleep(ms, execToken) {
    let left = Math.max(0, parseInt(ms, 10) || 0);
    const slice = 200;
    while (left > 0) {
      if (geBatchIsCancelled(execToken)) return;
      const step = Math.min(slice, left);
      await sleep(step);
      left -= step;
    }
  }

  /** 带批量令牌时可中断等待；无令牌时等同 sleep。 */
  async function geSleepMaybeBatch(ms, execToken) {
    if (execToken != null && String(execToken) !== '') return geBatchSleep(ms, execToken);
    return sleep(ms);
  }

  /** 取消批量已注册的 reload / 续跑定时器，避免点停止后仍跳转或刷新。 */
  function geClearBatchNavTimers() {
    try {
      if (window.__geBatchResumeTimer) {
        clearTimeout(window.__geBatchResumeTimer);
        window.__geBatchResumeTimer = null;
      }
    } catch (_) {}
    try {
      if (window.__geBatchIntervalReloadTimer) {
        clearTimeout(window.__geBatchIntervalReloadTimer);
        window.__geBatchIntervalReloadTimer = null;
      }
    } catch (_) {}
    try {
      if (window.__geBatchMegaReloadTimer) {
        clearTimeout(window.__geBatchMegaReloadTimer);
        window.__geBatchMegaReloadTimer = null;
      }
    } catch (_) {}
  }

  // 新增：店铺任务 id 与记录规范化（GM 持久化结构）
  function geGenShopTaskId() {
    return 'st_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 9);
  }
  function geMigrateShopTask(o) {
    if (!o || typeof o !== 'object') {
      return {
        id: geGenShopTaskId(),
        shopName: '',
        url: '',
        shopTag: '',
        monitorTimes: 1,
        enabled: true,
        extraRoundsEnabled: false,
        extraRoundsCount: 0,
        extraRoundsIntervalHours: 1,
        recordAll: true,
        recordNewest: false,
        recordTopSales: false,
        newestLimit: 30,
        topSalesLimit: 30,
      };
    }
    const er = o.extraRoundsEnabled;
    const extraRoundsEnabled = er === true || er === 'true' || er === 1 || er === '1';
    let extraRoundsCount = Math.max(0, parseInt(o.extraRoundsCount, 10) || 0);
    /** 仅勾选追加巡检、追加轮数填 0 时，按「至少再跑 1 个大轮」理解，否则永远不进入间隔巡检 */
    if (extraRoundsEnabled && extraRoundsCount === 0) extraRoundsCount = 1;
    let extraRoundsIntervalHours = parseFloat(o.extraRoundsIntervalHours);
    if (!Number.isFinite(extraRoundsIntervalHours) || extraRoundsIntervalHours < 0.1) extraRoundsIntervalHours = 1;

    const hasNewSchema =
      Object.prototype.hasOwnProperty.call(o, 'recordAll') ||
      Object.prototype.hasOwnProperty.call(o, 'recordNewest') ||
      Object.prototype.hasOwnProperty.call(o, 'recordTopSales') ||
      Object.prototype.hasOwnProperty.call(o, 'newestLimit') ||
      Object.prototype.hasOwnProperty.call(o, 'topSalesLimit');

    let recordAll = true;
    let recordNewest = false;
    let recordTopSales = false;
    if (hasNewSchema) {
      recordAll = !(o.recordAll === false || o.recordAll === 0 || o.recordAll === '0');
      if (o.recordAll === true || o.recordAll === 1 || o.recordAll === '1') recordAll = true;
      recordNewest =
        o.recordNewest === true || o.recordNewest === 'true' || o.recordNewest === 1 || o.recordNewest === '1';
      recordTopSales =
        o.recordTopSales === true || o.recordTopSales === 'true' || o.recordTopSales === 1 || o.recordTopSales === '1';
    } else {
      const rm = String(o.recordMode || '').trim();
      if (rm === 'new_only') {
        recordAll = false;
        recordNewest = true;
      } else if (rm === 'high_sales_only') {
        recordAll = false;
        recordTopSales = true;
      } else {
        recordAll = true;
      }
    }

    let newestLimit = parseInt(o.newestLimit, 10);
    if (!Number.isFinite(newestLimit) || newestLimit < 1) newestLimit = 1;
    let topSalesLimit = parseInt(o.topSalesLimit, 10);
    if (!Number.isFinite(topSalesLimit) || topSalesLimit < 1) topSalesLimit = 1;

    /** 全店可与「新品」「高销」同时勾选；仅当三者都未选时才默认全店 */
    if (!recordAll && !recordNewest && !recordTopSales) {
      recordAll = true;
    }

    return {
      id: String(o.id || '').trim() || geGenShopTaskId(),
      shopName: String(o.shopName != null ? o.shopName : '').trim(),
      url: String(o.url || '').trim(),
      shopTag: String(o.shopTag != null ? o.shopTag : '').trim(),
      monitorTimes: Math.max(1, parseInt(o.monitorTimes, 10) || 1),
      enabled: (function () {
        const e = o.enabled;
        if (e === false || e === 0 || e === '0') return false;
        if (typeof e === 'string' && /^false$/i.test(e.trim())) return false;
        return true;
      })(),
      extraRoundsEnabled,
      extraRoundsCount,
      extraRoundsIntervalHours,
      recordAll,
      recordNewest,
      recordTopSales,
      newestLimit,
      topSalesLimit,
    };
  }

  /** 从配置表 tbody 解析店铺任务（弹窗保存与「开始监控」共用，避免未保存 GM 时追加巡检不生效） */
  function geReadBatchShopTasksFromTbody(tbody) {
    const out = [];
    if (!tbody) return out;
    tbody.querySelectorAll('tr[data-ge-shop-row]').forEach(function (tr) {
      let id = tr.getAttribute('data-task-id');
      if (!id) id = geGenShopTaskId();
      const q = function (k) {
        const el = tr.querySelector('[data-field="' + k + '"]');
        /** 找不到控件时：启用类复选框默认 false，避免误当成「全部启用」 */
        if (!el) {
          if (k === 'enabled' || k === 'extraRoundsEnabled') return false;
          return '';
        }
        if (el.type === 'checkbox') return el.checked;
        return (el.value || '').trim();
      };
      const mt = Math.max(1, parseInt(q('monitorTimes'), 10) || 1);
      const extraRoundsEnabled = q('extraRoundsEnabled') === true;
      const extraRoundsCount = Math.max(0, parseInt(q('extraRoundsCount'), 10) || 0);
      let extraRoundsIntervalHours = parseFloat(q('extraRoundsIntervalHours'));
      if (!Number.isFinite(extraRoundsIntervalHours) || extraRoundsIntervalHours < 0.1) extraRoundsIntervalHours = 1;
      out.push(
        geMigrateShopTask({
          id: id,
          shopName: q('shopName'),
          url: q('url'),
          shopTag: String(q('shopTag') || ''),
          monitorTimes: mt,
          enabled: q('enabled') !== false,
          extraRoundsEnabled,
          extraRoundsCount,
          extraRoundsIntervalHours,
          recordAll: q('recordAll') === true,
          recordNewest: q('recordNewest') === true,
          recordTopSales: q('recordTopSales') === true,
          newestLimit: Math.max(1, parseInt(q('newestLimit'), 10) || 0),
          topSalesLimit: Math.max(1, parseInt(q('topSalesLimit'), 10) || 0),
        })
      );
    });
    return out;
  }

  /** 开始监控：配置弹窗若打开，优先用弹窗表格并写回 GM */
  function geGetShopTasksForBatchStart() {
    try {
      const modal = document.getElementById('ge-batch-modal');
      if (modal) {
        const tb = modal.querySelector('#ge-batch-shop-tbody');
        if (tb) {
          const fromModal = geReadBatchShopTasksFromTbody(tb);
          if (fromModal.length) return fromModal;
        }
      }
    } catch (_) {}
    return loadBatchShops();
  }

  function geValidateShopTasksForRun(arr) {
    for (let i = 0; i < arr.length; i++) {
      const r = arr[i];
      if (!String(r.url || '').trim()) return { ok: false, msg: '第 ' + (i + 1) + ' 行：店铺链接不能为空' };
      if (!geLooksLikeBuyinShopUrl(r.url))
        return { ok: false, msg: '第 ' + (i + 1) + ' 行：链接须含 shop_id 或为百应店铺页' };
      if (r.monitorTimes < 1) return { ok: false, msg: '第 ' + (i + 1) + ' 行：监控次数须 ≥1' };
      if (r.extraRoundsEnabled) {
        if (r.extraRoundsCount < 0)
          return { ok: false, msg: '第 ' + (i + 1) + ' 行：追加巡检轮数须 ≥0' };
        if (!Number.isFinite(r.extraRoundsIntervalHours) || r.extraRoundsIntervalHours < 0.1) {
          return { ok: false, msg: '第 ' + (i + 1) + ' 行：追加间隔须 ≥0.1 小时' };
        }
      }
    }
    return { ok: true, msg: '' };
  }

  /** 配置表任意单元格变更时防抖写入 GM（尤其「启用」），避免未点保存时开始监控仍用旧列表 */
  function geDebouncedPersistBatchShopTable(tbody) {
    try {
      if (!document.getElementById('ge-batch-modal') || !tbody) return;
      clearTimeout(window.__geBatchShopTableAutosaveTimer);
      window.__geBatchShopTableAutosaveTimer = setTimeout(function () {
        try {
          const rows = geReadBatchShopTasksFromTbody(tbody);
          saveBatchShops(rows);
          geUpdateBatchStatusLine();
        } catch (_) {}
      }, 400);
    } catch (_) {}
  }

  function loadBatchShops() {
    try {
      const s = gmGet(GE_BATCH_SHOPS_KEY, '[]');
      const a = JSON.parse(s);
      if (!Array.isArray(a)) return [];
      return a.map((x) => geMigrateShopTask(x));
    } catch (_) {
      return [];
    }
  }
  function saveBatchShops(arr) {
    gmSet(GE_BATCH_SHOPS_KEY, JSON.stringify(arr || []));
  }
  function loadBatchState() {
    try {
      const o = JSON.parse(gmGet(GE_BATCH_STATE_KEY, '{}') || '{}');
      const phase = o.phase === 'interval_wait' ? 'interval_wait' : 'running';
      return {
        running: !!o.running,
        shopIdx: Math.max(0, parseInt(o.shopIdx, 10) || 0),
        roundIdx: Math.max(1, parseInt(o.roundIdx, 10) || 1),
        lastRunAt: Math.max(0, parseInt(o.lastRunAt, 10) || 0),
        /** 追加巡检：两大大轮之间的等待 */
        phase,
        intervalUntil: Math.max(0, parseInt(o.intervalUntil, 10) || 0),
        /** 当前大轮序号（第几次周期巡检），从 1 起 */
        megaIdx: Math.max(1, parseInt(o.megaIdx, 10) || 1),
        /** 与 geRunBuyinBatchRoundAsync 本轮绑定；停止监控时清空，使旧异步链失效 */
        execToken: String(o.execToken || ''),
      };
    } catch (_) {
      return {
        running: false,
        shopIdx: 0,
        roundIdx: 1,
        lastRunAt: 0,
        phase: 'running',
        intervalUntil: 0,
        megaIdx: 1,
        execToken: '',
      };
    }
  }
  function saveBatchState(s) {
    const prev = loadBatchState();
    let execToken = prev.execToken;
    if (s.running === false) {
      execToken = '';
    } else if (s.execToken !== undefined) {
      execToken = String(s.execToken || '');
    }
    gmSet(
      GE_BATCH_STATE_KEY,
      JSON.stringify({
        running: !!s.running,
        shopIdx: Math.max(0, parseInt(s.shopIdx, 10) || 0),
        roundIdx: Math.max(1, parseInt(s.roundIdx, 10) || 1),
        lastRunAt:
          typeof s.lastRunAt === 'number' && s.lastRunAt > 0 ? s.lastRunAt : Date.now(),
        phase: s.phase === 'interval_wait' ? 'interval_wait' : 'running',
        intervalUntil: Math.max(0, parseInt(s.intervalUntil, 10) || 0),
        megaIdx: Math.max(1, parseInt(s.megaIdx, 10) || 1),
        execToken: execToken,
      })
    );
  }
  function loadBatchAccum() {
    try {
      const s = gmGet(GE_BATCH_ACCUM_KEY, '[]');
      const a = JSON.parse(s);
      return Array.isArray(a) ? a : [];
    } catch (_) {
      return [];
    }
  }
  function saveBatchAccum(rows) {
    gmSet(GE_BATCH_ACCUM_KEY, JSON.stringify(rows || []));
  }

  function geGenWriteBatchId() {
    return 'wb_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 10);
  }

  /** 单店「大轮」总数：未启用追加巡检为 1；启用时为 1 + 追加轮数（迁移后追加轮数至少为 1） */
  function geShopTotalMegas(shop) {
    if (!shop || shop.extraRoundsEnabled !== true) return 1;
    return 1 + Math.max(0, parseInt(shop.extraRoundsCount, 10) || 0);
  }

  function geAccumRowDedupeKey(r) {
    if (!r || typeof r !== 'object') return '';
    return [r.link || '', r.title || '', r.price || '', r.monitorIndex || '', r.inspectCycle || '', r.shopTag || ''].join('\x1e');
  }

  /**
   * 飞书写入成功后从 GM 缓冲剔除对应行：优先按 _geWriteBatchId；老数据无批次 id 时按链接+标题等组合键剔除。
   * 失败勿调用，以便重试。
   */
  function geRemoveAccumRowsWritten(writtenRows) {
    const rows = writtenRows && writtenRows.length ? writtenRows : [];
    const acc = loadBatchAccum();
    const batchIds = new Set();
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (r && r._geWriteBatchId) batchIds.add(r._geWriteBatchId);
    }
    let next;
    if (batchIds.size) {
      next = acc.filter(function (r) {
        return !r || !r._geWriteBatchId || !batchIds.has(r._geWriteBatchId);
      });
    } else {
      const rm = new Set();
      for (let j = 0; j < rows.length; j++) {
        const k = geAccumRowDedupeKey(rows[j]);
        if (k) rm.add(k);
      }
      next = acc.filter(function (r) {
        const k = geAccumRowDedupeKey(r);
        return !(k && rm.has(k));
      });
    }
    saveBatchAccum(next);
    geUpdateBatchStatusLine();
  }

  /**
   * 批量监控：单组写飞书并在接口成功返回后立即剔缓冲，与 stop/cancel 解耦。
   * cancel 不得把已成功提交的组改判为失败；组间停止请用 geBatchIsCancelled + break，勿对本函数抛 GE_BATCH_CANCELLED。
   */
  async function geFeishuUploadGroupAndClearBuffer(gRows, cfg, logCtx) {
    const ctx = logCtx || {};
    const groupKey = ctx.groupKey != null ? String(ctx.groupKey) : 'group';
    const scope = (ctx.scope && String(ctx.scope)) || '';
    const n = gRows && gRows.length ? gRows.length : 0;
    console.log('[飞书写入·分组]', {
      phase: 'upload_started',
      group_key: groupKey,
      row_count: n,
      scope: scope,
    });
    let up = null;
    try {
      up = await uploadRowsToFeishu(gRows, cfg);
    } catch (err) {
      console.warn('[飞书写入·分组]', {
        phase: 'upload_failed',
        group_key: groupKey,
        row_count: n,
        scope: scope,
        err: err && err.message ? err.message : String(err),
      });
      throw err;
    }
    try {
      geRemoveAccumRowsWritten(gRows);
    } catch (rmErr) {
      console.error('[飞书写入·分组]', {
        phase: 'buffer_remove_error',
        group_key: groupKey,
        row_count: n,
        err: rmErr && rmErr.message ? rmErr.message : String(rmErr),
      });
    }
    console.log('[飞书写入·分组]', {
      phase: 'upload_succeeded_buffer_removed',
      group_key: groupKey,
      row_count: n,
      feishu_total: up && up.total != null ? up.total : n,
      scope: scope,
    });
    return { total: up.total, columnsCreated: up.columnsCreated || 0 };
  }

  function saveBatchRunSnapshot(arr) {
    gmSet(GE_BATCH_RUN_SNAPSHOT_KEY, JSON.stringify((arr || []).map((x) => geMigrateShopTask(x))));
  }
  function clearBatchRunSnapshot() {
    gmSet(GE_BATCH_RUN_SNAPSHOT_KEY, '');
  }
  function loadExportPanelPrefs() {
    try {
      const o = JSON.parse(gmGet(GE_EXPORT_PANEL_PREFS_KEY, '{}') || '{}');
      return {
        panelCollapsed: !!o.panelCollapsed,
        batchAutoFeishuOnComplete: !!o.batchAutoFeishuOnComplete,
      };
    } catch (_) {
      return { panelCollapsed: false, batchAutoFeishuOnComplete: false };
    }
  }
  function saveExportPanelPrefs(p) {
    const cur = loadExportPanelPrefs();
    gmSet(
      GE_EXPORT_PANEL_PREFS_KEY,
      JSON.stringify({
        panelCollapsed: p.panelCollapsed != null ? !!p.panelCollapsed : cur.panelCollapsed,
        batchAutoFeishuOnComplete:
          p.batchAutoFeishuOnComplete != null ? !!p.batchAutoFeishuOnComplete : cur.batchAutoFeishuOnComplete,
      })
    );
  }

  function loadExportPanelPosition() {
    try {
      const raw = gmGet(GE_EXPORT_PANEL_POS_KEY, '');
      if (!raw || !String(raw).trim()) return null;
      const o = JSON.parse(raw);
      if (!o || typeof o !== 'object') return null;
      const left = parseFloat(o.left);
      const top = parseFloat(o.top);
      if (!Number.isFinite(left) || !Number.isFinite(top)) return null;
      return { left, top };
    } catch (_) {
      return null;
    }
  }

  function saveExportPanelPosition(left, top) {
    gmSet(GE_EXPORT_PANEL_POS_KEY, JSON.stringify({ left, top }));
  }

  function geClampPanelPosition(wrap, left, top) {
    if (!wrap) return { left, top };
    let w = 200;
    let h = 80;
    try {
      const r = wrap.getBoundingClientRect();
      if (r.width > 0) w = r.width;
      if (r.height > 0) h = r.height;
    } catch (_) {}
    const vw = Math.max(320, window.innerWidth || 800);
    const vh = Math.max(240, window.innerHeight || 600);
    const m = 6;
    let nl = Math.min(Math.max(m, left), vw - w - m);
    let nt = Math.min(Math.max(m, top), vh - h - m);
    if (nl < m) nl = m;
    if (nt < m) nt = m;
    return { left: nl, top: nt };
  }

  function applyExportPanelPosition(wrap, left, top) {
    const c = geClampPanelPosition(wrap, left, top);
    wrap.style.left = Math.round(c.left) + 'px';
    wrap.style.top = Math.round(c.top) + 'px';
    wrap.style.right = 'auto';
    wrap.style.bottom = 'auto';
    return c;
  }

  /** 已插入 DOM 后调用：恢复上次拖动位置 */
  function applySavedExportPanelPosition(wrap) {
    const p = loadExportPanelPosition();
    if (!p) return;
    applyExportPanelPosition(wrap, p.left, p.top);
  }

  function installExportPanelDrag(dragHandle, wrap) {
    let dragging = false;
    let sx = 0;
    let sy = 0;
    let ox = 0;
    let oy = 0;
    function onMove(e) {
      if (!dragging) return;
      if (e.cancelable) e.preventDefault();
      const nx = ox + (e.clientX - sx);
      const ny = oy + (e.clientY - sy);
      const c = geClampPanelPosition(wrap, nx, ny);
      wrap.style.left = c.left + 'px';
      wrap.style.top = c.top + 'px';
      wrap.style.right = 'auto';
      wrap.style.bottom = 'auto';
    }
    function onUp() {
      if (!dragging) return;
      dragging = false;
      dragHandle.style.cursor = 'grab';
      document.removeEventListener('mousemove', onMove, true);
      document.removeEventListener('mouseup', onUp, true);
      try {
        document.body.style.userSelect = '';
      } catch (_) {}
      let r;
      try {
        r = wrap.getBoundingClientRect();
      } catch (_) {
        return;
      }
      const c = geClampPanelPosition(wrap, r.left, r.top);
      applyExportPanelPosition(wrap, c.left, c.top);
      saveExportPanelPosition(c.left, c.top);
    }
    dragHandle.addEventListener(
      'mousedown',
      function (e) {
        if (e.button !== 0) return;
        e.preventDefault();
        e.stopPropagation();
        dragging = true;
        dragHandle.style.cursor = 'grabbing';
        let r;
        try {
          r = wrap.getBoundingClientRect();
        } catch (_) {
          dragging = false;
          return;
        }
        ox = r.left;
        oy = r.top;
        sx = e.clientX;
        sy = e.clientY;
        try {
          document.body.style.userSelect = 'none';
        } catch (_) {}
        document.addEventListener('mousemove', onMove, true);
        document.addEventListener('mouseup', onUp, true);
      },
      true
    );
  }

  function loadBatchActiveOrEnabledShops() {
    try {
      const raw = gmGet(GE_BATCH_RUN_SNAPSHOT_KEY, '');
      if (raw) {
        const a = JSON.parse(raw);
        if (Array.isArray(a) && a.length) return a.map((x) => geMigrateShopTask(x));
      }
    } catch (_) {}
    return loadBatchShops().filter(function (s) {
      return s && s.enabled !== false && String(s.url || '').trim();
    });
  }

  /** 链接须含 shop_id 或明显为百应店铺相关页 */
  function geLooksLikeBuyinShopUrl(url) {
    const s = String(url || '').trim();
    if (!s || !/^https?:\/\//i.test(s)) return false;
    if (geExtractShopIdFromUrl(s)) return true;
    if (/buyin\.jinritemai\.com/i.test(s) && /(shop|merchant|detail|library|picking|store|选品|店铺)/i.test(s)) return true;
    return false;
  }

  // 新增：批量粘贴解析（链接 / 链接,标记,次数）
  function geParseBulkShopPasteLines(text, skipBad) {
    const lines = String(text || '').split(/\r?\n/);
    const tasks = [];
    const errors = [];
    for (let i = 0; i < lines.length; i++) {
      const raw = lines[i].trim();
      if (!raw) continue;
      const parts = raw.split(',').map(function (x) {
        return String(x).trim();
      });
      const url = parts[0] || '';
      const lineNo = i + 1;
      if (!url) {
        errors.push({ line: lineNo, msg: '缺少链接' });
        if (!skipBad) return { tasks: [], errors, aborted: true };
        continue;
      }
      if (!geLooksLikeBuyinShopUrl(url)) {
        errors.push({ line: lineNo, msg: '链接须含 shop_id 或像百应店铺页' });
        if (!skipBad) return { tasks: [], errors, aborted: true };
        continue;
      }
      const shopTag = parts.length > 1 ? parts[1] : '';
      let monitorTimes = 1;
      if (parts.length >= 3 && parts[2] !== '') {
        const n = parseInt(parts[2], 10);
        if (!Number.isFinite(n) || n < 1 || String(n) !== String(Math.floor(n))) {
          errors.push({ line: lineNo, msg: '监控次数须为 ≥1 的整数' });
          if (!skipBad) return { tasks: [], errors, aborted: true };
          continue;
        }
        monitorTimes = n;
      }
      let extraRoundsEnabled = false;
      let extraRoundsCount = 0;
      let extraRoundsIntervalHours = 1;
      if (parts.length >= 4 && parts[3] !== '') {
        extraRoundsEnabled = parts[3] === '1' || /^true$/i.test(parts[3]) || parts[3] === '是';
      }
      if (parts.length >= 5 && parts[4] !== '') {
        const ec = parseInt(parts[4], 10);
        if (!Number.isFinite(ec) || ec < 0) {
          errors.push({ line: lineNo, msg: '追加轮数须为 ≥0 的整数' });
          if (!skipBad) return { tasks: [], errors, aborted: true };
          continue;
        }
        extraRoundsCount = ec;
      }
      if (parts.length >= 6 && parts[5] !== '') {
        const eh = parseFloat(parts[5]);
        if (!Number.isFinite(eh) || eh < 0.1) {
          errors.push({ line: lineNo, msg: '间隔小时须 ≥0.1' });
          if (!skipBad) return { tasks: [], errors, aborted: true };
          continue;
        }
        extraRoundsIntervalHours = eh;
      }
      tasks.push(
        geMigrateShopTask({
          id: geGenShopTaskId(),
          shopName: '',
          url,
          shopTag,
          monitorTimes,
          enabled: true,
          extraRoundsEnabled,
          extraRoundsCount,
          extraRoundsIntervalHours,
        })
      );
    }
    return { tasks, errors, aborted: false };
  }

  // 新增：飞书多维表格（复制文本）解析，支持“店铺名称/店铺链接”两列批量导入
  function geParseFeishuShopRows(text, skipBad) {
    const lines = String(text || '')
      .split(/\r?\n/)
      .map(function (x) {
        return String(x || '').trim();
      })
      .filter(Boolean);
    const tasks = [];
    const errors = [];
    if (!lines.length) return { tasks, errors, aborted: false };
    const normalizeHead = function (s) {
      return String(s || '')
        .replace(/\s+/g, '')
        .replace(/[：:]/g, '')
        .toLowerCase();
    };
    const nameHeads = {
      '店铺名称': 1,
      '店铺名': 1,
      shopname: 1,
      storename: 1,
      merchantname: 1,
      sellername: 1,
    };
    const linkHeads = {
      '店铺链接': 1,
      '店铺地址': 1,
      '店铺url': 1,
      '店铺link': 1,
      shoplink: 1,
      shopurl: 1,
      storeurl: 1,
      merchanturl: 1,
      link: 1,
      url: 1,
    };
    const firstCells = lines[0].split(/\t|,/).map(function (x) {
      return String(x || '').trim();
    });
    let nameIdx = -1;
    let urlIdx = -1;
    for (let i = 0; i < firstCells.length; i++) {
      const k = normalizeHead(firstCells[i]);
      if (nameIdx < 0 && (nameHeads[k] || k.indexOf('shopname') >= 0 || k.indexOf('storename') >= 0)) nameIdx = i;
      if (
        urlIdx < 0 &&
        (linkHeads[k] || ((k.indexOf('shop') >= 0 || k.indexOf('store') >= 0) && (k.indexOf('url') >= 0 || k.indexOf('link') >= 0)))
      )
        urlIdx = i;
    }
    let start = 0;
    if (nameIdx >= 0 || urlIdx >= 0) {
      start = 1;
      if (urlIdx < 0) urlIdx = nameIdx === 0 ? 1 : 0;
      if (nameIdx < 0) nameIdx = urlIdx === 0 ? 1 : 0;
    } else {
      // 无表头：默认第1列名称，第2列链接
      nameIdx = 0;
      urlIdx = 1;
    }
    for (let i = start; i < lines.length; i++) {
      const cells = lines[i].split(/\t|,/).map(function (x) {
        return String(x || '').trim();
      });
      const lineNo = i + 1;
      const shopName = String(cells[nameIdx] || '').trim();
      const rawUrl = String(cells[urlIdx] || '').trim();
      if (!rawUrl) {
        errors.push({ line: lineNo, msg: '缺少店铺链接' });
        if (!skipBad) return { tasks: [], errors, aborted: true };
        continue;
      }
      const url = normalizeBuyinHref(rawUrl);
      if (!geLooksLikeBuyinShopUrl(url)) {
        errors.push({ line: lineNo, msg: '店铺链接不合法（须含 shop_id 或为百应店铺页）' });
        if (!skipBad) return { tasks: [], errors, aborted: true };
        continue;
      }
      tasks.push(
        geMigrateShopTask({
          id: geGenShopTaskId(),
          shopName: shopName,
          url: url,
          shopTag: '',
          monitorTimes: 1,
          enabled: true,
          extraRoundsEnabled: false,
          extraRoundsCount: 0,
          extraRoundsIntervalHours: 1,
        })
      );
    }
    return { tasks, errors, aborted: false };
  }

  function geParseFeishuBitableLink(link) {
    const s = String(link || '').trim();
    if (!s) throw new Error('请先粘贴飞书多维表格链接');
    let u;
    try {
      u = new URL(s);
    } catch (_) {
      throw new Error('链接格式无效');
    }
    const seg = u.pathname
      .split('/')
      .map(function (x) {
        return String(x || '').trim();
      })
      .filter(Boolean);
    let appToken = '';
    let wikiNodeToken = '';
    const iBase = seg.indexOf('base');
    const iWiki = seg.indexOf('wiki');
    if (iBase >= 0 && seg[iBase + 1]) appToken = String(seg[iBase + 1]).trim();
    else if (iWiki >= 0 && seg[iWiki + 1]) wikiNodeToken = String(seg[iWiki + 1]).trim();
    const tableId = String(u.searchParams.get('table') || '').trim();
    if (!tableId) throw new Error('链接中缺少 table 参数（table_id）');
    if (!appToken && !wikiNodeToken) throw new Error('链接中未识别到 /base/{app_token} 或 /wiki/{node_token}');
    return { appToken, wikiNodeToken, tableId };
  }

  function geFeishuCellToText(cell, preferUrl) {
    if (cell == null) return '';
    if (typeof cell === 'string' || typeof cell === 'number' || typeof cell === 'boolean') return String(cell).trim();
    if (Array.isArray(cell)) {
      for (let i = 0; i < cell.length; i++) {
        const t = geFeishuCellToText(cell[i], preferUrl);
        if (t) return t;
      }
      return '';
    }
    if (typeof cell === 'object') {
      if (preferUrl) {
        const direct = [cell.link, cell.url, cell.href, cell.hyperlink];
        for (let i = 0; i < direct.length; i++) {
          const v = String(direct[i] || '').trim();
          if (/^https?:\/\//i.test(v)) return v;
        }
      }
      const cands = [cell.text, cell.name, cell.value, cell.display_value, cell.link, cell.url, cell.href];
      for (let i = 0; i < cands.length; i++) {
        const v = String(cands[i] || '').trim();
        if (!v) continue;
        if (preferUrl && /^https?:\/\//i.test(v)) return v;
        if (!preferUrl) return v;
      }
    }
    return '';
  }

  function gePickFeishuField(obj, aliases, preferUrl) {
    const o = obj && typeof obj === 'object' ? obj : {};
    const keys = Object.keys(o);
    if (!keys.length) return '';
    for (let i = 0; i < keys.length; i++) {
      const k = keys[i];
      const nk = String(k || '')
        .replace(/\s+/g, '')
        .replace(/[：:]/g, '')
        .toLowerCase();
      for (let j = 0; j < aliases.length; j++) {
        const a = aliases[j];
        if (nk === a || nk.indexOf(a) >= 0) {
          const val = geFeishuCellToText(o[k], preferUrl);
          if (val) return val;
        }
      }
    }
    return '';
  }

  async function geImportShopsFromFeishuLink(link, skipBad) {
    const parsed = geParseFeishuBitableLink(link);
    const cfg = loadFeishuConfig();
    const cfgWithLink = {
      ...cfg,
      appToken: parsed.appToken || '',
      wikiNodeToken: parsed.wikiNodeToken || '',
      tableId: parsed.tableId,
    };
    const cfgTok = await ensureFeishuAccessToken(cfgWithLink);
    if (!String(cfgTok.accessToken || '').trim()) {
      throw new Error('未找到可用 access_token。请先在「飞书配置」里填写 access_token，或填写 App ID + App Secret。');
    }
    const bitableAppToken = await resolveBitableAppToken(cfgTok);
    if (!bitableAppToken) throw new Error('链接解析后未得到 app_token');
    const tasks = [];
    const errors = [];
    let pageToken = '';
    const nameAliases = ['店铺名称', '店铺名', 'shopname', 'storename', 'merchantname', 'sellername'];
    const linkAliases = ['店铺链接', '店铺地址', '店铺url', '店铺link', 'shoplink', 'shopurl', 'storeurl', 'url', 'link'];
    for (let guard = 0; guard < 200; guard++) {
      const page = await feishuBitableListRecordsPage(bitableAppToken, cfgTok.tableId, cfgTok.accessToken, pageToken);
      for (let i = 0; i < page.items.length; i++) {
        const rec = page.items[i] || {};
        const lineNo = tasks.length + errors.length + 1;
        const fields = rec.fields && typeof rec.fields === 'object' ? rec.fields : {};
        const shopName = gePickFeishuField(fields, nameAliases, false);
        const rawUrl = gePickFeishuField(fields, linkAliases, true);
        if (!rawUrl) {
          errors.push({ line: lineNo, msg: '缺少店铺链接' });
          if (!skipBad) return { tasks: [], errors, aborted: true };
          continue;
        }
        const url = normalizeBuyinHref(rawUrl);
        if (!geLooksLikeBuyinShopUrl(url)) {
          errors.push({ line: lineNo, msg: '店铺链接不合法（须含 shop_id 或为百应店铺页）' });
          if (!skipBad) return { tasks: [], errors, aborted: true };
          continue;
        }
        tasks.push(
          geMigrateShopTask({
            id: geGenShopTaskId(),
            shopName: String(shopName || '').trim(),
            url: url,
            shopTag: '',
            monitorTimes: 1,
            enabled: true,
            extraRoundsEnabled: false,
            extraRoundsCount: 0,
            extraRoundsIntervalHours: 1,
          })
        );
      }
      if (!page.hasMore || !page.pageToken) break;
      pageToken = page.pageToken;
      await sleep(120);
    }
    return { tasks, errors, aborted: false };
  }

  async function geEnsureFeishuShopImportColumns(bitableAppToken, tableId, cfgTok) {
    if (cfgTok.feishuAutoCreateFields === false) return { created: 0 };
    const existing = await feishuBitableListAllFieldNames(bitableAppToken, tableId, cfgTok.accessToken);
    let created = 0;
    if (!existing.has('店铺名称')) {
      const okName = await feishuBitableCreateField(bitableAppToken, tableId, cfgTok.accessToken, '店铺名称', 1);
      if (okName) created++;
      existing.add('店铺名称');
      await sleep(120);
    }
    if (!existing.has('店铺链接')) {
      const typ = cfgTok.useHyperlink ? 15 : 1;
      const okLink = await feishuBitableCreateField(bitableAppToken, tableId, cfgTok.accessToken, '店铺链接', typ);
      if (okLink) created++;
      existing.add('店铺链接');
      await sleep(120);
    }
    return { created: created };
  }

  async function geFindFeishuShopByLink(bitableAppToken, tableId, accessToken, targetLink) {
    const target = normalizeBuyinHref(targetLink || '');
    if (!target) return false;
    const linkAliases = ['店铺链接', '店铺地址', '店铺url', '店铺link', 'shoplink', 'shopurl', 'storeurl', 'url', 'link'];
    let pageToken = '';
    for (let guard = 0; guard < 200; guard++) {
      const page = await feishuBitableListRecordsPage(bitableAppToken, tableId, accessToken, pageToken);
      for (let i = 0; i < page.items.length; i++) {
        const rec = page.items[i] || {};
        const fields = rec.fields && typeof rec.fields === 'object' ? rec.fields : {};
        const raw = gePickFeishuField(fields, linkAliases, true);
        const ex = normalizeBuyinHref(raw || '');
        if (ex && ex === target) return true;
      }
      if (!page.hasMore || !page.pageToken) break;
      pageToken = page.pageToken;
      await sleep(100);
    }
    return false;
  }

  async function geWriteSingleShopToFeishu(task) {
    const shopName = String(task && task.shopName != null ? task.shopName : '').trim();
    const rawUrl = String(task && task.url ? task.url : '').trim();
    const shopUrl = normalizeBuyinHref(rawUrl);
    if (!shopName) throw new Error('店铺名称不能为空');
    if (!shopUrl) throw new Error('店铺链接不能为空');
    if (!geLooksLikeBuyinShopUrl(shopUrl)) throw new Error('店铺链接不合法（须含 shop_id 或为百应店铺页）');
    const cfg = loadFeishuConfig();
    if (!String(cfg.tableId || '').trim()) throw new Error('请先在飞书配置里填写 table_id');
    const cfgTok = await ensureFeishuAccessToken(cfg);
    if (!String(cfgTok.accessToken || '').trim()) {
      throw new Error('请先在飞书配置里填写 access_token，或填写 App ID + App Secret');
    }
    const bitableAppToken = await resolveBitableAppToken(cfgTok);
    if (!bitableAppToken) throw new Error('请先在飞书配置里填写「Wiki 节点 token」或「app_token」其一');
    await geEnsureFeishuShopImportColumns(bitableAppToken, cfgTok.tableId, cfgTok);
    const existed = await geFindFeishuShopByLink(bitableAppToken, cfgTok.tableId, cfgTok.accessToken, shopUrl);
    if (existed) return { duplicated: true };
    const fields = {
      店铺名称: shopName,
      店铺链接: cfgTok.useHyperlink ? { text: shopUrl.length > 120 ? shopUrl.slice(0, 117) + '...' : shopUrl, link: shopUrl } : shopUrl,
    };
    await feishuBatchCreate(bitableAppToken, cfgTok.tableId, cfgTok.accessToken, [{ fields }]);
    return { duplicated: false };
  }

  function geExtractShopIdFromUrl(href) {
    try {
      const u = new URL(href, 'https://buyin.jinritemai.com');
      return (u.searchParams.get('shop_id') || u.searchParams.get('shopId') || '').trim();
    } catch (_) {
      const m = String(href).match(/[?&]shop_id=([^&]+)/i);
      return m ? decodeURIComponent(m[1]).trim() : '';
    }
  }
  function geUrlsSameBuyinShop(a, b) {
    const ia = geExtractShopIdFromUrl(a);
    const ib = geExtractShopIdFromUrl(b);
    if (ia && ib) return ia === ib;
    return String(a).split('#')[0] === String(b).split('#')[0];
  }

  function normalizeBuyinRowDefaults(r) {
    if (!r || typeof r !== 'object') return r;
    let platform = r.platform != null ? String(r.platform).trim() : '';
    if (!platform) {
      if (isBuyin()) platform = '抖音';
      else if (isTaobao()) platform = '淘宝';
    }
    // 飞书「商品链接」最后一道强校验：
    // 1) 任何不满足商品详情链规则（如图片 CDN 链）都清空；
    // 2) 若 detail_url 未返回但 id 字段存在，则用 id 回退构造详情链，避免“最近上架/销量Top”出现空链接。
    let imgOut = r.imgSrc != null ? String(r.imgSrc) : '';
    try {
      if (isBuyin()) {
        let lk = r.link != null ? String(r.link).trim() : '';
        if (lk && !buyinProductLinkLooksValid(lk)) lk = '';
        if (!lk) {
          // 兜底：通过 product_id / commodity_id 构造 haohuo 详情链
          lk = tryBuildBuyinProductUrlFromIds(
            { product_id: r.product_id, productId: r.product_id, item_id: r.product_id, itemId: r.product_id },
            {
              commodity_id: r.commodity_id,
              commodityId: r.commodity_id,
              goods_id: r.commodity_id,
              goodsId: r.commodity_id,
            }
          );
        }
        if (lk && !buyinProductLinkLooksValid(lk)) lk = '';
        r.link = lk;
      }
      if (isBuyin() && imgOut) imgOut = geNormalizeBuyinImgSrcForFeishu(imgOut);
    } catch (_) {}
    return {
      ...r,
      platform,
      imgSrc: imgOut,
      shopTag: r.shopTag != null ? String(r.shopTag) : '',
      monitorTimes: r.monitorTimes != null && r.monitorTimes !== '' ? String(r.monitorTimes) : '',
      monitorIndex: r.monitorIndex != null && r.monitorIndex !== '' ? String(r.monitorIndex) : '',
      inspectCycle: r.inspectCycle != null && r.inspectCycle !== '' ? String(r.inspectCycle) : '',
      shelfTime: r.shelfTime != null ? String(r.shelfTime) : '',
      recordType: r.recordType != null ? String(r.recordType) : '',
      promotion_id: r.promotion_id != null ? String(r.promotion_id) : '',
      product_id: r.product_id != null ? String(r.product_id) : '',
      commodity_id: r.commodity_id != null ? String(r.commodity_id) : '',
      guarantee: r.guarantee != null ? String(r.guarantee) : '',
      deliveryTime: r.deliveryTime != null ? String(r.deliveryTime) : '',
    };
  }

  /** 用户常把 tenant_access_token 误填进「Wiki 节点 token」（一般以 t-g / u- 开头） */
  function looksLikeFeishuAccessTokenNotWikiNode(s) {
    const v = (s || '').trim();
    if (!v) return false;
    if (/^t-g[a-z0-9]{8,}$/i.test(v)) return true;
    if (/^u-[a-z0-9_-]{10,}$/i.test(v)) return true;
    if (/^pat_[a-z0-9]{8,}$/i.test(v)) return true;
    return false;
  }

  /** Wiki 地址栏 /wiki/{token}?... 中的 token；用 tenant_access_token 换真正的 bitable app_token（obj_token） */
  async function wikiNodeToBitableAppToken(wikiNodeToken, accessToken) {
    const nodeTok = wikiNodeToken.trim();
    if (looksLikeFeishuAccessTokenNotWikiNode(nodeTok)) {
      throw new Error(
        '【填错格子】「Wiki 节点 token」里填的是鉴权令牌（如以 t-g、u- 开头），不是网址里的节点 ID。\n\n请改成浏览器地址栏中 /wiki/ 与 ? 之间那一段（例如 HjwDwPyUtidOTQk6T2scehMPnif）；t-g… 只填在「access_token」。'
      );
    }
    const tok = accessToken.trim();
    if (!tok) throw new Error('access_token 为空，无法请求 Wiki 接口');
    const auth = tok.toLowerCase().startsWith('bearer ') ? tok : 'Bearer ' + tok;
    const url =
      'https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node?token=' + encodeURIComponent(nodeTok);
    const r = await gmXhr({
      method: 'GET',
      url,
      headers: { Authorization: auth },
    });
    let json;
    try {
      json = JSON.parse(r.responseText || '{}');
    } catch (_) {
      throw new Error('Wiki 节点接口返回非 JSON（HTTP ' + r.status + '）');
    }
    if (json.code !== 0) {
      const msg = json.msg || '获取 Wiki 节点失败';
      const code = json.code;
      let hint =
        '请为应用开通「查看知识空间节点信息 wiki:node:read」或「wiki:wiki」等权限，并把应用授权到该知识库。';
      if (code === 99991668 || /invalid access token/i.test(msg)) {
        hint =
          '若已确认 access_token 有效：多数是「Wiki 节点 token」填成了 t-g…/u-… 鉴权串。节点 token 必须来自地址栏 /wiki/xxx? 里的 xxx（与调试台 Authorization 不是同一个值）。';
      }
      throw new Error(msg + ' code=' + code + '。' + hint);
    }
    const node = json.data && json.data.node;
    if (!node) throw new Error('Wiki 接口未返回 node');
    if (node.obj_type !== 'bitable') {
      throw new Error(
        '该 Wiki 节点类型为「' +
          (node.obj_type || '?') +
          '」，不是多维表格 bitable。请从多维表格页面复制链接，或确认节点指向的是多维表格。'
      );
    }
    if (!node.obj_token) throw new Error('Wiki 节点未返回 obj_token');
    return String(node.obj_token);
  }

  async function resolveBitableAppToken(cfg) {
    const wiki = (cfg.wikiNodeToken || '').trim();
    if (wiki) return wikiNodeToBitableAppToken(wiki, cfg.accessToken);
    return (cfg.appToken || '').trim();
  }

  /** 与其它 RPA 一致：用 App ID + Secret 换 tenant_access_token（写入前调用） */
  async function feishuFetchTenantAccessToken(appId, appSecret) {
    const r = await gmXhr({
      method: 'POST',
      url: 'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',
      headers: { 'Content-Type': 'application/json; charset=utf-8' },
      body: JSON.stringify({ app_id: appId.trim(), app_secret: appSecret.trim() }),
    });
    let json;
    try {
      json = JSON.parse(r.responseText || '{}');
    } catch (_) {
      throw new Error('换取 token 返回非 JSON（HTTP ' + r.status + '）');
    }
    if (json.code !== 0) {
      throw new Error((json.msg || '换取 tenant_access_token 失败') + ' code=' + json.code);
    }
    const t = json.tenant_access_token;
    if (!t) throw new Error('响应中无 tenant_access_token');
    return String(t);
  }

  /** 若配置了 App ID+Secret 则每次写入前刷新 access_token，避免与其它工具不一致 */
  async function ensureFeishuAccessToken(cfg) {
    const id = (cfg.feishuAppId || '').trim();
    const sec = (cfg.feishuAppSecret || '').trim();
    if (id && sec) {
      const t = await feishuFetchTenantAccessToken(id, sec);
      return { ...cfg, accessToken: t };
    }
    return cfg;
  }

  /** 飞书多维表格字段类型：1 文本 2 数字 15 超链接（与开放平台文档一致） */
  function feishuBitableTypeForMappedKey(internalKey, cfg) {
    if (cfg.feishuCoerceNumberFields && (internalKey === 'price' || internalKey === 'sales')) return 2;
    if (cfg.useHyperlink && (internalKey === 'link' || internalKey === 'shopLink')) return 15;
    return 1;
  }

  async function feishuBitableListFieldsPage(appToken, tableId, accessToken, pageToken) {
    let url =
      'https://open.feishu.cn/open-apis/bitable/v1/apps/' +
      encodeURIComponent(appToken) +
      '/tables/' +
      encodeURIComponent(tableId) +
      '/fields?page_size=100';
    if (pageToken) url += '&page_token=' + encodeURIComponent(pageToken);
    const tok = accessToken.trim();
    const auth = tok.toLowerCase().startsWith('bearer ') ? tok : 'Bearer ' + tok;
    const r = await gmXhr({
      method: 'GET',
      url,
      headers: { Authorization: auth },
    });
    let json;
    try {
      json = JSON.parse(r.responseText || '{}');
    } catch (_) {
      throw new Error('列出字段返回非 JSON（HTTP ' + r.status + '）');
    }
    if (json.code !== 0) {
      throw new Error((json.msg || '列出多维表格字段失败') + ' code=' + json.code);
    }
    const d = json.data || {};
    return {
      items: Array.isArray(d.items) ? d.items : [],
      hasMore: !!d.has_more,
      pageToken: d.page_token || '',
    };
  }

  async function feishuBitableListAllFieldNames(appToken, tableId, accessToken) {
    const names = new Set();
    let pt = '';
    for (let guard = 0; guard < 50; guard++) {
      const page = await feishuBitableListFieldsPage(appToken, tableId, accessToken, pt);
      for (const it of page.items) {
        const n = (it && it.field_name != null ? String(it.field_name) : '').trim();
        if (n) names.add(n);
      }
      if (!page.hasMore || !page.pageToken) break;
      pt = page.pageToken;
      await sleep(120);
    }
    return names;
  }

  async function feishuBitableListRecordsPage(appToken, tableId, accessToken, pageToken) {
    let url =
      'https://open.feishu.cn/open-apis/bitable/v1/apps/' +
      encodeURIComponent(appToken) +
      '/tables/' +
      encodeURIComponent(tableId) +
      '/records?page_size=500';
    if (pageToken) url += '&page_token=' + encodeURIComponent(pageToken);
    const tok = accessToken.trim();
    const auth = tok.toLowerCase().startsWith('bearer ') ? tok : 'Bearer ' + tok;
    const r = await gmXhr({
      method: 'GET',
      url,
      headers: { Authorization: auth },
    });
    let json;
    try {
      json = JSON.parse(r.responseText || '{}');
    } catch (_) {
      throw new Error('读取多维表格记录返回非 JSON（HTTP ' + r.status + '）');
    }
    if (json.code !== 0) {
      throw new Error((json.msg || '读取多维表格记录失败') + ' code=' + json.code);
    }
    const d = json.data || {};
    return {
      items: Array.isArray(d.items) ? d.items : [],
      hasMore: !!d.has_more,
      pageToken: d.page_token || '',
      total: Math.max(0, parseInt(d.total, 10) || 0),
    };
  }

  async function feishuBitableCreateField(appToken, tableId, accessToken, fieldName, typeNum) {
    const url =
      'https://open.feishu.cn/open-apis/bitable/v1/apps/' +
      encodeURIComponent(appToken) +
      '/tables/' +
      encodeURIComponent(tableId) +
      '/fields';
    const tok = accessToken.trim();
    const auth = tok.toLowerCase().startsWith('bearer ') ? tok : 'Bearer ' + tok;
    const r = await gmXhr({
      method: 'POST',
      url,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        Authorization: auth,
      },
      body: JSON.stringify({ field_name: fieldName, type: typeNum }),
    });
    let json;
    try {
      json = JSON.parse(r.responseText || '{}');
    } catch (_) {
      throw new Error('新增字段返回非 JSON（HTTP ' + r.status + '）');
    }
    if (json.code === 0) return true;
    const c = Number(json.code);
    if (c === 1254014 || /FieldNameDuplicated|duplicate/i.test(String(json.msg || ''))) return false;
    throw new Error((json.msg || '新增多维表格字段失败') + ' code=' + json.code);
  }

  /**
   * 按当前列映射在目标子表上补齐缺失列（写入前调用）。
   * @returns {{ created: number }}
   */
  async function ensureFeishuBitableMissingColumns(bitableAppToken, tableId, cfgTok) {
    if (cfgTok.feishuAutoCreateFields === false) return { created: 0 };
    const merged = mergeFieldMap(cfgTok);
    const existing = await feishuBitableListAllFieldNames(bitableAppToken, tableId, cfgTok.accessToken);
    let created = 0;
    for (const k of FEISHU_UPLOAD_FIELD_KEYS) {
      const colTitle = (merged[k] || '').trim();
      if (!colTitle) continue;
      if (existing.has(colTitle)) continue;
      const typ = feishuBitableTypeForMappedKey(k, cfgTok);
      const ok = await feishuBitableCreateField(bitableAppToken, tableId, cfgTok.accessToken, colTitle, typ);
      if (ok) {
        existing.add(colTitle);
        created++;
      } else existing.add(colTitle);
      await sleep(150);
    }
    return { created };
  }

  async function feishuBatchCreate(appToken, tableId, accessToken, records) {
    const url =
      'https://open.feishu.cn/open-apis/bitable/v1/apps/' +
      encodeURIComponent(appToken) +
      '/tables/' +
      encodeURIComponent(tableId) +
      '/records/batch_create';
    const tok = accessToken.trim();
    const auth = tok.toLowerCase().startsWith('bearer ') ? tok : 'Bearer ' + tok;
    const r = await gmXhr({
      method: 'POST',
      url,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        Authorization: auth,
      },
      body: JSON.stringify({ records }),
    });
    let json;
    try {
      json = JSON.parse(r.responseText || '{}');
    } catch (_) {
      throw new Error('飞书返回非 JSON（HTTP ' + r.status + '）');
    }
    if (json.code !== 0) {
      const extra = json.error ? JSON.stringify(json.error) : '';
      const codeNum = Number(json.code);
      let tail = (json.msg || '飞书错误') + (extra ? ' ' + extra : '') + ' code=' + json.code;
      if (codeNum === 1254061 || /NumberFieldConvFail|must be a number/i.test(tail)) {
        tail +=
          '\n\n【数字列类型不符】飞书里「价格/销量」若为数字列，不能传「¥211」等字符串。打开飞书配置勾选「价格、销量按数字列写入」，或把该列改为文本。';
      }
      if (codeNum === 91403) {
        tail +=
          '\n\n【Forbidden 91403 = 云文档资源未授权给「应用」】\n' +
          '开放平台里勾选 bitable:app、能调通调试台，只代表「应用具备能力」；写入某张表还需把该应用当作协作者授权到<strong>这一份</strong>多维表格上。\n\n' +
          '① 飞书桌面端打开<strong>同一张</strong>多维表格 → 右上角「分享」→ 搜索你的<strong>应用名称</strong>（与开放平台应用名一致）→ 权限选「可编辑」或「可管理」。\n' +
          '② 表在<strong>知识库 Wiki</strong>：除上述外，还需在知识空间把应用加为成员/可管理，或完成文档：\n' +
          '   https://open.feishu.cn/document/ukTMukTMukTM/uUDN04SN0QjL1QDN/wiki-v2/wiki-qa\n' +
          '③ 其它 RPA 能写入而脚本不能：多为那边用了<strong>用户身份</strong>或已在企业内给表做过授权；你仍用 tenant 时，必须完成 ①②。\n' +
          '④ 仍失败：用对该表有编辑权的账号获取 user_access_token，只填 access_token（可不填 App ID/Secret）。';
      }
      throw new Error(tail);
    }
    return json;
  }

  async function uploadRowsToFeishu(rows, cfg) {
    const cfgTok = await ensureFeishuAccessToken(cfg);
    const bitableAppToken = await resolveBitableAppToken(cfgTok);
    if (!bitableAppToken) throw new Error('请填写「Wiki 节点 token」或「app_token」其一');
    const merged = mergeFieldMap(cfgTok);
    const colResult = await ensureFeishuBitableMissingColumns(bitableAppToken, cfgTok.tableId, cfgTok);
    if (geShelfDebugEnabled() && rows && rows.length) {
      const sample = rows.slice(0, 5).map(function (r) {
        return {
          title: ((r && r.title) || '').toString().slice(0, 28),
          shelfTime: ((r && r.shelfTime) || '').toString(),
        };
      });
      geShelfDebugLog('before feishu rows sample (前5条 title + shelfTime):', sample);
    }
    const batches = chunk(rows, 100);
    let total = 0;
    for (let i = 0; i < batches.length; i++) {
      const records = batches[i].map((row) => ({ fields: rowToFeishuFields(row, merged, cfgTok) }));
      await feishuBatchCreate(bitableAppToken, cfgTok.tableId, cfgTok.accessToken, records);
      total += records.length;
      if (i < batches.length - 1) await sleep(320);
    }
    return { total, columnsCreated: colResult.created };
  }

  /** 店铺管理专用：仅写入「店铺名称 / 店铺链接」两列，避免自动补齐商品相关列 */
  async function uploadShopTasksToFeishu(tasks, cfg) {
    const cfgTok = await ensureFeishuAccessToken(cfg);
    const bitableAppToken = await resolveBitableAppToken(cfgTok);
    if (!bitableAppToken) throw new Error('请填写「Wiki 节点 token」或「app_token」其一');
    const merged = mergeFieldMap(cfgTok);
    const shopNameCol = (merged.shopName || '店铺名称').trim();
    const shopLinkCol = (merged.shopLink || '店铺链接').trim();
    const existing = await feishuBitableListAllFieldNames(bitableAppToken, cfgTok.tableId, cfgTok.accessToken);
    let created = 0;
    if (shopNameCol && !existing.has(shopNameCol)) {
      const ok1 = await feishuBitableCreateField(bitableAppToken, cfgTok.tableId, cfgTok.accessToken, shopNameCol, 1);
      if (ok1) created++;
      existing.add(shopNameCol);
      await sleep(120);
    }
    if (shopLinkCol && !existing.has(shopLinkCol)) {
      const typ = cfgTok.useHyperlink ? 15 : 1;
      const ok2 = await feishuBitableCreateField(bitableAppToken, cfgTok.tableId, cfgTok.accessToken, shopLinkCol, typ);
      if (ok2) created++;
      existing.add(shopLinkCol);
      await sleep(120);
    }
    const rows = [];
    for (let i = 0; i < tasks.length; i++) {
      const t = tasks[i] || {};
      const name = String(t.shopName || '').trim();
      const link = String(t.url || '').trim();
      if (!link) continue;
      const fields = {};
      if (shopNameCol && name) fields[shopNameCol] = name;
      if (shopLinkCol && link) fields[shopLinkCol] = cfgTok.useHyperlink ? { text: link, link } : link;
      if (Object.keys(fields).length) rows.push({ fields });
    }
    const batches = chunk(rows, 100);
    let total = 0;
    for (let i = 0; i < batches.length; i++) {
      await feishuBatchCreate(bitableAppToken, cfgTok.tableId, cfgTok.accessToken, batches[i]);
      total += batches[i].length;
      if (i < batches.length - 1) await sleep(320);
    }
    return { total, columnsCreated: created };
  }

  function showFeishuSettingsModal() {
    if (document.getElementById('ge-feishu-modal')) return;
    const cfg = loadFeishuConfig();
    const mask = document.createElement('div');
    mask.id = 'ge-feishu-modal';
    mask.style.cssText =
      'position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:2147483646;display:flex;align-items:center;justify-content:center;padding:16px;box-sizing:border-box';
    const box = document.createElement('div');
    box.style.cssText =
      'background:#fff;border-radius:12px;max-width:min(640px,100vw - 32px);width:100%;max-height:90vh;overflow:auto;padding:20px;font:14px/1.55 -apple-system,BlinkMacSystemFont,sans-serif;color:#1f2329;box-shadow:0 8px 32px rgba(0,0,0,.2)';
    const defaultMapStr = JSON.stringify(FEISHU_DEFAULT_FIELD_MAP, null, 2);
    const guideStyle = 'margin:0 0 8px;font-size:13px;color:#1f2329;line-height:1.65';
    const h4 = 'margin:14px 0 6px;font-size:13px;font-weight:600;color:#1f2329';
    const codeBox =
      'display:block;margin:8px 0;padding:10px 12px;background:#f5f6f7;border:1px solid #e5e6eb;border-radius:6px;font:12px/1.5 Consolas,monospace;white-space:pre-wrap;word-break:break-all;color:#3370ff';
    box.innerHTML =
      '<div style="font-weight:600;font-size:17px;margin-bottom:4px">飞书多维表格 · 写入配置</div>' +
      '<p style="margin:0 0 14px;font-size:13px;color:#646a73">下方三项填好后点「保存」，回到商品页再点「写入飞书表格」。请按折叠区内的步骤操作（点击蓝色标题可收起说明）。</p>' +
      '<details open style="margin-bottom:16px;border:1px solid #e5e6eb;border-radius:10px;padding:12px 14px;background:#fafbfc">' +
      '<summary style="cursor:pointer;font-weight:600;color:#3370ff;outline:none">配置步骤（建议按顺序做）</summary>' +
      '<div style="margin-top:12px">' +
      '<p style="' +
      h4 +
      '">① 子表与列</p>' +
      '<p style="' +
      guideStyle +
      '">在飞书里打开<strong>多维表格</strong>，新建或选中一张<strong>数据表</strong>（子表）。可<strong>只建空子表</strong>：勾选下方「写入前自动建列」后，首次写入前会按 JSON 映射调用飞书接口<strong>自动新增缺失列</strong>（列名与类型与下方「超链接 / 数字列」选项一致）。若你习惯手建列，可取消勾选并自行建列，列名须与映射一致（默认：标题、价格、佣金、销量、商品链接、图片、店铺名称、店铺链接、平台、保障、发货时效）。</p>' +
      '<p style="' +
      h4 +
      '">② 从浏览器地址栏抄 app_token、table_id</p>' +
      '<p style="' +
      guideStyle +
      '">用<strong>电脑浏览器</strong>打开该多维表格，并<strong>点选要写入的那张子表</strong>，看地址栏完整 URL：</p>' +
      '<ul style="margin:0 0 10px 18px;padding:0;font-size:13px;color:#1f2329;line-height:1.65">' +
      '<li>若 URL 含 <code style="background:#eee;padding:2px 6px;border-radius:4px">/base/</code>：在 <code>/base/</code> 后面、直到下一个 <code>/</code> 或 <code>?</code> 之前的那一段，就是 <strong>app_token</strong>（通常类似 <code>BaseXXXXXXXX</code>）。</li>' +
      '<li>同一 URL 里查找参数 <code style="background:#eee;padding:2px 6px;border-radius:4px">table=</code>，等号后面整段（以 <code>tbl</code> 开头）是 <strong>table_id</strong>，例如 <code>tblAbCdEfGh</code>。</li>' +
      '<li>示例：<code style="font-size:12px">https://xxx.feishu.cn/base/<b>BaseXXXX</b>?table=<b>tblYYYY</b></code> → app_token=<code>BaseXXXX</code>，table_id=<code>tblYYYY</code>。</li>' +
      '<li><strong>知识库 Wiki 里打开的多维表格</strong>（如 <code style="font-size:11px;word-break:break-all">my.feishu.cn/wiki/<b>HjwDwPyUtidOTQk6T2scehMPnif</b>?table=<b>tblzOh9PaQSgcl3L</b></code>）：<code>/wiki/</code> 与 <code>?</code> 之间是 <strong>Wiki 节点 token</strong>（不是写入 API 用的 app_token）。把它填到下方「Wiki 节点 token」；<code>table=</code> 后仍是 <strong>table_id</strong>。脚本会用你已填的 <code>access_token</code>（tenant_access_token）请求 <a href="https://open.feishu.cn/document/server-docs/docs/wiki-v2/space-node/get_node" target="_blank" rel="noopener noreferrer">获取知识空间节点信息</a>，读出 <code>obj_type=bitable</code> 时的 <code>obj_token</code> 作为真正的多维表格 app_token 再写入数据。</li>' +
      '</ul>' +
      '<p style="' +
      h4 +
      '">③ 开放平台建应用、开权限、拿令牌</p>' +
      '<ol style="margin:0 0 10px 18px;padding:0;font-size:13px;color:#1f2329;line-height:1.65">' +
      '<li>打开 <a href="https://open.feishu.cn/app" target="_blank" rel="noopener noreferrer">飞书开放平台</a> → 登录企业账号 → <strong>创建企业自建应用</strong>（名称随意）。</li>' +
      '<li>进入应用 → <strong>权限管理</strong> → 添加 API 权限 → 搜索「<strong>多维表格</strong>」或「<strong>bitable</strong>」→ 勾选 <strong>查看、评论、编辑和管理多维表格</strong>（文档名 <code>bitable:app</code>，含新增记录与<strong>新增字段</strong>能力）。保存。</li>' +
      '<li>表格若在<strong>知识库 Wiki</strong>内打开，还需添加 Wiki 权限（任一项即可），例如 <strong>查看知识空间节点信息</strong>（<code>wiki:node:read</code>）或 <strong>查看、编辑和管理知识库</strong>（<code>wiki:wiki</code>）。否则无法解析节点。另需在知识库侧将<strong>应用添加为知识空间成员/管理员</strong>或按 <a href="https://open.feishu.cn/document/ukTMukTMukTM/uUDN04SN0QjL1QDN/wiki-v2/wiki-qa" target="_blank" rel="noopener noreferrer">飞书：应用访问知识库文档</a> 完成资源授权。</li>' +
      '<li><strong>版本管理与发布</strong>：创建版本并提交发布（若企业需审批，等审批通过后再试接口）。</li>' +
      '<li>应用 → <strong>凭证与基础信息</strong>：复制 <code>App ID</code>、<code>App Secret</code>。无论多维表格在独立 base 还是 Wiki 里，都用这一对换取 <strong>tenant_access_token</strong>（下面接口与 Wiki 无关）。</li>' +
      '<li>获取 <strong>tenant_access_token</strong>（机器人写表推荐）：' +
      '<ul style="margin:8px 0 0 16px">' +
      '<li>在开放平台该应用页打开「<strong>API 调试台</strong>」或文档 <a href="https://open.feishu.cn/document/server-docs/docs/auth-v3/tenant_access_token/internal" target="_blank" rel="noopener noreferrer">获取 tenant_access_token</a> 的在线调试；或</li>' +
      '<li>用 Postman / curl 按下方示例 POST（<code>app_id</code> 填你的 App ID，如 <code>cli_a8f4579b00f3d00c</code>；<code>app_secret</code> 填应用密钥）：</li>' +
      '</ul></li>' +
      '</ol>' +
      '<div style="' +
      codeBox +
      '">POST https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal\nContent-Type: application/json\n\n{\n  "app_id": "你的App ID",\n  "app_secret": "你的App Secret"\n}</div>' +
      '<p style="' +
      guideStyle +
      '">响应 JSON 里取字段 <code>tenant_access_token</code> 的字符串，粘贴到下方「access_token」。<strong>不要</strong>带引号；是否加前缀 <code>Bearer </code> 均可，脚本会自动处理。</p>' +
      '<p style="' +
      guideStyle +
      '">令牌约 <strong>2 小时</strong>过期，过期后需重新请求一次再粘贴。若你更熟悉 OAuth，也可使用 <code>user_access_token</code>（需用户授权流程），同样贴在下方。</p>' +
      '<p style="' +
      h4 +
      '">④ 常见报错</p>' +
      '<ul style="margin:0 0 0 18px;padding:0;font-size:13px;color:#646a73;line-height:1.65">' +
      '<li><strong>403 / 无权限</strong>：应用未发布、权限未勾选、或该多维表格未对应用所在企业可见；请在表格「···」共享/权限里确认企业内可访问。</li>' +
      '<li><strong>Forbidden code=91403</strong>：接口权限已开，但<strong>文档资源未授权给应用</strong>。请到该多维表格（或 Wiki 知识库）把<strong>自建应用机器人添加为可编辑协作者</strong>；Wiki 表另需按 <a href="https://open.feishu.cn/document/ukTMukTMukTM/uUDN04SN0QjL1QDN/wiki-v2/wiki-qa" target="_blank" rel="noopener noreferrer">知识库-应用授权</a> 操作。仍失败可改用 <code>user_access_token</code>。</li>' +
      '<li><strong>字段不存在 / NumberFieldConvFail / code 1254061</strong>：飞书「价格」「销量」若为<strong>数字</strong>列，不能写入「¥211」等文本。请勾选下方「价格、销量按数字列写入」，或把飞书里对应列改为文本。</li>' +
      '<li><strong>其它类型不匹配</strong>：列名与 JSON 映射不一致；超链接列需勾选「超链接格式」。</li>' +
      '<li><strong>从其它数据源同步的表</strong>：飞书禁止 API 新增行，请换一张普通数据表。</li>' +
      '</ul>' +
      '<p style="margin:12px 0 0;font-size:12px;color:#8f959e">官方文档：<a href="https://open.feishu.cn/document/server-docs/docs/bitable-v1/app-table-record/batch_create" target="_blank" rel="noopener noreferrer">批量新增记录</a> · <a href="https://open.feishu.cn/document/server-docs/docs/bitable-v1/app-table-field/create" target="_blank" rel="noopener noreferrer">新增字段</a> · <a href="https://open.feishu.cn/document/server-docs/docs/auth-v3/tenant_access_token/internal" target="_blank" rel="noopener noreferrer">获取 tenant_access_token</a></p>' +
      '</div></details>' +
      '<hr style="border:none;border-top:1px solid #e5e6eb;margin:16px 0" />' +
      '<p style="margin:0 0 10px;font-size:13px;color:#646a73"><strong>Wiki 与 base 二选一：</strong>知识库内打开的表填「Wiki 节点 token」；独立多维表格（URL 含 <code>/base/</code>）填「app_token」，另一项留空。</p>' +
      '<label style="display:block;margin-bottom:6px;font-weight:500">Wiki 节点 token <span style="color:#8f959e;font-weight:400">（仅网址里 <code>/wiki/此处</code>，<strong>不是</strong> t-g… 令牌）</span></label>' +
      '<p style="margin:-2px 0 8px;padding:8px 10px;background:#fff7e8;border:1px solid #ffd591;border-radius:8px;font-size:12px;color:#ad4e00;line-height:1.55"><strong>不要填错：</strong>调试台「Authorization」里的 <code>Bearer t-g…</code> 是 <strong>access_token</strong>（或改用下方 App ID+Secret 自动生成）。本框只填地址栏 <code>…/wiki/<b>节点ID</b>?table=…</code> 中的<strong>节点 ID</strong>（不以 t-g、u- 开头）。</p>' +
      '<input id="ge-fs-wiki" type="text" style="width:100%;box-sizing:border-box;padding:8px 10px;border:1px solid #dee0e3;border-radius:6px;margin-bottom:12px" placeholder="例：HjwDwPyUtidOTQk6T2scehMPnif（勿填 t-g…）" />' +
      '<label style="display:block;margin-bottom:6px;font-weight:500">app_token <span style="color:#8f959e;font-weight:400">（仅独立表：<code>/base/</code> 与 <code>?</code> 之间，如 BaseXXX）</span></label>' +
      '<input id="ge-fs-app" type="text" style="width:100%;box-sizing:border-box;padding:8px 10px;border:1px solid #dee0e3;border-radius:6px;margin-bottom:12px" placeholder="独立多维表格填此项；Wiki 表留空" />' +
      '<label style="display:block;margin-bottom:6px;font-weight:500">table_id <span style="color:#8f959e;font-weight:400">（参数 table= 后的 tbl…）</span></label>' +
      '<input id="ge-fs-tbl" type="text" style="width:100%;box-sizing:border-box;padding:8px 10px;border:1px solid #dee0e3;border-radius:6px;margin-bottom:12px" placeholder="例如 tblXXXXXXXX" />' +
      '<p style="margin:0 0 8px;font-size:12px;color:#646a73"><strong>与其它 RPA 一致（可选）：</strong>填写开放平台「应用凭证」里的 App ID、App Secret 后，每次写入前会自动换取 tenant_access_token，<strong>可不填</strong> access_token。</p>' +
      '<label style="display:block;margin-bottom:6px;font-weight:500">飞书 App ID <span style="color:#8f959e;font-weight:400">（cli_ 开头，可选）</span></label>' +
      '<input id="ge-fs-aid" type="text" style="width:100%;box-sizing:border-box;padding:8px 10px;border:1px solid #dee0e3;border-radius:6px;margin-bottom:12px" placeholder="可与 App Secret 成对填写，密钥仅存于本机脚本存储" />' +
      '<label style="display:block;margin-bottom:6px;font-weight:500">飞书 App Secret <span style="color:#8f959e;font-weight:400">（可选）</span></label>' +
      '<input id="ge-fs-asec" type="password" style="width:100%;box-sizing:border-box;padding:8px 10px;border:1px solid #dee0e3;border-radius:6px;margin-bottom:12px" autocomplete="off" />' +
      '<label style="display:block;margin-bottom:6px;font-weight:500">access_token <span style="color:#8f959e;font-weight:400">（未填 App ID+Secret 时必填；t-g… 可含或不含 Bearer）</span></label>' +
      '<input id="ge-fs-tok" type="password" style="width:100%;box-sizing:border-box;padding:8px 10px;border:1px solid #dee0e3;border-radius:6px;margin-bottom:12px" autocomplete="off" placeholder="与 App ID+Secret 二选一；勿填进 Wiki 节点框" />' +
      '<label style="display:flex;align-items:flex-start;gap:8px;margin-bottom:12px;cursor:pointer;font-size:13px"><input id="ge-fs-autocol" type="checkbox" style="margin-top:4px" checked /> <span><strong>写入前自动建列</strong>：子表缺少映射中的列名时，写入前先调用飞书接口创建（与下方「超链接」「数字列」选项一致的字段类型）。仅手建表头时请取消勾选。</span></label>' +
      '<label style="display:flex;align-items:flex-start;gap:8px;margin-bottom:12px;cursor:pointer;font-size:13px"><input id="ge-fs-hyp" type="checkbox" style="margin-top:4px" /> <span>商品链接、店铺链接两列在飞书里是「<strong>超链接</strong>」类型时勾选；否则保持不选（按纯文本写入 URL）。</span></label>' +
      '<label style="display:flex;align-items:flex-start;gap:8px;margin-bottom:12px;cursor:pointer;font-size:13px"><input id="ge-fs-num" type="checkbox" style="margin-top:4px" /> <span>价格、销量在飞书里是「<strong>数字</strong>」列时<strong>务必勾选</strong>（¥211→211；<strong>抓取为空则不传该格</strong>，表里留空；佣金含 % 勿设数字列）。</span></label>' +
      '<label style="display:block;margin-bottom:6px;font-weight:500">列标题映射 <span style="color:#8f959e;font-weight:400">（JSON：含 title/price/…/platform/<strong>shopTag</strong>/<strong>monitorTimes</strong>/<strong>monitorIndex</strong>/<strong>inspectCycle</strong>巡检周期/<strong>shelfTime</strong>上架时间/<strong>recordType</strong>录入类型 等 → 飞书列名）</span></label>' +
      '<textarea id="ge-fs-map" style="width:100%;height:160px;box-sizing:border-box;padding:10px;border:1px solid #dee0e3;border-radius:6px;font:12px monospace;margin-bottom:16px"></textarea>' +
      '<div style="display:flex;gap:10px;justify-content:flex-end;flex-wrap:wrap">' +
      '<button type="button" id="ge-fs-cancel" style="padding:8px 16px;border:1px solid #dee0e3;border-radius:6px;background:#fff;cursor:pointer">取消</button>' +
      '<button type="button" id="ge-fs-save" style="padding:8px 16px;border:none;border-radius:6px;background:#3370ff;color:#fff;cursor:pointer">保存</button></div>';
    mask.appendChild(box);
    (document.body || document.documentElement).appendChild(mask);
    const wikiEl = box.querySelector('#ge-fs-wiki');
    const appEl = box.querySelector('#ge-fs-app');
    const tblEl = box.querySelector('#ge-fs-tbl');
    const aidEl = box.querySelector('#ge-fs-aid');
    const asecEl = box.querySelector('#ge-fs-asec');
    const tokEl = box.querySelector('#ge-fs-tok');
    const autocolEl = box.querySelector('#ge-fs-autocol');
    const hypEl = box.querySelector('#ge-fs-hyp');
    const numEl = box.querySelector('#ge-fs-num');
    const mapEl = box.querySelector('#ge-fs-map');
    wikiEl.value = cfg.wikiNodeToken;
    appEl.value = cfg.appToken;
    tblEl.value = cfg.tableId;
    aidEl.value = cfg.feishuAppId;
    asecEl.value = cfg.feishuAppSecret;
    tokEl.value = cfg.accessToken;
    autocolEl.checked = cfg.feishuAutoCreateFields !== false;
    hypEl.checked = cfg.useHyperlink;
    numEl.checked = cfg.feishuCoerceNumberFields;
    mapEl.value = cfg.fieldMapJson && cfg.fieldMapJson.trim() ? cfg.fieldMapJson : defaultMapStr;
    function close() {
      try {
        mask.remove();
      } catch (_) {}
    }
    box.querySelector('#ge-fs-cancel').addEventListener('click', close);
    mask.addEventListener('click', (e) => {
      if (e.target === mask) close();
    });
    box.querySelector('#ge-fs-save').addEventListener('click', () => {
      const wTok = (wikiEl.value || '').trim();
      if (wTok && looksLikeFeishuAccessTokenNotWikiNode(wTok)) {
        alert(
          '「Wiki 节点 token」不能填 t-g… / u-… 这类鉴权串。\n\n请填浏览器地址栏 /wiki/ 与 ? 之间的节点 ID；鉴权串只填在「access_token」。'
        );
        return;
      }
      saveFeishuConfig({
        wikiNodeToken: wikiEl.value,
        appToken: appEl.value,
        tableId: tblEl.value,
        feishuAppId: aidEl.value,
        feishuAppSecret: asecEl.value,
        accessToken: tokEl.value,
        fieldMapJson: mapEl.value,
        useHyperlink: hypEl.checked,
        feishuCoerceNumberFields: numEl.checked,
        feishuAutoCreateFields: autocolEl.checked,
      });
      alert('飞书配置已保存');
      close();
    });
  }

  function getBuyinPageShopContext() {
    let shopName = '';
    try {
      const u = new URL(location.href);
      const shopId = u.searchParams.get('shop_id') || u.searchParams.get('shopId') || '';
      const shopLink = location.href.split('#')[0];
      const h =
        document.querySelector('h1') ||
        document.querySelector('[class*="shopName"]') ||
        document.querySelector('[class*="ShopName"]') ||
        document.querySelector('[class*="merchantName"]');
      shopName = (h && (h.textContent || '').trim()) || '';
      return { shopId, shopLink, shopName };
    } catch (_) {
      return { shopId: '', shopLink: location.href.split('#')[0], shopName: '' };
    }
  }

  function geFormatDateYmdHms(d) {
    const p = function (n) {
      return String(n).padStart(2, '0');
    };
    return (
      d.getFullYear() +
      '-' +
      p(d.getMonth() + 1) +
      '-' +
      p(d.getDate()) +
      ' ' +
      p(d.getHours()) +
      ':' +
      p(d.getMinutes()) +
      ':' +
      p(d.getSeconds())
    );
  }

  /** 将接口里可能是秒/毫秒时间戳或 ISO 字符串转为 YYYY-MM-DD HH:mm:ss；无法识别则返回 '' */
  function geFormatMaybeTimestamp(v) {
    if (v == null || v === '') return '';
    if (typeof v === 'object' && v !== null && !Array.isArray(v)) {
      const inner =
        v.timestamp ??
        v.Timestamp ??
        v.seconds ??
        v.sec ??
        v.milliseconds ??
        v.ms ??
        v.time ??
        v.Time ??
        v.value ??
        v.create_time ??
        v.createTime ??
        v.shelf_time ??
        v.shelfTime;
      if (inner != null && inner !== v) return geFormatMaybeTimestamp(inner);
      return '';
    }
    if (typeof v === 'number' && Number.isFinite(v)) {
      const ms = v < 1e12 ? v * 1000 : v;
      const d = new Date(ms);
      if (!Number.isFinite(d.getTime())) return '';
      return geFormatDateYmdHms(d);
    }
    if (typeof v === 'string') {
      const s = v.trim();
      if (!s) return '';
      if (/^\d{10,13}$/.test(s)) return geFormatMaybeTimestamp(Number(s));
      const d = new Date(s);
      if (Number.isFinite(d.getTime())) return geFormatDateYmdHms(d);
    }
    return '';
  }

  const GE_SHELF_API_KEY_RE =
    /^(create_time|created_at|shelf_time|list_time|publish_time|online_time|on_shelf_time|on_shelf_timestamp|first_on_shelf|first_shelf_time|shelf_timestamp|put_on_time|grounding_time|product_create_time|gmt_create|createdTime|shelfTime|listTime|add_time|added_at|in_stock_time|listing_time|firstShelfTime|onShelfTime)$/i;

  /** 与接口约定字段名一致：当前节点优先按此顺序尝试（再递归子对象） */
  const GE_SHELF_DIRECT_KEYS = [
    'create_time',
    'created_at',
    'shelf_time',
    'list_time',
    'publish_time',
    'online_time',
    'on_shelf_time',
    'first_on_shelf',
    'put_on_time',
    'grounding_time',
    'product_create_time',
    'gmt_create',
    'createdTime',
    'shelfTime',
    'listTime',
    'add_time',
    'added_at',
    'on_shelf_timestamp',
    'first_shelf_time',
    'shelf_timestamp',
    'in_stock_time',
    'listing_time',
    'firstShelfTime',
    'onShelfTime',
    'start_time',
    'begin_time',
    'promotion_create_time',
  ];

  function geShelfDebugEnabled() {
    if (GE_SHELF_DEBUG) return true;
    try {
      const v = gmGet('ge_shelf_debug_v1', '');
      return v === '1' || v === 1 || v === true;
    } catch (_) {
      return false;
    }
  }

  function geShelfDebugLog(msg, extra) {
    if (!geShelfDebugEnabled()) return;
    try {
      if (arguments.length > 1) console.log('[GE shelf]', msg, extra);
      else console.log('[GE shelf]', msg);
    } catch (_) {}
  }

  function geEmptyShelfHit() {
    return { value: '', hitKey: '', hitRaw: null };
  }

  function geTryShelfHitFromRaw(raw, keyName) {
    if (raw == null || raw === '') return null;
    const norm = geNormalizeShelfTimeString(raw);
    if (norm) return { value: norm, hitKey: keyName, hitRaw: raw };
    const fmt = geFormatMaybeTimestamp(raw);
    if (fmt) return { value: fmt, hitKey: keyName, hitRaw: raw };
    return null;
  }

  /**
   * 从任意接口对象子树提取上架时间（不编造）。先直扫约定字段名，再正则键名，再模糊键名，最后递归。
   */
  function geExtractShelfTimeFromAnyObject(obj, depth, seen) {
    const d = depth || 0;
    if (!obj || typeof obj !== 'object' || d > 14) return geEmptyShelfHit();
    if (!seen) seen = new WeakSet();
    if (seen.has(obj)) return geEmptyShelfHit();
    seen.add(obj);

    if (Array.isArray(obj)) {
      for (let i = 0; i < obj.length; i++) {
        const r = geExtractShelfTimeFromAnyObject(obj[i], d + 1, seen);
        if (r.value) return r;
      }
      return geEmptyShelfHit();
    }

    let j;
    let k;
    let name;
    let hit;

    for (k = 0; k < GE_SHELF_DIRECT_KEYS.length; k++) {
      name = GE_SHELF_DIRECT_KEYS[k];
      if (!Object.prototype.hasOwnProperty.call(obj, name)) continue;
      hit = geTryShelfHitFromRaw(obj[name], name);
      if (hit) {
        geShelfDebugLog('命中字段 ' + name + ':', hit.hitRaw);
        return hit;
      }
    }

    const keys = Object.keys(obj);
    for (j = 0; j < keys.length; j++) {
      name = keys[j];
      if (GE_SHELF_API_KEY_RE.test(name)) {
        hit = geTryShelfHitFromRaw(obj[name], name);
        if (hit) {
          geShelfDebugLog('命中正则键 ' + name + ':', hit.hitRaw);
          return hit;
        }
      }
    }

    for (j = 0; j < keys.length; j++) {
      name = keys[j];
      if (
        /(shelf|list_time|publish|online|create|ground|put_on|in_stock|listing|first_on|added_at|gmt_create|begin|start)/i.test(
          name
        ) &&
        !/(update|modify|edit|expire|end_time|deadline|payment|shipping|pay_time|deliver)_?time/i.test(name)
      ) {
        hit = geTryShelfHitFromRaw(obj[name], name);
        if (hit) {
          geShelfDebugLog('命中模糊键 ' + name + ':', hit.hitRaw);
          return hit;
        }
      }
    }

    for (j = 0; j < keys.length; j++) {
      const v = obj[keys[j]];
      if (v && typeof v === 'object') {
        const r = geExtractShelfTimeFromAnyObject(v, d + 1, seen);
        if (r.value) return r;
      }
    }
    return geEmptyShelfHit();
  }

  /**
   * 百应单条促销根对象 p → 上架时间。优先 product_info，再 promotion_info、marketing、整棵 base_model、根对象。
   */
  function geExtractShelfTimeFromApiItem(p) {
    if (!p || typeof p !== 'object') return { shelfTime: '', hitKey: '', hitRaw: null, scope: '' };
    const b = p.base_model || {};
    const pi = b.product_info || {};
    const promo = b.promotion_info || p.promotion_info || {};
    const mkt = b.marketing_info || {};

    const order = [
      { obj: pi, scope: 'product_info' },
      { obj: promo, scope: 'promotion_info' },
      { obj: mkt, scope: 'marketing_info' },
      { obj: b, scope: 'base_model' },
      { obj: p, scope: 'promotion_root' },
    ];

    for (let i = 0; i < order.length; i++) {
      const o = order[i].obj;
      if (!o || typeof o !== 'object') continue;
      const r = geExtractShelfTimeFromAnyObject(o, 0, new WeakSet());
      if (r.value)
        return {
          shelfTime: r.value,
          hitKey: r.hitKey,
          hitRaw: r.hitRaw,
          scope: order[i].scope,
        };
    }
    return { shelfTime: '', hitKey: '', hitRaw: null, scope: '' };
  }

  /** 兼容旧调用：深度遍历返回首个可解析上架时间字符串 */
  function geDeepFindShelfTimeFromApiObject(obj, depth) {
    if (!obj || typeof obj !== 'object') return '';
    const r = geExtractShelfTimeFromAnyObject(obj, depth || 0, new WeakSet());
    return r.value || '';
  }

  /** DOM 卡片文案兜底：出现「上架时间」等标签时才解析，避免误匹配 */
  function geTryExtractShelfTimeFromCardText(text) {
    const t = String(text || '').replace(/\s+/g, ' ');
    if (!/(上架时间|发布时间|最近上架|创建时间)/i.test(t)) return '';
    const m =
      t.match(/(?:上架时间|发布时间|最近上架|创建时间)[:：\s]*(\d{4}[-/.年]\d{1,2}[-/.月]\d{1,2}(?:日)?(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?)/i) ||
      t.match(/(\d{4}[-/.]\d{1,2}[-/.]\d{1,2}\s+\d{1,2}:\d{2}(?::\d{2})?)/);
    if (!m) return '';
    let s = m[1].replace(/年|月/g, '-').replace(/日/g, '');
    s = s.replace(/\//g, '-').replace(/\./g, '-').replace(/\s+/g, ' ').trim();
    const d = new Date(s.replace(/-/g, '/'));
    if (Number.isFinite(d.getTime())) return geFormatDateYmdHms(d);
    return '';
  }

  /**
   * 上架时间 → 可排序毫秒时间戳。支持：YYYY-MM-DD [HH:mm:ss]、ISO8601、纯秒/毫秒时间戳字符串。
   * 百应接口里常见为时间戳或嵌套对象，仅认「YYYY-MM-DD 开头」会导致「录新品」候选为空。
   */
  function geShelfTimeSortTs(s) {
    const v = String(s || '').trim();
    if (!v) return 0;
    const m = v.match(/^(\d{4})-(\d{2})-(\d{2})(?:[\sT](\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
    if (m) {
      const t = new Date(
        parseInt(m[1], 10),
        parseInt(m[2], 10) - 1,
        parseInt(m[3], 10),
        m[4] != null ? parseInt(m[4], 10) : 0,
        m[5] != null ? parseInt(m[5], 10) : 0,
        m[6] != null ? parseInt(m[6], 10) : 0
      );
      const x = t.getTime();
      if (Number.isFinite(x)) return x;
    }
    if (/^\d{10,13}$/.test(v)) {
      const num = Number(v);
      const ms = num < 1e12 ? num * 1000 : num;
      const d = new Date(ms);
      return Number.isFinite(d.getTime()) ? d.getTime() : 0;
    }
    const d2 = new Date(v);
    return Number.isFinite(d2.getTime()) ? d2.getTime() : 0;
  }

  /** 统一写成 YYYY-MM-DD HH:mm:ss，便于飞书列展示与 gePickNewestRows；失败返回 '' */
  function geNormalizeShelfTimeString(raw) {
    if (raw == null || raw === '') return '';
    const via = geFormatMaybeTimestamp(raw);
    if (via) return via;
    const s = String(raw).trim();
    if (!s) return '';
    const ts = geShelfTimeSortTs(s);
    if (ts > 0) return geFormatDateYmdHms(new Date(ts));
    return '';
  }

  /** 行内统一规范化入口（API/DOM 写入 row.shelfTime 前调用） */
  function geNormalizeShelfTimeForRow(v) {
    return geNormalizeShelfTimeString(v);
  }

  /** 按上架时间戳倒序（无有效 shelfTime 视为 0，排在最后） */
  function geSortRowsByShelfTimeDesc(rows) {
    if (!rows || !rows.length) return [];
    return rows.slice().sort(function (a, b) {
      return geShelfTimeSortTs(b && b.shelfTime) - geShelfTimeSortTs(a && a.shelfTime);
    });
  }

  /** 按解析后销量数字倒序；无销量或解析失败视为 -1，排在最后 */
  function geSortRowsBySalesDesc(rows) {
    if (!rows || !rows.length) return [];
    return rows.slice().sort(function (a, b) {
      const na = geParseSalesToNumber(a && a.sales);
      const nb = geParseSalesToNumber(b && b.sales);
      const va = na != null ? na : -1;
      const vb = nb != null ? nb : -1;
      return vb - va;
    });
  }

  /**
   * 取前 limit 条（保持入参行顺序）。
   * 「新品」：上游须已按 create_time 降序（新款在上）加载，并与 DOM 卡片顺序一致；仅列表顺序表示新旧，不解析 row.create_time、不用 shelfTime、不重排。
   */
  function gePickNewestRows(rows, limit) {
    const n = Math.max(0, parseInt(limit, 10) || 0);
    if (!n || !rows || !rows.length) return [];
    return rows.slice(0, n);
  }

  /** 取前 limit 条（保持入参行顺序），与 gePickNewestRows 对称 */
  function gePickTopSalesRows(rows, limit) {
    const n = Math.max(0, parseInt(limit, 10) || 0);
    if (!n || !rows || !rows.length) return [];
    return rows.slice(0, n);
  }

  /** 商品去重键：promotion_id → product_id → commodity_id → link → imgSrc+title */
  function geRowIdentityDedupeKey(r) {
    if (!r || typeof r !== 'object') return '';
    const pid = String(r.promotion_id || '').trim();
    if (pid) return 'p:' + pid;
    const pr = String(r.product_id || '').trim();
    if (pr) return 'u:' + pr;
    const c = String(r.commodity_id || '').trim();
    if (c) return 'c:' + c;
    const lk = String(r.link || '').trim();
    if (lk) return 'l:' + lk;
    const im = String(r.imgSrc || '').trim();
    const tt = String(r.title || '').trim();
    if (im || tt) return 't:' + im + '\x1e' + tt;
    return '';
  }

  /** 按 geRowIdentityDedupeKey 保留首次出现行（无键则逐条保留，不强行合并） */
  function geDedupRowsByIdentity(rows) {
    const out = [];
    const seen = new Set();
    for (let i = 0; i < (rows || []).length; i++) {
      const r = rows[i];
      const k = geRowIdentityDedupeKey(r);
      if (k) {
        if (seen.has(k)) continue;
        seen.add(k);
      }
      out.push(r);
    }
    return out;
  }

  /** 销量Top 诊断：统计 >100/>500/>1000 条数 */
  function geTopSalesCountSalesBuckets(rows) {
    let c100 = 0;
    let c500 = 0;
    let c1000 = 0;
    for (let i = 0; i < (rows || []).length; i++) {
      const n = geParseSalesToNumber(rows[i] && rows[i].sales);
      if (n == null) continue;
      if (n > 100) c100++;
      if (n > 500) c500++;
      if (n > 1000) c1000++;
    }
    return { c100, c500, c1000 };
  }

  function geTopSalesMaxParsed(rows) {
    let m = null;
    for (let i = 0; i < (rows || []).length; i++) {
      const n = geParseSalesToNumber(rows[i] && rows[i].sales);
      if (n != null && (m == null || n > m)) m = n;
    }
    return m;
  }

  function geTopSalesPreviewRows(rows, maxN) {
    const n = Math.max(1, parseInt(maxN, 10) || 20);
    const out = [];
    const list = rows || [];
    for (let i = 0; i < Math.min(n, list.length); i++) {
      const r = list[i];
      out.push({
        i,
        title: String(r.title || '').slice(0, 48),
        sales: r.sales,
        promotion_id: r.promotion_id,
        product_id: r.product_id,
        commodity_id: r.commodity_id,
        linkTail: String(r.link || '').slice(-48),
      });
    }
    return out;
  }

  /**
   * 销量Top 专用控制台日志：条数、分桶、当前数组内最大解析销量、前 N 条预览。
   * 用于判断高销量是在快照/合并/去重/排序哪一步异常。
   */
  function geLogTopSalesPipeline(step, rows, note) {
    const buckets = geTopSalesCountSalesBuckets(rows);
    const maxN = geTopSalesMaxParsed(rows);
    console.log(
      '[销量Top·链路] ' +
        step +
        ' | 条数=' +
        (rows || []).length +
        ' | max解析销量=' +
        (maxN != null ? maxN : '(无)') +
        ' | >100:' +
        buckets.c100 +
        ' >500:' +
        buckets.c500 +
        ' >1000:' +
        buckets.c1000 +
        (note ? ' | ' + note : '')
    );
    try {
      console.table(geTopSalesPreviewRows(rows, 20));
    } catch (_) {
      console.log('[销量Top·链路] preview', geTopSalesPreviewRows(rows, 20));
    }
  }

  function geTopSalesWarnIfDropped(stepFrom, maxFrom, stepTo, maxTo) {
    if (maxFrom == null || maxTo == null) return;
    if (maxFrom > 200 && maxTo < maxFrom * 0.45) {
      console.warn(
        '[销量Top·异常] 解析销量峰值明显下降: ' +
          stepFrom +
          ' max=' +
          maxFrom +
          ' → ' +
          stepTo +
          ' max=' +
          maxTo +
          '（请对照上表看是否在该步丢行/覆盖 sales）'
      );
    }
  }

  /** 飞书「录入类型」：双命中须为「新品,销量Top」（半角逗号、无空格，与表头约定一致） */
  function geRecordTypeLabelFromFlags(flags) {
    if (flags && flags.new && flags.sales) return '新品,销量Top';
    if (flags && flags.new) return '新品';
    if (flags && flags.sales) return '销量Top';
    return '全店录入';
  }

  /**
   * 按店铺录入规则合并商品行（写入缓冲前调用）。
   * 全店：每行 recordType=全店录入；否则按新品 TopN / 销量 TopN 合并去重，双命中为「新品,销量Top」。
   */
  function geMergeRecordRuleRows(rows, shopTask) {
    const t = geMigrateShopTask(shopTask || {});
    const list = rows && rows.length ? rows.slice() : [];
    if (!list.length) return [];

    if (t.recordAll) {
      return list.map(function (r) {
        return Object.assign({}, r, { recordType: '全店录入' });
      });
    }

    if (!t.recordNewest && !t.recordTopSales) {
      return list.map(function (r) {
        return Object.assign({}, r, { recordType: '全店录入' });
      });
    }

    const combined = [];
    const seen = new Map();

    function addRow(src, fromNew, fromSales) {
      const k = geRowIdentityDedupeKey(src);
      if (k) {
        const idx = seen.get(k);
        if (idx != null) {
          const ex = combined[idx];
          const flags = ex._geRt || { new: false, sales: false };
          if (fromNew) flags.new = true;
          if (fromSales) flags.sales = true;
          ex._geRt = flags;
          ex.recordType = geRecordTypeLabelFromFlags(flags);
          return;
        }
        const flags = { new: !!fromNew, sales: !!fromSales };
        const nr = Object.assign({}, src, {
          recordType: geRecordTypeLabelFromFlags(flags),
          _geRt: flags,
        });
        seen.set(k, combined.length);
        combined.push(nr);
        return;
      }
      const flags = { new: !!fromNew, sales: !!fromSales };
      combined.push(
        Object.assign({}, src, { recordType: geRecordTypeLabelFromFlags(flags), _geRt: flags })
      );
    }

    if (t.recordNewest) {
      const picks = gePickNewestRows(list, t.newestLimit);
      for (let i = 0; i < picks.length; i++) addRow(picks[i], true, false);
    }
    if (t.recordTopSales) {
      const picks = gePickTopSalesRows(list, t.topSalesLimit);
      for (let i = 0; i < picks.length; i++) addRow(picks[i], false, true);
    }

    return combined.map(function (x) {
      const y = Object.assign({}, x);
      delete y._geRt;
      return y;
    });
  }

  /**
   * 批量监控：新品、销量 Top 各抓一份后的合并。
   * - 录入类型：新品 / 销量Top / 新品,销量Top（交集用 geRowIdentityDedupeKey，不按标题单独去重）。
   * - 输出顺序：先整段「新品列表」保持上架时间抓取顺序；再追加「仅命中销量 Top、不在新品列表」的行，按解析销量倒序。
   * - 不在此对两段做统一重排，避免新品序与销量序互相冲掉。
   */
  function geMergeRecordRuleRowsFromSortCaptures(shopTask, newestRows, topSalesRows) {
    const t = geMigrateShopTask(shopTask || {});
    const nrRaw = newestRows && newestRows.length ? newestRows : [];
    const trRaw = topSalesRows && topSalesRows.length ? topSalesRows : [];

    if (!t.recordNewest && !t.recordTopSales) return [];

    if (t.recordNewest && !t.recordTopSales) {
      return geDedupRowsByIdentity(nrRaw).map(function (r) {
        return Object.assign({}, r, { recordType: '新品' });
      });
    }

    if (!t.recordNewest && t.recordTopSales) {
      return geDedupRowsByIdentity(geSortRowsBySalesDesc(trRaw)).map(function (r) {
        return Object.assign({}, r, { recordType: '销量Top' });
      });
    }

    const nr = geDedupRowsByIdentity(nrRaw);
    const tr = geDedupRowsByIdentity(geSortRowsBySalesDesc(trRaw.slice()));

    const topKeySet = new Set();
    for (let j = 0; j < tr.length; j++) {
      const k = geRowIdentityDedupeKey(tr[j]);
      if (k) topKeySet.add(k);
    }

    const newestKeySet = new Set();
    for (let i = 0; i < nr.length; i++) {
      const k = geRowIdentityDedupeKey(nr[i]);
      if (k) newestKeySet.add(k);
    }

    const out = [];
    for (let i = 0; i < nr.length; i++) {
      const src = nr[i];
      const k = geRowIdentityDedupeKey(src);
      const inTop = !!(k && topKeySet.has(k));
      out.push(
        Object.assign({}, src, {
          recordType: inTop ? '新品,销量Top' : '新品',
        })
      );
    }

    const appendedTopOnly = new Set();
    for (let j = 0; j < tr.length; j++) {
      const src = tr[j];
      const k = geRowIdentityDedupeKey(src);
      if (k && newestKeySet.has(k)) continue;
      if (k) {
        if (appendedTopOnly.has(k)) continue;
        appendedTopOnly.add(k);
      }
      out.push(Object.assign({}, src, { recordType: '销量Top' }));
    }

    return out;
  }

  function geSortBuyinRowsByShelfTimeDesc(rows) {
    if (!rows || !rows.length) return rows || [];
    const any = rows.some(function (r) {
      return r && r.shelfTime && String(r.shelfTime).trim() !== '';
    });
    if (!any) return rows;
    return rows.slice().sort(function (a, b) {
      return geShelfTimeSortTs(b && b.shelfTime) - geShelfTimeSortTs(a && a.shelfTime);
    });
  }

  function scrapeBuyinDom(opts) {
    const ctx = getBuyinPageShopContext();
    const cards =
      opts && Array.isArray(opts.cards) ? opts.cards : document.querySelectorAll('div[class*="card___"]');
    const results = [];
    const seen = new Set();
    for (const card of cards) {
      if (buyinCardInNoiseHost(card)) continue;
      const text = card.textContent || '';
      const hasPickIntent = /加选品车|加入选品车|加橱窗/.test(text);
      const priceMatch = text.match(/[¥￥]\s*[\d.]+/);
      const price = priceMatch ? priceMatch[0].replace(/\s/g, '') : '';
      const commissionMatch = text.match(/佣金\s*[\d.]+%|[\d.]+%/);
      const commission = commissionMatch ? commissionMatch[0].replace(/^佣金\s*/, '') : '';
      // 销量：避免把「¥9.9」等价格误当销量（要求数字前不紧邻 ¥/￥）
      const pickSales = function (blob) {
        const s = String(blob || '');
        let m = s.match(/(\d+(?:\.\d+)?[万千]?\+?)\s*(?:人付款|已售|销量|月销)/);
        if (m && m.index != null) {
          const idx = m.index;
          const num = m[1] || '';
          // 数字前 2 个字符内出现货币符号则判为价格噪声
          const pre = s.slice(Math.max(0, idx - 2), idx);
          if (/[¥￥]/.test(pre)) m = null;
          else return num;
        }
        const m2 = s.match(/月销\s*(\d+)/);
        if (m2) return m2[1] || '';
        return '';
      };
      const sales = pickSales(text);
      const imgEl = card.querySelector('img');
      const imgSrc = (imgEl?.src || imgEl?.getAttribute('data-src') || '').trim();
      let title = '';
      const named = card.querySelectorAll('[class*="title"],[class*="Title"],[class*="name"],[class*="Name"]');
      for (const el of named) {
        const t = (el.textContent || '').trim().slice(0, 200);
        if (t.length >= 6 && !isBuyinGarbageTitle(t) && !/^佣金|^到手价|^¥|^￥/.test(t)) {
          title = t;
          break;
        }
      }
      if (!title) {
        const titleEl = Array.from(card.querySelectorAll('span, div')).find((el) => {
          const t = (el.textContent || '').trim();
          if (t.length < 6 || t.length >= 200) return false;
          if (isBuyinGarbageTitle(t)) return false;
          if (/[¥￥]\s*[\d.]+|[\d.]+%|加选品车|到手价|佣金|月销|已售/.test(t)) return false;
          return true;
        });
        title = titleEl ? (titleEl.textContent || '').trim().slice(0, 200) : '';
      }
      if (isBuyinNavMenuTitle(title)) continue;
      let link = scrapeBuyinProductLinkFromCard(card);
      if (!title && !price && !link) continue;
      const gdCard = scrapeBuyinGuaranteeDeliveryFromCard(card);
      const shelfFromDom = geTryExtractShelfTimeFromCardText(text);
      const row = {
        title,
        price,
        commission,
        sales,
        link,
        imgSrc: imgSrc && !/placeholder|loading|data:image/i.test(imgSrc) ? imgSrc : '',
        shopName: ctx.shopName,
        shopLink: ctx.shopLink,
        shopTag: '',
        monitorTimes: '',
        monitorIndex: '',
        inspectCycle: '',
        shelfTime: geNormalizeShelfTimeForRow(shelfFromDom) || shelfFromDom || '',
        guarantee: gdCard.guarantee || '',
        deliveryTime: gdCard.deliveryTime || '',
      };
      if (!buyinRowHasPlausibleProductSignals(row)) continue;
      if (!hasPickIntent && !buyinProductLinkLooksValid(link) && !(price && commission && title.length >= 8)) continue;
      const key = link || title + '|' + price;
      if (seen.has(key)) continue;
      seen.add(key);
      results.push(row);
    }
    if (results.length) console.log('[导出] 百应 DOM 兜底抓取 ' + results.length + ' 个');
    return results;
  }

  /**
   * 返回当前视口可见的商品卡片（仅用于「排序后只采集首屏」）。
   * - 通过 getBoundingClientRect() 判断：卡片底部>0 且顶部<window.innerHeight
   * - 排除 display:none / visibility:hidden / opacity:0
   * - 排除明显骨架/占位卡（基于文案/图片 src）
   * - 按屏幕自然顺序排序：top 再 left
   */
  function geGetVisibleBuyinCards() {
    const raw = (() => {
      try {
        return document.querySelectorAll('div[class*="card___"]');
      } catch (_) {
        return [];
      }
    })();
    const vh = window.innerHeight || 0;
    const wMin = 24;
    const hMin = 24;
    const out = [];
    for (const card of raw) {
      if (buyinCardInNoiseHost(card)) continue;
      if (!card || typeof card.getBoundingClientRect !== 'function') continue;

      const st = (() => {
        try {
          return getComputedStyle(card);
        } catch (_) {
          return null;
        }
      })();
      if (st) {
        if (st.display === 'none') continue;
        if (st.visibility === 'hidden') continue;
        const op = parseFloat(st.opacity || '1');
        if (!Number.isFinite(op) || op <= 0.01) continue;
      }

      const rect = card.getBoundingClientRect();
      if (!rect) continue;
      if (rect.width < wMin || rect.height < hMin) continue;
      if (rect.bottom <= 0 || rect.top >= vh) continue;

      const text = (card.textContent || '').replace(/\s+/g, ' ').trim();
      if (/加载中|placeholder|占位|骨架|skeleton/i.test(text)) continue;

      // 若完全没有可用文案且图片也是占位，则认为是骨架/占位
      if (!text || text.length < 8) {
        const img = card.querySelector('img');
        const imgSrc = (img && (img.src || img.getAttribute('data-src') || '')).trim();
        if (!imgSrc || /placeholder|loading|data:image/i.test(imgSrc)) continue;
      }

      out.push({ card, top: rect.top, left: rect.left });
    }

    out.sort(function (a, b) {
      if (a.top !== b.top) return a.top - b.top;
      return a.left - b.left;
    });

    return out.map((x) => x.card);
  }

  /**
   * 百应列表常在内部 overflow 容器内滚动，仅 window.scrollBy 可能无位移。
   * 优先只滚「含商品卡且可滚动空间最大」的一个容器，避免与 window 同时滚导致位移加倍。
   */
  function geBuyinScrollMainBy(delta) {
    const d = Math.round(Number(delta) || 0);
    if (!d) return false;
    const candidates = [];
    try {
      const nodes = document.querySelectorAll(
        'main, #root > div, [class*="content"], [class*="scroll"], [class*="layout"], [class*="container"]'
      );
      for (let i = 0; i < nodes.length; i++) {
        const el = nodes[i];
        const st = getComputedStyle(el);
        const oy = st.overflowY;
        if (!(oy === 'auto' || oy === 'scroll' || oy === 'overlay')) continue;
        const room = el.scrollHeight - el.clientHeight;
        if (room <= 50) continue;
        const hasCard =
          (el.querySelector && el.querySelector('[class*="card"]')) ||
          (el.querySelector && el.querySelector('img[src*="ecombdimg"]')) ||
          (el.querySelector && el.querySelector('img[src*="jinritemai"]'));
        if (!hasCard) continue;
        candidates.push({ el, room });
      }
    } catch (_) {}
    if (candidates.length) {
      candidates.sort(function (a, b) {
        return b.room - a.room;
      });
      try {
        candidates[0].el.scrollTop += d;
        return true;
      } catch (_) {}
    }
    try {
      window.scrollBy(0, d);
      return true;
    } catch (_) {}
    try {
      const de = document.documentElement;
      if (de && de.scrollHeight > de.clientHeight + 40) {
        de.scrollTop += d;
        return true;
      }
    } catch (_) {}
    try {
      const bd = document.body;
      if (bd && bd.scrollHeight > bd.clientHeight + 40) {
        bd.scrollTop += d;
        return true;
      }
    } catch (_) {}
    return false;
  }

  /**
   * 与 geBuyinScrollMainBy 相同规则，选出最可能承载商品网格的 overflow 容器（不执行滚动）。
   * @returns {HTMLElement|null}
   */
  function geBuyinFindPrimaryProductListScroller() {
    try {
      const nodes = document.querySelectorAll(
        'main, #root > div, [class*="content"], [class*="scroll"], [class*="layout"], [class*="container"]'
      );
      const candidates = [];
      for (let i = 0; i < nodes.length; i++) {
        const el = nodes[i];
        const st = getComputedStyle(el);
        const oy = st.overflowY;
        if (!(oy === 'auto' || oy === 'scroll' || oy === 'overlay')) continue;
        const room = el.scrollHeight - el.clientHeight;
        if (room <= 50) continue;
        const hasCard =
          (el.querySelector && el.querySelector('[class*="card"]')) ||
          (el.querySelector && el.querySelector('img[src*="ecombdimg"]')) ||
          (el.querySelector && el.querySelector('img[src*="jinritemai"]'));
        if (!hasCard) continue;
        candidates.push({ el, room });
      }
      if (!candidates.length) return null;
      candidates.sort(function (a, b) {
        return b.room - a.room;
      });
      return candidates[0].el;
    } catch (_) {
      return null;
    }
  }

  function geBuyinApplyNewestListScrollBy(deltaY, ctx) {
    const d = Math.round(Number(deltaY) || 0);
    if (!d || !ctx) return;
    const mode = ctx.mode || 'both';
    const inner = ctx.innerEl || geBuyinFindPrimaryProductListScroller();
    if (mode === 'document' || mode === 'both') {
      geBuyinApplyMainViewportScrollBy(d);
    }
    if (mode === 'container' || mode === 'both') {
      if (inner) {
        try {
          inner.scrollTop += d;
        } catch (_) {}
      } else {
        try {
          geBuyinScrollMainBy(d);
        } catch (_) {}
      }
    }
  }

  /**
   * 将百应店铺列表滚回顶部（window + 含商品卡的可滚动容器），便于按「排序后列表前 N 条」取 seed。
   */
  async function geBuyinScrollFeedToTop(optExecToken) {
    try {
      window.scrollTo(0, 0);
    } catch (_) {}
    try {
      if (document.documentElement) document.documentElement.scrollTop = 0;
      if (document.body) document.body.scrollTop = 0;
    } catch (_) {}
    try {
      const candidates = document.querySelectorAll('div,main,section,article');
      for (let i = 0; i < candidates.length; i++) {
        const el = candidates[i];
        const st = getComputedStyle(el);
        const oy = st.overflowY;
        if (!(oy === 'auto' || oy === 'scroll' || oy === 'overlay')) continue;
        if (el.scrollHeight <= el.clientHeight + 60) continue;
        if (el.querySelector && el.querySelector('div[class*="card___"]')) {
          el.scrollTop = 0;
        }
      }
    } catch (_) {}
    await geSleepMaybeBatch(280, optExecToken);
  }

  /**
   * 最近上架专用：在「上架时间·最新在前」已点选并稳定后，通过列表真实滚动扩大虚拟列表在 DOM 中的挂载量，
   * 直到 scrapeBuyinDom() 至少能解析出 N 条商品（再回顶截取前 N 作多字段种子）。
   * 说明：这是滚载/懒加载触达，与 Chrome 地址栏旁「页面缩放」不是同一机制，也不能用 CSS zoom 代替。
   *
   * @param {number} limit 目标条数 N
   * @param {{ setStatus?: function(string): void, prefixStatus?: string, innerEl?: HTMLElement|null }} ctx
   * @returns {Promise<number>} 回顶后 scrapeBuyinDom 的总条数（可能仍 < N，由调用方兜底）
   */
  async function geBuyinScrollLoadUntilDomRowCountAtLeast(limit, ctx) {
    const n = Math.max(1, parseInt(limit, 10) || 1);
    const setStatus = ctx && ctx.setStatus;
    const prefix = (ctx && ctx.prefixStatus) || '';
    const innerEl = ctx && ctx.innerEl ? ctx.innerEl : geBuyinFindPrimaryProductListScroller();
    const execToken = ctx && ctx.execToken;
    const vhBase = window.innerHeight || 700;
    const maxSteps = Math.min(160, Math.max(48, n * 10));
    let stagnant = 0;
    let bestCount = 0;

    for (let step = 0; step < maxSteps; step++) {
      geBatchAbortIfCancelled(execToken);
      await geBuyinScrollFeedToTop(execToken);
      await geSleepMaybeBatch(140, execToken);
      const rows = scrapeBuyinDom();
      const cnt = rows && rows.length ? rows.length : 0;
      if (cnt > bestCount) {
        bestCount = cnt;
        stagnant = 0;
      } else {
        stagnant++;
      }
      if (cnt >= n) {
        if (setStatus) {
          setStatus(prefix + ' · 最近上架：列表 DOM 已≥' + n + ' 条（当前 ' + cnt + '），回顶采前 N…');
        }
        return cnt;
      }
      if (setStatus && step % 7 === 0) {
        setStatus(
          prefix + ' · 最近上架：滚载以凑满前 N（DOM ' + cnt + '/' + n + '，步 ' + step + '/' + maxSteps + '）…'
        );
      }
      geBuyinApplyNewestListScrollBy(Math.round(vhBase * 0.85), { mode: 'both', innerEl: innerEl });
      await geSleepMaybeBatch(440, execToken);
      if (stagnant >= 14) {
        for (let burst = 0; burst < 10; burst++) {
          geBatchAbortIfCancelled(execToken);
          geBuyinApplyNewestListScrollBy(Math.round(vhBase * 1.05), { mode: 'both', innerEl: innerEl });
          await geSleepMaybeBatch(240, execToken);
        }
        stagnant = 0;
      }
    }

    await geBuyinScrollFeedToTop(execToken);
    await geSleepMaybeBatch(160, execToken);
    const finalRows = scrapeBuyinDom();
    const fc = finalRows && finalRows.length ? finalRows.length : 0;
    if (setStatus) {
      setStatus(prefix + ' · 最近上架：滚载步数用尽，DOM 共 ' + fc + ' 条（目标≥' + n + '），仍将回顶取前 N…');
    }
    if (fc < n) {
      console.warn(
        '[最近上架] 列表 DOM 商品条数仍不足 N：' + fc + ' < ' + n + '；种子位可能占位，后续依赖全店快照匹配补链'
      );
    }
    return fc;
  }

  /**
   * 仅驱动「浏览器视口/文档」层滚动（window + scrollingElement + html/body），不滚商品列表内部 overflow 容器。
   * 百应店铺页若可滚动，用户应能看到整页明显下移；用于最近上架触发前 N 张卡片的链接懒加载。
   */
  function geBuyinApplyMainViewportScrollBy(deltaY) {
    const d = Math.round(Number(deltaY) || 0);
    if (!d) return;
    try {
      window.scrollBy(0, d);
    } catch (_) {}
    try {
      const se = document.scrollingElement;
      if (se && typeof se.scrollTop === 'number') se.scrollTop += d;
    } catch (_) {}
    try {
      if (document.documentElement) document.documentElement.scrollTop += d;
    } catch (_) {}
    try {
      if (document.body) document.body.scrollTop += d;
    } catch (_) {}
  }

  /**
   * 轻微滚动以触发“首屏卡片”里的懒加载（如商品链接/销量文案）。
   * - 目标：不扩展到屏幕外商品；最终仍以 geGetVisibleBuyinCards() 结果为准。
   */
  async function geHydrateBuyinLinksForVisibleCards(cards, hydrateOpts) {
    const safeCards = Array.isArray(cards) ? cards : [];
    if (!safeCards.length) return safeCards;
    const countOkLinks = function (list) {
      let ok = 0;
      for (let i = 0; i < list.length; i++) {
        const c = list[i];
        try {
          const lk = scrapeBuyinProductLinkFromCard(c);
          if (buyinProductLinkLooksValid(lk)) ok++;
        } catch (_) {}
      }
      return ok;
    };
    const countOkSales = function (list) {
      let ok = 0;
      for (let i = 0; i < list.length; i++) {
        const c = list[i];
        const text = (c && c.textContent) || '';
        const m =
          text.match(/(\d+(?:\.\d+)?[万千]?\+?)\s*(?:人付款|已售|销量|月销)/) ||
          text.match(/月销\s*(\d+)/);
        if (m) ok++;
      }
      return ok;
    };

    const y0 = typeof window.scrollY === 'number' ? window.scrollY : document.documentElement.scrollTop || 0;
    const attempts = hydrateOpts && hydrateOpts.attempts != null ? Math.max(1, parseInt(hydrateOpts.attempts, 10) || 1) : 2;
    const deltaScale = hydrateOpts && hydrateOpts.deltaScale != null ? Number(hydrateOpts.deltaScale) : 1;
    const baseDelta = Math.round((window.innerHeight || 800) * 0.12);
    const delta = Math.max(60, Math.min(220, Math.round(baseDelta * (Number.isFinite(deltaScale) ? deltaScale : 1))));

    const ok0 = countOkLinks(safeCards);
    try {
      const sales0 = countOkSales(safeCards);
      const okTarget = Math.ceil(safeCards.length * 0.7);

      // 轻微下/回弹：触发首屏懒加载，但不让我们“继续采集更多条”
      for (let attempt = 0; attempt < attempts; attempt++) {
        const d = attempt === 0 ? delta : Math.round(delta * 1.4);
        geBuyinScrollMainBy(d);
        await sleep(320);
        geBuyinScrollMainBy(-d);
        await sleep(420);

        const afterCards = geGetVisibleBuyinCards();
        const ok1 = countOkLinks(afterCards);
        const sales1 = countOkSales(afterCards);

        if (ok1 >= okTarget || sales1 >= okTarget || (ok1 >= ok0 + 1 && sales1 >= sales0)) {
          return afterCards && afterCards.length ? afterCards : safeCards;
        }
      }
    } catch (_) {}

    const afterCardsFinal = geGetVisibleBuyinCards();
    const ok1Final = countOkLinks(afterCardsFinal);
    if (afterCardsFinal && afterCardsFinal.length && ok1Final >= ok0) return afterCardsFinal;
    return safeCards;
  }

  /**
   * 销量Top：固定首屏 seed 行后，逐步向下滚动触发懒加载 + 补拉 material_list，
   * 反复用 snapshot 回填 seed，直到「有效商品链接」数达到比例阈值或步数上限。
   * 结束后 scroll 恢复到进入本函数时的位置（尽量不改变用户最终视口）。
   */
  async function geRefillBuyinSeedLinksWithProgressiveScroll(opts) {
    const seedRows = opts && Array.isArray(opts.seedRows) ? opts.seedRows : [];
    const cards0 = opts && Array.isArray(opts.cards0) ? opts.cards0 : [];
    const setStatus = opts && opts.setStatus;
    const prefix = (opts && opts.prefixStatus) || '';
    const modeLabel = (opts && opts.modeLabel && String(opts.modeLabel).trim()) || '首屏';
    const minRatio =
      opts && opts.minRatio != null && Number.isFinite(Number(opts.minRatio))
        ? Math.min(1, Math.max(0.1, Number(opts.minRatio)))
        : 0.7;
    const maxSteps = Math.max(1, parseInt(opts && opts.maxSteps, 10) || 14);
    const stepDelta =
      opts && opts.stepDelta != null
        ? Math.max(40, parseInt(opts.stepDelta, 10) || 120)
        : Math.max(80, Math.round((window.innerHeight || 600) * 0.14));
    /** 回填前先向下滚若干步，让约第 4 排及后续进入视口，触发首屏卡片的链接懒加载 */
    const preScrollSteps = Math.max(0, parseInt(opts && opts.preScrollSteps, 10) || 4);
    const preScrollDelta =
      opts && opts.preScrollDelta != null
        ? Math.max(36, parseInt(opts.preScrollDelta, 10) || 72)
        : Math.max(60, Math.round(stepDelta * 0.85));

    if (!seedRows.length) return [];

    const countOk = function (rows) {
      return (rows || []).filter(function (r) {
        return buyinProductLinkLooksValid((r && r.link) || '');
      }).length;
    };

    const needOk = Math.max(1, Math.ceil(seedRows.length * minRatio));
    const yStart =
      typeof window.scrollY === 'number' ? window.scrollY : document.documentElement.scrollTop || 0;

    for (let ps = 0; ps < preScrollSteps; ps++) {
      if (setStatus) {
        setStatus(
          prefix + ' · ' + modeLabel + ' 预滚动 ' + (ps + 1) + '/' + preScrollSteps + '（触发懒加载）…'
        );
      }
      try {
        geBuyinScrollMainBy(preScrollDelta);
      } catch (_) {}
      await sleep(360);
      try {
        await geRefetchBuyinMaterialListForLinks(true);
      } catch (_) {}
      await sleep(220);
    }

    // 注意：不要立刻回滚到 yStart。部分懒加载需要卡片在视口内停留，
    // 这里保持在预滚动位置做 snapshot 回填，最后再恢复滚动位置。
    await sleep(280);

    let bestMatched = geMatchBuyinRowsBySeed(seedRows, buildBuyinExportRowsSnapshot({ preserveRowOrder: true }));
    let bestOk = countOk(bestMatched);

    try {
      await geRefetchBuyinMaterialListForLinks(true);
    } catch (_) {}
    try {
      await waitForBuyinDetailLinksIfNeeded(null);
    } catch (_) {}

    bestMatched = geMatchBuyinRowsBySeed(seedRows, buildBuyinExportRowsSnapshot({ preserveRowOrder: true }));
    bestOk = countOk(bestMatched);

    let stagnant = 0;
    for (let step = 0; step < maxSteps; step++) {
      if (bestOk >= needOk || bestOk >= seedRows.length) break;

      if (setStatus) {
        setStatus(
          prefix +
            ' · ' +
            modeLabel +
            ' 逐步滚动补链接 ' +
            (step + 1) +
            '/' +
            maxSteps +
            ' · 首屏有效链接 ' +
            bestOk +
            '/' +
            seedRows.length +
            '（目标≥' +
            needOk +
            '）'
        );
      }

      try {
        geBuyinScrollMainBy(stepDelta);
      } catch (_) {}
      await sleep(480);

      try {
        await geRefetchBuyinMaterialListForLinks(true);
      } catch (_) {}
      await sleep(420);
      if (step % 3 === 2 || step === maxSteps - 1) {
        try {
          await waitForBuyinDetailLinksIfNeeded(null);
        } catch (_) {}
      }

      const matched = geMatchBuyinRowsBySeed(seedRows, buildBuyinExportRowsSnapshot({ preserveRowOrder: true }));
      const ok = countOk(matched);
      if (ok > bestOk) {
        bestOk = ok;
        bestMatched = matched;
        stagnant = 0;
      } else {
        stagnant++;
        if (stagnant >= 4 && step >= 3) break;
      }

      if (ok >= needOk || ok >= seedRows.length) {
        bestMatched = matched;
        bestOk = ok;
        break;
      }
    }

    try {
      window.scrollTo(0, yStart);
    } catch (_) {}
    await sleep(200);

    console.log(
      'seed=' + seedRows.length + ' 逐步滚动后有效商品链接=' + bestOk + '（目标≥' + needOk + '）'
    );
    if (setStatus) {
      setStatus(
        prefix +
          ' · ' +
          modeLabel +
          '（固定 ' +
          seedRows.length +
          ' 条）链接有效 ' +
          bestOk +
          '/' +
          seedRows.length
      );
    }

    return bestMatched;
  }

  /** 按当前页商品卡片 DOM 顺序排列 snapshot 行（与「上架时间/销量」排序后视觉顺序一致） */
  function geOrderBuyinRowsByDomCardOrder(rows) {
    if (!rows || !rows.length) return [];
    let domOrder = [];
    try {
      domOrder = scrapeBuyinDom();
    } catch (_) {
      domOrder = [];
    }
    if (!domOrder.length) return rows.slice();

    const used = new Set();
    const out = [];

    for (let i = 0; i < domOrder.length; i++) {
      const d = domOrder[i];
      const dt = (d.title || '').trim().slice(0, 36);
      const dl = (d.link || '').trim();
      let best = -1;
      for (let j = 0; j < rows.length; j++) {
        if (used.has(j)) continue;
        const r = rows[j];
        const rl = (r.link || '').trim();
        if (dl && rl) {
          const a = rl.split('?')[0];
          const b = dl.split('?')[0];
          if (a === b || rl === dl) {
            best = j;
            break;
          }
        }
        const rt = (r.title || '').trim();
        if (dt.length >= 8 && rt) {
          if (rt.indexOf(dt) === 0 || dt.indexOf(rt.slice(0, Math.min(24, rt.length))) === 0) {
            best = j;
            break;
          }
        }
      }
      if (best >= 0) {
        used.add(best);
        out.push(rows[best]);
      }
    }
    for (let j = 0; j < rows.length; j++) {
      if (!used.has(j)) out.push(rows[j]);
    }
    return out;
  }

  /** 排序抓取前清空内存列表，避免旧顺序与旧商品混入 */
  function geClearBuyinDataForSortCapture() {
    try {
      buyinData.length = 0;
    } catch (_) {}
  }

  /** fetch 安装后赋实现：从 Performance 找回 material_list URL + shop_id 猜测，补拉 summary_promotions */
  let geRefetchBuyinMaterialListForLinks = async function (_force) {};
  let geBuyinMaterialListRefetchCooldownAt = 0;

  /** 百应待导出行快照（无 alert）；供导出前等待 material_list 补全 detail_url */
  function buildBuyinExportRowsSnapshot(opts) {
    tryReingestBuyinLastApiFromStorage();
    const ctx = getBuyinPageShopContext();
    let rows;
    if (buyinData.length === 0) {
      const domRows = scrapeBuyinDom();
      if (domRows.length === 0) return [];
      rows = enrichBuyinRowsWithDomLinks(
        consolidateBuyinRowsByTitlePrice(
          domRows.map((x) => ({
            ...x,
            shopName: x.shopName || ctx.shopName,
            shopLink: x.shopLink || ctx.shopLink,
          }))
        )
      );
    } else {
      rows = enrichBuyinRowsWithDomLinks(
        consolidateBuyinRowsByTitlePrice(
          buyinData
            .filter(buyinRowHasPlausibleProductSignals)
            .map((x) => ({
              ...x,
              shopName: x.shopName || ctx.shopName,
              shopLink: x.shopLink || ctx.shopLink,
            }))
        )
      );
    }
    const st = loadBatchState();
    if (st.running && isBuyin()) {
      const shops = loadBatchActiveOrEnabledShops();
      if (shops.length) {
        const idx = Math.min(st.shopIdx, shops.length - 1);
        const shop = shops[idx];
        if (shop && String(shop.url || '').trim()) {
          const tag = String(shop.shopTag || '').trim();
          const round = String(st.roundIdx);
          const mtStr = String(Math.max(1, parseInt(shop.monitorTimes, 10) || 1));
          const megaStr = String(Math.max(1, parseInt(st.megaIdx, 10) || 1));
          rows = rows.map(function (r) {
            return {
              ...r,
              shopTag: (r.shopTag || '').trim() || tag,
              monitorTimes: (r.monitorTimes != null && String(r.monitorTimes).trim() !== '' ? r.monitorTimes : mtStr),
              monitorIndex:
                r.monitorIndex != null && String(r.monitorIndex).trim() !== '' ? r.monitorIndex : round,
              inspectCycle:
                r.inspectCycle != null && String(r.inspectCycle).trim() !== '' ? r.inspectCycle : megaStr,
            };
          });
        }
      }
    }
    if (!(opts && opts.preserveRowOrder)) {
      rows = geSortBuyinRowsByShelfTimeDesc(rows);
    }
    return rows.map(normalizeBuyinRowDefaults);
  }

  function countBuyinRowsMissingProductLink(rows) {
    if (!rows || !rows.length) return 0;
    return rows.filter((r) => {
      const hasLine = (r.title || '').trim() || (r.price || '').trim();
      const lk = (r.link || '').trim();
      return hasLine && !/^https?:\/\//i.test(lk);
    }).length;
  }

  /**
   * 防呆：多行尚无 https 商品链时，主动补拉 material_list（summary_promotions）并短轮询。
   * 含仅 DOM 有数据、buyinData 仍为空的情况（此前会误判直接返回）。
   */
  async function waitForBuyinDetailLinksIfNeeded(triggerBtn, waitOpts) {
    if (!isBuyin()) return;
    const execToken = waitOpts && waitOpts.execToken;
    let snapshot = buildBuyinExportRowsSnapshot();
    if (snapshot.length === 0) return;
    let miss = countBuyinRowsMissingProductLink(snapshot);
    if (miss === 0) return;
    if (miss < 2 && miss / snapshot.length < 0.2) return;
    const btn = triggerBtn;
    const maxMs = 20000;
    const step = 500;
    let w = 0;
    const origText = btn ? btn.textContent : '';
    const origDisabled = btn ? btn.disabled : false;
    try {
      await geRefetchBuyinMaterialListForLinks();
      snapshot = buildBuyinExportRowsSnapshot();
      miss = countBuyinRowsMissingProductLink(snapshot);
      if (miss === 0) return;
      while (w < maxMs) {
        geBatchAbortIfCancelled(execToken);
        if (btn) {
          btn.disabled = true;
          btn.textContent =
            '等待商品链接(material_list)… ' + Math.max(0, Math.ceil((maxMs - w) / 1000)) + 's';
        }
        await geSleepMaybeBatch(step, execToken);
        w += step;
        await geRefetchBuyinMaterialListForLinks();
        snapshot = buildBuyinExportRowsSnapshot();
        if (snapshot.length === 0) break;
        miss = countBuyinRowsMissingProductLink(snapshot);
        if (miss === 0) break;
        if (miss < 2 && miss / snapshot.length < 0.2) break;
      }
    } finally {
      if (btn) {
        btn.disabled = origDisabled;
        btn.textContent =
          origText || (btn.id === 'goods-export-feishu-btn' ? '写入飞书表格' : '导出商品 CSV');
      }
    }
    const finalMiss = countBuyinRowsMissingProductLink(buildBuyinExportRowsSnapshot());
    if (finalMiss > 0) {
      console.warn(
        '[导出] 百应仍有 ' + finalMiss + ' 行无商品链接，可能需再滚动或稍后重试写入。'
      );
    }
  }

  function collectExportPayload() {
    if (isBuyin()) {
      const data = buildBuyinExportRowsSnapshot();
      if (data.length === 0) {
        if (buyinData.length === 0) {
          alert(
            '暂无数据。请先在本页向下滚动加载商品；若仍无数据，打开 DevTools → Network 看是否有商品列表接口返回 JSON。\n\nv3.7.0：店铺任务为表格配置（非 JSON 手填）、批量粘贴/备份、当前店加入任务；批量监控仍用 GM 续跑。仍无数据再试强刷或「补拉 material_list」。'
          );
        } else {
          alert(
            '当前内存里的百应数据经校验均为异常行（常见于验证码组件被当成商品卡片）。请刷新页面、完成验证后重新滚动加载商品再导出。'
          );
        }
        return null;
      }
      return { data, prefix: '百应商品' };
    }
    if (isTaobao()) {
      if (taobaoData.length === 0) tryIngestLastShopMtopFromStorage();
      if (taobaoData.length === 0) {
        findTaobaoInWindow();
        if (taobaoData.length === 0) tryIngestLastShopMtopFromStorage();
        if (taobaoData.length === 0) {
          const domData = scrapeTaobaoDom();
          if (domData.length > 0) taobaoData = domData;
        }
        if (taobaoData.length === 0) tryIngestLastShopMtopFromStorage();
        if (taobaoData.length === 0) {
          alert(
            '暂无数据。请先滚动加载完商品，或确认在店铺/搜索列表页。\n\n建议：DevTools → Network → 勾选 Disable cache，再 Ctrl+F5 强刷。\n\n若 Network 里能看到 mtop.taobao.shop.simple.fetch 的 Response，说明接口有数据；请使用脚本 3.3.1+。\n\n兜底：控制台执行 tryIngestLastShopMtop() 从 sessionStorage / localStorage / #ge-mtop-stash 回填。\n\n排查：TaobaoDebug() 看 taobaoDataCount 与三项缓存长度；Console 上下文选 shop*.world.taobao.com'
          );
          return null;
        }
      }
      let data = taobaoData;
      if (/shop\d+\.(taobao|world\.taobao)\.com/.test(location.hostname) && !data.some((x) => x.shopLink)) {
        const sn =
          document.querySelector('[class*="shopName"], [class*="shop-name"]')?.textContent?.trim() ||
          document.title?.replace(/-淘宝网|_淘宝网/, '').trim() ||
          '';
        const sl = location.href.split('?')[0];
        data = data.map((x) => ({ ...x, shopName: x.shopName || sn, shopLink: x.shopLink || sl }));
      }
      return { data: data.map(normalizeBuyinRowDefaults), prefix: '淘宝商品' };
    }
    alert('当前页面不支持');
    return null;
  }

  // 新增：批量粘贴导入子窗
  function geOpenBatchPasteModal(onDone) {
    if (document.getElementById('ge-batch-paste-modal')) return;
    const mask = document.createElement('div');
    mask.id = 'ge-batch-paste-modal';
    mask.style.cssText =
      'position:fixed;inset:0;background:rgba(0,0,0,.35);z-index:2147483647;display:flex;align-items:center;justify-content:center;padding:16px';
    const box = document.createElement('div');
    box.style.cssText =
      'background:#fff;border-radius:10px;padding:16px;width:min(560px,100%);max-height:80vh;display:flex;flex-direction:column;gap:10px';
    const t1 = document.createElement('div');
    t1.style.fontWeight = '600';
    t1.textContent = '批量粘贴店铺';
    const t2 = document.createElement('div');
    t2.style.cssText = 'font-size:12px;color:#86909c;line-height:1.5';
    t2.textContent =
      '每行一条：链接 或 链接,店铺标记,监控次数[,追加巡检1/0,追加轮数,间隔小时]（从第4列起可省略；追加巡检填 1 表示启用）';
    const ta = document.createElement('textarea');
    ta.style.cssText =
      'width:100%;min-height:160px;font-size:12px;padding:8px;border:1px solid #e5e6eb;border-radius:8px;box-sizing:border-box';
    const skip = document.createElement('label');
    skip.style.cssText = 'font-size:12px;display:flex;align-items:center;gap:6px;cursor:pointer';
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.checked = true;
    skip.appendChild(cb);
    skip.appendChild(document.createTextNode('跳过错误行，其余照常导入'));
    const row = document.createElement('div');
    row.style.cssText = 'display:flex;gap:8px;justify-content:flex-end';
    const bOk = document.createElement('button');
    bOk.textContent = '导入到列表';
    bOk.type = 'button';
    bOk.style.cssText = 'padding:8px 16px;background:#3370ff;color:#fff;border:none;border-radius:8px;cursor:pointer';
    const bCancel = document.createElement('button');
    bCancel.textContent = '取消';
    bCancel.type = 'button';
    bCancel.style.cssText = 'padding:8px 16px;background:#f2f3f5;border:1px solid #dee0e3;border-radius:8px;cursor:pointer';
    function shut() {
      try {
        mask.remove();
      } catch (_) {}
    }
    bCancel.addEventListener('click', shut);
    bOk.addEventListener('click', function () {
      const r = geParseBulkShopPasteLines(ta.value, cb.checked);
      if (r.aborted && r.errors.length) {
        const msg = r.errors
          .slice(0, 10)
          .map(function (e) {
            return '第 ' + e.line + ' 行：' + e.msg;
          })
          .join('\n');
        alert(msg + (r.errors.length > 10 ? '\n…' : ''));
        return;
      }
      if (r.errors.length && cb.checked) {
        alert('已跳过 ' + r.errors.length + ' 行（格式或链接不合规）');
      }
      if (!r.tasks.length) {
        alert('没有可导入的行');
        return;
      }
      onDone(r.tasks);
      shut();
    });
    row.appendChild(bCancel);
    row.appendChild(bOk);
    box.appendChild(t1);
    box.appendChild(t2);
    box.appendChild(ta);
    box.appendChild(skip);
    box.appendChild(row);
    mask.appendChild(box);
    mask.addEventListener('click', function (e) {
      if (e.target === mask) shut();
    });
    (document.body || document.documentElement).appendChild(mask);
  }

  // 新增：飞书多维表格导入（支持链接直拉 + 复制粘贴）
  function geOpenFeishuShopImportModal(onDone) {
    if (document.getElementById('ge-feishu-shop-import-modal')) return;
    const mask = document.createElement('div');
    mask.id = 'ge-feishu-shop-import-modal';
    mask.style.cssText =
      'position:fixed;inset:0;background:rgba(0,0,0,.35);z-index:2147483647;display:flex;align-items:center;justify-content:center;padding:16px';
    const box = document.createElement('div');
    box.style.cssText =
      'background:#fff;border-radius:10px;padding:16px;width:min(620px,100%);max-height:80vh;display:flex;flex-direction:column;gap:10px';
    const t1 = document.createElement('div');
    t1.style.fontWeight = '600';
    t1.textContent = '飞书店铺导入';
    const t2 = document.createElement('div');
    t2.style.cssText = 'font-size:12px;color:#86909c;line-height:1.5';
    t2.textContent =
      '支持两种方式：1）粘贴飞书多维表格链接直接拉取；2）复制表格内容后粘贴。都至少需要“店铺名称、店铺链接”两列（顺序不限）。';
    const linkRow = document.createElement('div');
    linkRow.style.cssText = 'display:flex;gap:8px;align-items:center';
    const linkInp = document.createElement('input');
    linkInp.type = 'text';
    linkInp.placeholder = '粘贴飞书多维表格链接（base 或 wiki）';
    linkInp.style.cssText =
      'flex:1;min-width:0;padding:8px 10px;border:1px solid #e5e6eb;border-radius:8px;font-size:12px;box-sizing:border-box';
    const bFromLink = document.createElement('button');
    bFromLink.type = 'button';
    bFromLink.textContent = '从链接导入';
    bFromLink.style.cssText = 'padding:8px 12px;background:#3370ff;color:#fff;border:none;border-radius:8px;cursor:pointer;white-space:nowrap';
    const ta = document.createElement('textarea');
    ta.style.cssText =
      'width:100%;min-height:180px;font-size:12px;padding:8px;border:1px solid #e5e6eb;border-radius:8px;box-sizing:border-box';
    const skip = document.createElement('label');
    skip.style.cssText = 'font-size:12px;display:flex;align-items:center;gap:6px;cursor:pointer';
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.checked = true;
    skip.appendChild(cb);
    skip.appendChild(document.createTextNode('跳过错误行，其余照常导入'));
    const row = document.createElement('div');
    row.style.cssText = 'display:flex;gap:8px;justify-content:flex-end';
    const bOk = document.createElement('button');
    bOk.textContent = '导入到列表';
    bOk.type = 'button';
    bOk.style.cssText = 'padding:8px 16px;background:#3370ff;color:#fff;border:none;border-radius:8px;cursor:pointer';
    const bCancel = document.createElement('button');
    bCancel.textContent = '取消';
    bCancel.type = 'button';
    bCancel.style.cssText = 'padding:8px 16px;background:#f2f3f5;border:1px solid #dee0e3;border-radius:8px;cursor:pointer';
    function shut() {
      try {
        mask.remove();
      } catch (_) {}
    }
    bFromLink.addEventListener('click', async function () {
      const rawLink = String(linkInp.value || '').trim();
      if (!rawLink) {
        alert('请先粘贴飞书多维表格链接');
        return;
      }
      const oldTxt = bFromLink.textContent;
      bFromLink.disabled = true;
      bFromLink.textContent = '导入中...';
      try {
        const r = await geImportShopsFromFeishuLink(rawLink, cb.checked);
        if (r.aborted && r.errors.length) {
          const msg = r.errors
            .slice(0, 10)
            .map(function (e) {
              return '第 ' + e.line + ' 行：' + e.msg;
            })
            .join('\n');
          alert(msg + (r.errors.length > 10 ? '\n…' : ''));
          return;
        }
        if (r.errors.length && cb.checked) {
          alert('已跳过 ' + r.errors.length + ' 行（缺少链接或链接不合规）');
        }
        if (!r.tasks.length) {
          alert('没有可导入的店铺数据');
          return;
        }
        onDone(r.tasks);
        shut();
      } catch (e) {
        alert('从链接导入失败：' + (e && e.message ? e.message : e));
      } finally {
        bFromLink.disabled = false;
        bFromLink.textContent = oldTxt;
      }
    });
    bCancel.addEventListener('click', shut);
    bOk.addEventListener('click', function () {
      const r = geParseFeishuShopRows(ta.value, cb.checked);
      if (r.aborted && r.errors.length) {
        const msg = r.errors
          .slice(0, 10)
          .map(function (e) {
            return '第 ' + e.line + ' 行：' + e.msg;
          })
          .join('\n');
        alert(msg + (r.errors.length > 10 ? '\n…' : ''));
        return;
      }
      if (r.errors.length && cb.checked) {
        alert('已跳过 ' + r.errors.length + ' 行（缺少链接或链接不合规）');
      }
      if (!r.tasks.length) {
        alert('没有可导入的店铺数据');
        return;
      }
      onDone(r.tasks);
      shut();
    });
    row.appendChild(bCancel);
    row.appendChild(bOk);
    box.appendChild(t1);
    box.appendChild(t2);
    linkRow.appendChild(linkInp);
    linkRow.appendChild(bFromLink);
    box.appendChild(linkRow);
    box.appendChild(ta);
    box.appendChild(skip);
    box.appendChild(row);
    mask.appendChild(box);
    mask.addEventListener('click', function (e) {
      if (e.target === mask) shut();
    });
    (document.body || document.documentElement).appendChild(mask);
  }

  // 新增：表格式店铺任务配置 UI（无 JSON 手填）
  function showBuyinBatchConfigModal() {
    if (document.getElementById('ge-batch-modal')) return;
    const mask = document.createElement('div');
    mask.id = 'ge-batch-modal';
    mask.style.cssText =
      'position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:2147483646;display:flex;align-items:center;justify-content:center;padding:12px;box-sizing:border-box';
    const box = document.createElement('div');
    box.style.cssText =
      'background:#fff;border-radius:12px;width:min(1320px,calc(100vw - 24px));max-height:92vh;overflow:hidden;display:flex;flex-direction:column;font:13px/1.5 -apple-system,BlinkMacSystemFont,sans-serif';
    const head = document.createElement('div');
    head.style.cssText = 'padding:14px 16px;border-bottom:1px solid #e5e6eb';
    const h1 = document.createElement('div');
    h1.style.cssText = 'font-weight:600;font-size:15px';
    h1.textContent = '配置店铺任务';
    const h2 = document.createElement('div');
    h2.style.cssText = 'font-size:12px;color:#86909c;margin-top:4px';
    h2.textContent =
      '改表格（含勾选启用）约 0.4s 后自动写入本机存储。录入规则：可勾选「全店录入」或组合「录新品」「录销量高」（取上架/销量 Top N，可合并去重）。关窗后「开始监控」读已保存数据。';
    head.appendChild(h1);
    head.appendChild(h2);
    const toolbar = document.createElement('div');
    toolbar.style.cssText =
      'padding:10px 16px;display:flex;flex-wrap:wrap;gap:8px;align-items:center;border-bottom:1px solid #f2f3f5';
    const tableWrap = document.createElement('div');
    tableWrap.style.cssText = 'overflow:auto;flex:1;min-height:200px;padding:0 8px 8px';
    const table = document.createElement('table');
    table.style.cssText = 'width:100%;border-collapse:collapse;font-size:12px';
    const thead = document.createElement('thead');
    const thr = document.createElement('tr');
    thr.style.cssText = 'background:#f7f8fa;color:#4e5969';
    function th(txt, w) {
      const x = document.createElement('th');
      x.textContent = txt;
      x.style.cssText = 'padding:8px 6px;text-align:left;font-weight:500' + (w ? ';' + w : '');
      return x;
    }
    thr.appendChild(th('店铺名称', 'width:110px'));
    thr.appendChild(th('店铺链接', 'min-width:280px'));
    thr.appendChild(th('店铺标记', 'width:100px'));
    thr.appendChild(th('全店', 'width:44px'));
    thr.appendChild(th('新品', 'width:44px'));
    thr.appendChild(th('新品数', 'width:56px'));
    thr.appendChild(th('高销', 'width:44px'));
    thr.appendChild(th('排行数', 'width:56px'));
    thr.appendChild(th('监控次数', 'width:72px'));
    thr.appendChild(th('追加巡检', 'width:72px'));
    (function () {
      const thx = th('追加轮数', 'width:72px');
      thx.title = '勾选追加巡检后：填 0 也会按至少再跑 1 个大轮处理；≥1 表示在首巡后再追加几轮';
      thr.appendChild(thx);
    })();
    thr.appendChild(th('间隔(h)', 'width:64px'));
    thr.appendChild(th('启用', 'width:52px'));
    thr.appendChild(th('操作', 'width:210px'));
    thead.appendChild(thr);
    const tbody = document.createElement('tbody');
    tbody.id = 'ge-batch-shop-tbody';
    tbody.addEventListener('change', function () {
      geDebouncedPersistBatchShopTable(tbody);
    });
    tbody.addEventListener('input', function () {
      geDebouncedPersistBatchShopTable(tbody);
    });
    table.appendChild(thead);
    table.appendChild(tbody);
    tableWrap.appendChild(table);
    const foot = document.createElement('div');
    foot.style.cssText =
      'padding:12px 16px;border-top:1px solid #e5e6eb;display:flex;flex-wrap:wrap;gap:8px;justify-content:flex-end';

    function mkBtn(txt, bg, fg, bd) {
      const b = document.createElement('button');
      b.type = 'button';
      b.textContent = txt;
      b.style.cssText =
        'padding:6px 12px;font-size:12px;border-radius:6px;cursor:pointer;border:1px solid ' +
        (bd || '#dee0e3') +
        ';background:' +
        (bg || '#fff') +
        ';color:' +
        (fg || '#1f2329');
      return b;
    }

    function inp(type, field, val) {
      const i = document.createElement('input');
      i.type = type || 'text';
      i.setAttribute('data-field', field);
      if (type === 'checkbox') {
        i.checked = val !== false;
        i.style.cssText = 'width:18px;height:18px;cursor:pointer';
        return i;
      }
      if (type === 'number') {
        if (field === 'monitorTimes' || field === 'extraRoundsCount') {
          i.min = field === 'extraRoundsCount' ? '0' : '1';
          i.step = '1';
        } else if (field === 'extraRoundsIntervalHours') {
          i.min = '0.1';
          i.step = '0.5';
        } else if (field === 'newestLimit' || field === 'topSalesLimit') {
          i.min = '1';
          i.step = '1';
          i.title =
            field === 'newestLimit'
              ? '录新品：按页面「上架时间」降序（create_time 新款在上）排序后取列表前 N 条，N 取本行「新品数」，不解析上架时间字段'
              : '销量Top：全店加载后按解析销量取前 N 个，N 取本行「排行数」';
        } else {
          i.min = '1';
          i.step = '1';
        }
      }
      i.value = val != null ? String(val) : '';
      i.style.cssText =
        'width:100%;box-sizing:border-box;padding:6px 8px;border:1px solid #e5e6eb;border-radius:6px;font-size:12px';
      return i;
    }

    function readRowsFromDom() {
      return geReadBatchShopTasksFromTbody(tbody);
    }

    function validateTasks(arr) {
      return geValidateShopTasksForRun(arr);
    }

    function geBuildShopTaskTableRow(task) {
      const tr = document.createElement('tr');
      tr.setAttribute('data-ge-shop-row', '1');
      tr.setAttribute('data-task-id', task.id || geGenShopTaskId());
      tr.style.borderBottom = '1px solid #f2f3f5';
      const tm = geMigrateShopTask(task);
      const td0 = document.createElement('td');
      td0.appendChild(inp('text', 'shopName', tm.shopName));
      const td1 = document.createElement('td');
      const u = inp('text', 'url', tm.url);
      u.style.minWidth = '260px';
      td1.appendChild(u);
      const td2 = document.createElement('td');
      td2.appendChild(inp('text', 'shopTag', tm.shopTag));
      const tdAll = document.createElement('td');
      tdAll.style.textAlign = 'center';
      const cbAll = inp('checkbox', 'recordAll', tm.recordAll === true);
      tdAll.appendChild(cbAll);
      const tdRn = document.createElement('td');
      tdRn.style.textAlign = 'center';
      const cbNew = inp('checkbox', 'recordNewest', tm.recordNewest === true);
      tdRn.appendChild(cbNew);
      const tdNl = document.createElement('td');
      const numNew = inp('number', 'newestLimit', tm.newestLimit);
      tdNl.appendChild(numNew);
      const tdRs = document.createElement('td');
      tdRs.style.textAlign = 'center';
      const cbSales = inp('checkbox', 'recordTopSales', tm.recordTopSales === true);
      tdRs.appendChild(cbSales);
      const tdTsl = document.createElement('td');
      const numSales = inp('number', 'topSalesLimit', tm.topSalesLimit);
      tdTsl.appendChild(numSales);
      function syncRecordRuleUi() {
        cbNew.disabled = false;
        cbSales.disabled = false;
        numNew.disabled = !cbNew.checked;
        numSales.disabled = !cbSales.checked;
        tdRn.style.opacity = '1';
        tdNl.style.opacity = cbNew.checked ? '1' : '0.45';
        tdRs.style.opacity = '1';
        tdTsl.style.opacity = cbSales.checked ? '1' : '0.45';
      }
      cbAll.addEventListener('change', function () {
        syncRecordRuleUi();
        geDebouncedPersistBatchShopTable(tbody);
      });
      cbNew.addEventListener('change', function () {
        syncRecordRuleUi();
        geDebouncedPersistBatchShopTable(tbody);
      });
      cbSales.addEventListener('change', function () {
        syncRecordRuleUi();
        geDebouncedPersistBatchShopTable(tbody);
      });
      syncRecordRuleUi();
      const td3 = document.createElement('td');
      td3.appendChild(inp('number', 'monitorTimes', tm.monitorTimes));
      const td3b = document.createElement('td');
      td3b.style.textAlign = 'center';
      td3b.appendChild(inp('checkbox', 'extraRoundsEnabled', tm.extraRoundsEnabled === true));
      const td3c = document.createElement('td');
      td3c.appendChild(inp('number', 'extraRoundsCount', tm.extraRoundsCount));
      const td3d = document.createElement('td');
      td3d.appendChild(inp('number', 'extraRoundsIntervalHours', tm.extraRoundsIntervalHours));
      const td4 = document.createElement('td');
      td4.style.textAlign = 'center';
      td4.appendChild(inp('checkbox', 'enabled', tm.enabled));
      const td5 = document.createElement('td');
      td5.style.whiteSpace = 'nowrap';
      function addMini(label, fn) {
        const b = mkBtn(label, '#f7f8fa', '#1d2129', '#e5e6eb');
        b.style.padding = '4px 6px';
        b.style.fontSize = '11px';
        b.style.marginRight = '4px';
        b.addEventListener('click', function (e) {
          e.preventDefault();
          fn(b);
        });
        td5.appendChild(b);
      }
      addMini('复制', function () {
        const rows = readRowsFromDom();
        const curId = tr.getAttribute('data-task-id');
        const self = rows.filter(function (x) {
          return x.id === curId;
        })[0];
        const base = self || task;
        const dup = geMigrateShopTask({
          id: geGenShopTaskId(),
          shopName: base.shopName,
          url: base.url,
          shopTag: base.shopTag,
          monitorTimes: base.monitorTimes,
          enabled: base.enabled,
          extraRoundsEnabled: base.extraRoundsEnabled,
          extraRoundsCount: base.extraRoundsCount,
          extraRoundsIntervalHours: base.extraRoundsIntervalHours,
          recordAll: base.recordAll,
          recordNewest: base.recordNewest,
          recordTopSales: base.recordTopSales,
          newestLimit: base.newestLimit,
          topSalesLimit: base.topSalesLimit,
        });
        const i = rows.findIndex(function (x) {
          return x.id === curId;
        });
        rows.splice(i + 1, 0, dup);
        renderRows(rows);
      });
      addMini('上移', function () {
        const rows = readRowsFromDom();
        const curId = tr.getAttribute('data-task-id');
        const i = rows.findIndex(function (x) {
          return x.id === curId;
        });
        if (i > 0) {
          const t = rows[i - 1];
          rows[i - 1] = rows[i];
          rows[i] = t;
          renderRows(rows);
        }
      });
      addMini('下移', function () {
        const rows = readRowsFromDom();
        const curId = tr.getAttribute('data-task-id');
        const i = rows.findIndex(function (x) {
          return x.id === curId;
        });
        if (i >= 0 && i < rows.length - 1) {
          const t = rows[i + 1];
          rows[i + 1] = rows[i];
          rows[i] = t;
          renderRows(rows);
        }
      });
      addMini('删行', function () {
        let rows = readRowsFromDom();
        const curId = tr.getAttribute('data-task-id');
        rows = rows.filter(function (x) {
          return x.id !== curId;
        });
        renderRows(rows);
      });
      addMini('下行', function () {
        const rows = readRowsFromDom();
        const curId = tr.getAttribute('data-task-id');
        const i = rows.findIndex(function (x) {
          return x.id === curId;
        });
        rows.splice(i + 1, 0, geMigrateShopTask({ id: geGenShopTaskId() }));
        renderRows(rows);
      });
      addMini('写飞书', async function (btn) {
        const rows = readRowsFromDom();
        const curId = tr.getAttribute('data-task-id');
        const self = rows.filter(function (x) {
          return x.id === curId;
        })[0];
        const cur = geMigrateShopTask(self || task || {});
        if (!String(cur.shopName || '').trim() || !String(cur.url || '').trim()) {
          alert('请先填写该行店铺名称和店铺链接');
          return;
        }
        const oldTxt = btn.textContent;
        btn.disabled = true;
        btn.textContent = '写入中...';
        try {
          const rs = await geWriteSingleShopToFeishu(cur);
          if (rs && rs.duplicated) {
            alert('该店铺已存在，无需重复写入');
          } else {
            alert('已成功写入飞书');
          }
        } catch (e) {
          alert('写入飞书失败：' + (e && e.message ? e.message : e));
        } finally {
          btn.disabled = false;
          btn.textContent = oldTxt;
        }
      });
      tr.appendChild(td0);
      tr.appendChild(td1);
      tr.appendChild(td2);
      tr.appendChild(tdAll);
      tr.appendChild(tdRn);
      tr.appendChild(tdNl);
      tr.appendChild(tdRs);
      tr.appendChild(tdTsl);
      tr.appendChild(td3);
      tr.appendChild(td3b);
      tr.appendChild(td3c);
      tr.appendChild(td3d);
      tr.appendChild(td4);
      tr.appendChild(td5);
      return tr;
    }

    function renderRows(list) {
      tbody.innerHTML = '';
      const arr = list && list.length ? list : [geMigrateShopTask({ id: geGenShopTaskId() })];
      for (let i = 0; i < arr.length; i++) tbody.appendChild(geBuildShopTaskTableRow(arr[i]));
    }

    function close() {
      try {
        clearTimeout(window.__geBatchShopTableAutosaveTimer);
        const rows = geReadBatchShopTasksFromTbody(tbody);
        if (rows.length) saveBatchShops(rows);
        geUpdateBatchStatusLine();
      } catch (_) {}
      try {
        mask.remove();
      } catch (_) {}
    }

    renderRows(loadBatchShops());

    toolbar.appendChild(
      (function () {
        const b = mkBtn('新增店铺', '#3370ff', '#fff', '#3370ff');
        b.addEventListener('click', function () {
          const rows = readRowsFromDom();
          rows.push(geMigrateShopTask({ id: geGenShopTaskId() }));
          renderRows(rows);
        });
        return b;
      })()
    );
    toolbar.appendChild(
      (function () {
        const b = mkBtn('批量粘贴', '#fff', '#1d2129', '#dee0e3');
        b.addEventListener('click', function () {
          geOpenBatchPasteModal(function (newTasks) {
            renderRows(readRowsFromDom().concat(newTasks));
          });
        });
        return b;
      })()
    );
    toolbar.appendChild(
      (function () {
        const b = mkBtn('飞书导入', '#fff', '#1d2129', '#dee0e3');
        b.addEventListener('click', function () {
          geOpenFeishuShopImportModal(function (newTasks) {
            renderRows(readRowsFromDom().concat(newTasks));
          });
        });
        return b;
      })()
    );
    toolbar.appendChild(
      (function () {
        const b = mkBtn('写入飞书店铺', '#fff', '#1d2129', '#dee0e3');
        b.addEventListener('click', async function () {
          const rows = readRowsFromDom();
          const usable = rows.filter(function (x) {
            return String(x.url || '').trim();
          });
          if (!usable.length) {
            alert('当前没有可写入的店铺（至少需有店铺链接）');
            return;
          }
          const cfg = loadFeishuConfig();
          const hasAccess =
            (cfg.accessToken || '').trim() ||
            ((cfg.feishuAppId || '').trim() && (cfg.feishuAppSecret || '').trim());
          if (!cfg.tableId || !hasAccess || (!cfg.wikiNodeToken && !cfg.appToken)) {
            showFeishuSettingsModal();
            alert(
              '请先完成飞书配置：table_id；「Wiki 节点 token」或「app_token」其一；access_token 与「App ID + App Secret」至少填一种。'
            );
            return;
          }
          const oldTxt = b.textContent;
          b.disabled = true;
          b.textContent = (cfg.wikiNodeToken || '').trim() ? '解析 Wiki 并写入…' : '写入中…';
          try {
            const up = await uploadShopTasksToFeishu(usable, cfg);
            const extra = up.columnsCreated > 0 ? '，并新建缺失列 ' + up.columnsCreated + ' 个' : '';
            alert('已写入飞书店铺表 ' + up.total + ' 条（仅店铺名称/店铺链接）' + extra);
          } catch (e) {
            alert('写入飞书店铺失败：' + (e && e.message ? e.message : e));
          } finally {
            b.disabled = false;
            b.textContent = oldTxt;
          }
        });
        return b;
      })()
    );
    toolbar.appendChild(
      (function () {
        const b = mkBtn('保存配置', '#3370ff', '#fff', '#3370ff');
        b.addEventListener('click', function () {
          const rows = readRowsFromDom();
          const v = validateTasks(rows);
          if (!v.ok) {
            alert(v.msg);
            return;
          }
          saveBatchShops(rows);
          alert('已保存 ' + rows.length + ' 条');
        });
        return b;
      })()
    );
    toolbar.appendChild(
      (function () {
        const b = mkBtn('导出备份', '#fff', '#1d2129', '#dee0e3');
        b.addEventListener('click', function () {
          const rows = readRowsFromDom();
          const v = validateTasks(rows);
          if (!v.ok && !confirm(v.msg + '\n\n仍导出当前表格内容？')) return;
          const blob = new Blob([JSON.stringify(rows, null, 2)], { type: 'application/json;charset=utf-8' });
          const a = document.createElement('a');
          a.href = URL.createObjectURL(blob);
          a.download = '百应店铺任务备份_' + Date.now() + '.json';
          a.click();
          setTimeout(function () {
            URL.revokeObjectURL(a.href);
          }, 2500);
        });
        return b;
      })()
    );
    toolbar.appendChild(
      (function () {
        const b = mkBtn('导入备份', '#fff', '#1d2129', '#dee0e3');
        const f = document.createElement('input');
        f.type = 'file';
        f.accept = '.json,application/json';
        f.style.display = 'none';
        b.addEventListener('click', function () {
          f.click();
        });
        f.addEventListener('change', function () {
          const file = f.files && f.files[0];
          if (!file) return;
          const rd = new FileReader();
          rd.onload = function () {
            try {
              const arr = JSON.parse(String(rd.result || '[]'));
              if (!Array.isArray(arr)) {
                alert('文件内容须为任务列表');
                return;
              }
              const norm = arr.map(function (x) {
                return geMigrateShopTask(x);
              });
              const v = validateTasks(norm);
              if (!v.ok) {
                alert(v.msg);
                return;
              }
              renderRows(norm);
              alert('已载入 ' + norm.length + ' 条，请点「保存配置」写入存储。');
            } catch (e) {
              alert('读取失败：' + (e && e.message ? e.message : e));
            }
            f.value = '';
          };
          rd.readAsText(file, 'utf-8');
        });
        toolbar.appendChild(f);
        return b;
      })()
    );
    toolbar.appendChild(
      (function () {
        const b = mkBtn('清空抓取缓冲', '#646a73', '#fff', '#646a73');
        b.addEventListener('click', function () {
          if (!confirm('清空批量监控已累积的商品数据？')) return;
          saveBatchAccum([]);
          alert('已清空缓冲');
        });
        return b;
      })()
    );
    toolbar.appendChild(
      (function () {
        const b = mkBtn('清空全部任务', '#f53f3f', '#fff', '#f53f3f');
        b.addEventListener('click', function () {
          if (!confirm('删除列表中全部店铺任务？')) return;
          saveBatchShops([]);
          renderRows([]);
          alert('已清空任务表');
        });
        return b;
      })()
    );

    foot.appendChild(
      (function () {
        const b = mkBtn('关闭', '#f2f3f5', '#1d2129', '#dee0e3');
        b.addEventListener('click', close);
        return b;
      })()
    );

    mask.addEventListener('click', function (e) {
      if (e.target === mask) close();
    });
    box.appendChild(head);
    box.appendChild(toolbar);
    box.appendChild(tableWrap);
    box.appendChild(foot);
    mask.appendChild(box);
    (document.body || document.documentElement).appendChild(mask);
  }

  // 新增：当前店铺一键加入任务列表
  function geAddCurrentShopToTaskList() {
    if (!isBuyin()) {
      alert('请在百应店铺页使用');
      return;
    }
    const ctx = getBuyinPageShopContext();
    const url = location.href.split('#')[0];
    if (!geLooksLikeBuyinShopUrl(url)) {
      alert('当前页不像店铺详情（请打开含 shop_id 的店铺链接）');
      return;
    }
    const shops = loadBatchShops();
    let existsIdx = -1;
    for (let i = 0; i < shops.length; i++) {
      if (geUrlsSameBuyinShop(shops[i].url, url)) {
        existsIdx = i;
        break;
      }
    }
    if (existsIdx >= 0) {
      if (
        !confirm('列表中已有该店铺链接。\n点「确定」覆盖原行信息；点「取消」则不修改。')
      )
        return;
      shops[existsIdx] = geMigrateShopTask({
        ...shops[existsIdx],
        shopName: ctx.shopName || shops[existsIdx].shopName,
        url,
        shopTag: shops[existsIdx].shopTag,
        monitorTimes: shops[existsIdx].monitorTimes,
        enabled: shops[existsIdx].enabled !== false,
        extraRoundsEnabled: shops[existsIdx].extraRoundsEnabled,
        extraRoundsCount: shops[existsIdx].extraRoundsCount,
        extraRoundsIntervalHours: shops[existsIdx].extraRoundsIntervalHours,
      });
    } else {
      shops.push(
        geMigrateShopTask({
          id: geGenShopTaskId(),
          shopName: ctx.shopName,
          url,
          shopTag: '',
          monitorTimes: 1,
          enabled: true,
        })
      );
    }
    saveBatchShops(shops);
    alert(existsIdx >= 0 ? '已覆盖该店铺任务' : '已加入任务列表，可在「配置店铺任务」中查看');
  }

  function geStartBuyinBatch() {
    const all = geGetShopTasksForBatchStart();
    const v = geValidateShopTasksForRun(all);
    if (!v.ok) {
      alert(v.msg + '\n\n提示：若刚改表格，请先点「保存配置」；或保持配置窗口打开后再点「开始监控」将自动按当前表格生效。');
      return;
    }
    if (document.getElementById('ge-batch-modal')) {
      saveBatchShops(all);
    }
    const shops = all.filter(function (s) {
      return s && s.enabled !== false && String(s.url || '').trim();
    });
    if (!shops.length) {
      alert(
        '没有已启用的店铺任务。\n\n请在「配置任务」里勾选「启用」，等待约半秒自动保存（或点「保存配置」）；再关闭窗口后点「开始监控」。'
      );
      return;
    }
    for (let i = 0; i < shops.length; i++) {
      const s = shops[i];
      if (!geLooksLikeBuyinShopUrl(s.url)) {
        alert('第 ' + (i + 1) + ' 个启用任务：链接不合法');
        return;
      }
    }
    saveBatchRunSnapshot(shops);
    geClearBatchNavTimers();
    try {
      window.__geBatchScheduled = false;
    } catch (_) {}
    saveBatchState({
      running: true,
      shopIdx: 0,
      roundIdx: 1,
      lastRunAt: Date.now(),
      phase: 'running',
      intervalUntil: 0,
      megaIdx: 1,
      execToken: geGenBatchExecToken(),
    });
    alert('已开始批量监控（共 ' + shops.length + ' 店）。即将打开第 1 个店铺，请保持脚本启用。');
    location.href = shops[0].url;
  }

  function geStopBuyinBatch() {
    const st = loadBatchState();
    geClearBatchNavTimers();
    clearBatchRunSnapshot();
    saveBatchState({
      running: false,
      shopIdx: st.shopIdx,
      roundIdx: st.roundIdx,
      lastRunAt: Date.now(),
      phase: 'running',
      intervalUntil: 0,
      megaIdx: 1,
      execToken: '',
    });
    try {
      window.__geBatchScheduled = false;
    } catch (_) {}
    geStopBatchLiveStatusTicker();
    try {
      window.__geBatchBusy = false;
    } catch (_) {}
    try {
      const live = document.getElementById('ge-batch-live-status');
      if (live) {
        live.textContent = '';
        live.style.display = 'none';
      }
    } catch (_) {}
    try {
      const rEl = document.getElementById('ge-batch-live-round');
      if (rEl) {
        rEl.textContent = '';
        rEl.style.display = 'none';
      }
    } catch (_) {}
    geUpdateBatchStatusLine();
    alert(
      '已停止批量监控（running=false、execToken 已失效并写入 GM）。已清除挂起的刷新/续跑/跳店定时器；当前页上的异步采集会在下一检查点退出。缓冲未清空。'
    );
  }

  function geExportBatchAccumCsv() {
    const rows = loadBatchAccum();
    if (!rows.length) {
      alert('批量缓冲为空');
      return;
    }
    downloadCsv(rows, '百应批量监控累积');
  }

  async function geDoExportFeishuBatchAccum() {
    const cfg = loadFeishuConfig();
    const hasAccess =
      (cfg.accessToken || '').trim() ||
      ((cfg.feishuAppId || '').trim() && (cfg.feishuAppSecret || '').trim());
    if (!cfg.tableId || !hasAccess || (!cfg.wikiNodeToken && !cfg.appToken)) {
      showFeishuSettingsModal();
      alert('请先完成飞书配置。');
      return;
    }
    const rows = loadBatchAccum();
    if (!rows.length) {
      alert('批量缓冲为空');
      return;
    }
    try {
      const up = await uploadRowsToFeishu(rows, cfg);
      geRemoveAccumRowsWritten(rows);
      const extra = up.columnsCreated > 0 ? '（本次新建缺失列 ' + up.columnsCreated + ' 个）' : '';
      alert('已将缓冲中 ' + up.total + ' 条写入飞书' + extra);
    } catch (e) {
      alert('写入飞书失败（缓冲已保留）：' + (e && e.message ? e.message : e));
    }
  }

  /** 百应列表底部「到底」类文案（与滚动多信号判定配合，避免单靠高度误判） */
  function geBuyinDomShowsNoMoreMarker() {
    try {
      const t = (document.body && (document.body.innerText || '')) || '';
      if (t.indexOf('没有更多了') !== -1) return true;
      return /没有更多|暂无更多|已经到底|已展示全部|没有更多商品|已经看完了/i.test(t);
    } catch (_) {
      return false;
    }
  }

  function geCountBuyinDomProductCards() {
    try {
      return document.querySelectorAll('div[class*="card___"]').length;
    } catch (_) {
      return 0;
    }
  }

  /** 列表指纹：用于判断排序切换后 DOM/数据是否变化 */
  function geBuyinListFingerprint() {
    try {
      const dom = scrapeBuyinDom();
      if (dom.length) {
        return dom
          .slice(0, 14)
          .map(function (r) {
            return ((r.title || '') + '|' + (r.sales || '')).slice(0, 48);
          })
          .join('¦');
      }
      const n = typeof buyinData !== 'undefined' && Array.isArray(buyinData) ? buyinData.length : 0;
      return 'api:' + n;
    } catch (_) {
      return '';
    }
  }

  /**
   * 百应店铺 Feed：order_by=create_time 时，降序 = 新款在上（与页面「上架时间」新→旧一致）。
   * 当前接口约定 sort=1 为 create_time 降序；若与线上相反，改此常量或在 opts.createTimeSort 覆盖。
   */
  const GE_BUYIN_CREATE_TIME_SORT_DESC = 1;

  /**
   * 查找列表排序控件（文案须为「上架时间」或「销量」），返回可 click 的元素。
   */
  function geFindSortTabClickTargetByText(sortName) {
    const want = String(sortName || '').trim();
    if (want !== '上架时间' && want !== '销量') return null;
    const selectors =
      'button, [role="tab"], [role="button"], [class*="tab"], [class*="Tab"], [class*="sort"], [class*="Sort"], span, div, li, a';
    const candidates = document.querySelectorAll(selectors);
    for (let i = 0; i < candidates.length; i++) {
      const el = candidates[i];
      const raw = (el.textContent || '').replace(/\s+/g, ' ').trim();
      if (raw !== want) continue;
      const rect = el.getBoundingClientRect();
      if (rect.width < 6 || rect.height < 6) continue;
      let clickEl = el;
      if (clickEl.tagName === 'SPAN' && clickEl.parentElement) {
        const p = clickEl.parentElement;
        const tag = p.tagName;
        const role = (p.getAttribute && p.getAttribute('role')) || '';
        if (tag === 'BUTTON' || role === 'tab' || role === 'button') clickEl = p;
      }
      return clickEl;
    }
    return null;
  }

  /**
   * 点击列表排序项（文案须为「上架时间」或「销量」）。返回是否触发 click。
   */
  function geClickSortTabByText(sortName) {
    const clickEl = geFindSortTabClickTargetByText(sortName);
    if (!clickEl) return false;
    try {
      clickEl.click();
      return true;
    } catch (_) {
      return false;
    }
  }

  /**
   * 新品录入：在已点「上架时间」后，若能从 aria-sort / class 判断当前为升序（旧→新），再点一次切为降序（新→旧）。
   * 无法判断时不二次点击，避免误切；此时仍依赖请求体 patch 中的 sort=降序。
   */
  async function geEnsureShelfTimeSortNewestFirst(opts) {
    const timeoutMs = (opts && opts.timeoutMs) != null ? opts.timeoutMs : 12000;
    const setStatus = opts && opts.setStatus;
    const prefix = (opts && opts.prefixStatus) || '';
    const target = geFindSortTabClickTargetByText('上架时间');
    if (!target) return;

    const nodeClass = function (el) {
      if (!el) return '';
      if (typeof el.className === 'string') return el.className;
      try {
        return el.getAttribute('class') || '';
      } catch (_) {
        return '';
      }
    };

    const isAscendingState = function () {
      let el = target;
      for (let hop = 0; hop < 10 && el; hop++) {
        const as = el.getAttribute && el.getAttribute('aria-sort');
        if (as) {
          const s = String(as).toLowerCase();
          if (s.indexOf('descend') >= 0 || s === 'descend') return false;
          if (s.indexOf('ascend') >= 0 || s === 'ascend') return true;
        }
        const cls = nodeClass(el);
        if (/\bdescend|sort-desc|sort-down|caret-down|arrow-down|down-outlined/i.test(cls)) {
          if (!/\bascend|sort-asc|sort-up|caret-up|arrow-up|up-outlined/i.test(cls)) return false;
        }
        if (/\bascend|sort-asc|sort-up|caret-up|arrow-up|up-outlined/i.test(cls)) {
          if (!/\bdescend|sort-desc|sort-down|caret-down|arrow-down|down-outlined/i.test(cls)) return true;
        }
        el = el.parentElement;
      }
      return null;
    };

    if (isAscendingState() === true) {
      if (setStatus) setStatus(prefix + ' · 「上架时间」为升序，切换为降序（新款在上）…');
      try {
        target.click();
      } catch (_) {}
      await geWaitSortResultStable('上架时间', timeoutMs, opts && opts.execToken);
      await geSleepMaybeBatch(400, opts && opts.execToken);
    }
  }

  /**
   * 排序点击后等待列表稳定：指纹变化或卡片数明显变化，辅以短等待；不单独依赖固定长 sleep。
   */
  async function geWaitSortResultStable(sortName, timeoutMs, execToken) {
    const deadline = Date.now() + (timeoutMs != null ? timeoutMs : 16000);
    const fp0 = geBuyinListFingerprint();
    const c0 = geCountBuyinDomProductCards();
    await geSleepMaybeBatch(380, execToken);
    while (Date.now() < deadline) {
      geBatchAbortIfCancelled(execToken);
      await geSleepMaybeBatch(420, execToken);
      const fp1 = geBuyinListFingerprint();
      const c1 = geCountBuyinDomProductCards();
      if (fp1 && fp1 !== fp0) {
        await geSleepMaybeBatch(480, execToken);
        return true;
      }
      if (c1 !== c0 && fp1.length > 0) {
        await geSleepMaybeBatch(480, execToken);
        return true;
      }
    }
    return geCountBuyinDomProductCards() > 0;
  }

  /**
   * 下拉直到 buyinData 至少 minRows 条或触底逻辑成立（在 geScrollBuyinUntilNoMore 基础上增加「条数够即停」）。
   */
  async function geScrollBuyinUntilEnoughOrNoMore(opts) {
    const setStatus = opts && opts.setStatus;
    const prefix = (opts && opts.prefixStatus) || '';
    const minRows = Math.max(1, parseInt(opts && opts.minRows, 10) || 1);
    const maxSteps = 160;
    const pauseMs = 480;
    const STABLE_N = 6;
    const MIN_STEPS = 8;
    let lastH = -1;
    let lastCards = -1;
    let lastDataLen = -1;
    let stable = 0;

    const execToken = opts && opts.execToken;
    for (let step = 0; step < maxSteps; step++) {
      if (execToken) geBatchAbortIfCancelled(execToken);
      const de = document.documentElement;
      const body = document.body;
      const h = Math.max(de ? de.scrollHeight : 0, body ? body.scrollHeight : 0, 0);
      window.scrollTo(0, h);
      try {
        const candidates = document.querySelectorAll('div,main,section,article');
        for (let i = 0; i < candidates.length; i++) {
          const el = candidates[i];
          const st = getComputedStyle(el);
          const oy = st.overflowY;
          if ((oy === 'auto' || oy === 'scroll') && el.scrollHeight > el.clientHeight + 60) {
            el.scrollTop = el.scrollHeight;
          }
        }
      } catch (_) {}
      await geSleepMaybeBatch(pauseMs, execToken);

      const h2 = Math.max(document.documentElement.scrollHeight, document.body.scrollHeight, 0);
      const cards = geCountBuyinDomProductCards();
      const dataLen = typeof buyinData !== 'undefined' && Array.isArray(buyinData) ? buyinData.length : 0;
      const marker = geBuyinDomShowsNoMoreMarker();

      if (lastH >= 0) {
        const grew = h2 > lastH + 2 || cards > lastCards + 2 || dataLen > lastDataLen;
        if (!grew) stable++;
        else stable = 0;
      } else stable = 0;

      lastH = h2;
      lastCards = cards;
      lastDataLen = dataLen;

      if (dataLen >= minRows && stable >= 2 && step >= 3) {
        if (setStatus) setStatus(prefix + ' · 已攒够接口约 ' + dataLen + ' 条（目标≥' + minRows + '）');
        await geSleepMaybeBatch(280, execToken);
        return true;
      }

      if (marker && stable >= 2 && step >= 2) {
        if (setStatus) setStatus(prefix + ' · 已触底（底部文案+连续无新增）');
        await geSleepMaybeBatch(280, execToken);
        return true;
      }

      if (stable >= STABLE_N && step >= MIN_STEPS) {
        if (dataLen >= minRows) {
          if (setStatus) setStatus(prefix + ' · 已攒够接口约 ' + dataLen + ' 条');
          await geSleepMaybeBatch(280, execToken);
          return true;
        }
        if (marker) {
          if (setStatus) setStatus(prefix + ' · 已触底（多信号+底部提示）');
          await geSleepMaybeBatch(280, execToken);
          return true;
        }
        /** 未达目标条数且无底部文案：继续滚，避免过早结束 */
        stable = 0;
      }

      if (setStatus && step % 6 === 0) {
        setStatus(prefix + ' · 下拉 ' + (step + 1) + '/' + maxSteps + ' · 接口' + dataLen + '条');
      }
    }
    if (setStatus) setStatus(prefix + ' · 已达最大滚动步数（当前接口约 ' + lastDataLen + ' 条）');
    return lastDataLen >= minRows;
  }

  /**
   * 切换排序 Tab → 等稳定 → 下拉攒条数 → 补链 → 取前 limit 条。
   * 最近上架（上架时间）：create_time 降序 + 全店滚动至目标条数后，直接 buildBuyinExportRowsSnapshot 有序快照去重取前 N，不依赖 DOM 种子/模糊匹配。
   * 其他：抓取窗口内可开启 geBuyinNewestListRequestPatch（visibleOnly=false 时）。
   */
  async function geCollectTopNRowsUnderSort(opts) {
    const sortLabel = (opts && opts.sortLabel) || '';
    const limitParsed = parseInt(opts && opts.limit, 10);
    const limit = Number.isFinite(limitParsed) && limitParsed >= 1 ? limitParsed : 1;
    const setStatus = opts && opts.setStatus;
    const prefix = (opts && opts.prefixStatus) || '';

    // 模式A默认：只采集当前可视区域；模式B（visibleOnly=false）才允许滚动补抓。
    const visibleOnly = opts && opts.visibleOnly != null ? !!opts.visibleOnly : true;
    const noScroll = opts && opts.noScroll != null ? !!opts.noScroll : visibleOnly;

    const minBuf = Math.min(Math.max(limit + 20, limit * 2), 500);
    const isNewestByCreateTime =
      sortLabel === '上架时间' || (opts && opts.treatAsCreateTimeSort) === true;
    const useCreateTimePatch = isNewestByCreateTime && !visibleOnly;

    if (useCreateTimePatch) {
      geBuyinNewestListRequestPatch = {
        size: limit,
        sort:
          opts && opts.createTimeSort != null
            ? parseInt(opts.createTimeSort, 10) || GE_BUYIN_CREATE_TIME_SORT_DESC
            : GE_BUYIN_CREATE_TIME_SORT_DESC,
        scene: 'PCShopDetailFeed',
      };
    } else {
      geBuyinNewestListRequestPatch = null;
    }

    // 「当前模式禁止」：如果内部不小心触发滚动，必须打印日志。
    let restoreScrollHooks = null;
    if (visibleOnly && noScroll) {
      let origScrollTo = null;
      let origScrollIntoView = null;
      let lastWarnAt = 0;
      try {
        if (typeof window.scrollTo === 'function') {
          origScrollTo = window.scrollTo;
          window.scrollTo = function () {
            const now = Date.now();
            if (now - lastWarnAt > 600) {
              lastWarnAt = now;
              console.warn('检测到滚动调用（当前模式禁止）');
            }
            return origScrollTo.apply(this, arguments);
          };
        }
        if (
          window.HTMLElement &&
          window.HTMLElement.prototype &&
          typeof window.HTMLElement.prototype.scrollIntoView === 'function'
        ) {
          origScrollIntoView = window.HTMLElement.prototype.scrollIntoView;
          window.HTMLElement.prototype.scrollIntoView = function () {
            const now = Date.now();
            if (now - lastWarnAt > 600) {
              lastWarnAt = now;
              console.warn('检测到滚动调用（当前模式禁止）');
            }
            return origScrollIntoView.apply(this, arguments);
          };
        }
      } catch (_) {}
      restoreScrollHooks = function () {
        try {
          if (origScrollTo) window.scrollTo = origScrollTo;
        } catch (_) {}
        try {
          if (origScrollIntoView && window.HTMLElement && window.HTMLElement.prototype) {
            window.HTMLElement.prototype.scrollIntoView = origScrollIntoView;
          }
        } catch (_) {}
      };
    }

    try {
      geClearBuyinDataForSortCapture();
      const batchExecToken = opts && opts.execToken;

      /** 销量Top：必须先切到「销量」排序再全店滚载，否则 buyinData 仍按上一排序（如上架时间）累积，Top 会严重偏低 */
      if (sortLabel === '销量') {
        if (setStatus) setStatus(prefix + ' · 销量Top：切换「销量」排序并等待列表稳定…');
        let clickedSales = false;
        for (let attempt = 0; attempt < 3; attempt++) {
          geBatchAbortIfCancelled(batchExecToken);
          if (geClickSortTabByText('销量')) {
            clickedSales = true;
            break;
          }
          await geSleepMaybeBatch(600, batchExecToken);
        }
        if (!clickedSales) {
          console.warn('[销量Top] 未能点击「销量」排序 Tab，仍将滚动；若页面仍为「上架时间」等，快照会按错误排序累积');
        }
        await geWaitSortResultStable('销量', 15000, batchExecToken);
        await geSleepMaybeBatch(500, batchExecToken);

        if (setStatus) setStatus(prefix + ' · 销量Top：全店加载后按销量取前 ' + limit + '…');
        await geScrollBuyinUntilNoMore({
          setStatus,
          prefixStatus: prefix + ' · 销量Top',
          execToken: batchExecToken,
        });
        const btnSales = document.getElementById('goods-export-btn');
        await waitForBuyinDetailLinksIfNeeded(btnSales, { execToken: batchExecToken });
        await geSleepMaybeBatch(520, batchExecToken);
        try {
          await geRefetchBuyinMaterialListForLinks(true);
        } catch (_) {}
        await geSleepMaybeBatch(420, batchExecToken);

        const buyinLenBeforeSnap = buyinData.length;
        let full = buildBuyinExportRowsSnapshot({ preserveRowOrder: true });
        const maxA = geTopSalesMaxParsed(full);
        geLogTopSalesPipeline(
          '①快照 buildBuyinExportRowsSnapshot（去重/排序前）',
          full,
          'buyinData.length(快照前)=' + buyinLenBeforeSnap
        );

        const beforeDedup = full.length;
        const maxBeforeDedup = maxA;
        full = geDedupRowsByIdentity(full);
        const maxB = geTopSalesMaxParsed(full);
        geLogTopSalesPipeline(
          '②去重 geDedupRowsByIdentity 后',
          full,
          '去重前条数=' + beforeDedup + ' 去掉=' + (beforeDedup - full.length)
        );
        geTopSalesWarnIfDropped('①快照', maxBeforeDedup, '②去重后', maxB);

        const sorted = geSortRowsBySalesDesc(full);
        const maxC = geTopSalesMaxParsed(sorted);
        geLogTopSalesPipeline('③按销量降序 geSortRowsBySalesDesc 后（前 table 行为全店 Top 序前 20）', sorted, '');
        geTopSalesWarnIfDropped('②去重后', maxB, '③排序后', maxC);

        const finalRows = sorted.slice(0, limit);
        const maxD = geTopSalesMaxParsed(finalRows);
        geLogTopSalesPipeline('④最终导出 slice(0,' + limit + ')', finalRows, '');
        geTopSalesWarnIfDropped('③排序后', maxC, '④最终', maxD);

        return finalRows;
      }

      if (visibleOnly) console.log('开始按【' + sortLabel + '】排序，模式=仅当前可视区域');

      let clicked = false;
      for (let attempt = 0; attempt < 3; attempt++) {
        geBatchAbortIfCancelled(batchExecToken);
        if (geClickSortTabByText(sortLabel)) {
          clicked = true;
          break;
        }
        await geSleepMaybeBatch(600, batchExecToken);
      }
      if (!clicked) {
        console.warn('[GE sort] 未点到排序：' + sortLabel + '（仍将尝试当前列表顺序抓取）');
      }

      await geWaitSortResultStable(sortLabel, 15000, batchExecToken);
      await geSleepMaybeBatch(400, batchExecToken);

      if (isNewestByCreateTime) {
        await geEnsureShelfTimeSortNewestFirst({
          timeoutMs: 12000,
          setStatus,
          prefixStatus: prefix,
          execToken: batchExecToken,
        });
      }

      /** 最近上架：全量有序快照前 N（与 DOM 种子/模糊匹配脱钩；批量须 visibleOnly:false + create_time patch） */
      if (sortLabel === '上架时间') {
        if (visibleOnly) {
          console.warn(
            '[最近上架] visibleOnly=true 仍会走全量快照前 N（与 false 一致）；批量请显式传 visibleOnly:false'
          );
        }
        if (setStatus) {
          setStatus(prefix + ' · 最近上架：create_time 降序全店加载后取快照前 ' + limit + ' 条…');
        }
        await geScrollBuyinUntilEnoughOrNoMore({
          setStatus,
          prefixStatus: prefix + ' · 最近上架',
          minRows: minBuf,
          execToken: batchExecToken,
        });
        const btnNewest = document.getElementById('goods-export-btn');
        await waitForBuyinDetailLinksIfNeeded(btnNewest, { execToken: batchExecToken });
        await geSleepMaybeBatch(600, batchExecToken);
        try {
          await geRefetchBuyinMaterialListForLinks(true);
        } catch (_) {}
        await geSleepMaybeBatch(400, batchExecToken);

        let snap = buildBuyinExportRowsSnapshot({ preserveRowOrder: true });
        snap = geDedupRowsByIdentity(snap);
        console.log(
          '[最近上架] create_time sort =',
          geBuyinNewestListRequestPatch && geBuyinNewestListRequestPatch.sort
        );
        console.log(
          '[最近上架] DOM首屏前5标题 =',
          scrapeBuyinDom()
            .slice(0, 5)
            .map(function (x) {
              return x.title;
            })
        );
        try {
          console.table(
            snap.slice(0, Math.min(limit + 5, snap.length)).map(function (r, i) {
              return {
                i: i,
                title: String((r && r.title) || '').slice(0, 40),
                price: (r && r.price) || '',
                sales: (r && r.sales) || '',
                hasLink: buyinProductLinkLooksValid((r && r.link) || ''),
                linkTail: String((r && r.link) || '').slice(-40),
                pid: (r && (r.product_id || r.promotion_id || r.commodity_id)) || '',
              };
            })
          );
        } catch (_) {}
        console.log(
          '[最近上架·快照前N预览] limit=' +
            limit +
            ' 快照总行数=' +
            snap.length +
            ' 前N条有效链=' +
            snap.slice(0, limit).filter(function (r) {
              return buyinProductLinkLooksValid((r && r.link) || '');
            }).length
        );
        return snap.slice(0, limit);
      }

      if (visibleOnly) {
        console.log(sortLabel + '排序完成，开始读取（如需补齐链接会滚动触发懒加载）');
        if (setStatus) setStatus(prefix + ' · ' + sortLabel + ' 读取中…');

        const cards0 = geGetVisibleBuyinCards();
        const seedRows = scrapeBuyinDom({ cards: cards0 });

        console.log('当前可视区域商品数：' + cards0.length);
        if (setStatus) setStatus(prefix + ' · ' + sortLabel + '（仅当前可视区域）共 ' + seedRows.length + ' 个');
        return seedRows;
      }

      if (setStatus) {
        setStatus(
          prefix +
            ' · 「' +
            sortLabel +
            '」' +
            (isNewestByCreateTime ? '（create_time 降序·新款在上）' : '') +
            ' 排序后下拉（目标≥' +
            limit +
            '）…'
        );
      }
      await geScrollBuyinUntilEnoughOrNoMore({
        setStatus,
        prefixStatus: prefix + ' · ' + sortLabel,
        minRows: minBuf,
        execToken: batchExecToken,
      });

      const btn = document.getElementById('goods-export-btn');
      await waitForBuyinDetailLinksIfNeeded(btn, { execToken: batchExecToken });
      await geSleepMaybeBatch(600, batchExecToken);

      const snap = buildBuyinExportRowsSnapshot({ preserveRowOrder: true });
      const ordered = geOrderBuyinRowsByDomCardOrder(snap);
      const pickFn = sortLabel === '销量' ? gePickTopSalesRows : gePickNewestRows;
      return pickFn(ordered, limit);
    } finally {
      geBuyinNewestListRequestPatch = null;
      if (restoreScrollHooks) {
        try {
          restoreScrollHooks();
        } catch (_) {}
      }
    }
  }

  /**
   * 反复滚到底并等待懒加载。到底须「底部文案 + 连续多轮高度/卡片/接口条数均无增长」等综合确认；无底部文案时仅多信号稳定则返回 false 继续采集。
   * @returns {Promise<boolean>} 是否在强确认下判定为列表底部
   */
  async function geScrollBuyinUntilNoMore(opts) {
    const setStatus = opts && opts.setStatus;
    const prefix = (opts && opts.prefixStatus) || '';
    const execToken = opts && opts.execToken;
    const maxSteps = 160;
    const pauseMs = 480;
    /** 连续 N 次滚动周期内 scrollHeight、卡片数、buyinData 条数均未增长，才参与「稳定」判断 */
    const STABLE_N = 6;
    const MIN_STEPS = 14;
    let lastH = -1;
    let lastCards = -1;
    let lastDataLen = -1;
    let stable = 0;

    for (let step = 0; step < maxSteps; step++) {
      if (execToken) geBatchAbortIfCancelled(execToken);
      const de = document.documentElement;
      const body = document.body;
      const h = Math.max(de ? de.scrollHeight : 0, body ? body.scrollHeight : 0, 0);
      window.scrollTo(0, h);
      try {
        const candidates = document.querySelectorAll('div,main,section,article');
        for (let i = 0; i < candidates.length; i++) {
          const el = candidates[i];
          const st = getComputedStyle(el);
          const oy = st.overflowY;
          if ((oy === 'auto' || oy === 'scroll') && el.scrollHeight > el.clientHeight + 60) {
            el.scrollTop = el.scrollHeight;
          }
        }
      } catch (_) {}
      await geSleepMaybeBatch(pauseMs, execToken);

      const h2 = Math.max(document.documentElement.scrollHeight, document.body.scrollHeight, 0);
      const cards = geCountBuyinDomProductCards();
      const dataLen = typeof buyinData !== 'undefined' && Array.isArray(buyinData) ? buyinData.length : 0;
      const marker = geBuyinDomShowsNoMoreMarker();

      if (lastH >= 0) {
        const grew = h2 > lastH + 2 || cards > lastCards + 2 || dataLen > lastDataLen;
        if (!grew) stable++;
        else stable = 0;
      } else stable = 0;

      lastH = h2;
      lastCards = cards;
      lastDataLen = dataLen;

      if (marker && stable >= 2 && step >= 2) {
        if (setStatus) setStatus(prefix + ' · 已触底（底部文案+连续无新增）');
        await geSleepMaybeBatch(280, execToken);
        return true;
      }

      if (stable >= STABLE_N && step >= MIN_STEPS) {
        if (marker) {
          if (setStatus) setStatus(prefix + ' · 已触底（多信号+底部提示）');
          await geSleepMaybeBatch(280, execToken);
          return true;
        }
        if (setStatus) setStatus(prefix + ' · 高度/卡片/接口暂稳（无底部文案，继续采集）');
        await geSleepMaybeBatch(300, execToken);
        return false;
      }

      if (setStatus && step % 6 === 0) {
        setStatus(prefix + ' · 下拉加载 ' + (step + 1) + '/' + maxSteps);
      }
    }
    if (setStatus) setStatus(prefix + ' · 已达最大滚动步数');
    return false;
  }

  function geUpdateBatchStatusLine() {
    const bState = document.getElementById('ge-batch-badge-state');
    const bTasks = document.getElementById('ge-batch-badge-tasks');
    const bBuf = document.getElementById('ge-batch-badge-buffer');
    if (!bState || !bTasks || !bBuf) return;
    const bs = loadBatchState();
    const runShops = bs.running ? loadBatchActiveOrEnabledShops() : [];
    const shopTotal =
      bs.running && runShops.length
        ? runShops.length
        : loadBatchShops().filter(function (s) {
            return s && s.enabled !== false;
          }).length;
    const bufN = loadBatchAccum().length;
    bBuf.textContent = '缓冲' + bufN;
    bBuf.className = 'ge-batch-badge ge-batch-badge--buffer';
    if (bs.running) {
      if (bs.phase === 'interval_wait') {
        bState.textContent = '追加等待';
        bState.className = 'ge-batch-badge ge-batch-badge--running';
      } else {
        bState.textContent = '运行中';
        bState.className = 'ge-batch-badge ge-batch-badge--running';
      }
      let megaSeg = '';
      if (runShops.length && bs.shopIdx >= 0 && bs.shopIdx < runShops.length) {
        const curShop = runShops[bs.shopIdx];
        const mtot = geShopTotalMegas(curShop);
        megaSeg = '·大' + (bs.megaIdx || 1) + '/' + mtot;
      }
      bTasks.textContent = bs.shopIdx + 1 + '/' + shopTotal + megaSeg + '·小' + bs.roundIdx + '轮';
      bTasks.className = 'ge-batch-badge ge-batch-badge--tasks';
    } else {
      bState.textContent = '未运行';
      bState.className = 'ge-batch-badge ge-batch-badge--idle';
      bTasks.textContent = '任务' + shopTotal;
      bTasks.className = 'ge-batch-badge ge-batch-badge--tasks';
    }
    if (bs.running) {
      if (!window.__geBatchUiTicker) geEnsureBatchLiveStatusTicker();
    } else {
      geStopBatchLiveStatusTicker();
      try {
        const rEl = document.getElementById('ge-batch-live-round');
        if (rEl) {
          rEl.textContent = '';
          rEl.style.display = 'none';
        }
      } catch (_) {}
    }
  }

  function geFormatMsAsCountdown(ms) {
    if (ms < 0) ms = 0;
    const sec = Math.floor(ms / 1000);
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = sec % 60;
    const p = function (n) {
      return (n < 10 ? '0' : '') + n;
    };
    return h + ':' + p(m) + ':' + p(s);
  }

  function geStopBatchLiveStatusTicker() {
    try {
      if (window.__geBatchUiTicker) {
        clearInterval(window.__geBatchUiTicker);
        window.__geBatchUiTicker = null;
      }
    } catch (_) {}
  }

  /** 浮动面板：轮次 + 追加巡检倒计时（每秒刷新，与 GM 状态同步） */
  function geRefreshBatchLiveRoundPanel() {
    const roundEl = document.getElementById('ge-batch-live-round');
    if (!roundEl) return;
    const bs = loadBatchState();
    if (!bs.running) {
      roundEl.style.display = 'none';
      roundEl.textContent = '';
      return;
    }
    roundEl.style.display = 'block';
    const runShops = loadBatchActiveOrEnabledShops();
    const shopTotal = runShops.length || 0;
    const idx = shopTotal ? Math.min(bs.shopIdx, shopTotal - 1) : 0;
    const cur = runShops[idx];
    const mtot = cur ? geShopTotalMegas(cur) : 1;
    const mega = bs.megaIdx || 1;
    const sr = bs.roundIdx || 1;
    const mt = cur ? Math.max(1, parseInt(cur.monitorTimes, 10) || 1) : 1;
    const lines = [];
    lines.push(
      '【本轮批量】店 ' + (idx + 1) + '/' + shopTotal + ' · 大轮 ' + mega + '/' + mtot + ' · 小轮 ' + sr + '/' + mt
    );
    if (bs.phase === 'interval_wait' && bs.intervalUntil > 0) {
      const msLeft = bs.intervalUntil - Date.now();
      lines.push('距下次巡检还有 ' + geFormatMsAsCountdown(msLeft));
      if (cur && cur.extraRoundsEnabled === true) {
        lines.push(
          '间隔 ' +
            (cur.extraRoundsIntervalHours != null ? String(cur.extraRoundsIntervalHours) : '') +
            ' 小时 · 第 ' +
            mega +
            ' 次巡本店'
        );
      }
    } else if (window.__geBatchBusy) {
      lines.push('正在下拉采集 / 写缓冲…');
    } else {
      lines.push('待命（将自动继续）');
    }
    roundEl.textContent = lines.join('\n');
  }

  function geEnsureBatchLiveStatusTicker() {
    if (window.__geBatchUiTicker) return;
    if (!loadBatchState().running) return;
    geRefreshBatchLiveRoundPanel();
    window.__geBatchUiTicker = setInterval(function () {
      if (!loadBatchState().running) {
        geStopBatchLiveStatusTicker();
        geRefreshBatchLiveRoundPanel();
        return;
      }
      geRefreshBatchLiveRoundPanel();
    }, 1000);
  }

  function geClearBatchAccumFromPanel() {
    if (!confirm('确定清除当前批量监控缓冲？（未导出或未写入飞书的数据将丢失）')) return;
    saveBatchAccum([]);
    geUpdateBatchStatusLine();
    alert('已清除缓冲');
  }

  async function geRunBuyinBatchRoundAsync() {
    if (!isBuyin()) return;
    let state = loadBatchState();
    if (!state.running) return;
    if (!String(state.execToken || '').trim()) {
      saveBatchState(Object.assign({}, state, { execToken: geGenBatchExecToken() }));
      state = loadBatchState();
    }
    const execToken = String(state.execToken || '');
    const shops = loadBatchActiveOrEnabledShops();
    if (!shops.length) {
      clearBatchRunSnapshot();
      saveBatchState({
        running: false,
        shopIdx: 0,
        roundIdx: 1,
        lastRunAt: Date.now(),
        phase: 'running',
        intervalUntil: 0,
        megaIdx: 1,
      });
      return;
    }
    const idx = Math.min(state.shopIdx, shops.length - 1);
    const shop = shops[idx];
    const targetUrl = String(shop.url || '').trim();
    if (!targetUrl) {
      clearBatchRunSnapshot();
      saveBatchState({
        running: false,
        shopIdx: 0,
        roundIdx: 1,
        lastRunAt: Date.now(),
        phase: 'running',
        intervalUntil: 0,
        megaIdx: 1,
      });
      return;
    }
    const here = location.href;
    const setStatus = function (t) {
      const live = document.getElementById('ge-batch-live-status');
      if (!live) return;
      const s = t != null ? String(t) : '';
      live.textContent = s;
      live.style.display = s ? 'block' : 'none';
    };

    /** 追加巡检：间隔未到则定时刷新，状态存 GM，跳页/刷新不丢 */
    if (state.phase === 'interval_wait') {
      if (geBatchIsCancelled(execToken)) return;
      if (!geUrlsSameBuyinShop(here, targetUrl)) {
        if (!geBatchIsCancelled(execToken)) location.href = targetUrl;
        return;
      }
      if (Date.now() < state.intervalUntil) {
        const msLeft = state.intervalUntil - Date.now();
        setStatus('追加巡检等待中（约 ' + Math.max(1, Math.ceil(msLeft / 60000)) + ' 分钟后继续）…');
        try {
          if (window.__geBatchIntervalReloadTimer) clearTimeout(window.__geBatchIntervalReloadTimer);
        } catch (_) {}
        window.__geBatchIntervalReloadTimer = setTimeout(function () {
          window.__geBatchIntervalReloadTimer = null;
          if (geBatchIsCancelled(execToken)) return;
          try {
            location.reload();
          } catch (_) {}
        }, Math.min(msLeft + 800, 2147483647));
        return;
      }
      if (geBatchIsCancelled(execToken)) return;
      saveBatchState({
        running: true,
        shopIdx: idx,
        roundIdx: 1,
        megaIdx: Math.max(1, parseInt(state.megaIdx, 10) || 1),
        phase: 'running',
        intervalUntil: 0,
        lastRunAt: Date.now(),
      });
      location.reload();
      return;
    }

    if (!geUrlsSameBuyinShop(here, targetUrl)) {
      if (!geBatchIsCancelled(execToken)) location.href = targetUrl;
      return;
    }
    const mt = Math.max(1, parseInt(shop.monitorTimes, 10) || 1);
    const round = Math.min(Math.max(1, state.roundIdx), mt);
    const megaIdx = Math.max(1, parseInt(state.megaIdx, 10) || 1);
    const totalMegas = geShopTotalMegas(shop);
    const tag = String(shop.shopTag || '').trim();
    try {
      try {
        window.__geBatchBusy = true;
      } catch (_) {}
      // 每店独立页面文档（跳转/刷新后重新加载）；采集仅依赖本页与 GM 任务状态，不依赖上一家店铺的页面级副作用。
      const roundPrefix =
        '监控中 店' +
        (idx + 1) +
        '/' +
        shops.length +
        ' 大' +
        megaIdx +
        '/' +
        totalMegas +
        ' 小' +
        round +
        '/' +
        mt;
      setStatus(roundPrefix + ' · 页面就绪…');
      await geBatchSleep(1200, execToken);
      if (geBatchIsCancelled(execToken)) return;
      const batchId = geGenWriteBatchId();
      const taskNorm = geMigrateShopTask(shop);
      const prefsMid = loadExportPanelPrefs();
      let canImmediateFeishuWrite = false;
      let cfgMForImmediate = null;
      if (prefsMid.batchAutoFeishuOnComplete && round >= mt) {
        try {
          const cfgTmp = loadFeishuConfig();
          const hasM =
            (cfgTmp.accessToken || '').trim() ||
            ((cfgTmp.feishuAppId || '').trim() && (cfgTmp.feishuAppSecret || '').trim());
          const okWiki = (cfgTmp.wikiNodeToken || '').trim();
          const okAppToken = (cfgTmp.appToken || '').trim();
          if (cfgTmp.tableId && hasM && (okWiki || okAppToken)) {
            cfgMForImmediate = cfgTmp;
            canImmediateFeishuWrite = true;
          }
        } catch (_) {}
      }
      let rows = [];
      let rowsNewestRaw = [];
      let rowsTopSalesRaw = [];
      let beforeRecordRules = 0;

      const cloneRows = function (arr) {
        if (!arr || !arr.length) return [];
        if (typeof structuredClone === 'function') return structuredClone(arr);
        return arr.map(function (r) {
          return Object.assign({}, r);
        });
      };
      const normalizeForRecordType = function (rawRows, recordType) {
        if (!rawRows || !rawRows.length) return [];
        return rawRows.map(function (r) {
          return normalizeBuyinRowDefaults({
            ...r,
            shopTag: tag,
            monitorTimes: String(mt),
            monitorIndex: String(round),
            inspectCycle: String(megaIdx),
            recordType,
            _geWriteBatchId: batchId,
            _geShopIdx: idx,
            _geCycleIdx: megaIdx,
          });
        });
      };

      if (taskNorm.recordAll) {
        setStatus(roundPrefix + ' · 全店抓取：滚动加载尽可能完整…');
        await geScrollBuyinUntilNoMore({ setStatus, prefixStatus: roundPrefix, execToken: execToken });
        if (geBatchIsCancelled(execToken)) return;
        setStatus(roundPrefix + ' · 全店抓取：等待商品链接…');
        const btnAll = document.getElementById('goods-export-btn');
        await waitForBuyinDetailLinksIfNeeded(btnAll, { execToken: execToken });
        await geBatchSleep(1500, execToken);
        if (geBatchIsCancelled(execToken)) return;
        rows = buildBuyinExportRowsSnapshot();
        beforeRecordRules += rows.length;
        rows = geMergeRecordRuleRows(rows, taskNorm);
      }

      if (taskNorm.recordNewest) {
        setStatus(
          roundPrefix +
            ' · 最近上架：按配置抓取 ' +
            taskNorm.newestLimit +
            ' 个（上架时间最新在前）…'
        );
        const newestRows = cloneRows(
          await geCollectTopNRowsUnderSort({
            sortLabel: '上架时间',
            limit: taskNorm.newestLimit,
            setStatus,
            prefixStatus: roundPrefix,
            treatAsCreateTimeSort: true,
            visibleOnly: false,
            noScroll: false,
            createTimeSort: GE_BUYIN_CREATE_TIME_SORT_DESC,
            execToken: execToken,
          })
        );
        beforeRecordRules += newestRows.length;
        if (geBatchIsCancelled(execToken)) return;
        if (canImmediateFeishuWrite && cfgMForImmediate && newestRows.length) {
          const newestNorm = normalizeForRecordType(newestRows, '最近上架');
          try {
            geBatchAbortIfCancelled(execToken);
            const accN = loadBatchAccum();
            accN.push.apply(accN, newestNorm);
            saveBatchAccum(accN);
            geUpdateBatchStatusLine();
            await geFeishuUploadGroupAndClearBuffer(newestNorm, cfgMForImmediate, {
              groupKey: '最近上架',
              scope: 'immediate_small_round',
            });
            console.log('[导出] 已立即写入飞书「最近上架」' + newestNorm.length + ' 条并已清理缓冲');
          } catch (e) {
            if (e && e.code === 'GE_BATCH_CANCELLED') {
              console.log('[飞书写入·分组]', {
                phase: 'cancelled_immediate_pipeline',
                group_key: '最近上架',
                note: '可能在入缓冲后、上传前停止；缓冲已保留',
              });
            } else {
              console.warn('[导出] 写入飞书「最近上架」失败（已保留缓冲可重试）', e);
            }
          }
        } else {
          rowsNewestRaw.push.apply(
            rowsNewestRaw,
            newestRows.map(function (r) {
              return Object.assign({}, r, { recordType: '最近上架' });
            })
          );
        }
      }

      if (taskNorm.recordTopSales) {
        setStatus(
          roundPrefix +
            ' · 销量Top：按配置抓取 ' +
            taskNorm.topSalesLimit +
            ' 个（全店加载后按销量）…'
        );
        const topSalesRows = cloneRows(
          await geCollectTopNRowsUnderSort({
            sortLabel: '销量',
            limit: taskNorm.topSalesLimit,
            setStatus,
            prefixStatus: roundPrefix,
            execToken: execToken,
          })
        );
        beforeRecordRules += topSalesRows.length;
        if (geBatchIsCancelled(execToken)) return;
        if (canImmediateFeishuWrite && cfgMForImmediate && topSalesRows.length) {
          const topNorm = normalizeForRecordType(topSalesRows, '销量Top');
          try {
            geBatchAbortIfCancelled(execToken);
            const accT = loadBatchAccum();
            accT.push.apply(accT, topNorm);
            saveBatchAccum(accT);
            geUpdateBatchStatusLine();
            await geFeishuUploadGroupAndClearBuffer(topNorm, cfgMForImmediate, {
              groupKey: '销量Top',
              scope: 'immediate_small_round',
            });
            console.log('[导出] 已立即写入飞书「销量Top」' + topNorm.length + ' 条并已清理缓冲');
          } catch (e) {
            if (e && e.code === 'GE_BATCH_CANCELLED') {
              console.log('[飞书写入·分组]', {
                phase: 'cancelled_immediate_pipeline',
                group_key: '销量Top',
                note: '可能在入缓冲后、上传前停止；缓冲已保留',
              });
            } else {
              console.warn('[导出] 写入飞书「销量Top」失败（已保留缓冲可重试）', e);
            }
          }
        } else {
          rowsTopSalesRaw.push.apply(
            rowsTopSalesRaw,
            topSalesRows.map(function (r) {
              return Object.assign({}, r, { recordType: '销量Top' });
            })
          );
        }
      }

      const rawRowsCount =
        rows.length +
        (rowsNewestRaw ? rowsNewestRaw.length : 0) +
        (rowsTopSalesRaw ? rowsTopSalesRaw.length : 0);
      if (
        !canImmediateFeishuWrite &&
        rawRowsCount === 0 &&
        (taskNorm.recordAll || taskNorm.recordNewest || taskNorm.recordTopSales)
      ) {
        console.warn(
          '[导出] 本店本轮无有效商品写入缓冲（请检查全店/最近上架/销量Top 是否勾选、列表是否加载、排序是否可点）。'
        );
      }
      const toNormalizedRow = function (r) {
        return normalizeBuyinRowDefaults({
          ...r,
          shopTag: tag,
          monitorTimes: String(mt),
          monitorIndex: String(round),
          inspectCycle: String(megaIdx),
          recordType: r.recordType != null && String(r.recordType).trim() ? String(r.recordType).trim() : '全店录入',
          _geWriteBatchId: batchId,
          _geShopIdx: idx,
          _geCycleIdx: megaIdx,
        });
      };

      const accum = loadBatchAccum();
      let addedCount = 0;
      if (taskNorm.recordAll && rows.length) {
        const fullNorm = rows.map(toNormalizedRow);
        accum.push.apply(accum, fullNorm);
        addedCount += fullNorm.length;
      }
      if (rowsNewestRaw.length) {
        const rn = rowsNewestRaw.map(toNormalizedRow);
        accum.push.apply(accum, rn);
        addedCount += rn.length;
      }
      if (rowsTopSalesRaw.length) {
        const rs = rowsTopSalesRaw.map(toNormalizedRow);
        accum.push.apply(accum, rs);
        addedCount += rs.length;
      }
      saveBatchAccum(accum);
      geUpdateBatchStatusLine();
      console.log('[导出] 批量缓冲 +' + addedCount + ' 合计 ' + accum.length);

      if (round < mt) {
        if (geBatchIsCancelled(execToken)) return;
        saveBatchState({
          running: true,
          shopIdx: idx,
          roundIdx: round + 1,
          megaIdx,
          phase: 'running',
          intervalUntil: 0,
          lastRunAt: Date.now(),
        });
        await geBatchSleep(600, execToken);
        if (geBatchIsCancelled(execToken)) return;
        location.reload();
        return;
      }

      /** 一小轮（monitorTimes）结束：可选按本大轮切片自动写飞书并剔除缓冲，避免定时多轮重复上传 */
      if (prefsMid.batchAutoFeishuOnComplete) {
        try {
          if (!geBatchIsCancelled(execToken)) {
            const cfgM = loadFeishuConfig();
            const hasM =
              (cfgM.accessToken || '').trim() ||
              ((cfgM.feishuAppId || '').trim() && (cfgM.feishuAppSecret || '').trim());
            if (cfgM.tableId && hasM && ((cfgM.wikiNodeToken || '').trim() || (cfgM.appToken || '').trim())) {
              const accM = loadBatchAccum();
              const sliceM = accM.filter(function (r) {
                return r && r._geShopIdx === idx && r._geCycleIdx === megaIdx;
              });
              if (sliceM.length) {
                const groups = {};
                for (let i = 0; i < sliceM.length; i++) {
                  const r = sliceM[i] || {};
                  const k =
                    r.recordType != null && String(r.recordType).trim()
                      ? String(r.recordType).trim()
                      : '全店录入';
                  if (!groups[k]) groups[k] = [];
                  groups[k].push(r);
                }
                const keys = Object.keys(groups);
                for (let j = 0; j < keys.length; j++) {
                  if (geBatchIsCancelled(execToken)) {
                    console.log('[飞书写入·分组]', {
                      phase: 'cancelled_before_group',
                      group_key: keys[j],
                      scope: 'slice_end_round',
                    });
                    break;
                  }
                  const k = keys[j];
                  const gRows = groups[k];
                  if (!gRows || !gRows.length) continue;
                  try {
                    const upM = await geFeishuUploadGroupAndClearBuffer(gRows, cfgM, {
                      groupKey: k,
                      scope: 'slice_end_round',
                    });
                    console.log(
                      '[导出] 本大轮自动写飞书（按录入类型）' + k + ' ' + upM.total + ' 条并已清理对应缓冲'
                    );
                  } catch (upErr) {
                    console.warn('[导出] 本大轮自动写飞书接口失败（已保留缓冲可重试）', upErr);
                    break;
                  }
                }
              }
            }
          }
        } catch (errMid) {
          if (errMid && errMid.code === 'GE_BATCH_CANCELLED') {
            console.log('[飞书写入·分组]', { phase: 'slice_round_cancelled_outer', scope: 'slice_end_round' });
          } else {
            console.warn('[导出] 本大轮自动写飞书异常（已保留缓冲可重试）', errMid);
          }
        }
      }

      if (megaIdx < totalMegas) {
        if (geBatchIsCancelled(execToken)) return;
        const hrs = Math.max(0.1, parseFloat(shop.extraRoundsIntervalHours) || 1);
        const msWait = Math.round(hrs * 3600000);
        saveBatchState({
          running: true,
          shopIdx: idx,
          roundIdx: 1,
          megaIdx: megaIdx + 1,
          phase: 'interval_wait',
          intervalUntil: Date.now() + msWait,
          lastRunAt: Date.now(),
        });
        setStatus(
          '本店第 ' + megaIdx + ' 大轮已完成；约 ' + Math.round(hrs * 10) / 10 + ' 小时后第 ' + (megaIdx + 1) + ' 大轮…'
        );
        geUpdateBatchStatusLine();
        try {
          if (window.__geBatchMegaReloadTimer) clearTimeout(window.__geBatchMegaReloadTimer);
        } catch (_) {}
        window.__geBatchMegaReloadTimer = setTimeout(function () {
          window.__geBatchMegaReloadTimer = null;
          if (geBatchIsCancelled(execToken)) return;
          try {
            location.reload();
          } catch (_) {}
        }, Math.min(msWait + 800, 2147483647));
        return;
      }

      if (idx + 1 < shops.length) {
        if (geBatchIsCancelled(execToken)) return;
        saveBatchState({
          running: true,
          shopIdx: idx + 1,
          roundIdx: 1,
          megaIdx: 1,
          phase: 'running',
          intervalUntil: 0,
          lastRunAt: Date.now(),
        });
        await geBatchSleep(600, execToken);
        if (geBatchIsCancelled(execToken)) return;
        location.href = String(shops[idx + 1].url || '').trim();
        return;
      }
      /** 全部店采集完成：先收尾写飞书，再 GM 落 running=false（否则 geBatchIsCancelled 会把正常收官误判为已取消而跳过上传） */
      setStatus('批量已完成');
      geUpdateBatchStatusLine();
      const prefs = loadExportPanelPrefs();
      let tail = '';
      const leftover = loadBatchAccum();
      if (prefs.batchAutoFeishuOnComplete) {
        try {
          const cfg = loadFeishuConfig();
          const hasAccess =
            (cfg.accessToken || '').trim() ||
            ((cfg.feishuAppId || '').trim() && (cfg.feishuAppSecret || '').trim());
          if (!cfg.tableId || !hasAccess || (!cfg.wikiNodeToken && !cfg.appToken)) {
            tail = '\n\n已勾选「自动写入飞书」，但飞书配置不完整，请点面板 ⚙ 打开配置后，再手动点「写飞书」。';
          } else if (!leftover.length) {
            tail = '\n\n缓冲已空（可能已在各轮自动写入并清理）。';
          } else if (geBatchIsCancelled(execToken)) {
            tail =
              '\n\n已停止监控：未执行收尾自动写飞书（飞书侧无本段新写入；缓冲仍保留，可手动「写飞书」）。';
          } else {
            const groups = {};
            for (let i = 0; i < leftover.length; i++) {
              const r = leftover[i] || {};
              const k =
                r.recordType != null && String(r.recordType).trim()
                  ? String(r.recordType).trim()
                  : '全店录入';
              if (!groups[k]) groups[k] = [];
              groups[k].push(r);
            }
            let totalAll = 0;
            let columnsCreatedAll = 0;
            let stoppedMidGroups = false;
            let apiErrorMsg = '';
            const keys = Object.keys(groups);
            for (let j = 0; j < keys.length; j++) {
              if (geBatchIsCancelled(execToken)) {
                stoppedMidGroups = true;
                console.log('[飞书写入·分组]', {
                  phase: 'cancelled_before_group',
                  group_key: keys[j],
                  scope: 'batch_all_done',
                  rows_committed_so_far: totalAll,
                });
                break;
              }
              const k = keys[j];
              const gRows = groups[k];
              if (!gRows || !gRows.length) continue;
              try {
                const up = await geFeishuUploadGroupAndClearBuffer(gRows, cfg, {
                  groupKey: k,
                  scope: 'batch_all_done',
                });
                totalAll += up.total;
                if (up.columnsCreated > 0) columnsCreatedAll += up.columnsCreated;
              } catch (upErr) {
                apiErrorMsg = upErr && upErr.message ? String(upErr.message) : String(upErr);
                console.warn('[飞书写入·分组]', {
                  phase: 'upload_failed',
                  group_key: k,
                  scope: 'batch_all_done',
                  err: apiErrorMsg,
                });
                break;
              }
            }
            const ex = columnsCreatedAll > 0 ? '，新建列 ' + columnsCreatedAll + ' 个' : '';
            if (apiErrorMsg) {
              tail =
                '\n\n自动写入飞书失败（飞书接口）：' +
                apiErrorMsg +
                '\n已成功写入的组已清理缓冲；失败组仍保留在缓冲，可手动点「写飞书」重试。';
            } else if (stoppedMidGroups && totalAll > 0) {
              tail =
                '\n\n已写入飞书 ' +
                totalAll +
                ' 条' +
                ex +
                '，对应缓冲已清理；停止监控后剩余分组未上传（非写入失败）。';
            } else if (stoppedMidGroups && totalAll === 0) {
              tail = '\n\n已停止监控：收尾写飞书未执行任何分组（缓冲仍保留，可手动上传）。';
            } else {
              tail = '\n\n已自动写入飞书 ' + totalAll + ' 条' + ex + '，并已清理对应缓冲。';
            }
          }
        } catch (err) {
          if (err && err.code === 'GE_BATCH_CANCELLED') {
            tail =
              '\n\n已停止监控：收尾写飞书流程中断（若控制台已有 upload_succeeded_buffer_removed，则该组已成功且已清缓冲，并非写入失败）。';
          } else {
            tail =
              '\n\n自动写入飞书失败：' +
              (err && err.message ? err.message : err) +
              '\n可手动点击「写飞书」重试。';
          }
        }
      } else {
        tail = '\n\n可在确认无误后点击「写飞书」或「导出 CSV」。';
      }
      alert('批量监控结束。缓冲剩余 ' + loadBatchAccum().length + ' 条。' + tail);
      clearBatchRunSnapshot();
      saveBatchState({
        running: false,
        shopIdx: 0,
        roundIdx: 1,
        megaIdx: 1,
        phase: 'running',
        intervalUntil: 0,
        lastRunAt: Date.now(),
      });
      try {
        geUpdateBatchStatusLine();
      } catch (_) {}
    } catch (e) {
      if (e && e.code === 'GE_BATCH_CANCELLED') {
        setStatus('已停止监控（执行链已中断）');
        console.log('[导出] 批量监控已由用户停止');
      } else {
        console.error(e);
        setStatus('本轮异常（见控制台）');
      }
    } finally {
      try {
        window.__geBatchBusy = false;
      } catch (_) {}
      try {
        geRefreshBatchLiveRoundPanel();
      } catch (_) {}
    }
  }

  function geScheduleBuyinBatchResume() {
    if (!isBuyin()) return;
    if (!loadBatchState().running) return;
    if (window.__geBatchScheduled) return;
    window.__geBatchScheduled = true;
    const resumeTok = String(loadBatchState().execToken || '');
    try {
      if (window.__geBatchResumeTimer) clearTimeout(window.__geBatchResumeTimer);
    } catch (_) {}
    window.__geBatchResumeTimer = setTimeout(function () {
      window.__geBatchResumeTimer = null;
      window.__geBatchScheduled = false;
      if (geBatchIsCancelled(resumeTok)) return;
      geRunBuyinBatchRoundAsync().catch(function (err) {
        console.error(err);
      });
    }, 11000);
  }

  async function doExportFeishu() {
    const cfg = loadFeishuConfig();
    const hasAccess =
      (cfg.accessToken || '').trim() ||
      ((cfg.feishuAppId || '').trim() && (cfg.feishuAppSecret || '').trim());
    if (!cfg.tableId || !hasAccess || (!cfg.wikiNodeToken && !cfg.appToken)) {
      showFeishuSettingsModal();
      alert(
        '请填写：table_id；「Wiki 节点 token」或「app_token」其一；access_token 与「App ID + App Secret」至少填一种（与其它 RPA 一致可只填 ID+Secret）。'
      );
      return;
    }
    const btn = document.getElementById('goods-export-feishu-btn');
    await waitForBuyinDetailLinksIfNeeded(btn);
    const p = collectExportPayload();
    if (!p) return;
    if (btn) {
      btn.disabled = true;
      btn.textContent = (cfg.wikiNodeToken || '').trim() ? '解析 Wiki 并写入…' : '写入中…';
    }
    try {
      const up = await uploadRowsToFeishu(p.data, cfg);
      const extra = up.columnsCreated > 0 ? '，并新建缺失列 ' + up.columnsCreated + ' 个' : '';
      alert('已写入飞书多维表格 ' + up.total + ' 条（列名按配置映射）' + extra);
    } finally {
      if (btn) {
        btn.disabled = false;
        btn.textContent = '写入飞书表格';
      }
    }
  }

  /** 仅根据当前映射在飞书子表上补齐缺失列，不写入数据（须开启「写入前自动建列」逻辑等同，此函数会强制执行一次建列） */
  async function doFeishuSyncColumnsOnly() {
    const cfg = loadFeishuConfig();
    const hasAccess =
      (cfg.accessToken || '').trim() ||
      ((cfg.feishuAppId || '').trim() && (cfg.feishuAppSecret || '').trim());
    if (!cfg.tableId || !hasAccess || (!cfg.wikiNodeToken && !cfg.appToken)) {
      showFeishuSettingsModal();
      alert('请先完成飞书配置。');
      return;
    }
    try {
      const cfgTok = await ensureFeishuAccessToken(cfg);
      const bitableAppToken = await resolveBitableAppToken(cfgTok);
      if (!bitableAppToken) throw new Error('请填写「Wiki 节点 token」或「app_token」其一');
      const forceCfg = { ...cfgTok, feishuAutoCreateFields: true };
      const r = await ensureFeishuBitableMissingColumns(bitableAppToken, cfgTok.tableId, forceCfg);
      alert(
        r.created > 0
          ? '已新建 ' + r.created + ' 个缺失列（其余列已存在或与映射同名）。'
          : '映射中的列均已存在，未新建。'
      );
    } catch (e) {
      alert((e && e.message) || String(e));
    }
  }

  function downloadCsv(data, prefix) {
    if (!data || !data.length) return;
    const esc = (v) => String(v || '').replace(/\r?\n/g, ' ').replace(/"/g, '""').trim();
    const col = (k) => data.some((x) => x[k] != null && String(x[k]).trim() !== '');
    const hasShop = col('shopName') || col('shopLink');
    const parts = [
      { h: '标题', k: 'title' },
      { h: '价格', k: 'price' },
      { h: '佣金', k: 'commission' },
      { h: '销量', k: 'sales' },
      { h: '商品链接', k: 'link' },
      { h: '图片', k: 'imgSrc' },
    ];
    if (hasShop) {
      parts.push({ h: '店铺名称', k: 'shopName' }, { h: '店铺链接', k: 'shopLink' });
    }
    if (col('platform')) parts.push({ h: '平台', k: 'platform' });
    if (col('shopTag')) parts.push({ h: '店铺标记', k: 'shopTag' });
    if (col('monitorTimes')) parts.push({ h: '监控次数', k: 'monitorTimes' });
    if (col('monitorIndex')) parts.push({ h: '监控轮次', k: 'monitorIndex' });
    if (col('inspectCycle')) parts.push({ h: '巡检周期', k: 'inspectCycle' });
    if (col('shelfTime')) parts.push({ h: '上架时间', k: 'shelfTime' });
    if (col('recordType')) parts.push({ h: '录入类型', k: 'recordType' });
    if (col('guarantee')) parts.push({ h: '保障', k: 'guarantee' });
    if (col('deliveryTime')) parts.push({ h: '发货时效', k: 'deliveryTime' });
    const headers = parts.map((p) => p.h).join(',');
    const csv =
      '\uFEFF' +
      headers +
      '\n' +
      data
        .map((x) => {
          const row = parts.map((p) => esc(x[p.k]));
          return row.map((v) => '"' + v + '"').join(',');
        })
        .join('\n');
    const a = document.createElement('a');
    a.href = 'data:text/csv;charset=utf-8,' + encodeURIComponent(csv);
    a.download = (prefix || '商品') + '_' + Date.now() + '.csv';
    a.click();
  }

  let buyinData = [];
  /**
   * 仅「录新品」排序抓取期间非 null：fetch/XHR 在 material_list 等 POST 上合并 order_by=create_time、sort=降序(新款在上)、scene、size。
   * 不写商品级上架时间；新品 = 该排序下列表前 N 条（N=任务行「新品数」），不依 row.shelfTime / create_time 字段解析。
   */
  let geBuyinNewestListRequestPatch = null;
  let taobaoData = [];

  function normalizeBuyinHref(u) {
    if (u == null || u === '') return '';
    let s = String(u).trim();
    if (!s) return '';
    if (/^https?:\/\//i.test(s)) return s;
    if (s.startsWith('//')) return 'https:' + s;
    if (s.startsWith('/')) {
      try {
        return new URL(s, 'https://buyin.jinritemai.com').href;
      } catch (_) {
        return 'https://buyin.jinritemai.com' + s;
      }
    }
    return s;
  }

  /**
   * 飞书「图片」列无法识别带 ~tplv-...q75.image 的百应 CDN 模板后缀；与「全店录入」可用的 /obj/ecom-shop-material/ 形式对齐。
   */
  function geNormalizeBuyinImgSrcForFeishu(u) {
    let s = String(u || '').trim();
    if (!s) return '';
    if (!/ecombdimg\.com/i.test(s)) return s;
    const til = s.indexOf('~');
    if (til >= 0) s = s.slice(0, til);
    if (/\/ecom-shop-material\//i.test(s) && !/\/obj\/ecom-shop-material\//i.test(s)) {
      s = s.replace(/\/ecom-shop-material\//i, '/obj/ecom-shop-material/');
    }
    return s;
  }

  function pickShopFromObject(o) {
    if (!o || typeof o !== 'object') return { shopName: '', shopLink: '' };
    const nameFields = [
      o.shop_name,
      o.shopName,
      o.nickname,
      o.author_name,
      o.name,
      o.store_name,
      o.brand_name,
      o.title,
    ];
    let name = '';
    for (const v of nameFields) {
      if (v == null || v === '') continue;
      const s = String(v).trim();
      if (s) {
        name = s;
        break;
      }
    }
    const linkFields = [
      o.shop_url,
      o.shopUrl,
      o.store_url,
      o.jump_url,
      o.web_url,
      o.share_url,
      o.detail_url,
      o.schema_url,
      o.url,
      o.link,
      o.href,
    ];
    let linkRaw = '';
    for (const v of linkFields) {
      if (v == null || v === '') continue;
      linkRaw = v;
      break;
    }
    let shopLink = normalizeBuyinHref(linkRaw);
    const sec = o.sec_uid || o.sec_user_id;
    if (!shopLink && sec) shopLink = 'https://www.douyin.com/user/' + String(sec);
    return { shopName: name, shopLink };
  }

  /** 从百应 summary_promotions 单条中尽量解析店铺名、店铺链接（字段名随接口版本可能变化） */
  function extractBuyinShopFromPromotion(p) {
    if (!p || typeof p !== 'object') return { shopName: '', shopLink: '' };
    const b = p.base_model || {};
    const pi = b.product_info || {};
    const promo = b.promotion_info || {};
    let shopName = '';
    let shopLink = '';

    const merge = (n, l) => {
      if (n && !shopName) shopName = String(n).trim();
      if (l && !shopLink) shopLink = normalizeBuyinHref(l);
    };

    const tryObjs = [
      p.shop_info,
      p.author_info,
      p.anchor_info,
      b.shop_info,
      b.shop,
      b.author_info,
      b.anchor_info,
      b.store_info,
      b.promotion_account,
      b.promotion_author,
      b.kol_info,
      b.seller_info,
      pi.shop_info,
      pi.shop,
      pi.author_info,
      pi.store_info,
      pi.promotion_author,
      pi.seller_info,
      promo.shop_info,
      promo.author_info,
      promo.anchor_info,
      b.marketing_info && b.marketing_info.shop_info,
    ];
    for (const o of tryObjs) {
      if (!o || typeof o !== 'object') continue;
      const { shopName: n, shopLink: l } = pickShopFromObject(o);
      merge(n, l);
    }

    merge(pi.shop_name || b.shop_name || p.shop_name, pi.shop_url || b.shop_url || p.shop_url);

    if (!shopName || !shopLink) {
      const keyHints = ['shop', 'author', 'anchor', 'store', 'kol', 'seller', 'brand', 'promoter'];
      const scan = (o, depth) => {
        if (!o || typeof o !== 'object' || depth > 5) return;
        for (const [k, v] of Object.entries(o)) {
          const kl = k.toLowerCase();
          if (!v || typeof v !== 'object' || Array.isArray(v)) continue;
          if (!keyHints.some((h) => kl.includes(h))) continue;
          const { shopName: n, shopLink: l } = pickShopFromObject(v);
          merge(n, l);
          scan(v, depth + 1);
        }
      };
      scan(p, 0);
      scan(b, 0);
    }

    return { shopName, shopLink };
  }

  function buyinRowDedupeKey(x) {
    const l = x.link != null ? String(x.link).trim() : '';
    const tag = (x.shopTag || '').trim();
    const mi = x.monitorIndex != null && x.monitorIndex !== '' ? String(x.monitorIndex) : '';
    const sfx = tag || mi ? '\u0002' + tag + '\u0002' + mi : '';
    if (l) return 'l:' + l + sfx;
    const t = x.title != null ? String(x.title).trim() : '';
    const p = x.price != null ? String(x.price).trim() : '';
    if (t || p) return 't:' + t + '|' + p + sfx;
    return '';
  }

  /** 验证码/素材文件名等，勿当商品标题写入飞书 */
  function isBuyinGarbageTitle(t) {
    const s = (t || '').trim();
    if (!s) return false;
    if (/captcha|验证码|seccode|滑动验证|安全验证/i.test(s)) return true;
    if (/\.(png|jpe?g|gif|webp|zip|svg|bmp)(\?|#|$|\s)/i.test(s)) return true;
    if (/^image\s*\(\d+\)/i.test(s)) return true;
    if (/^image\s*\d+\.(png|jpe?g|gif)$/i.test(s)) return true;
    if (/^(左|中|右)(\.png)?$/i.test(s)) return true;
    if (s.length <= 48 && /^[\w\-.\s()（）]+\.(png|jpe?g|gif|zip|webp)$/i.test(s)) return true;
    return false;
  }

  /** 非商品详情：百应控制台、罗盘等（勿写入「商品链接」列） */
  function isBuyinNonProductPageUrl(u) {
    const s = String(u || '').trim();
    if (!s) return false;
    if (/compass\.jinritemai\.com/i.test(s)) return true;
    if (/buyin\.jinritemai\.com/i.test(s)) return true;
    if (/oceanengine\.com|qianchuan\.com/i.test(s)) return true;
    return false;
  }

  /** 图片/静态资源链接：绝不能当作「商品链接」写入 */
  function isBuyinImageAssetUrl(u) {
    const s = String(u || '').trim();
    if (!s) return false;
    // ecombdimg 基本均为素材/图片/CDN 资源，不是商品详情链
    if (/ecombdimg\.com/i.test(s)) return true;
    // 常见：字节电商图片 CDN 的 .image 资源；也可能是常规图片后缀
    if (/(?:\\.image)(?:\\?|#|$)/i.test(s)) return true;
    if (/\\.(png|jpe?g|gif|webp|svg|bmp)(\\?|#|$)/i.test(s)) return true;
    // 兜底：明显是 data/blob 资源
    if (/^(data:|blob:)/i.test(s)) return true;
    return false;
  }

  /**
   * 最近上架阶段①②：由「上架时间·最新在前」下列表 DOM 前 N 条构造多字段种子（匹配键，不要求链接已齐）。
   * 锁定字段：title / price / commission / sales / imgSrc；顺序与「前 N 条」一致；固定 N 槽，缺 DOM 时空占位。
   */
  function geBuildNewestListingTitleSeeds(domRows, limit) {
    const n = Math.max(1, parseInt(limit, 10) || 1);
    const slice = Array.isArray(domRows) ? domRows.slice(0, n) : [];
    let ctx = { shopName: '', shopLink: '' };
    try {
      ctx = getBuyinPageShopContext();
    } catch (_) {}
    const out = [];
    for (let i = 0; i < n; i++) {
      if (i < slice.length) {
        const r = slice[i] || {};
        out.push(
          normalizeBuyinRowDefaults({
            title: String(r.title != null ? r.title : '').trim(),
            price: String(r.price != null ? r.price : '').trim(),
            commission: String(r.commission != null ? r.commission : '').trim(),
            sales: String(r.sales != null ? r.sales : '').trim(),
            imgSrc: String(r.imgSrc != null ? r.imgSrc : '').trim(),
            link: '',
            shopName: (r.shopName && String(r.shopName).trim()) || ctx.shopName,
            shopLink: (r.shopLink && String(r.shopLink).trim()) || ctx.shopLink,
            shopTag: String(r.shopTag != null ? r.shopTag : '').trim(),
            _geNewestSeedIndex: i,
          })
        );
      } else {
        out.push(
          normalizeBuyinRowDefaults({
            title: '',
            price: '',
            commission: '',
            sales: '',
            link: '',
            imgSrc: '',
            shopName: ctx.shopName,
            shopLink: ctx.shopLink,
            shopTag: '',
            _geNewestSeedIndex: i,
          })
        );
      }
    }
    return out;
  }

  function geNewestSeedTitleNormForMatch(t) {
    return String(t || '')
      .replace(/\s+/g, ' ')
      .replace(/[\u200b-\u200f\ufeff]/g, '')
      .trim()
      .toLowerCase()
      .replace(/[^\u4e00-\u9fa5a-z0-9]+/g, '')
      .slice(0, 72);
  }

  /** 最近上架 seed 是否仍缺商品链或有效主图（仅用于 DOM 定向补齐阶段） */
  function geBuyinNewestSeedNeedsDomMediaFill(row) {
    const r = row || {};
    const needLink = !buyinProductLinkLooksValid(r.link);
    const im = String(r.imgSrc || '').trim();
    const needImg =
      !im ||
      !/^https?:\/\//i.test(im) ||
      /placeholder|loading|data:image/i.test(im);
    return needLink || needImg;
  }

  /**
   * 上架时间列表 DOM 行与 seed 是否同一商品：标题兼容规则与全店匹配一致 + 价键（双非空须一致）。
   */
  function geBuyinNewestSeedTitlePriceDomCompatible(seed, dr) {
    const normTrim = function (s) {
      return String(s || '')
        .replace(/\s+/g, ' ')
        .trim();
    };
    const st = normTrim(seed.title);
    const dt = normTrim(dr.title);
    const ntS = geNewestSeedTitleNormForMatch(seed.title);
    const ntD = geNewestSeedTitleNormForMatch(dr.title);
    if (!st && !ntS) return false;
    let tOk = false;
    if (st && dt && st === dt) tOk = true;
    else if (ntS && ntD && ntS === ntD) tOk = true;
    else if (
      ntS &&
      ntD &&
      Math.min(ntS.length, ntD.length) >= 8 &&
      (ntD.indexOf(ntS) >= 0 || ntS.indexOf(ntD) >= 0)
    ) {
      tOk = true;
    }
    if (!tOk) return false;
    const pkS = geNewestSeedPriceKey(seed.price);
    const pkD = geNewestSeedPriceKey(dr.price);
    if (pkS && pkD && pkS !== pkD) return false;
    return true;
  }

  function geBuyinMergeDomMediaIntoNewestSeed(seed, dr) {
    if (buyinProductLinkLooksValid(dr.link) && !buyinProductLinkLooksValid(seed.link)) {
      seed.link = dr.link;
    }
    const dis = String(dr.imgSrc || '').trim();
    if (dis && /^https?:\/\//i.test(dis) && !/placeholder|loading|data:image/i.test(dis)) {
      const sis = String(seed.imgSrc || '').trim();
      if (!sis || !/^https?:\/\//i.test(sis) || /placeholder|loading|data:image/i.test(sis)) {
        seed.imgSrc = dis;
      }
    }
  }

  /**
   * 最近上架②b：在全店滚动之前，保持「上架时间·最新在前」视图，对缺 link/img 的 seed 定向滚列表触达懒加载，
   * 再按序位或标题+价从 DOM 回填；不增删槽位、不改变前 N 顺序。
   */
  async function geBuyinBackfillNewestSeedsDomMedia(seeds, limit, ctx) {
    const n = Math.max(1, parseInt(limit, 10) || 1);
    const execToken = ctx && ctx.execToken;
    const innerEl = ctx && ctx.innerEl ? ctx.innerEl : geBuyinFindPrimaryProductListScroller();
    const setStatus = ctx && ctx.setStatus;
    const prefix = (ctx && ctx.prefixStatus) || '';
    const list = Array.isArray(seeds) ? seeds : [];
    const vh = window.innerHeight || 700;
    const maxRounds = Math.min(56, Math.max(20, n * 4));

    const countNeeding = function () {
      let c = 0;
      for (let i = 0; i < Math.min(n, list.length); i++) {
        if (geBuyinNewestSeedNeedsDomMediaFill(list[i])) c++;
      }
      return c;
    };

    let beforeNeed = countNeeding();
    if (beforeNeed === 0) {
      console.log('[最近上架·DOM定向补齐] 无需补齐（链与图已齐）');
      return;
    }

    let stagnant = 0;
    let prevNeed = beforeNeed;

    for (let round = 0; round < maxRounds; round++) {
      geBatchAbortIfCancelled(execToken);
      const needNow = countNeeding();
      if (needNow === 0) break;

      if (setStatus && round % 5 === 0) {
        setStatus(
          prefix + ' · 最近上架②b：定向补齐缺链/图（仍缺 ' + needNow + '/' + n + '，轮 ' + (round + 1) + '/' + maxRounds + '）…'
        );
      }

      await geBuyinScrollFeedToTop(execToken);
      await geSleepMaybeBatch(200 + Math.min(100, round * 8), execToken);
      const domRows = scrapeBuyinDom();

      for (let i = 0; i < Math.min(n, list.length, domRows.length); i++) {
        if (!geBuyinNewestSeedNeedsDomMediaFill(list[i])) continue;
        if (!geBuyinNewestSeedTitlePriceDomCompatible(list[i], domRows[i])) continue;
        geBuyinMergeDomMediaIntoNewestSeed(list[i], domRows[i]);
      }

      for (let i = 0; i < Math.min(n, list.length); i++) {
        if (!geBuyinNewestSeedNeedsDomMediaFill(list[i])) continue;
        const seed = list[i];
        for (let j = 0; j < domRows.length; j++) {
          if (!geBuyinNewestSeedTitlePriceDomCompatible(seed, domRows[j])) continue;
          geBuyinMergeDomMediaIntoNewestSeed(seed, domRows[j]);
          if (!geBuyinNewestSeedNeedsDomMediaFill(seed)) break;
        }
      }

      geBuyinApplyNewestListScrollBy(Math.round(vh * 0.62), { mode: 'both', innerEl: innerEl });
      await geSleepMaybeBatch(400, execToken);

      const afterNeed = countNeeding();
      if (afterNeed >= prevNeed) stagnant++;
      else stagnant = 0;
      prevNeed = afterNeed;
      if (stagnant >= 12) {
        console.warn('[最近上架·DOM定向补齐] 连续多轮无进展，提前结束（将依赖全店快照匹配兜底）');
        break;
      }
    }

    await geBuyinScrollFeedToTop(execToken);
    await geSleepMaybeBatch(260, execToken);
    const domFinal = scrapeBuyinDom();
    for (let i = 0; i < Math.min(n, list.length, domFinal.length); i++) {
      if (!geBuyinNewestSeedNeedsDomMediaFill(list[i])) continue;
      if (!geBuyinNewestSeedTitlePriceDomCompatible(list[i], domFinal[i])) continue;
      geBuyinMergeDomMediaIntoNewestSeed(list[i], domFinal[i]);
    }
    for (let i = 0; i < Math.min(n, list.length); i++) {
      if (!geBuyinNewestSeedNeedsDomMediaFill(list[i])) continue;
      const seed = list[i];
      for (let j = 0; j < domFinal.length; j++) {
        if (!geBuyinNewestSeedTitlePriceDomCompatible(seed, domFinal[j])) continue;
        geBuyinMergeDomMediaIntoNewestSeed(seed, domFinal[j]);
        if (!geBuyinNewestSeedNeedsDomMediaFill(seed)) break;
      }
    }

    const afterNeed = countNeeding();
    console.log(
      '[最近上架·DOM定向补齐] 缺链/图：补前 ' +
        beforeNeed +
        ' → 补后 ' +
        afterNeed +
        '（仍缺的将走全店匹配④）'
    );
  }

  function geNewestSeedPriceKey(p) {
    return String(p || '')
      .trim()
      .replace(/[¥￥,\s]/g, '')
      .replace(/[^\d.]/g, '');
  }

  function geNewestSeedImgFingerprint(img) {
    const s = String(img || '').trim().split('?')[0];
    if (!s) return '';
    const slash = s.lastIndexOf('/');
    const tail = slash >= 0 ? s.slice(slash + 1) : s;
    return tail.toLowerCase().replace(/[^a-z0-9._-]/gi, '').slice(0, 64);
  }

  /** 佣金文案 → 可比数字串（如 18% / 佣金10%） */
  function geNewestSeedCommissionKey(c) {
    return String(c || '')
      .trim()
      .replace(/^佣金\s*/i, '')
      .replace(/%/g, '')
      .replace(/[^\d.]/g, '');
  }

  function geNewestSeedSalesCompact(s) {
    return String(s || '')
      .trim()
      .replace(/\s+/g, '')
      .replace(/月销|已售|人付款|销量|\+/g, '')
      .toLowerCase();
  }

  function geNewestImgFingerprintLooseEqual(a, b) {
    if (!a || !b) return false;
    if (a === b) return true;
    return b.indexOf(a) >= 0 || a.indexOf(b) >= 0;
  }

  function geNewestSalesLooseEqual(seedSales, fullSales) {
    const a = geNewestSeedSalesCompact(seedSales);
    const b = geNewestSeedSalesCompact(fullSales);
    if (!a || !b) return false;
    if (a === b) return true;
    return b.indexOf(a) >= 0 || a.indexOf(b) >= 0;
  }

  function geNewestTruncateForLog(s, maxLen) {
    const t = String(s || '').trim();
    const m = maxLen != null ? maxLen : 96;
    if (t.length <= m) return t;
    return t.slice(0, m) + '…';
  }

  function geNewestMatchTierName(tier) {
    const map = {
      5: 'T5_标题+种子非空字段(price/commission/sales/img)全对齐',
      4: 'T4_标题+价格+图(图空则仅价)',
      3: 'T3_标题+价格+销量',
      2: 'T2_标题+图 或 无价时标题+销量',
      1: 'T1_标题+价格(重复标题必须靠价/佣/销/图区分，不单靠标题)',
      0: 'T0_未达任一层',
    };
    return map[tier] || 'T?';
  }

  /** 与首条「标题兼容」的全店行对比，列出主要字段差异（用于未匹配日志） */
  function geNewestMismatchFieldsSummary(seed, fr) {
    const parts = [];
    const pkS = geNewestSeedPriceKey(seed.price);
    const pkF = geNewestSeedPriceKey(fr.price);
    if (pkS && pkF && pkS !== pkF) parts.push('price:' + pkS + '≠' + pkF);
    else if (pkS && !pkF) parts.push('price:full缺');
    const ckS = geNewestSeedCommissionKey(seed.commission);
    const ckF = geNewestSeedCommissionKey(fr.commission);
    if (ckS && ckF && ckS !== ckF) parts.push('commission:' + ckS + '≠' + ckF);
    else if (ckS && !ckF) parts.push('commission:full缺');
    const skS = geNewestSeedSalesCompact(seed.sales);
    if (skS && !geNewestSalesLooseEqual(seed.sales, fr.sales)) {
      parts.push('sales:种子[' + geNewestTruncateForLog(seed.sales, 24) + ']≠全店[' + geNewestTruncateForLog(fr.sales, 24) + ']');
    }
    const imS = geNewestSeedImgFingerprint(seed.imgSrc);
    const imF = geNewestSeedImgFingerprint(fr.imgSrc);
    if (imS && imF && !geNewestImgFingerprintLooseEqual(imS, imF)) parts.push('imgFp:不一致');
    else if (imS && !imF) parts.push('img:full缺指纹');
    return parts.length ? parts.join(' | ') : '标题相近但未满足T1~T5任一层(检查价/图/销量是否需退化匹配)';
  }

  function geNewestDiagnoseFailedSeed(seed, full, used, titleCompatible, classify) {
    const normTrim = (s) => String(s || '').replace(/\s+/g, ' ').trim();
    const st = normTrim(seed.title);
    const ntS = geNewestSeedTitleNormForMatch(seed.title);
    if (!st && !ntS) {
      return {
        code: 'EMPTY_SEED_TITLE',
        maxTierAny: 0,
        maxTierUnused: 0,
        hint: '种子无标题，无法在全店快照中匹配',
        fieldHint: '',
      };
    }
    let maxTierAny = 0;
    let maxTierUnused = 0;
    let titleCompatRows = 0;
    let firstCompatJ = -1;
    for (let j = 0; j < full.length; j++) {
      const fr = full[j] || {};
      if (titleCompatible(seed, fr)) {
        titleCompatRows++;
        if (firstCompatJ < 0) firstCompatJ = j;
      }
      const cl = classify(seed, fr);
      if (cl.tier > maxTierAny) maxTierAny = cl.tier;
      if (!used.has(j) && cl.tier > maxTierUnused) maxTierUnused = cl.tier;
    }
    if (titleCompatRows === 0) {
      return {
        code: 'NO_TITLE_COMPATIBLE_IN_FULL',
        maxTierAny: 0,
        maxTierUnused: 0,
        hint: '全店快照中无任何与种子标题兼容的行',
        fieldHint: '',
      };
    }
    if (maxTierUnused >= 1) {
      return {
        code: 'INTERNAL_TIER_UNUSED_POSITIVE',
        maxTierAny,
        maxTierUnused,
        hint: '未使用行中仍有tier≥1，请反馈（可能为平局逻辑问题）',
        fieldHint: '',
      };
    }
    if (maxTierAny >= 1) {
      return {
        code: 'ELIGIBLE_ROWS_CONSUMED_BY_EARLIER_SEEDS',
        maxTierAny,
        maxTierUnused: 0,
        hint: '存在tier' + maxTierAny + '的全店行，但均已被排序更靠前的种子占用（同一条全店商品不重复分配）',
        fieldHint: geNewestMatchTierName(maxTierAny),
      };
    }
    const fr0 = firstCompatJ >= 0 ? full[firstCompatJ] || {} : {};
    return {
      code: 'TITLE_OK_BUT_NO_TIER1_PLUS',
      maxTierAny: 0,
      maxTierUnused: 0,
      hint: '有标题兼容行，但价/佣/销/图组合未满足任一匹配层（最高仅T0）',
      fieldHint: geNewestMismatchFieldsSummary(seed, fr0),
    };
  }

  function geLogNewestListingMatchReport(report) {
    const r = report || {};
    console.log(
      '%c[最近上架·匹配汇总]',
      'font-weight:bold;color:#1677ff',
      '配置N=' +
        (r.nConfig != null ? r.nConfig : '?') +
        ' | DOM实取种子=' +
        (r.domCapturedCount != null ? r.domCapturedCount : '?') +
        ' | 全店快照条数=' +
        (r.fullSnapshotCount != null ? r.fullSnapshotCount : '?') +
        ' | 有效链接(matched)=' +
        (r.matchedWithLinkCount != null ? r.matchedWithLinkCount : '?') +
        ' | seed_only=' +
        (r.seedOnlyCount != null ? r.seedOnlyCount : '?') +
        '（含：未命中全店行 / 命中但全店行无有效商品链）'
    );
    const fails = r.unmatchedDetails || [];
    if (!fails.length) {
      console.log('[最近上架·未匹配明细] 无（全部 matched 或仅占位空标题槽）');
      return;
    }
    console.log('[最近上架·未匹配明细] 共 ' + fails.length + ' 条：');
    for (let i = 0; i < fails.length; i++) {
      const u = fails[i] || {};
      console.warn(
        '[最近上架·#' + (u.index != null ? u.index : i) + ']',
        u.code || '',
        '| 最高tier(全店任意行)=' + (u.maxTierAny != null ? u.maxTierAny : '?'),
        '| 未用行最高tier=' + (u.maxTierUnused != null ? u.maxTierUnused : '?'),
        '\n  title:',
        geNewestTruncateForLog(u.title, 80),
        '\n  price:',
        u.price,
        '| commission:',
        u.commission,
        '| sales:',
        u.sales,
        '\n  imgSrc:',
        geNewestTruncateForLog(u.imgSrc, 96),
        '\n  说明:',
        u.hint || '',
        '\n  字段:',
        u.fieldHint || ''
      );
    }
  }

  /**
   * 最近上架④：全店快照中按多字段组合匹配种子（顺序与 seeds 一致，长度恒为 N，不输出全店）。
   *
   * 【匹配层级·优先级从高到低】
   * 1) T5：标题兼容 + 种子中非空的 price、commission、sales、imgSrc 均与全店行一致（字段缺失则不校验该项）。
   * 2) T4：标题 + 价格 + 图（种子无图则不要求图）。
   * 3) T3：标题 + 价格 + 销量（种子无销量则不要求销量）。
   * 4) T2：标题 + 图；或种子无价格时 标题 + 销量。
   * 5) T1：标题 + 价格（重复标题不能只靠标题，至少需价格等辅助字段命中退化层）。
   *
   * 【兜底】同一全店行仅分配给一个种子(used)；未命中仍输出种子行并标 seed_only，不删行不改序。
   */
  function geMatchNewestSeedsToFullCatalog(seeds, fullRows, opts) {
    const seedsArr = Array.isArray(seeds) ? seeds : [];
    const full = geDedupRowsByIdentity(Array.isArray(fullRows) ? fullRows.slice() : []);
    const used = new Set();
    const out = [];
    const normTrim = (s) => String(s || '').replace(/\s+/g, ' ').trim();
    const nConfig = (opts && opts.nConfig != null ? parseInt(opts.nConfig, 10) : seedsArr.length) || seedsArr.length;
    let domCapturedCount =
      opts && opts.domCapturedCount != null ? parseInt(opts.domCapturedCount, 10) : NaN;
    if (!Number.isFinite(domCapturedCount)) {
      domCapturedCount = seedsArr.filter(function (s) {
        return normTrim((s && s.title) || '');
      }).length;
    }

    function titleCompatible(seed, fr) {
      const st = normTrim(seed.title);
      const ft = normTrim(fr.title);
      const ntS = geNewestSeedTitleNormForMatch(seed.title);
      const ntF = geNewestSeedTitleNormForMatch(fr.title);
      if (!st && !ntS) return false;
      if (st && ft && st === ft) return true;
      if (ntS && ntF && ntS === ntF) return true;
      if (ntS && ntF && Math.min(ntS.length, ntF.length) >= 8 && (ntF.indexOf(ntS) >= 0 || ntS.indexOf(ntF) >= 0)) {
        return true;
      }
      return false;
    }

    function classify(seed, fr) {
      if (!titleCompatible(seed, fr)) return { tier: 0, tie: 0 };

      const pkS = geNewestSeedPriceKey(seed.price);
      const pkF = geNewestSeedPriceKey(fr.price);
      const priceOk = pkS && pkF && pkS === pkF;
      const priceEmptySeed = !pkS;

      const ckS = geNewestSeedCommissionKey(seed.commission);
      const ckF = geNewestSeedCommissionKey(fr.commission);
      const commOk = ckS && ckF && ckS === ckF;
      const commEmptySeed = !ckS;

      const skS = geNewestSeedSalesCompact(seed.sales);
      const salesOk = skS ? geNewestSalesLooseEqual(seed.sales, fr.sales) : true;
      const salesEmptySeed = !skS;

      const imS = geNewestSeedImgFingerprint(seed.imgSrc);
      const imF = geNewestSeedImgFingerprint(fr.imgSrc);
      const imgOk = imS && imF && geNewestImgFingerprintLooseEqual(imS, imF);
      const imgEmptySeed = !imS;

      const needPriceForTier5 = !priceEmptySeed;
      const needCommForTier5 = !commEmptySeed;
      const needSalesForTier5 = !salesEmptySeed;
      const needImgForTier5 = !imgEmptySeed;

      if (
        (needPriceForTier5 ? priceOk : true) &&
        (needCommForTier5 ? commOk : true) &&
        (needSalesForTier5 ? salesOk : true) &&
        (needImgForTier5 ? imgOk : true) &&
        (needPriceForTier5 || needCommForTier5 || needSalesForTier5 || needImgForTier5)
      ) {
        let tie = 4;
        if (priceOk) tie++;
        if (commOk && ckS) tie++;
        if (salesOk && skS) tie++;
        if (imgOk && imS) tie++;
        return { tier: 5, tie: tie };
      }

      if (priceOk && (imgEmptySeed || imgOk)) {
        return { tier: 4, tie: 3 + (imgOk && imS ? 2 : 0) + (commOk && ckS ? 1 : 0) };
      }

      if (priceOk && (salesEmptySeed || salesOk)) {
        return { tier: 3, tie: 2 + (salesOk && skS ? 2 : 0) };
      }

      let t2tie = 0;
      if (!pkS && skS && salesOk) t2tie = Math.max(t2tie, 2 + (imgOk && imS ? 1 : 0));
      if (imS && imgOk) t2tie = Math.max(t2tie, 3 + (priceOk ? 1 : 0));
      if (t2tie > 0) return { tier: 2, tie: t2tie };

      if (priceOk) {
        return { tier: 1, tie: 1 + (commOk && ckS ? 1 : 0) + (salesOk && skS ? 1 : 0) };
      }

      return { tier: 0, tie: 0 };
    }

    const unmatchedDetails = [];

    for (let si = 0; si < seedsArr.length; si++) {
      const seed = seedsArr[si] || {};
      const seedTitleTrim = normTrim(seed.title);
      const ntS = geNewestSeedTitleNormForMatch(seed.title);
      if (!seedTitleTrim && !ntS) {
        out.push(
          Object.assign({}, seed, {
            _geNewestMatchStatus: 'seed_only',
            _geNewestMatchDetail: 'EMPTY_SEED_TITLE|占位槽无标题',
          })
        );
        unmatchedDetails.push({
          index: si,
          code: 'EMPTY_SEED_TITLE',
          maxTierAny: 0,
          maxTierUnused: 0,
          title: '',
          price: seed.price,
          commission: seed.commission,
          sales: seed.sales,
          imgSrc: seed.imgSrc,
          hint: '配置N的占位行，DOM未取到该序位商品',
          fieldHint: '',
        });
        continue;
      }

      let bestJ = -1;
      let bestTier = -1;
      let bestTie = -1;
      let bestHasLink = false;

      for (let j = 0; j < full.length; j++) {
        if (used.has(j)) continue;
        const fr = full[j] || {};
        const cl = classify(seed, fr);
        if (cl.tier <= 0) continue;
        const hasLk = buyinProductLinkLooksValid(fr.link);
        if (
          cl.tier > bestTier ||
          (cl.tier === bestTier &&
            (cl.tie > bestTie || (cl.tie === bestTie && hasLk && !bestHasLink) || (cl.tie === bestTie && hasLk === bestHasLink && bestJ >= 0 && j < bestJ)))
        ) {
          bestTier = cl.tier;
          bestTie = cl.tie;
          bestJ = j;
          bestHasLink = hasLk;
        }
      }

      if (bestJ >= 0) {
        used.add(bestJ);
        const fr = full[bestJ] || {};
        const mergedLink = buyinProductLinkLooksValid(fr.link) ? fr.link : seed.link || '';
        const hasValid = buyinProductLinkLooksValid(mergedLink);
        out.push(
          Object.assign({}, seed, {
            title: normTrim(fr.title) ? fr.title : seed.title,
            price: normTrim(fr.price) ? fr.price : seed.price,
            commission: fr.commission != null && String(fr.commission).trim() ? fr.commission : seed.commission,
            sales: fr.sales != null && String(fr.sales).trim() ? fr.sales : seed.sales,
            link: mergedLink,
            imgSrc: normTrim(fr.imgSrc) ? fr.imgSrc : seed.imgSrc || '',
            promotion_id: fr.promotion_id || seed.promotion_id || '',
            product_id: fr.product_id || seed.product_id || '',
            commodity_id: fr.commodity_id || seed.commodity_id || '',
            shopName: fr.shopName || seed.shopName || '',
            shopLink: fr.shopLink || seed.shopLink || '',
            guarantee: fr.guarantee || seed.guarantee || '',
            deliveryTime: fr.deliveryTime || seed.deliveryTime || '',
            _geNewestMatchStatus: hasValid ? 'matched' : 'seed_only',
            _geNewestMatchDetail: hasValid
              ? 'matched|' + geNewestMatchTierName(bestTier) + '|fullRow#' + bestJ
              : 'seed_only|TIER' + bestTier + '_BUT_FULL_ROW_NO_VALID_PRODUCT_LINK|fullRow#' + bestJ,
          })
        );
        if (!hasValid) {
          unmatchedDetails.push({
            index: si,
            code: 'MATCHED_ROW_BUT_NO_VALID_LINK',
            maxTierAny: bestTier,
            maxTierUnused: bestTier,
            title: seed.title,
            price: seed.price,
            commission: seed.commission,
            sales: seed.sales,
            imgSrc: seed.imgSrc,
            hint: '命中全店第' + bestJ + '行(' + geNewestMatchTierName(bestTier) + ')但该行无有效商品详情链',
            fieldHint: 'full.link=' + geNewestTruncateForLog(fr.link, 64),
          });
        }
      } else {
        const diag = geNewestDiagnoseFailedSeed(seed, full, used, titleCompatible, classify);
        out.push(
          Object.assign({}, seed, {
            _geNewestMatchStatus: 'seed_only',
            _geNewestMatchDetail: 'seed_only|' + diag.code + '|maxTierAny=' + diag.maxTierAny,
          })
        );
        unmatchedDetails.push({
          index: si,
          code: diag.code,
          maxTierAny: diag.maxTierAny,
          maxTierUnused: diag.maxTierUnused,
          title: seed.title,
          price: seed.price,
          commission: seed.commission,
          sales: seed.sales,
          imgSrc: seed.imgSrc,
          hint: diag.hint,
          fieldHint: diag.fieldHint,
        });
      }
    }

    let matchedWithLinkCount = 0;
    let seedOnlyCount = 0;
    for (let i = 0; i < out.length; i++) {
      if ((out[i] || {})._geNewestMatchStatus === 'matched') matchedWithLinkCount++;
      else seedOnlyCount++;
    }

    const report = {
      nConfig: nConfig,
      domCapturedCount: domCapturedCount,
      fullSnapshotCount: full.length,
      matchedWithLinkCount: matchedWithLinkCount,
      seedOnlyCount: seedOnlyCount,
      unmatchedDetails: unmatchedDetails,
    };

    return { rows: out, report: report };
  }

  /**
   * 将“首屏 seed 行”与“已补齐链接的全量 snapshot 行”做匹配，仅回填 seed 对应的 link/sales 等字段。
   * - 不新增 seed 以外的行，确保“最先加载的商品”不被后续滚动污染
   */
  function geMatchBuyinRowsBySeed(seedRows, fullRows) {
    const seeds = Array.isArray(seedRows) ? seedRows : [];
    const full = Array.isArray(fullRows) ? fullRows : [];
    if (!seeds.length) return [];
    if (!full.length) return seeds.slice();

    const used = new Set();
    const norm = (s) => String(s || '').replace(/\\s+/g, ' ').trim();
    const titleKey = (t) => norm(t).toLowerCase().replace(/[^\\u4e00-\\u9fa5a-z0-9]+/g, '').slice(0, 36);
    const priceKey = (p) => norm(p).replace(/[^\\d.]/g, '');

    const out = [];
    for (let i = 0; i < seeds.length; i++) {
      const sr = seeds[i] || {};
      const st = titleKey(sr.title);
      const sp = priceKey(sr.price);
      let best = -1;
      let bestScore = -1;

      for (let j = 0; j < full.length; j++) {
        if (used.has(j)) continue;
        const fr = full[j] || {};

        // 先按 link 精确匹配（若 seed 已有有效 link）
        const sl = norm(sr.link);
        const fl = norm(fr.link);
        if (sl && fl && sl.split('?')[0] === fl.split('?')[0] && buyinProductLinkLooksValid(fl)) {
          best = j;
          bestScore = 100;
          break;
        }

        const ft = titleKey(fr.title);
        if (!st || !ft) continue;
        let sc = 0;
        if (ft === st) sc += 40;
        else if (ft.includes(st) || st.includes(ft)) sc += 26;
        else continue;

        const fp = priceKey(fr.price);
        if (sp && fp && sp === fp) sc += 10;
        if (buyinProductLinkLooksValid(fr.link)) sc += 8;
        if (String(fr.sales || '').trim()) sc += 3;

        if (sc > bestScore) {
          bestScore = sc;
          best = j;
        }
      }

      if (best >= 0) {
        used.add(best);
        const fr = full[best] || {};
        out.push({
          ...sr,
          link: buyinProductLinkLooksValid(fr.link) ? fr.link : sr.link,
          sales: fr.sales != null && String(fr.sales).trim() ? fr.sales : sr.sales,
          imgSrc: sr.imgSrc || fr.imgSrc || '',
          promotion_id: sr.promotion_id || fr.promotion_id || '',
          product_id: sr.product_id || fr.product_id || '',
          commodity_id: sr.commodity_id || fr.commodity_id || '',
        });
      } else {
        out.push(sr);
      }
    }
    return out;
  }

  /**
   * 商品详情链：以 haohuo 交易/商品页为主；允许抖音商品相关链；排除 buyin 后台与罗盘（曾误把 /dashboard/.../live/ 等当商品）。
   */
  function buyinProductLinkLooksValid(link) {
    const u = (link || '').trim();
    if (!u || !/^https?:\/\//i.test(u)) return false;
    if (isBuyinImageAssetUrl(u)) return false;
    if (isProbablyBuyinShopUrl(u)) return false;
    if (isBuyinNonProductPageUrl(u)) return false;
    if (/haohuo\.jinritemai\.com/i.test(u)) {
      if (/\.(js|mjs|css|png|jpe?g|gif|webp|svg|ico|woff2?)(\?|$)/i.test(u)) return false;
      return /ecommerce\/trade\/detail|views\/product\/item|commodity-detail|\/ecommerce\/|\/views\/product\/|\/goods\/|ec_goods/i.test(u);
    }
    if (/douyin\.com/i.test(u)) {
      return /\/(goods|product|item|aweme|video|note\/|shop\/detail)/i.test(u);
    }
    if (/item\.htm|detail\.tmall|tmall\.com\/item/i.test(u)) return true;
    return false;
  }

  /** 百应侧栏/路由标题，误当商品名写入时整行丢弃（精确匹配） */
  const GE_BUYIN_NAV_TITLE_BLOCK = new Set(
    `直播预告
直播中控台
直播数据
直播明细
直播闪购
直播回放
直播货盘
直播设置
创建直播预告
直播实时数据
主播提词板
主播新提词板
直播带货数据详情
创建待播商品
直播货盘详情
橱窗商品管理
橱窗装修
橱窗设置
发布精选商品
配置福利品专区
编辑主题清单
管理主题清单
装修页面
数据详情
视频管理
电商好看计划
图文管理
选品库
品牌馆
决策页
福袋选品页
榜单
话题榜
达人榜
专题页
专属选品广场
话题详情
老决策页
福袋订单确认页
商品详情页
福袋券中心鹊桥
小二精选
选品广场
选品AI助手
同行跟选
热点趋势
联盟榜单
联盟活动
抖Link大会
选品广场新版决策页
选品广场服饰精选页
优选货盘
选品广场专题页
选品广场画像页
选品广场类目选择页
热点话题中心页
热点话题详情页
福袋选品
福袋详情页
直播课代表频道页
直播课代表福利页
同款更优
细分主题榜单
低价频道
抖Link报名信息
抖Link会场
商家主页
达人主页
机构主页
福袋券中心
口碑优选
价格力变化通知
选品车
合作订单管理
历史带货评价
合作订单详情
发起合作订单
官方客服
达人主页设置
服务费设置
联系方式
商品推广权限
带货设置
达人飞书绑定
商品权限准入流程页
创意首页
热门直播
爆款视频
直播详情
创意搜索
超级福袋
红包管理
惊喜好礼
直播间发券
达人券管理
直播间福利任务
福袋订单管理
创建奖品
福袋数据
红包资金明细
新建活动
活动数据
单个活动数据
活动详情
创券资金明细
主播券单商品订单维度数据
任务数据
单个任务数据
任务详情
粉丝活跃转化
购物粉丝团管理
帐号违规管理
概览
违规记录
物流监控
物流明细
售后监控
小额打款
物流监控详情
售后待办
售后订单查询
赔付中心
违规赔付详情
成长首页
收藏作者
作者详情
数据概览
成交明细
创作数据
直播诊断
橱窗诊断
视频诊断
商品诊断
人群分析
购物粉丝团数据
直播成交金额诊断详情
直播流量诊断详情
直播时长诊断详情
直播转化率诊断详情
橱窗成交金额详情页
橱窗流量诊断详情页
橱窗转化诊断详情页
短视频成交金额诊断详情页
短视频数量诊断详情页
短视频流量诊断详情页
短视频转化率诊断详情页
联盟订单明细
导出记录
余额管理
佣金分成账单
返佣管理
服务费账单
动帐明细
提现记录
冻结记录
状态记录
冻结原因
平台开票
达人开票
保证金管理
合同管理
授权管理
个人信息管理
资质认证
授权打通
电商学习中心
电商罗盘
巨量千川`
      .split(/\r?\n/)
      .map((s) => s.trim())
      .filter(Boolean)
  );

  function isBuyinNavMenuTitle(t) {
    const s = (t || '').trim();
    return s.length > 0 && GE_BUYIN_NAV_TITLE_BLOCK.has(s);
  }

  /** 店铺页链接，勿写入「商品链接」 */
  function isProbablyBuyinShopUrl(u) {
    const s = String(u || '').trim();
    if (!s) return false;
    return /\/shop-detail[\/?#]|merch-picking-library\/shop-detail|\/shop\/home|store\/home/i.test(s);
  }

  /**
   * material_list 等接口里 product_id 常在促销根对象 p 上，不在 product_info 内。
   * 百应当前常用 haohuo「views/product/item」链（与 trade/detail 并存，优先前者）。
   */
  function tryBuildBuyinProductUrlFromIds(...objs) {
    for (let i = 0; i < objs.length; i++) {
      const o = objs[i];
      if (!o || typeof o !== 'object') continue;
      const id =
        o.product_id ??
        o.productId ??
        o.goods_id ??
        o.goodsId ??
        o.item_id ??
        o.itemId ??
        o.commodity_id ??
        o.commodityId;
      if (id == null) continue;
      const sid = String(id).trim();
      if (!/^\d{8,}$/.test(sid)) continue;
      return 'https://haohuo.jinritemai.com/views/product/item?id=' + encodeURIComponent(sid);
    }
    return '';
  }

  function buyinDeepFindProductUrlInObject(obj, depth, seen) {
    if (!obj || typeof obj !== 'object' || depth > 5) return '';
    if (seen.has(obj)) return '';
    seen.add(obj);
    if (Array.isArray(obj)) {
      for (let i = 0; i < obj.length; i++) {
        const el = obj[i];
        if (typeof el === 'string') {
          const u = normalizeBuyinHref(el);
          if (
            /^https?:\/\//i.test(u) &&
            !isProbablyBuyinShopUrl(u) &&
            !isBuyinNonProductPageUrl(u) &&
            buyinProductLinkLooksValid(u)
          )
            return u;
        } else if (el && typeof el === 'object') {
          const r = buyinDeepFindProductUrlInObject(el, depth + 1, seen);
          if (r) return r;
        }
      }
      return '';
    }
    for (const v of Object.values(obj)) {
      if (typeof v === 'string') {
        if (!/^https?:\/\//i.test(v) && !v.startsWith('//')) continue;
        const u = normalizeBuyinHref(v);
        if (!/^https?:\/\//i.test(u)) continue;
        if (isProbablyBuyinShopUrl(u)) continue;
        if (isBuyinNonProductPageUrl(u)) continue;
        if (buyinProductLinkLooksValid(u)) return u;
      } else if (v && typeof v === 'object') {
        const r = buyinDeepFindProductUrlInObject(v, depth + 1, seen);
        if (r) return r;
      }
    }
    return '';
  }

  function extractBuyinProductLinkFromApi(pi, b, root) {
    const tryOne = (o) => {
      if (!o || typeof o !== 'object') return '';
      const keys = [
        'detail_url',
        'detailUrl',
        'product_url',
        'productUrl',
        'h5_url',
        'h5Url',
        'jump_url',
        'jumpUrl',
        'share_url',
        'shareUrl',
        'web_url',
        'webUrl',
        'url',
        'link',
        'href',
        'detail_h5_url',
        'detailH5Url',
        'product_detail_url',
        'productDetailUrl',
        'item_url',
        'itemUrl',
        'detail_link',
        'detailLink',
        'goods_detail_url',
        'goodsDetailUrl',
      ];
      for (let i = 0; i < keys.length; i++) {
        const k = keys[i];
        const v = o[k];
        if (v == null || v === '') continue;
        let s = '';
        if (typeof v === 'string') s = v;
        else if (v && typeof v === 'object') {
          s =
            (typeof v.url === 'string' && v.url) ||
            (typeof v.h5 === 'string' && v.h5) ||
            (typeof v.h5_url === 'string' && v.h5_url) ||
            (typeof v.link === 'string' && v.link) ||
            (typeof v.detail_url === 'string' && v.detail_url) ||
            '';
        }
        if (!s) continue;
        const u = normalizeBuyinHref(String(s).trim());
        if (!/^https?:\/\//i.test(u)) continue;
        if (isProbablyBuyinShopUrl(u)) continue;
        if (isBuyinNonProductPageUrl(u)) continue;
        if (buyinProductLinkLooksValid(u)) return u;
      }
      return '';
    };
    let link =
      tryOne(pi) ||
      (b && tryOne(b.promotion_info)) ||
      (b && tryOne(b.marketing_info)) ||
      (root && tryOne(root));
    if (!link && pi && typeof pi === 'object') link = buyinDeepFindProductUrlInObject(pi, 0, new WeakSet());
    if (!link && b && typeof b === 'object') link = buyinDeepFindProductUrlInObject(b, 0, new WeakSet());
    if (!link) link = tryBuildBuyinProductUrlFromIds(root, pi, b);
    return link || '';
  }

  function scrapeBuyinProductLinkFromCard(card) {
    if (!card || !card.querySelectorAll) return '';
    const linkScore = (u) => {
      if (/trade\/detail|commodity-detail|ecommerce\/trade|haohuo\.jinritemai|\/goods\/|views\/product/i.test(u)) return 4;
      if (/item\.htm|detail\.tmall/i.test(u)) return 3;
      return 0;
    };
    const as = card.querySelectorAll('a[href]');
    let fallback = '';
    let fallbackScore = 0;
    for (let i = 0; i < as.length; i++) {
      const raw = (as[i].getAttribute('href') || '').trim();
      if (!raw || raw === '#' || /^javascript:/i.test(raw)) continue;
      const u = normalizeBuyinHref(raw);
      if (!/^https?:\/\//i.test(u)) continue;
      if (isBuyinImageAssetUrl(u)) continue;
      if (isProbablyBuyinShopUrl(u)) continue;
      if (isBuyinNonProductPageUrl(u)) continue;
      if (buyinProductLinkLooksValid(u)) return u;
      const sc = linkScore(u);
      if (sc > fallbackScore) {
        fallback = u;
        fallbackScore = sc;
      }
    }
    if (fallback && fallbackScore >= 4) return fallback;
    const attrs = [
      'data-href',
      'data-url',
      'data-link',
      'data-detail-url',
      'data-detailUrl',
      'data-jump-url',
      'data-jumpUrl',
      'data-to',
      'data-path',
    ];
    for (let a = 0; a < attrs.length; a++) {
      const nodes = card.querySelectorAll('[' + attrs[a] + ']');
      for (let j = 0; j < nodes.length; j++) {
        const u = normalizeBuyinHref(nodes[j].getAttribute(attrs[a]) || '');
        if (!/^https?:\/\//i.test(u)) continue;
        if (isBuyinImageAssetUrl(u)) continue;
        if (isProbablyBuyinShopUrl(u)) continue;
        if (isBuyinNonProductPageUrl(u)) continue;
        if (buyinProductLinkLooksValid(u)) return u;
      }
    }
    return '';
  }

  function enrichBuyinRowsWithDomLinks(rows) {
    if (!rows || !rows.length || !isBuyin()) return rows;
    const cards = document.querySelectorAll('div[class*="card___"]');
    return rows.map((row) => {
      const needShelf = !(row.shelfTime && String(row.shelfTime).trim());
      const needLink = !buyinProductLinkLooksValid(row.link);
      if (!needShelf && !needLink) return row;

      const t = (row.title || '').trim();
      if (t.length < 4) return row;
      const needle = t.slice(0, Math.min(16, t.length));
      for (let i = 0; i < cards.length; i++) {
        const card = cards[i];
        if (buyinCardInNoiseHost(card)) continue;
        const ct = card.textContent || '';
        if (needle && !ct.includes(needle)) continue;
        const lk = scrapeBuyinProductLinkFromCard(card);
        const gd = scrapeBuyinGuaranteeDeliveryFromCard(card);
        const rawDom = needShelf ? geTryExtractShelfTimeFromCardText(ct) : '';
        const shelfDom = needShelf
          ? (geNormalizeShelfTimeForRow(rawDom) || (rawDom && String(rawDom).trim()) || '')
          : '';
        const linkOk = buyinProductLinkLooksValid(row.link) || buyinProductLinkLooksValid(lk);
        if (!linkOk && !shelfDom) continue;
        if (needLink && !buyinProductLinkLooksValid(lk) && !buyinProductLinkLooksValid(row.link)) {
          if (!shelfDom) continue;
        }

        if (geShelfDebugEnabled() && needShelf && shelfDom) {
          geShelfDebugLog('DOM 补足 shelfTime: ' + t.slice(0, 36), shelfDom);
        }

        return {
          ...row,
          link: buyinProductLinkLooksValid(row.link) ? row.link : lk || row.link,
          guarantee: (row.guarantee || '').trim() || gd.guarantee || '',
          deliveryTime: (row.deliveryTime || '').trim() || gd.deliveryTime || '',
          shelfTime: (row.shelfTime || '').trim() || shelfDom || '',
        };
      }
      return row;
    });
  }

  /** 过滤 DOM 误抓（验证码卡片）与 JSON 误解析行 */
  function buyinRowHasPlausibleProductSignals(row) {
    if (!row || typeof row !== 'object') return false;
    const link = (row.link || '').trim();
    const title = (row.title || '').trim();
    const price = (row.price || '').trim();
    const commission = (row.commission || '').trim();
    if (isBuyinNavMenuTitle(title)) return false;
    if (link && !buyinProductLinkLooksValid(link)) return false;
    if (buyinProductLinkLooksValid(link)) return true;
    if (isBuyinGarbageTitle(title)) return false;
    if (title.length >= 8 && (price || commission)) return true;
    if (title.length >= 6 && price && commission) return true;
    return false;
  }

  function buyinCardInNoiseHost(el) {
    if (!el || typeof el.closest !== 'function') return false;
    return !!el.closest(
      '[class*="captcha"],[class*="Captcha"],[class*="verify"],[class*="Verify"],[class*="seccode"],[id*="captcha"],[class*="slide-verify"],[class*="slider-verify"],[class*="shield"],[class*="risk-verify"],[class*="awsc-"],[class*="nc_"]'
    );
  }

  /** 从接口对象中尽量提取保障、发货时效（字段名多变，辅以全文关键词） */
  function extractBuyinGuaranteeDeliveryFromModel(p, pi, b) {
    const parts = [p, pi, b, b && b.product_info, b && b.promotion_info, b && b.marketing_info, b && b.logistics_info].filter(
      (x) => x && typeof x === 'object'
    );
    let blob = '';
    try {
      blob = parts.map((o) => JSON.stringify(o)).join('\n');
    } catch (_) {
      blob = '';
    }
    const gHits = [];
    if (/运费险|freight_insurance|freightInsurance|退货包运|return_freight/i.test(blob)) gHits.push('运费险');
    if (/7\s*天无理由|seven_day|no_reason.*7|sevenDay/i.test(blob)) gHits.push('7天无理由');
    if (/破损包退|破损.*退/i.test(blob)) gHits.push('破损包退');
    if (/假一赔|假赔三/i.test(blob)) gHits.push('假一赔');
    if (/晚发即赔|晚发赔/i.test(blob)) gHits.push('晚发即赔');
    const guarantee = [...new Set(gHits)].join('、');

    let deliveryTime = '';
    const mH = blob.match(/(\d{1,3})\s*小时[\s]*内?发(?:货)?/);
    if (mH) deliveryTime = mH[1] + '小时发货';
    else if (/48\s*小时发|48h|twenty_four|24\s*小时发/.test(blob)) {
      if (/48/.test(blob) && /小时|hour/i.test(blob)) deliveryTime = '48小时发货';
      else if (/24/.test(blob)) deliveryTime = '24小时发货';
    }
    const mD = blob.match(/(\d{1,3})\s*天[\s]*预售|预售[^"\n]{0,40}?(\d{1,3})\s*天/);
    if (mD) {
      const d = mD[1] || mD[2];
      deliveryTime = deliveryTime ? deliveryTime + '；' + d + '天预售' : d + '天预售';
    }
    if (!deliveryTime && /现货|当日发|次日发|闪电发/.test(blob)) {
      const m2 = blob.match(/(现货|当日发|次日发|闪电发)/);
      if (m2) deliveryTime = m2[1];
    }
    try {
      const ld =
        pi.logistics_desc ||
        pi.logisticsDesc ||
        b.logistics_desc ||
        b.logisticsDesc ||
        p.logistics_desc ||
        p.logisticsDesc;
      if (ld != null && String(ld).trim()) {
        const s = String(ld).trim();
        if (!deliveryTime || s.length > deliveryTime.length) deliveryTime = s.slice(0, 80);
      }
    } catch (_) {}
    return { guarantee, deliveryTime };
  }

  function scrapeBuyinGuaranteeDeliveryFromCard(card) {
    if (!card) return { guarantee: '', deliveryTime: '' };
    const t = (card.textContent || '').replace(/\s+/g, ' ');
    const gHits = [];
    if (/运费险/.test(t)) gHits.push('运费险');
    if (/7\s*天无理由|七天无理由/.test(t)) gHits.push('7天无理由');
    if (/破损包退/.test(t)) gHits.push('破损包退');
    if (/假一赔/.test(t)) gHits.push('假一赔');
    if (/晚发即赔/.test(t)) gHits.push('晚发即赔');
    const guarantee = [...new Set(gHits)].join('、');
    let deliveryTime = '';
    const mH = t.match(/(\d{1,3})\s*小时[\s]*内?发(?:货)?/);
    if (mH) deliveryTime = mH[1] + '小时发货';
    else if (/48\s*小时发|24\s*小时发|当日发|次日发|闪电发|现货/.test(t)) {
      const m2 = t.match(/(48\s*小时发|24\s*小时发|当日发|次日发|闪电发|现货)/);
      if (m2) deliveryTime = m2[1].replace(/\s+/g, '');
    }
    const mP = t.match(/(\d{1,3})\s*天\s*预售/);
    if (mP) deliveryTime = deliveryTime ? deliveryTime + '；' + mP[1] + '天预售' : mP[1] + '天预售';
    return { guarantee, deliveryTime };
  }

  function parseBuyinPromotionItem(p) {
    if (!p || typeof p !== 'object') return null;
    let shopName = '';
    let shopLink = '';
    try {
      const z = extractBuyinShopFromPromotion(p);
      shopName = z.shopName;
      shopLink = z.shopLink;
    } catch (e) {
      console.warn('[导出] 百应店铺字段解析跳过:', e);
    }
    const b = p.base_model || {};
    const pi = b.product_info || {};
    const cos = b.promotion_info?.cos_info?.cos || {};
    const price = b.marketing_info?.price_desc?.price || {};
    const priceYuan = price.origin != null ? (price.origin / 100).toFixed(2) : '';
    const cosRatio = cos.cos_ratio?.integer != null ? cos.cos_ratio.integer + (cos.cos_ratio.suffix || '%') : '';
    const cosFee = cos.cos_fee?.origin != null ? (cos.cos_fee.origin / 100).toFixed(2) + '元' : '';
    let link = '';
    const rawDetail = pi.detail_url != null ? pi.detail_url : pi.detailUrl;
    if (rawDetail != null && String(rawDetail).trim() !== '') {
      if (typeof rawDetail === 'string') link = normalizeBuyinHref(rawDetail.trim());
      else if (typeof rawDetail === 'object' && rawDetail) {
        const inner =
          (typeof rawDetail.url === 'string' && rawDetail.url) ||
          (typeof rawDetail.h5 === 'string' && rawDetail.h5) ||
          (typeof rawDetail.link === 'string' && rawDetail.link) ||
          '';
        if (inner) link = normalizeBuyinHref(String(inner).trim());
      }
    }
    if (!link) link = extractBuyinProductLinkFromApi(pi, b, p);
    if (!link) link = tryBuildBuyinProductUrlFromIds(p, pi, b);
    if (link && !buyinProductLinkLooksValid(link)) {
      link = tryBuildBuyinProductUrlFromIds(p, pi, b) || '';
    }
    if (link && !buyinProductLinkLooksValid(link)) link = '';
    const gd = extractBuyinGuaranteeDeliveryFromModel(p, pi, b);
    const shelfEx = geExtractShelfTimeFromApiItem(p);
    let shelfTime = (shelfEx.shelfTime || '').trim();
    const promoRaw =
      p.promotion_id ??
      p.promotionId ??
      b.promotion_info?.promotion_id ??
      b.promotion_info?.promotionId ??
      '';
    const prodRaw = p.product_id ?? p.productId ?? pi.product_id ?? pi.productId ?? '';
    const commRaw = p.commodity_id ?? p.commodityId ?? pi.commodity_id ?? pi.commodityId ?? '';
    const row = {
      title: pi.name != null && pi.name !== '' ? String(pi.name) : '',
      price: priceYuan ? '¥' + priceYuan : '',
      commission: cosRatio || cosFee || '',
      sales: String(pi.month_sale?.origin ?? '')
        .trim()
        .replace(/,/g, '')
        .replace(/[^0-9万千.+]/g, ''),
      link,
      imgSrc: pi.main_img?.url_list?.[0] || '',
      shopName,
      shopLink,
      shopTag: '',
      monitorTimes: '',
      monitorIndex: '',
      inspectCycle: '',
      shelfTime,
      promotion_id: promoRaw != null && String(promoRaw).trim() !== '' ? String(promoRaw).trim() : '',
      product_id: prodRaw != null && String(prodRaw).trim() !== '' ? String(prodRaw).trim() : '',
      commodity_id: commRaw != null && String(commRaw).trim() !== '' ? String(commRaw).trim() : '',
      guarantee: gd.guarantee || '',
      deliveryTime: gd.deliveryTime || '',
    };
    if (geShelfDebugEnabled()) {
      const tn = row.title ? String(row.title).slice(0, 48) : '';
      if (shelfTime) {
        geShelfDebugLog(
          'row built: ' + tn + ' | shelfTime=' + shelfTime + ' | scope=' + shelfEx.scope + ' | key=' + shelfEx.hitKey
        );
      } else {
        geShelfDebugLog('未命中上架时间: ' + tn, {
          product_id: row.product_id,
          promotion_id: row.promotion_id,
        });
      }
    }
    if (!row.title && !row.price && !row.link) return null;
    if (isBuyinNavMenuTitle(row.title)) return null;
    if (!buyinRowHasPlausibleProductSignals(row)) return null;
    return normalizeBuyinRowDefaults(row);
  }

  function normalizeBuyinPromotionShape(el) {
    if (!el || typeof el !== 'object') return null;
    if (el.base_model && el.base_model.product_info) return el;
    if (el.product_info && typeof el.product_info === 'object') {
      return {
        product_id: el.product_id ?? el.productId,
        promotion_id: el.promotion_id ?? el.promotionId,
        base_model: {
          product_info: el.product_info,
          promotion_info: el.promotion_info,
          marketing_info: el.marketing_info,
          shop_info: el.shop_info,
          author_info: el.author_info,
        },
        shop_info: el.shop_info,
        author_info: el.author_info,
        anchor_info: el.anchor_info,
      };
    }
    if (el.detail_url || el.name || el.title || el.product_name) {
      return { base_model: { product_info: el } };
    }
    return null;
  }

  function buyinItemLooksLikeProduct(x) {
    if (!x || typeof x !== 'object') return false;
    if (x.base_model?.product_info && (x.base_model.product_info.name || x.base_model.product_info.detail_url)) return true;
    if (x.product_info && (x.product_info.name || x.product_info.detail_url)) return true;
    if ((x.name || x.title || x.product_name) && (x.detail_url || x.url)) return true;
    return false;
  }

  function deepCollectBuyinProductArrays(obj, out, seen, depth) {
    if (!obj || typeof obj !== 'object' || depth > 14) return;
    if (seen.has(obj)) return;
    seen.add(obj);
    if (Array.isArray(obj)) {
      if (obj.length > 0 && obj.length <= 2500 && buyinItemLooksLikeProduct(obj[0])) {
        out.push(obj);
      }
      const step = obj.length > 80 ? 20 : 1;
      for (let i = 0; i < obj.length; i += step) {
        const x = obj[i];
        if (x && typeof x === 'object') deepCollectBuyinProductArrays(x, out, seen, depth + 1);
      }
      return;
    }
    for (const v of Object.values(obj)) {
      if (v && typeof v === 'object') deepCollectBuyinProductArrays(v, out, seen, depth + 1);
    }
  }

  function ingestBuyinAlternateJson(json) {
    const arrays = [];
    deepCollectBuyinProductArrays(json, arrays, new WeakSet(), 0);
    const rows = [];
    const arrSeen = new WeakSet();
    for (const arr of arrays) {
      if (arrSeen.has(arr)) continue;
      arrSeen.add(arr);
      for (const el of arr) {
        const p = normalizeBuyinPromotionShape(el);
        if (!p) continue;
        const row = parseBuyinPromotionItem(p);
        if (row) rows.push(row);
      }
    }
    return rows;
  }

  function buyinArrayLooksLikePromotionList(arr) {
    if (!Array.isArray(arr) || !arr.length) return false;
    const x = arr[0];
    if (!x || typeof x !== 'object') return false;
    if (x.base_model && x.base_model.product_info) return true;
    if (x.product_id != null || x.promotion_id != null) return true;
    if (x.product_info && typeof x.product_info === 'object') return true;
    return false;
  }

  function buyinRawItemToPromotionP(item) {
    if (!item || typeof item !== 'object') return null;
    if (item.base_model && item.base_model.product_info) return item;
    const n = normalizeBuyinPromotionShape(item);
    return n || item;
  }

  function ingestBuyinFromJson(json) {
    if (json?.data?.summary_promotions?.length) {
      return (json.data.summary_promotions || []).map(parseBuyinPromotionItem).filter(Boolean);
    }
    if (json?.summary_promotions?.length) {
      return (json.summary_promotions || []).map(parseBuyinPromotionItem).filter(Boolean);
    }
    const d = json?.data;
    if (d && typeof d === 'object') {
      const listKeys = ['list', 'records', 'items', 'product_list', 'promotions', 'materials', 'rows'];
      for (let i = 0; i < listKeys.length; i++) {
        const arr = d[listKeys[i]];
        if (Array.isArray(arr) && arr.length && buyinArrayLooksLikePromotionList(arr)) {
          const rows = [];
          for (let j = 0; j < arr.length; j++) {
            const p = buyinRawItemToPromotionP(arr[j]);
            if (!p) continue;
            const row = parseBuyinPromotionItem(p);
            if (row) rows.push(row);
          }
          if (rows.length) return rows;
        }
      }
    }
    return ingestBuyinAlternateJson(json);
  }

  function pickBetterBuyinProductLink(a, b) {
    const A = (a || '').trim();
    const B = (b || '').trim();
    const vA = buyinProductLinkLooksValid(A);
    const vB = buyinProductLinkLooksValid(B);
    if (vA && vB) return A || B;
    if (vA) return A;
    if (vB) return B;
    return '';
  }

  function mergeBuyinCapturedRows(data) {
    const keySet = new Set(buyinData.map(buyinRowDedupeKey).filter(Boolean));
    data.forEach((x) => {
      if (isBuyinNavMenuTitle(x.title)) return;
      if (!buyinRowHasPlausibleProductSignals(x)) return;
      const key = buyinRowDedupeKey(x);
      if (!key) return;
      if (!keySet.has(key)) {
        keySet.add(key);
        buyinData.push(normalizeBuyinRowDefaults(x));
        return;
      }
      const idx = buyinData.findIndex((r) => buyinRowDedupeKey(r) === key);
      if (idx < 0) return;
      const cur = buyinData[idx];
      const curL = (cur.link || '').trim();
      const xL = (x.link || '').trim();
      buyinData[idx] = {
        ...cur,
        shopName: cur.shopName || x.shopName || '',
        shopLink: cur.shopLink || x.shopLink || '',
        platform: (cur.platform || '').trim() || (x.platform || '').trim() || '',
        link: pickBetterBuyinProductLink(curL, xL),
        imgSrc: cur.imgSrc || x.imgSrc || '',
        title: (cur.title || '').trim() || (x.title || '').trim() || '',
        commission: (cur.commission || '').trim() || (x.commission || '').trim() || '',
        sales: (cur.sales != null && String(cur.sales).trim()) || (x.sales != null && String(x.sales).trim()) || '',
        guarantee: (cur.guarantee || '').trim() || (x.guarantee || '').trim() || '',
        deliveryTime: (cur.deliveryTime || '').trim() || (x.deliveryTime || '').trim() || '',
        shopTag: (x.shopTag || '').trim() || (cur.shopTag || '').trim() || '',
        monitorTimes:
          (x.monitorTimes != null && String(x.monitorTimes).trim()) ||
          (cur.monitorTimes != null && String(cur.monitorTimes).trim()) ||
          '',
        monitorIndex:
          x.monitorIndex != null && x.monitorIndex !== ''
            ? x.monitorIndex
            : cur.monitorIndex != null && cur.monitorIndex !== ''
              ? cur.monitorIndex
              : '',
        inspectCycle:
          x.inspectCycle != null && String(x.inspectCycle).trim()
            ? x.inspectCycle
            : cur.inspectCycle != null && String(cur.inspectCycle).trim()
              ? cur.inspectCycle
              : '',
        shelfTime: (function () {
          const a = geShelfTimeSortTs(cur.shelfTime);
          const b = geShelfTimeSortTs(x.shelfTime);
          if (b >= a && (x.shelfTime || '').trim()) return x.shelfTime;
          if ((cur.shelfTime || '').trim()) return cur.shelfTime;
          return x.shelfTime || cur.shelfTime || '';
        })(),
        promotion_id: (cur.promotion_id || '').trim() || (x.promotion_id || '').trim() || '',
        product_id: (cur.product_id || '').trim() || (x.product_id || '').trim() || '',
        commodity_id: (cur.commodity_id || '').trim() || (x.commodity_id || '').trim() || '',
      };
    });
  }

  function persistBuyinApiJsonForReplay(json) {
    if (!json || typeof json !== 'object') return;
    try {
      if (!isBuyin()) return;
      const s = JSON.stringify(json);
      if (s.length < 80 || s.length > GE_BUYIN_LAST_API_MAX) return;
      if (!/summary_promotion|product_id|base_model|"list"\s*:/i.test(s)) return;
      sessionStorage.setItem(GE_BUYIN_LAST_API_KEY, s);
    } catch (_) {}
  }

  function tryReingestBuyinLastApiFromStorage() {
    if (!isBuyin()) return;
    try {
      const raw = sessionStorage.getItem(GE_BUYIN_LAST_API_KEY);
      if (!raw || raw.length < 80) return;
      const j = JSON.parse(raw);
      const data = ingestBuyinFromJson(j);
      if (data.length) mergeBuyinCapturedRows(data);
    } catch (_) {}
  }

  /** 同一标题+价格下合并「无链」与「有链」两条（与 3.4.5 仅按 link 去重时的差异补偿） */
  function consolidateBuyinRowsByTitlePrice(rows) {
    if (!rows || rows.length < 2) return rows;
    const keyOf = (r) => {
      const t = (r.title || '').trim().slice(0, 160);
      const p = (r.price || '').trim();
      const tag = (r.shopTag || '').trim();
      const mi = r.monitorIndex != null && r.monitorIndex !== '' ? String(r.monitorIndex) : '';
      const idKey =
        String(r.promotion_id || '').trim() ||
        String(r.product_id || '').trim() ||
        String(r.commodity_id || '').trim() ||
        (String(r.link || '').trim() ? String(r.link || '').trim().slice(0, 200) : '');
      return t + '\u0001' + p + '\u0001' + tag + '\u0001' + mi + '\u0001' + idKey;
    };
    const map = new Map();
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const k = keyOf(r);
      const prev = map.get(k);
      if (!prev) {
        map.set(k, { ...r });
        continue;
      }
      const pl = (prev.link || '').trim();
      const rl = (r.link || '').trim();
      map.set(k, {
        ...prev,
        link: pickBetterBuyinProductLink(pl, rl),
        imgSrc: prev.imgSrc || r.imgSrc || '',
        shopName: prev.shopName || r.shopName || '',
        shopLink: prev.shopLink || r.shopLink || '',
        platform: (prev.platform || '').trim() || (r.platform || '').trim() || '',
        commission: (prev.commission || '').trim() || (r.commission || '').trim() || '',
        sales: (function () {
          const ps = prev.sales != null && String(prev.sales).trim() ? String(prev.sales).trim() : '';
          const rs = r.sales != null && String(r.sales).trim() ? String(r.sales).trim() : '';
          if (!ps) return rs;
          if (!rs) return ps;
          const np = geParseSalesToNumber(ps);
          const nr = geParseSalesToNumber(rs);
          if (np != null && nr != null) return nr >= np ? rs : ps;
          if (nr != null) return rs;
          return ps;
        })(),
        guarantee: (prev.guarantee || '').trim() || (r.guarantee || '').trim() || '',
        deliveryTime: (prev.deliveryTime || '').trim() || (r.deliveryTime || '').trim() || '',
        shopTag: (prev.shopTag || '').trim() || (r.shopTag || '').trim() || '',
        monitorTimes:
          (r.monitorTimes != null && String(r.monitorTimes).trim()) ||
          (prev.monitorTimes != null && String(prev.monitorTimes).trim()) ||
          '',
        monitorIndex:
          r.monitorIndex != null && r.monitorIndex !== ''
            ? r.monitorIndex
            : prev.monitorIndex != null && prev.monitorIndex !== ''
              ? prev.monitorIndex
              : '',
        inspectCycle:
          r.inspectCycle != null && String(r.inspectCycle).trim()
            ? r.inspectCycle
            : prev.inspectCycle != null && String(prev.inspectCycle).trim()
              ? prev.inspectCycle
              : '',
        shelfTime: (function () {
          const a = geShelfTimeSortTs(prev.shelfTime);
          const b = geShelfTimeSortTs(r.shelfTime);
          if (b >= a && (r.shelfTime || '').trim()) return r.shelfTime;
          if ((prev.shelfTime || '').trim()) return prev.shelfTime;
          return r.shelfTime || prev.shelfTime || '';
        })(),
        promotion_id: (prev.promotion_id || '').trim() || (r.promotion_id || '').trim() || '',
        product_id: (prev.product_id || '').trim() || (r.product_id || '').trim() || '',
        commodity_id: (prev.commodity_id || '').trim() || (r.commodity_id || '').trim() || '',
      });
    }
    return [...map.values()];
  }

  function parseBuyin(res) {
    const list = res?.data?.summary_promotions || res?.summary_promotions || [];
    return list.map(parseBuyinPromotionItem).filter(Boolean);
  }

  /** 店铺列表接口 mtop.taobao.shop.simple.fetch：正文在 data.itemInfoDTO.data[] */
  function ingestItemInfoDtoData(json, results, seen) {
    const arr = json?.data?.itemInfoDTO?.data;
    if (!Array.isArray(arr) || !arr.length) return 0;
    let n = 0;
    for (const item of arr) {
      if (!item || typeof item !== 'object') continue;
      const d = parseTaobaoItem(item);
      if (!(d.title || d.price)) continue;
      const key = d.link || (d.title + '|' + d.price);
      if (seen.has(key)) continue;
      seen.add(key);
      results.push(d);
      n++;
    }
    return n;
  }

  function parseTaobaoItem(item) {
    const id = item.item_id || item.itemId || item.nid || item.id || item.num_iid;
    // 商品链优先 itemUrl（整页主链）；无有效绝对链时再考虑 skuInfoList[].itemSkuUrl / id 拼链
    let link = item.itemUrl || item.item_url || item.url || (id ? 'https://item.taobao.com/item.htm?id=' + id : '') || item.link || '';
    const skuUrl = item.skuInfoList?.[0]?.itemSkuUrl;
    const finalLink = /^https?:\/\//i.test(link) ? link : (skuUrl && /^https?:\/\//i.test(skuUrl) ? skuUrl : link);
    const price = item.price ?? item.view_price ?? item.reserve_price ?? '';
    const sales = item.sold ?? item.sales ?? item.view_sales ?? item.vagueSold365 ?? '';
    const priceEncoded = item.priceEncoded && item.discountPrice;
    const priceStr = priceEncoded ? '' : (typeof price === 'number' ? '¥' + price : (price ? '¥' + String(price).replace(/[^\d.]/g, '') : ''));
    let outLink = finalLink || '';
    if (outLink.startsWith('//')) outLink = 'https:' + outLink;
    else if (outLink && !/^https?:/i.test(outLink)) outLink = 'https:' + outLink;
    return {
      title: (item.title || item.raw_title || item.name || '').replace(/\s+/g, ' ').trim().slice(0, 200),
      price: priceStr,
      commission: item.commission || item.commissionRate || '',
      sales: String(sales || '').replace(/[^0-9万千+]/g, ''),
      link: outLink.split('&mi_id=')[0],
      imgSrc: item.image_url || item.pic_url || item.img || item.image || item.picUrl || '',
      shopName: '',
      shopLink: '',
      shopTag: '',
      monitorTimes: '',
      monitorIndex: '',
      guarantee: '',
      deliveryTime: '',
    };
  }

  function findTaobaoInWindow() {
    const results = [];
    const seen = new Set(taobaoData.map((x) => x.link || (x.title + '|' + x.price)));
    const targets = ['__INITIAL_STATE__', 'g_initialData', 'pageData', '__NUXT_DATA__', '__PRELOADED_STATE__'];

    for (const k of targets) {
      try {
        const o = window[k];
        if (o && typeof o === 'object') extractTaobao(o, results, seen);
      } catch (_) {}
    }
    results.forEach((x) => {
      const key = x.link || (x.title + '|' + x.price);
      if (!seen.has(key)) {
        seen.add(key);
        taobaoData.push(x);
      }
    });
    if (results.length > 0) console.log('[导出] 从页面数据层解析到 ' + results.length + ' 个商品');
    return results.length;
  }

  function extractTaobao(json, results, seen) {
    if (!json || typeof json !== 'object') return;
    function isItem(x) {
      return x && (x.title || x.raw_title || x.name) && (x.item_id || x.itemId || x.nid || x.id || x.num_iid || x.item_url || x.itemUrl || x.url || x.link || x.price || x.skuInfoList?.length);
    }
    function addItem(d) {
      const key = d.link || (d.title + '|' + d.price);
      if (!seen.has(key)) { seen.add(key); results.push(d); }
    }
    if (Array.isArray(json)) {
      json.forEach((x) => {
        if (isItem(x)) {
          const d = parseTaobaoItem(x);
          if (d.title || d.price) addItem(d);
        }
        extractTaobao(x, results, seen);
      });
      return;
    }
    const keys = ['mainData', 'items', 'item', 'itemList', 'list', 'data', 'result', 'auctions', 'itemListData', 'itemInfoDTO'];
    for (const k of keys) {
      let v = json[k];
      if (k === 'item' && v && !Array.isArray(v) && isItem(v)) v = [v];
      if (k === 'itemInfoDTO' && v?.data && Array.isArray(v.data) && v.data.length && isItem(v.data[0])) {
        v.data.forEach((x) => {
          const d = parseTaobaoItem(x);
          if (d.title || d.price) addItem(d);
        });
        return;
      }
      if (Array.isArray(v) && v.length && isItem(v[0])) {
        v.forEach((x) => {
          const d = parseTaobaoItem(x);
          if (d.title || d.price) addItem(d);
        });
        return;
      }
    }
    for (const v of Object.values(json)) extractTaobao(v, results, seen);
  }

  function toAbsUrl(u, win) {
    if (!u) return '';
    try {
      if (u.startsWith('//')) return (win || window).location.protocol + u;
      if (u.startsWith('/')) return (win || window).location.origin + u;
      if (u.startsWith('http')) return u;
      return new URL(u, (win || window).location.href).href;
    } catch { return u || ''; }
  }

  function extractTaobaoItemIdFromHref(h) {
    if (!h || typeof h !== 'string') return '';
    const x = h.trim();
    const m = x.match(/[?&]id=(\d{8,})(?:&|#|$)/i) || x.match(/[?&]id=(\d{8,})/i) || x.match(/\/item\/(\d{8,})(?:[^\d]|$)/i) || x.match(/\/i\/(\d{8,})(?:[^\d]|$)/i) || x.match(/item\.htm[^"'\s]*[?&]id=(\d{8,})/i);
    return m ? m[1] : '';
  }

  function collectFromDocFallback(doc, win, results, seen) {
    const d = doc || document;
    const shopName = d.querySelector('[class*="shopName"], [class*="shop-name"], [class*="ShopName"]')?.textContent?.trim() || d.querySelector('meta[property="og:title"]')?.content?.replace(/-淘宝网|_淘宝网/, '').trim() || d.title?.replace(/-淘宝网|_淘宝网/, '').trim() || '';
    const shopLink = /shop\d+\.(taobao|world\.taobao)\.com/.test(location.hostname) ? location.href.split('?')[0] : '';

    function getText(el) { return (el?.innerText || el?.textContent || '').replace(/\s+/g, ' ').trim(); }
    function getTextWithShadow(el, depth) {
      if (!el || (depth || 0) > 5) return '';
      depth = (depth || 0) + 1;
      let t = el.innerText || el.textContent || '';
      if (el.shadowRoot) {
        try {
          t = (el.shadowRoot.innerText || el.shadowRoot.textContent || '') + ' ' + t;
          el.shadowRoot.querySelectorAll('*').forEach((c) => { t += ' ' + getTextWithShadow(c, depth); });
        } catch (_) {}
      }
      return t.replace(/\s+/g, ' ').trim();
    }
    function queryIncludingShadow(root, sel, depth) {
      const out = [];
      if ((depth || 0) > 3) return out;
      try {
        root.querySelectorAll(sel).forEach((el) => out.push(el));
        root.querySelectorAll('*').forEach((el) => {
          if (el.shadowRoot) queryIncludingShadow(el.shadowRoot, sel, (depth || 0) + 1).forEach((e) => out.push(e));
        });
      } catch (_) {}
      return out;
    }
    function isProductLink(h) {
      const x = (h || '').trim();
      if (extractTaobaoItemIdFromHref(x)) return true;
      return /item\.(taobao|world\.taobao)\.com/i.test(x) || /detail\.(tmall|tmall\.hk)\.com/i.test(x) || /shop\d+\.(taobao|world\.taobao)\.com.*[?&]id=\d{8,}/i.test(x) || /\/item\.htm\?/i.test(x) || /(click|uland)\.taobao\.com.*id=\d{8,}/i.test(x) || /(m|h5)\.(taobao|tmall)\.com.*(?:item|id=\d{8,})/i.test(x);
    }
    function isCouponLike(text) {
      return !text ? true : /满\d+可用|会员专享|全店通用|领取\s*$|^\s*¥\d{1,2}\s+满/im.test(text) || /\d{4}\.\d{2}\.\d{2}\s*[-–]\s*\d{4}\.\d{2}\.\d{2}/.test(text);
    }
    function isLikelyProductCard(el) {
      const text = getText(el);
      if (!text || text.length < 15) return false;
      if (/arrowUp|arrowDown|店铺权益|加入店铺会员|满减|消费券/i.test(text)) return false;
      if (/综合排序|^(销量|新品|价格)\s*$|销量\s+新品\s+价格\s+综合/.test(text)) return false;
      if (isCouponLike(text)) return false;
      const hasPrice = /[¥￥]\s*\d+/.test(text);
      const hasImg = !!el.querySelector('img');
      const hasPriceEl = !!el.querySelector('[class*="price"], span.text-price, [class*="priceContainer"]');
      const hasSales = /人付款|人购买|已售|销量/.test(text);
      return hasImg && (hasPrice || hasPriceEl || hasSales);
    }

    function queryAll(root) {
      const out = [];
      try {
        root.querySelectorAll('div, li, dl, section, article, ul > li').forEach((el) => out.push(el));
        root.querySelectorAll('*').forEach((el) => {
          if (el.shadowRoot) queryAll(el.shadowRoot).forEach((e) => out.push(e));
        });
      } catch (_) {}
      return out;
    }
    const base = queryAll(d);
    const extra = d.querySelectorAll('[class*="itemCard"], [class*="item-card"], [class*="Card--"], [class*="Item--"], [class*="productCard"], .productItemTop');
    const all = [...new Set([...base, ...extra])];
    let candidates = all.filter(isLikelyProductCard);
    let filtered = candidates.filter((c) => !candidates.some((other) => other !== c && c.contains && c.contains(other)));
    if (filtered.length === 0 && candidates.length > 0) filtered = candidates;
    if (filtered.length === 0) {
      candidates = all.filter((el) => {
        const t = getText(el);
        return t && t.length >= 15 && el.querySelector('img') && !isCouponLike(t) && !/arrowUp|店铺权益|满减|消费券|综合排序|销量\s+新品\s+价格/i.test(t);
      });
      filtered = candidates.filter((c) => !candidates.some((other) => other !== c && c.contains && c.contains(other)));
      if (filtered.length === 0 && candidates.length > 0) filtered = candidates;
    }

    for (const card of filtered) {
      try {
      const text = getText(card);

      let price = '';
      const priceEls = queryIncludingShadow(card, 'span.text-price, [class*="priceContainer"], [class*="price--"], [class*="text-price"]');
      const priceEl = priceEls.find((el) => {
        const t = getTextWithShadow(el) || el.getAttribute('data-price') || '';
        const m = t.match(/(\d{1,6}(?:\.\d{1,2})?)/);
        return m && parseFloat(m[1]) < 100000 && parseFloat(m[1]) >= 0.01;
      }) || priceEls[0] || card.querySelector('[class*="price"]');
      if (priceEl) {
        const dataPrice = priceEl.getAttribute('data-price') || priceEl.closest('[data-price]')?.getAttribute('data-price') || '';
        if (/^\d{1,6}(?:\.\d{1,2})?$/.test(dataPrice)) price = '¥' + dataPrice;
        if (!price) {
          const priceText = getTextWithShadow(priceEl);
          const pm = priceText.match(/[¥￥]?\s*(\d{1,6}(?:\.\d{1,2})?)/);
          if (pm && !/^\d{4,}$/.test(pm[1]) && parseFloat(pm[1]) < 100000) price = '¥' + pm[1];
        }
        if (!price) {
          const ariaLabel = priceEl.getAttribute('aria-label') || priceEl.closest('[aria-label]')?.getAttribute('aria-label') || '';
          const am = ariaLabel.match(/[¥￥]?\s*(\d{1,6}(?:\.\d{1,2})?)/);
          if (am && parseFloat(am[1]) < 100000) price = '¥' + am[1];
        }
        if (!price) {
          for (const img of queryIncludingShadow(priceEl, 'img, canvas')) {
            const alt = (img.getAttribute('alt') || img.getAttribute('title') || '').trim();
            const am = alt.match(/(\d{1,6}(?:\.\d{1,2})?)/);
            if (am && parseFloat(am[1]) < 100000 && parseFloat(am[1]) >= 0.01) { price = '¥' + am[1]; break; }
            const src = (img.getAttribute('src') || img.getAttribute('data-src') || '').trim();
            const sm = src.match(/(?:price|amount)[_=]?(\d{1,6}(?:\.\d{1,2})?)/i) || src.match(/_(\d{2,4})\.(?:png|jpg|webp)/);
            if (sm && parseFloat(sm[1]) < 100000) { price = '¥' + sm[1]; break; }
          }
        }
      }
      if (!price) {
        const priceBeforeSales = text.match(/[¥￥]\s*(\d{1,6}(?:\.\d{1,2})?)\s+\d+(?:\.\d+)?[万千]?\+?\s*人/);
        if (priceBeforeSales) price = '¥' + priceBeforeSales[1];
      }
      if (!price) {
        const priceCandidates = [...text.matchAll(/[¥￥]\s*(\d{1,6}(?:\.\d{1,2})?)/g)];
        for (const m of priceCandidates) {
          const rest = text.slice((m.index || 0) + m[0].length);
          if (/^\s*\+?\s*人/.test(rest)) continue;
          if (/^\s*\d{4,}/.test(rest) || parseFloat(m[1]) >= 10000) continue;
          price = '¥' + m[1];
          break;
        }
        if (!price && priceCandidates.length) price = '¥' + priceCandidates[0][1];
      }

      let sales = '';
      const salesEls = queryIncludingShadow(card, '[class*="count"], [class*="count--"], [class*="deal-cnt"]');
      const salesEl = salesEls.find((el) => (getTextWithShadow(el) || '').match(/\d+[万千]?\+?/)) || salesEls[0] || card.querySelector('[class*="count"]');
      if (salesEl) {
        const salesText = getTextWithShadow(salesEl);
        const sm = salesText.match(/(\d+(?:\.\d+)?[万千]?\+?)/);
        if (sm) sales = sm[1];
      }
      if (!sales) {
        const salesMatch = text.match(/(\d+(?:\.\d+)?[万千]?\+?)\s*(?:人付款|付款|已售|销量)/) || text.match(/(?:人付款|付款|已售|销量)\s*(\d+(?:\.\d+)?[万千]?\+?)/);
        sales = salesMatch ? salesMatch[1] : '';
      }

      const imgs = [...card.querySelectorAll('img')];
      let bestLink = '';
      const findItemId = (str) => {
        if (!str) return '';
        const fromHref = extractTaobaoItemIdFromHref(str);
        if (fromHref) return fromHref;
        const m = str.match(/itemId[=:"](\d{8,})/i) || str.match(/item_id[=:"](\d{8,})/i);
        return m ? m[1] : '';
      };
      const parentA = card.closest('a[href]');
      const collectLinks = () => {
        const out = [];
        const addLink = (a) => { const h = (a?.getAttribute?.('href') || a?.href || '').trim(); if (h && !/^#|javascript:/i.test(h)) out.push(toAbsUrl(h, win)); };
        if (parentA) addLink(parentA);
        card.querySelectorAll('a[href]').forEach(addLink);
        queryIncludingShadow(card, 'a[href]').forEach(addLink);
        let p = card;
        for (let depth = 0; depth < 8 && p; depth++) {
          try {
            if (p.matches && p.matches('a[href]')) addLink(p);
            p.querySelectorAll && p.querySelectorAll('a[href]').forEach(addLink);
          } catch (_) {}
          p = p.parentElement;
        }
        for (const img of imgs) {
          const a = img.closest('a[href]');
          if (a) out.push(toAbsUrl(a.getAttribute('href') || a.href || '', win));
        }
        for (const el of [card, ...card.querySelectorAll('[data-id], [data-item-id], [data-nid]')]) {
          const id = el.getAttribute('data-id') || el.getAttribute('data-item-id') || el.getAttribute('data-nid') || '';
          if (/^\d{8,}$/.test(id)) out.push('https://item.taobao.com/item.htm?id=' + id);
        }
        const dataEl = card.querySelector('[data-href], [data-url], [data-link]');
        if (dataEl) {
          const dh = dataEl.getAttribute('data-href') || dataEl.getAttribute('data-url') || dataEl.getAttribute('data-link') || '';
          if (dh) out.push(toAbsUrl(dh, win));
        }
        const idFromHtml = findItemId(card.outerHTML || '');
        if (idFromHtml) out.push('https://item.taobao.com/item.htm?id=' + idFromHtml);
        const norm = [];
        const seenH = new Set();
        for (const u of out) {
          const id = extractTaobaoItemIdFromHref(u);
          const canonical = id ? 'https://item.taobao.com/item.htm?id=' + id : u;
          if (canonical && !seenH.has(canonical)) { seenH.add(canonical); norm.push(canonical); }
        }
        return norm;
      };
      for (const h of collectLinks()) {
        if (isProductLink(h)) {
          bestLink = h;
          const id = extractTaobaoItemIdFromHref(h) || findItemId(h);
          if (id) bestLink = 'https://item.taobao.com/item.htm?id=' + id;
          break;
        }
      }
      if (!bestLink && parentA) {
        const h = toAbsUrl(parentA.getAttribute('href') || parentA.href || '', win);
        if (h && !/alicdn\.com\/.*\.(jpg|png|webp|gif)(\?|$)/i.test(h)) bestLink = h;
      }
      if (!bestLink) {
        for (const el of [card, ...card.querySelectorAll('[data-id],[data-item-id],[data-nid]')]) {
          const id = el.getAttribute?.('data-id') || el.getAttribute?.('data-item-id') || el.getAttribute?.('data-nid') || '';
          if (/^\d{8,}$/.test(id)) { bestLink = 'https://item.taobao.com/item.htm?id=' + id; break; }
        }
        if (!bestLink) {
          const html = card.outerHTML || '';
          const mid = html.match(/itemId["']?\s*[:=]\s*["']?(\d{8,})/i) || html.match(/item_id["']?\s*[:=]\s*["']?(\d{8,})/i) || html.match(/["']id["']\s*:\s*["']?(\d{8,})/i) || html.match(/(?:item\.htm|item\.taobao)[^"']*[?&]id=(\d{8,})/i) || html.match(/[?&]id=(\d{8,})/);
          if (mid) bestLink = 'https://item.taobao.com/item.htm?id=' + mid[1];
        }
      }

      let imgSrc = '';
      for (const img of imgs) {
        const src = img.getAttribute('data-src') || img.getAttribute('data-lazy-src') || img.getAttribute('src') || '';
        const abs = toAbsUrl(src, win);
        if (abs && !/\.(gif|svg)/i.test(abs)) { imgSrc = abs; break; }
      }

      let title = '';
      for (const a of card.querySelectorAll('a[title]')) {
        const t = a.getAttribute('title')?.trim();
        if (t && t.length >= 6 && !/[¥￥]\d/.test(t)) { title = t; break; }
      }
      if (!title) {
        for (const img of imgs) {
          const t = img.getAttribute('alt')?.trim();
          if (t && t.length >= 6 && !/[¥￥]\d/.test(t)) { title = t; break; }
        }
      }
      if (!title) {
        const lines = text.split(/\n|\r/).map((s) => s.trim()).filter(Boolean)
          .filter((s) => !/^[¥￥]\s*\d+/.test(s))
          .filter((s) => !/^\d+(?:\.\d+)?[万千]?\+?\s*(?:人付款|付款|已售|销量)?$/.test(s))
          .filter((s) => !/^(综合|销量|新品|价格|全部宝贝|导出商品CSV)$/.test(s))
          .filter((s) => !/^¥\s*[\d\s]+\s*使用$/.test(s));
        title = lines.find((s) => s.length >= 6 && !/^[\d\s¥￥]+$/.test(s)) || '';
      }
      title = (title || text)
        .replace(/\s*¥\s*[\d.万千]+\+?\s*人付款.*$/, '')
        .replace(/\s*¥\s*[\d.,]+\s*$/, '')
        .replace(/\s*¥\s*[\d\s]+\s*使用\s*$/, '')
        .replace(/^(综合|销量|新品|价格)\s+/, '')
        .replace(/\s*¥\s*\d+(?:\s+\d+)*\s*使用\s*$/, '')
        .trim().slice(0, 200);

      if (price && sales && price.replace(/[¥￥\s]/g, '') === sales.replace(/\+/, '')) {
        price = '';
      }
      if (!price && !title) continue;
      if (bestLink && !isProductLink(bestLink)) bestLink = '';
      if (!bestLink && !title) continue;
      if (isCouponLike(title || text)) continue;
      if (/^[\d\s¥￥]+$/.test(title) || /^\s*¥\s*[\d\s]+\s*使用\s*$/.test(title)) continue;
      if (/^综合排序$|^销量\s+新品\s+价格/.test((title || '').trim()) || /^(销量|新品|价格)\s*$/.test((title || '').trim())) continue;
      if (text && /^[\s]*销量\s+新品\s+价格\s+综合排序[\s]*$/.test(text.replace(/\s+/g, ' ').trim())) continue;
      const key = bestLink || (title + '|' + price + '|' + imgSrc);
      if (seen.has(key)) continue;
      seen.add(key);
      results.push({
        title,
        price,
        commission: '',
        sales,
        link: bestLink,
        imgSrc,
        shopName,
        shopLink,
        shopTag: '',
        monitorTimes: '',
        monitorIndex: '',
        guarantee: '',
        deliveryTime: '',
      });
      } catch (e) { if (filtered.indexOf(card) < 3) console.warn('[导出] 解析卡片异常:', e); }
    }
    if (filtered.length > 0) {
      if (results.length === 0) console.log('[导出] 兜底找到 ' + filtered.length + ' 个候选但未通过过滤');
      else if (results.length < filtered.length) console.log('[导出] 兜底找到 ' + filtered.length + ' 个候选，通过 ' + results.length + ' 个');
    }
  }

  function scrapeTaobaoDom() {
    const results = [];
    const seen = new Set();
    const pricePat = /[¥￥]\s*[\d.,]+/;
    const salesPat = /[\d.]+[万千+]?\+?\s*人[付购]款?/;

    const shopName = document.querySelector('[class*="shopName"], [class*="shop-name"], [class*="ShopName"], .shop-name, [class*="sellerNick"]')?.textContent?.trim() || document.querySelector('meta[property="og:title"]')?.content?.replace(/-淘宝网|_淘宝网/, '').trim() || document.title?.replace(/-淘宝网|_淘宝网/, '').trim() || '';
    const shopLink = /shop\d+\.(taobao|world\.taobao)\.com/.test(location.hostname) ? location.href.split('?')[0] : '';

    if (/shop\d+\.(taobao|world\.taobao)\.com/.test(location.hostname)) {
      collectFromDocFallback(document, window, results, seen);
      if (results.length > 0) {
        console.log('[导出] 店铺页兜底抓取到 ' + results.length + ' 个商品');
        return results;
      }
    }

    function tryCard(card, a) {
      let link = a ? (a.href || a.getAttribute('href') || '') : '';
      if (link && !link.startsWith('http')) link = 'https:' + link;
      const nid = extractTaobaoItemIdFromHref(link);
      if (nid) link = 'https://item.taobao.com/item.htm?id=' + nid;
      if (!link || (!/item\.(taobao|world\.taobao)|detail\.(tmall|tmall\.hk)/i.test(link) && !nid)) return;
      if (/alicdn\.com|\.jpg|\.png|\.webp|imgextra/i.test(link)) return;
      const root = card || a;
      const titleEl = root.querySelector('[class*="titleContainer"], [class*="title"], [class*="Title"], [class*="desc"], .desc');
      const title = titleEl ? (titleEl.textContent || '').trim().slice(0, 200) : '';
      const priceEl = root.querySelector('[class*="price"], [class*="Price"], [class*="priceContainer"]');
      let rawText = priceEl ? (priceEl.textContent || '').replace(/\s/g, '') : '';
      if (!rawText) rawText = (root.textContent || '').replace(/\s/g, '');
      const priceMatch = rawText.match(pricePat);
      const price = priceMatch ? priceMatch[0] : '';
      const salesMatch = rawText.match(salesPat);
      const sales = salesMatch ? salesMatch[0].replace(/\s/g, '') : '';
      const img = root.querySelector('img');
      const imgSrc = img ? (img.src || img.getAttribute('data-src') || '') : '';
      if ((title || price) && !seen.has(link)) {
        seen.add(link);
        results.push({
          title,
          price,
          commission: '',
          sales,
          link,
          imgSrc,
          shopName,
          shopLink,
          shopTag: '',
          monitorTimes: '',
          monitorIndex: '',
          guarantee: '',
          deliveryTime: '',
        });
      }
    }

    const cards = document.querySelectorAll(
      '[class*="cardContainer"], [class*="CardContainer"], [class*="itemCard"], [data-item], .item, [class*="Item--"], [class*="Card--"], [class*="item"][class*="J_MouserOnverReq"], [class*="contentInner"], [class*="Item--mainPicAndDesc"], [class*="Card--mainPicAndDesc"]'
    );
    for (const card of cards) {
      const anchors = card.querySelectorAll('a[href]');
      let bestA = null;
      for (const aa of anchors) {
        const l = aa.href || aa.getAttribute('href') || '';
        if (l && (extractTaobaoItemIdFromHref(l) || /item\.(taobao|world\.taobao)|detail\.tmall|item\.htm|id=\d{8,}/.test(l)) && !/alicdn|\.jpg|\.png|\.webp/i.test(l)) {
          bestA = aa;
          break;
        }
      }
      if (!bestA) {
        const a = card.querySelector('a[href*="item"], a[href*="detail"]') || card.closest('a[href]');
        if (a) {
          const l = a.href || a.getAttribute('href') || '';
          if (l && !/alicdn|\.jpg|\.png|\.webp/i.test(l)) bestA = a;
        }
      }
      tryCard(card, bestA);
    }

    if (results.length === 0) {
      const productImgs = document.querySelectorAll('img[src*="alicdn"], img[src*="imgextra"], img[data-src*="alicdn"]');
      for (const img of productImgs) {
        const card = img.closest('[class*="card"], [class*="Card"], [class*="item"], [class*="Item"], [class*="container"], [class*="Container"]') || img.parentElement?.parentElement;
        const anchors = card ? card.querySelectorAll('a[href*="item"], a[href*="detail"], a[href*="id="]') : [];
        let bestA = null;
        for (const aa of anchors) {
          let l = aa.href || aa.getAttribute('href') || '';
          if (l && !l.startsWith('http')) l = 'https:' + l;
          if (l && (extractTaobaoItemIdFromHref(l) || /item\.(taobao|world\.taobao)|detail\.tmall|item\.htm|id=\d{8,}/.test(l)) && !/alicdn|\.jpg|\.png|\.webp/i.test(l)) {
            bestA = aa;
            break;
          }
        }
        if (!bestA) {
          const a = img.closest('a[href]');
          if (a) {
            const l = a.href || a.getAttribute('href') || '';
            if (l && !/alicdn|\.jpg|\.png|\.webp/i.test(l) && (extractTaobaoItemIdFromHref(l) || /item|detail|id=\d{8,}/.test(l))) bestA = a;
          }
        }
        if (bestA) tryCard(card || img.parentElement?.parentElement, bestA);
      }
    }

    if (results.length === 0) {
      const allLinks = document.querySelectorAll('a[href]');
      for (const a of allLinks) {
        let link = a.href || a.getAttribute('href') || '';
        if (link && !link.startsWith('http')) link = 'https:' + link;
        const aid = extractTaobaoItemIdFromHref(link);
        if (aid) link = 'https://item.taobao.com/item.htm?id=' + aid;
        else if (!/item\.(taobao|world\.taobao)|detail\.(tmall|tmall\.hk)|item\.htm/i.test(link)) continue;
        if (!link || seen.has(link)) continue;
        if (/login|cart|logout|bought|sold|feedback|my_taobao/.test(link)) continue;
        let card = a.closest('[class*="card"], [class*="Card"], [class*="item"], [class*="Item"], [class*="container"], [class*="Container"]');
        if (!card) {
          let p = a.parentElement;
          for (let i = 0; i < 6 && p; i++) {
            card = p;
            if (p.querySelector('img') || pricePat.test(p.textContent || '')) break;
            p = p.parentElement;
          }
        }
        card = card || a.parentElement?.parentElement || a;
        tryCard(card, a);
      }
    }

    if (results.length === 0) {
      collectFromDocFallback(document, window, results, seen);
    }
    if (results.length === 0) {
      const iframes = document.querySelectorAll('iframe');
      for (let i = 0; i < iframes.length; i++) {
        try {
          const w = iframes[i].contentWindow;
          const d = iframes[i].contentDocument || (w && w.document);
          if (d && w) collectFromDocFallback(d, w, results, seen);
        } catch (_) {}
      }
    }

    return results;
  }

  function onResponse(json) {
    if (!json || typeof json !== 'object') return;
    if (isBuyin()) {
      const data = ingestBuyinFromJson(json);
      if (data.length) {
        persistBuyinApiJsonForReplay(json);
        if (window !== window.top) {
          window.parent.postMessage({ type: 'buyin_data', data: json }, '*');
          return;
        }
        mergeBuyinCapturedRows(data);
        if (window === window.top) console.log('[导出] 百应捕获 ' + data.length + ' 个，累计 ' + buyinData.length);
      }
      return;
    }
    if (isTaobao() && (json.data?.itemInfoDTO || json.data || json.result || json.mainData || json.items || json.item || json.items_onsale_get_response || json.item_search_shop_response)) {
      if (window !== window.top) {
        window.parent.postMessage({ type: 'taobao_data', data: json }, '*');
        return;
      }
      const results = [];
      const seen = new Set(taobaoData.map((x) => x.link || (x.title + '|' + x.price)));
      const fromDto = ingestItemInfoDtoData(json, results, seen);
      extractTaobao(json, results, seen);
      // ingest/extract 已向 seen 写入 key，不能再以 !seen.has(key) 判断是否写入 taobaoData
      if (results.length) taobaoData.push(...results);
      if (results.length > 0) {
        const tag = fromDto > 0 ? '（含 itemInfoDTO.data ' + fromDto + ' 条）' : '';
        console.log('[导出] 淘宝捕获 ' + results.length + ' 个' + tag + '，累计 ' + taobaoData.length);
      }
    }
  }

  function persistShopMtopPayload(str) {
    if (!str || typeof str !== 'string' || str.length < 80) return;
    if (!/itemInfoDTO/.test(str)) return;
    const slice = str.length > 4e6 ? str.slice(0, 4e6) : str;
    try { sessionStorage.setItem('ge_last_shop_mtop', slice); } catch (_) {}
    try { localStorage.setItem('ge_last_shop_mtop', slice); } catch (_) {}
    try {
      let ta = document.getElementById('ge-mtop-stash');
      if (!ta) {
        ta = document.createElement('textarea');
        ta.id = 'ge-mtop-stash';
        ta.setAttribute('readonly', 'readonly');
        ta.setAttribute('autocomplete', 'off');
        ta.style.cssText = 'position:fixed!important;left:-9999px!important;top:0;width:1px;height:1px;opacity:0;pointer-events:none';
        (document.body || document.documentElement).appendChild(ta);
      }
      ta.value = slice;
    } catch (_) {}
  }

  function tryParseJsonObject(obj) {
    if (obj && typeof obj === 'object' && obj.data && obj.data.itemInfoDTO) {
      try {
        const s = JSON.stringify(obj);
        persistShopMtopPayload(s);
      } catch (_) {}
    }
    if (obj && typeof obj === 'object') onResponse(obj);
  }

  function saveLastShopMtopRaw(str) {
    persistShopMtopPayload(str);
  }

  function tryReadStashFromDom() {
    try {
      const ta = document.getElementById('ge-mtop-stash');
      const v = ta && typeof ta.value === 'string' ? ta.value : '';
      if (v.length < 80 || !/itemInfoDTO/.test(v)) return false;
      tryParse(v);
      return taobaoData.length > 0;
    } catch (_) {
      return false;
    }
  }

  function tryIngestLastShopMtopFromStorage() {
    try {
      let raw = '';
      try { raw = sessionStorage.getItem('ge_last_shop_mtop') || ''; } catch (_) {}
      if (raw.length < 80) {
        try { raw = localStorage.getItem('ge_last_shop_mtop') || ''; } catch (_) {}
      }
      if (raw.length >= 80) {
        tryParse(raw);
        if (taobaoData.length > 0) return true;
      }
      return tryReadStashFromDom();
    } catch (_) {
      return tryReadStashFromDom();
    }
  }

  function tryParse(text) {
    if (text == null) return;
    if (typeof text === 'object' && !Array.isArray(text) && typeof text !== 'string') {
      tryParseJsonObject(text);
      return;
    }
    if (typeof text !== 'string' || !text.length) return;
    saveLastShopMtopRaw(text);
    const tryJson = (s) => {
      try {
        const j = JSON.parse(s);
        if (j && typeof j === 'object') onResponse(j);
        return true;
      } catch (_) {
        return false;
      }
    };
    let s = text.trim();
    if (tryJson(s)) return;
    const jsonpPatterns = [
      /^mtopjsonp\d*\s*\(\s*(\{[\s\S]*\})\s*\)\s*;?\s*$/i,
      /^\s*callback\s*\(\s*(\{[\s\S]*\})\s*\)\s*;?\s*$/i,
      /^\s*[\w$.]+\s*\(\s*(\{[\s\S]*\})\s*\)\s*;?\s*$/,
    ];
    for (const re of jsonpPatterns) {
      const m = s.match(re);
      if (m && tryJson(m[1].trim())) return;
    }
    const idx = s.indexOf('"itemInfoDTO"');
    if (idx === -1) return;
    let start = s.indexOf('{');
    if (start < 0 || start > idx) start = s.lastIndexOf('{', idx);
    if (start < 0) return;
    let depth = 0;
    let end = -1;
    for (let i = start; i < s.length; i++) {
      const c = s[i];
      if (c === '{') depth++;
      else if (c === '}') {
        depth--;
        if (depth === 0) {
          end = i;
          break;
        }
      }
    }
    if (end > start) tryJson(s.slice(start, end + 1));
  }

  function installPageNetworkBridge() {
    if (!isTaobao() || window.__geBridgeInstalled) return;
    const go = () => {
      if (!document.documentElement || document.documentElement.hasAttribute('data-ge-bridge')) return;
      try {
        const code = '(' + String(function () {
          try {
            if (window.__gePageNet) return;
            window.__gePageNet = 1;
            function gePush(t) {
              if (t == null) return;
              var str = '';
              if (typeof t === 'string') str = t;
              else if (typeof t === 'object') { try { str = JSON.stringify(t); } catch (e) { return; } }
              else return;
              if (!str.length || str.length > 5e6) return;
              try {
                if (str.indexOf('itemInfoDTO') !== -1) {
                  try {
                    var sl = str.length > 4e6 ? str.slice(0, 4e6) : str;
                    sessionStorage.setItem('ge_last_shop_mtop', sl);
                    localStorage.setItem('ge_last_shop_mtop', sl);
                    var ta = document.getElementById('ge-mtop-stash');
                    if (!ta) {
                      ta = document.createElement('textarea');
                      ta.id = 'ge-mtop-stash';
                      ta.setAttribute('readonly', 'readonly');
                      ta.style.cssText = 'position:fixed!important;left:-9999px!important;top:0;width:1px;height:1px;opacity:0;pointer-events:none';
                      (document.body || document.documentElement).appendChild(ta);
                    }
                    ta.value = sl;
                  } catch (e) {}
                }
                var key = 'ge_net_' + Date.now() + '_' + Math.random().toString(36).slice(2);
                sessionStorage.setItem(key, str);
                var el = document.createElement('i');
                el.setAttribute('data-ge-key', key);
                el.setAttribute('data-ge', '1');
                el.style.cssText = 'display:none!important;position:absolute;width:0;height:0;overflow:hidden';
                (document.documentElement || document.body).appendChild(el);
                setTimeout(function () {
                  try { sessionStorage.removeItem(key); el.remove(); } catch (e) {}
                }, 15000);
              } catch (e) {}
            }
            var of = window.fetch;
            window.fetch = function () {
              return of.apply(this, arguments).then(function (r) {
                try {
                  var c = r.clone();
                  c.text().then(gePush);
                } catch (e) {}
                return r;
              });
            };
            var os = XMLHttpRequest.prototype.send;
            XMLHttpRequest.prototype.send = function () {
              var xhr = this;
              xhr.addEventListener('load', function () {
                try {
                  if (xhr.responseType === 'json' && xhr.response != null && typeof xhr.response === 'object') {
                    gePush(xhr.response);
                  } else if (xhr.responseType === '' || xhr.responseType === 'text') {
                    gePush(xhr.responseText);
                  }
                } catch (e) {}
              });
              return os.apply(this, arguments);
            };
          } catch (e) {}
        }) + ')();';
        const root = document.documentElement || document.head || document;
        try {
          const s = document.createElement('script');
          s.textContent = code;
          root.appendChild(s);
          s.remove();
        } catch (e1) {
          try {
            const blob = new Blob([code], { type: 'application/javascript' });
            const u = URL.createObjectURL(blob);
            const s2 = document.createElement('script');
            s2.src = u;
            s2.onload = function () {
              try { URL.revokeObjectURL(u); } catch (e) {}
            };
            root.appendChild(s2);
          } catch (e2) {}
        }
        document.documentElement.setAttribute('data-ge-bridge', '1');
        window.__geBridgeInstalled = true;
      } catch (_) {}
    };
    go();
    if (!window.__geBridgeInstalled) {
      document.addEventListener('DOMContentLoaded', go);
      setTimeout(go, 0);
    }
  }

  if (isTaobao()) {
    installPageNetworkBridge();
    setInterval(function () {
      try {
        document.querySelectorAll('i[data-ge-key][data-ge]').forEach(function (el) {
          const key = el.getAttribute('data-ge-key');
          if (!key || key.indexOf('ge_net_') !== 0) return;
          el.removeAttribute('data-ge-key');
          try { el.remove(); } catch (_) {}
          try {
            const t = sessionStorage.getItem(key);
            sessionStorage.removeItem(key);
            if (t) tryParse(t);
          } catch (_) {}
        });
      } catch (_) {}
    }, 350);
  }

  /** 百应站内 fetch 多为相对路径 /api/...，必须 resolve 后才能判断是否 jinritemai 域 */
  function geResolveRequestUrl(url) {
    const s = String(url || '').trim();
    if (!s) return '';
    try {
      if (/^https?:\/\//i.test(s)) return s;
      if (typeof location !== 'undefined' && location.href) return new URL(s, location.href).href;
    } catch (_) {}
    return s;
  }

  /** list/material_list 等常不带 application/json；相对 URL 时旧逻辑会误判从而不读 body */
  function geIsBuyinPageFetchDataUrl(url) {
    try {
      const h = location.hostname || '';
      if (!/buyin\.jinritemai|jinritemai\.com/i.test(h)) return false;
      const u = geResolveRequestUrl(url);
      if (!u) return false;
      let host = '';
      try {
        host = new URL(u).hostname;
      } catch (_) {
        return false;
      }
      if (/jinritemai\.com$/i.test(host)) {
        const path = u.split('?')[0].toLowerCase();
        if (/\.(js|mjs|css|png|jpe?g|gif|webp|svg|ico|woff2?|ttf|eot|map|html|htm|wasm)(\?|$)/i.test(path)) return false;
        return true;
      }
      try {
        if (location.origin && u.indexOf(location.origin + '/') === 0) {
          const pathname = new URL(u).pathname;
          if (/^\/(api|compass|bff|gateway)\b/i.test(pathname)) return true;
          if (/\/(material|product|pickup|promotion|list)\b/i.test(pathname)) return true;
        }
      } catch (_) {}
      return false;
    } catch (_) {
      return false;
    }
  }

  function geBuyinReplaySnapshotKey(s) {
    return (s.method || 'GET') + '\0' + s.urlAbs + '\0' + (s.body || '');
  }

  function gePushBuyinReplaySnapshot(urlRaw, method, headersPlain, bodyStr) {
    if (!isBuyin() || !geIsBuyinPageFetchDataUrl(urlRaw)) return;
    const urlAbs = geResolveRequestUrl(urlRaw);
    if (!urlAbs) return;
    const snap = {
      urlAbs,
      method: (method || 'GET').toUpperCase(),
      headers: headersPlain && typeof headersPlain === 'object' ? { ...headersPlain } : {},
      body: typeof bodyStr === 'string' && bodyStr ? bodyStr : undefined,
    };
    const k = geBuyinReplaySnapshotKey(snap);
    for (let i = geBuyinReplayBuffer.length - 1; i >= 0; i--) {
      if (geBuyinReplaySnapshotKey(geBuyinReplayBuffer[i]) === k) {
        geBuyinReplayBuffer.splice(i, 1);
        break;
      }
    }
    geBuyinReplayBuffer.push(snap);
    while (geBuyinReplayBuffer.length > GE_BUYIN_REPLAY_MAX) geBuyinReplayBuffer.shift();
  }

  function geTryRecordBuyinFetchArgs(args) {
    if (!isBuyin()) return;
    const a0 = args[0];
    const a1 = args[1];
    try {
      if (typeof Request !== 'undefined' && a0 instanceof Request) {
        const req = a0;
        const u = req.url;
        if (!geIsBuyinPageFetchDataUrl(u)) return;
        const method = (req.method || 'GET').toUpperCase();
        const hdrs = {};
        try {
          req.headers.forEach(function (v, k) {
            hdrs[k] = v;
          });
        } catch (_) {}
        const rq = req.clone();
        void rq
          .text()
          .then(function (txt) {
            gePushBuyinReplaySnapshot(u, method, hdrs, txt && txt.length ? txt : undefined);
          })
          .catch(function () {
            gePushBuyinReplaySnapshot(u, method, hdrs, undefined);
          });
        return;
      }
      const urlRaw = typeof a0 === 'string' ? a0 : '';
      if (!urlRaw || !geIsBuyinPageFetchDataUrl(urlRaw)) return;
      const init = a1 && typeof a1 === 'object' && !Array.isArray(a1) ? a1 : {};
      const method = String(init.method || 'GET').toUpperCase();
      const hdrs = {};
      if (init.headers instanceof Headers) {
        try {
          init.headers.forEach(function (v, k) {
            hdrs[k] = v;
          });
        } catch (_) {}
      } else if (init.headers && typeof init.headers === 'object') {
        Object.assign(hdrs, init.headers);
      }
      const b = init.body;
      if (typeof FormData !== 'undefined' && b instanceof FormData) return;
      if (typeof Blob !== 'undefined' && b instanceof Blob) return;
      if (typeof ArrayBuffer !== 'undefined' && b instanceof ArrayBuffer) return;
      if (typeof ReadableStream !== 'undefined' && b instanceof ReadableStream) return;
      let bodyStr = undefined;
      if (typeof b === 'string') bodyStr = b;
      else if (typeof URLSearchParams !== 'undefined' && b instanceof URLSearchParams) bodyStr = b.toString();
      gePushBuyinReplaySnapshot(urlRaw, method, hdrs, bodyStr);
    } catch (_) {}
  }

  function geTryRecordBuyinXhrForReplay(xhr) {
    if (!isBuyin() || !xhr) return;
    const urlRaw = xhr.__geReqUrl || '';
    if (!geIsBuyinPageFetchDataUrl(urlRaw)) return;
    const b = xhr.__geSendBody;
    if (typeof FormData !== 'undefined' && b instanceof FormData) return;
    if (typeof Blob !== 'undefined' && b instanceof Blob) return;
    if (typeof ArrayBuffer !== 'undefined' && b instanceof ArrayBuffer) return;
    let bodyStr = undefined;
    if (typeof b === 'string') bodyStr = b;
    else if (typeof URLSearchParams !== 'undefined' && b instanceof URLSearchParams) bodyStr = b.toString();
    gePushBuyinReplaySnapshot(urlRaw, xhr.__geReqMethod, xhr.__geReqHeaders, bodyStr);
  }

  /**
   * 强缓存下 Response 常无正文（304 或 200 但 body 未附带），与 DevTools「Disable cache」效果类似：用原参数再 fetch 一次且 cache:no-store。
   * 必须用 origFetch，避免进入本脚本包装导致递归。
   */
  function geBuyinBuildNoStoreFetchArgs(fetchArgs) {
    const a0 = fetchArgs[0];
    const a1 = fetchArgs[1];
    try {
      if (typeof Request !== 'undefined' && a0 instanceof Request) {
        const merged =
          a1 && typeof a1 === 'object' && !Array.isArray(a1)
            ? { ...a1, cache: 'no-store' }
            : { cache: 'no-store' };
        return [new Request(a0, merged)];
      }
      const url = typeof a0 === 'string' ? a0 : a0 != null ? String(a0) : '';
      if (!url) return null;
      const init =
        a1 && typeof a1 === 'object' && !Array.isArray(a1)
          ? { ...a1, cache: 'no-store' }
          : { cache: 'no-store' };
      return [url, init];
    } catch (_) {
      return null;
    }
  }

  /** 店铺商品列表 POST（如 material_list）；仅此类 URL 合并新品排序参数，避免误改其它接口。 */
  function geIsBuyinShopProductListUrl(urlRaw) {
    const u = geResolveRequestUrl(String(urlRaw || ''));
    const low = u.toLowerCase();
    if (!low) return false;
    return /\/material_list\b/i.test(low) || /\/pc\/selection\/common\/material_list/i.test(low);
  }

  /**
   * 在 JSON POST 体上合并 order_by=create_time、scene、sort、size（与页面 PCShopDetailFeed 一致）。
   * 依赖 geBuyinNewestListRequestPatch；无 patch 时原样返回。
   */
  function geApplyBuyinNewestPatchToBody(bodyStr) {
    const patch = geBuyinNewestListRequestPatch;
    if (!patch || bodyStr == null || typeof bodyStr !== 'string' || !String(bodyStr).trim()) return bodyStr;
    let j;
    try {
      j = JSON.parse(bodyStr);
    } catch (_) {
      return bodyStr;
    }
    if (!j || typeof j !== 'object' || Array.isArray(j)) return bodyStr;

    const inject = function (target) {
      if (!target || typeof target !== 'object' || Array.isArray(target)) return;
      target.order_by = 'create_time';
      if (patch.scene != null) target.scene = patch.scene;
      target.sort = patch.sort != null ? patch.sort : GE_BUYIN_CREATE_TIME_SORT_DESC;
      target.size = patch.size;
      if (Object.prototype.hasOwnProperty.call(target, 'page_size')) target.page_size = patch.size;
      if (Object.prototype.hasOwnProperty.call(target, 'pageSize')) target.pageSize = patch.size;
    };

    if (j.data != null && typeof j.data === 'object' && !Array.isArray(j.data)) {
      inject(j.data);
    } else {
      inject(j);
    }
    try {
      return JSON.stringify(j);
    } catch (_) {
      return bodyStr;
    }
  }

  /** 录新品窗口内：对 material_list 的 fetch 参数异步改写 body（支持 Request 与 url+init）。 */
  function gePromiseBuyinNewestPatchedFetchArgs(args) {
    if (!geBuyinNewestListRequestPatch || !isBuyin()) return Promise.resolve(args);
    const a0 = args[0];
    const a1 = args[1];
    const urlRaw = typeof a0 === 'string' ? a0 : (a0 && a0.url) || '';
    if (!geIsBuyinShopProductListUrl(urlRaw)) return Promise.resolve(args);

    if (typeof Request !== 'undefined' && a0 instanceof Request) {
      const method = (a0.method || 'GET').toUpperCase();
      if (method !== 'POST' && method !== 'PUT' && method !== 'PATCH') return Promise.resolve(args);
      return a0
        .clone()
        .text()
        .then(function (txt) {
          const next = geApplyBuyinNewestPatchToBody(txt);
          if (next === txt) return args;
          const merged = {
            method: a0.method,
            headers: a0.headers,
            body: next,
            mode: a0.mode,
            credentials: a0.credentials,
            cache: a0.cache,
            redirect: a0.redirect,
            referrer: a0.referrer,
            referrerPolicy: a0.referrerPolicy,
            integrity: a0.integrity,
            keepalive: a0.keepalive,
            signal: a0.signal,
          };
          if (a1 && typeof a1 === 'object' && !Array.isArray(a1)) Object.assign(merged, a1);
          merged.body = next;
          try {
            return [new Request(a0.url, merged)];
          } catch (_) {
            return args;
          }
        })
        .catch(function () {
          return args;
        });
    }

    const init = a1 && typeof a1 === 'object' && !Array.isArray(a1) ? a1 : {};
    const method = String(init.method || 'GET').toUpperCase();
    if (method !== 'POST' && method !== 'PUT' && method !== 'PATCH') return Promise.resolve(args);
    const b = init.body;
    if (typeof b !== 'string') return Promise.resolve(args);
    const patched = geApplyBuyinNewestPatchToBody(b);
    if (patched === b) return Promise.resolve(args);
    return Promise.resolve([a0, Object.assign({}, init, { body: patched })]);
  }

  const origFetch = window.fetch;

  function geReplayOneBuyinSnapshot(snap) {
    if (!snap || !snap.urlAbs) return Promise.resolve();
    const h = { ...snap.headers };
    if (!snap.body || snap.method === 'GET' || snap.method === 'HEAD') {
      delete h['Content-Type'];
      delete h['content-type'];
    }
    const init = {
      method: snap.method,
      headers: h,
      cache: 'no-store',
      credentials: 'include',
    };
    if (snap.body != null && snap.method !== 'GET' && snap.method !== 'HEAD') init.body = snap.body;
    return origFetch(snap.urlAbs, init)
      .then(function (r) {
        if (r && (r.status === 200 || r.status === 201)) return r.text();
        return '';
      })
      .then(function (raw) {
        if (raw && String(raw).trim()) tryParse(raw);
      });
  }

  function geRunBuyinBackgroundNetworkRefresh(tag, force) {
    if (!isBuyin()) return;
    if (force) geBuyinBgBusy = false;
    const now = Date.now();
    if (!force && now - geBuyinLastBgRefresh < GE_BUYIN_BG_MIN_GAP_MS) return;
    if (geBuyinBgBusy && !force) return;
    geBuyinLastBgRefresh = now;
    geBuyinBgBusy = true;
    const snaps = [];
    const seen = new Set();
    for (let i = geBuyinReplayBuffer.length - 1; i >= 0 && snaps.length < 8; i--) {
      const s = geBuyinReplayBuffer[i];
      const k = geBuyinReplaySnapshotKey(s);
      if (seen.has(k)) continue;
      seen.add(k);
      snaps.push(s);
    }
    let chain = Promise.resolve();
    for (let j = 0; j < snaps.length; j++) {
      const s = snaps[j];
      chain = chain.then(function () {
        return geReplayOneBuyinSnapshot(s).catch(function () {});
      });
    }
    void chain.finally(function () {
      geBuyinBgBusy = false;
    });
  }

  window.fetch = function (...args) {
    return gePromiseBuyinNewestPatchedFetchArgs(args).then(function (fetchArgs) {
      geTryRecordBuyinFetchArgs(fetchArgs);
      const urlRaw =
        typeof fetchArgs[0] === 'string' ? fetchArgs[0] : (fetchArgs[0] && fetchArgs[0].url) || '';
      const url = geResolveRequestUrl(urlRaw);
      const isShopSimpleFetch = /shop\.simple\.fetch|mtop\.taobao\.shop\.simple\.fetch/i.test(urlRaw + ' ' + url);
      return origFetch.apply(window, fetchArgs).then(async (r) => {
        try {
          const ct = (r.headers.get('content-type') || '').toLowerCase();
          const clone = r.clone();
          const isBuyinApiOk =
            geIsBuyinPageFetchDataUrl(urlRaw) && (r.status === 200 || r.status === 201);
          const isTaobaoLine = /mtop|h5api|\.taobao\.com|\.tmall\.com/i.test(url);
          const isTextualCt =
            ct.includes('json') ||
            ct.includes('text') ||
            ct.includes('javascript') ||
            ct.includes('plain') ||
            !ct;
          let t = '';
          if (isBuyinApiOk || isTaobaoLine || isTextualCt) {
            try {
              t = await clone.text();
            } catch (_) {
              t = '';
            }
          }
          if (
            isBuyin() &&
            geIsBuyinPageFetchDataUrl(urlRaw) &&
            (!t || !String(t).trim()) &&
            (r.status === 304 || r.status === 200 || r.status === 201)
          ) {
            const pair = geBuyinBuildNoStoreFetchArgs(fetchArgs);
            if (pair) {
              try {
                const r2 = await origFetch.apply(window, pair);
                if (r2 && (r2.status === 200 || r2.status === 201)) {
                  const t2 = await r2.clone().text();
                  if (t2 && String(t2).trim()) t = t2;
                }
              } catch (_) {}
            }
          }
          if (!t && r.status === 304 && isTaobao()) {
            console.warn('[导出] 接口返回 304 且无正文（多为缓存）。请在 DevTools → Network 勾选 Disable cache，再 Ctrl+F5 强刷页面后滚动加载商品。');
          }
          if (!t && r.status === 304 && geIsBuyinPageFetchDataUrl(urlRaw)) {
            console.warn(
              '[导出] 百应接口 304 仍无正文（已尝试 no-store 重拉）。请 DevTools → Network 勾选 Disable cache 后 Ctrl+F5，或清除本站缓存。'
            );
          }
          if (isTaobao() && isShopSimpleFetch && t && /itemInfoDTO/.test(t)) {
            console.log('[导出] 拦截 fetch：mtop.taobao.shop.simple.fetch，长度 ' + t.length);
          }
          tryParse(t);
        } catch (_) {}
        return r;
      });
    });
  };

  const origSetRequestHeader = XMLHttpRequest.prototype.setRequestHeader;
  XMLHttpRequest.prototype.setRequestHeader = function (name, value) {
    try {
      this.__geReqHeaders = this.__geReqHeaders || {};
      this.__geReqHeaders[String(name)] = String(value);
    } catch (_) {}
    return origSetRequestHeader.apply(this, arguments);
  };

  const origXhrOpen = XMLHttpRequest.prototype.open;
  XMLHttpRequest.prototype.open = function (method, url, ...rest) {
    try {
      this.__geReqUrl = typeof url === 'string' ? url : (url && String(url)) || '';
      this.__geReqMethod = String(method || 'GET').toUpperCase();
      this.__geReqHeaders = {};
    } catch (_) {
      this.__geReqUrl = '';
    }
    return origXhrOpen.apply(this, [method, url, ...rest]);
  };
  const origSend = XMLHttpRequest.prototype.send;
  XMLHttpRequest.prototype.send = function (...args) {
    const xhr = this;
    let sendArgs = args;
    try {
      if (geBuyinNewestListRequestPatch && isBuyin()) {
        const url = xhr.__geReqUrl || '';
        const m = (xhr.__geReqMethod || 'GET').toUpperCase();
        if (
          geIsBuyinShopProductListUrl(url) &&
          (m === 'POST' || m === 'PUT' || m === 'PATCH') &&
          args[0] != null &&
          typeof args[0] === 'string'
        ) {
          const patched = geApplyBuyinNewestPatchToBody(args[0]);
          if (patched !== args[0]) sendArgs = [patched];
        }
      }
      xhr.__geSendBody = sendArgs.length ? sendArgs[0] : undefined;
    } catch (_) {
      try {
        xhr.__geSendBody = args.length ? args[0] : undefined;
      } catch (_) {}
    }
    geTryRecordBuyinXhrForReplay(xhr);
    xhr.addEventListener('load', function onLoad() {
      try {
        const reqUrlRaw = xhr.responseURL || xhr.__geReqUrl || '';
        const reqUrl = geResolveRequestUrl(reqUrlRaw) || reqUrlRaw;
        const isShopSimple = /shop\.simple\.fetch|mtop\.taobao\.shop\.simple\.fetch/i.test(reqUrlRaw + ' ' + reqUrl);
        if (xhr.responseType === 'json' && xhr.response != null && typeof xhr.response === 'object') {
          if (isTaobao() && isShopSimple) {
            try {
              const s = JSON.stringify(xhr.response);
              if (s && /itemInfoDTO/.test(s)) console.log('[导出] 拦截 XHR(JSON)：mtop.taobao.shop.simple.fetch，长度 ' + s.length);
            } catch (_) {}
          }
          tryParseJsonObject(xhr.response);
        } else if (xhr.responseType === '' || xhr.responseType === 'text') {
          const t = xhr.responseText || '';
          if (isTaobao() && isShopSimple && t && /itemInfoDTO/.test(t)) {
            console.log('[导出] 拦截 XHR：mtop.taobao.shop.simple.fetch，长度 ' + t.length);
          }
          tryParse(t);
        } else if (xhr.responseType === 'document' && xhr.responseXML) {
          let docStr = '';
          try {
            docStr = new XMLSerializer().serializeToString(xhr.responseXML);
          } catch (_) {}
          if (isTaobao() && isShopSimple && docStr && /itemInfoDTO/.test(docStr)) {
            console.log('[导出] 拦截 XHR(document)：mtop.taobao.shop.simple.fetch');
          }
          try { if (docStr) tryParse(docStr); } catch (_) {}
        } else if (
          xhr.responseType === 'blob' &&
          xhr.response &&
          typeof Blob !== 'undefined' &&
          xhr.response instanceof Blob &&
          xhr.status === 200 &&
          geIsBuyinPageFetchDataUrl(reqUrlRaw)
        ) {
          const reader = new FileReader();
          reader.onloadend = function () {
            try {
              if (typeof reader.result === 'string' && reader.result) tryParse(reader.result);
            } catch (_) {}
          };
          try {
            reader.readAsText(xhr.response, 'utf-8');
          } catch (_) {}
        }
        if (isBuyin() && geIsBuyinPageFetchDataUrl(reqUrlRaw)) {
          let gotText = '';
          try {
            if (xhr.responseType === 'json' && xhr.response != null) {
              gotText = JSON.stringify(xhr.response);
            } else if (xhr.responseType === '' || xhr.responseType === 'text') {
              gotText = xhr.responseText || '';
            }
          } catch (_) {}
          const st = xhr.status;
          if ((!gotText || !String(gotText).trim()) && (st === 304 || st === 200 || st === 201)) {
            const urlAbs = geResolveRequestUrl(reqUrlRaw) || reqUrlRaw;
            const method = (xhr.__geReqMethod || 'GET').toUpperCase();
            const b = xhr.__geSendBody;
            const hdrs = { ...(xhr.__geReqHeaders || {}) };
            if (typeof FormData !== 'undefined' && b instanceof FormData) {
              delete hdrs['Content-Type'];
              delete hdrs['content-type'];
            }
            const init = {
              method,
              cache: 'no-store',
              credentials: 'include',
              headers: hdrs,
            };
            if (b != null && method !== 'GET' && method !== 'HEAD') init.body = b;
            void origFetch(urlAbs, init)
              .then((r2) => {
                if (!r2 || (r2.status !== 200 && r2.status !== 201)) return '';
                return r2.text();
              })
              .then((raw) => {
                if (raw && String(raw).trim()) tryParse(raw);
              })
              .catch(() => {});
          }
        }
      } catch (_) {}
    });
    return origSend.apply(this, sendArgs);
  };

  geRefetchBuyinMaterialListForLinks = async function (force) {
    if (!isBuyin()) return;
    const now = Date.now();
    if (!force && now - geBuyinMaterialListRefetchCooldownAt < 2200) return;
    geBuyinMaterialListRefetchCooldownAt = now;
    tryReingestBuyinLastApiFromStorage();
    const urls = [];
    const seen = new Set();
    const pushUrl = function (u) {
      const s = String(u || '').trim().split('#')[0];
      if (!s || seen.has(s)) return;
      if (!geIsBuyinPageFetchDataUrl(s)) return;
      seen.add(s);
      urls.push(s);
    };
    try {
      const perf = performance.getEntriesByType('resource');
      for (let i = perf.length - 1; i >= 0; i--) {
        const n = perf[i] && perf[i].name;
        if (typeof n !== 'string' || !n) continue;
        if (!/\/material_list\b/i.test(n)) continue;
        pushUrl(n);
        if (urls.length >= 12) break;
      }
    } catch (_) {}
    let ctx = { shopId: '' };
    try {
      ctx = getBuyinPageShopContext();
    } catch (_) {}
    if (ctx.shopId) {
      try {
        const u = new URL('https://buyin.jinritemai.com/pc/selection/common/material_list');
        u.searchParams.set('shop_id', ctx.shopId);
        pushUrl(u.href);
      } catch (_) {}
      try {
        const u2 = new URL('https://buyin.jinritemai.com/pc/selection/common/material_list');
        u2.searchParams.set('shopId', ctx.shopId);
        pushUrl(u2.href);
      } catch (_) {}
    }
    const maxGet = 5;
    for (let i = 0; i < urls.length && i < maxGet; i++) {
      try {
        const r = await origFetch(urls[i], { cache: 'no-store', credentials: 'include' });
        if (r && (r.status === 200 || r.status === 201)) {
          const t = await r.text();
          if (t && t.trim()) tryParse(t);
        }
      } catch (_) {}
    }
    if (ctx.shopId) {
      const postUrl = 'https://buyin.jinritemai.com/pc/selection/common/material_list';
      const bodies = [
        JSON.stringify({ shop_id: ctx.shopId }),
        JSON.stringify({ shopId: ctx.shopId, page_num: 1, page_size: 50 }),
      ];
      for (let b = 0; b < bodies.length; b++) {
        try {
          const r = await origFetch(postUrl, {
            method: 'POST',
            cache: 'no-store',
            credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
            body: bodies[b],
          });
          if (r && (r.status === 200 || r.status === 201)) {
            const t = await r.text();
            if (t && /summary_promotion|product_id|base_model/i.test(t)) {
              tryParse(t);
              break;
            }
          }
        } catch (_) {}
      }
    }
  };

  if (isBuyin()) {
    setInterval(function () {
      geRunBuyinBackgroundNetworkRefresh('interval', false);
    }, GE_BUYIN_BG_INTERVAL_MS);
    setTimeout(function () {
      geRunBuyinBackgroundNetworkRefresh('startup', false);
      geRefetchBuyinMaterialListForLinks(true).catch(function () {});
    }, 4000);
    if (typeof document !== 'undefined' && document.addEventListener) {
      document.addEventListener('visibilitychange', function () {
        if (document.visibilityState === 'visible') geRunBuyinBackgroundNetworkRefresh('visible', false);
      });
    }
    if (typeof GM_registerMenuCommand === 'function') {
      GM_registerMenuCommand('百应：立即后台补拉接口', function () {
        geRunBuyinBackgroundNetworkRefresh('menu', true);
      });
      GM_registerMenuCommand('百应：补拉 material_list', function () {
        geRefetchBuyinMaterialListForLinks(true).catch(function () {});
      });
    }
  }

  if (window === window.top) {
    window.addEventListener('message', function (e) {
      if (!e.data?.type || !e.data?.data) return;
      if (e.data.type === 'buyin_data') onResponse(e.data.data);
      else if (e.data.type === 'taobao_data') {
        const results = [];
        const seen = new Set(taobaoData.map((x) => x.link || (x.title + '|' + x.price)));
        ingestItemInfoDtoData(e.data.data, results, seen);
        extractTaobao(e.data.data, results, seen);
        if (results.length) taobaoData.push(...results);
        if (results.length > 0) console.log('[导出] 收到 iframe 数据 ' + results.length + ' 个，累计 ' + taobaoData.length);
      }
    });
  }

  async function doExport() {
    const btn = document.getElementById('goods-export-btn');
    await waitForBuyinDetailLinksIfNeeded(btn);
    const p = collectExportPayload();
    if (!p) return;
    downloadCsv(p.data, p.prefix);
    alert('已导出 ' + p.data.length + ' 个商品');
  }

  /** 右侧导出控制台：分组网格 + 紧凑批量区（只注入一次） */
  function injectGeExportPanelStyles() {
    if (document.getElementById('ge-export-panel-styles')) return;
    const st = document.createElement('style');
    st.id = 'ge-export-panel-styles';
    st.textContent =
      '.ge-export-panel-inner{' +
      'width:278px;max-width:100%;box-sizing:border-box;' +
      'font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;' +
      '}' +
      '.ge-panel-group{margin:0;padding:0;}' +
      '.ge-panel-divider{height:1px;background:#e5e6eb;margin:8px 0;border:0;padding:0;}' +
      '.ge-panel-grid-2{' +
      'display:grid;grid-template-columns:1fr 1fr;gap:8px;align-items:stretch;width:100%;' +
      '}' +
      '.ge-panel-grid-3{' +
      'display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px;align-items:stretch;width:100%;' +
      '}' +
      '.ge-panel-fs-cell{display:flex;gap:6px;align-items:stretch;min-width:0;}' +
      '.ge-panel-btn{' +
      'box-sizing:border-box;min-height:36px;padding:0 8px;margin:0;' +
      'font-size:13px;font-weight:500;line-height:1.15;border-radius:8px;cursor:pointer;' +
      'border:1px solid transparent;overflow:hidden;text-overflow:ellipsis;' +
      'transition:filter .12s ease,box-shadow .12s ease,background .12s ease;' +
      '}' +
      '.ge-panel-btn:hover{filter:brightness(1.03);}' +
      '.ge-panel-btn:active{filter:brightness(.97);}' +
      '.ge-panel-btn--flex{flex:1;min-width:0;}' +
      '.ge-panel-btn--orange{background:#ff6700;color:#fff;border-color:#e85d00;box-shadow:0 1px 4px rgba(255,103,0,.35);}' +
      '.ge-panel-btn--blue{background:#3370ff;color:#fff;border-color:#2b66f0;box-shadow:0 1px 4px rgba(51,112,255,.3);}' +
      '.ge-panel-btn--green{background:#00b42a;color:#fff;border-color:#009a29;box-shadow:0 1px 4px rgba(0,180,42,.28);}' +
      '.ge-panel-btn--danger{background:#f53f3f;color:#fff;border-color:#e02020;box-shadow:0 1px 4px rgba(245,63,63,.28);}' +
      '.ge-panel-btn--neutral{background:#f2f3f5;color:#1d2129;border-color:#dee0e3;}' +
      '.ge-panel-btn--neutral:hover{background:#e8eaed;}' +
      '.ge-panel-btn--green-outline{background:#fff;color:#00b42a;border-color:#00b42a;}' +
      '.ge-panel-btn--green-outline:hover{background:#f0fff4;}' +
      '.ge-panel-btn--blue-outline{background:#fff;color:#3370ff;border-color:#3370ff;}' +
      '.ge-panel-btn--blue-outline:hover{background:#f5f8ff;}' +
      '.ge-panel-btn--danger-text{background:#fff;color:#d03030;border-color:#f0c0c0;}' +
      '.ge-panel-btn--danger-text:hover{background:#fff5f5;}' +
      '.ge-panel-btn--icon{' +
      'width:36px;min-width:36px;flex-shrink:0;padding:0;font-size:17px;line-height:1;' +
      'background:#fff;color:#646a73;border-color:#c9cdd4;box-shadow:none;' +
      '}' +
      '.ge-panel-btn--icon:hover{background:#f7f8fa;color:#3370ff;border-color:#a8c4ff;}' +
      '.ge-panel-group--auto{' +
      'padding:8px 10px;background:#f7f9fc;border:1px solid #e1e4e8;border-radius:8px;' +
      '}' +
      '.ge-auto-write-row{' +
      'display:flex;flex-direction:row;align-items:center;gap:8px;flex-wrap:wrap;margin:0;padding:0;' +
      'cursor:pointer;user-select:none;' +
      '}' +
      '.ge-auto-write-row:hover .ge-batch-auto-title{color:#165dff;}' +
      '.ge-batch-auto-cb{' +
      'width:18px;height:18px;min-width:18px;cursor:pointer;flex-shrink:0;' +
      'accent-color:#165dff;border:2px solid #6b7280;border-radius:4px;box-sizing:border-box;' +
      '}' +
      '.ge-batch-auto-cb:hover{box-shadow:0 0 0 2px rgba(22,93,255,.2);}' +
      '.ge-batch-auto-title{font-size:13px;font-weight:600;color:#1d2129;line-height:1.35;}' +
      '.ge-panel-info-tip{' +
      'display:inline-flex;align-items:center;justify-content:center;' +
      'width:18px;height:18px;margin-left:2px;border-radius:50%;' +
      'font-size:11px;font-weight:700;color:#646a73;background:#e8eaed;border:1px solid #d0d3d6;' +
      'cursor:help;flex-shrink:0;line-height:1;' +
      '}' +
      '.ge-panel-info-tip:hover{background:#dbe8ff;color:#165dff;border-color:#9bbcf5;}' +
      '.ge-batch-badge-row{' +
      'display:flex;flex-wrap:wrap;gap:5px 6px;align-items:center;margin-top:8px;padding-top:8px;' +
      'border-top:1px solid #e8eaed;' +
      '}' +
      '.ge-batch-badge{' +
      'display:inline-flex;align-items:center;justify-content:center;' +
      'padding:3px 8px;border-radius:5px;font-size:11px;font-weight:600;line-height:1.3;' +
      'border:1px solid transparent;white-space:nowrap;max-width:100%;' +
      '}' +
      '.ge-batch-badge--idle{background:#eceff2;color:#272e3b;border-color:#cfd4dc;}' +
      '.ge-batch-badge--running{background:#d8f5e0;color:#006629;border-color:#7dcc93;}' +
      '.ge-batch-badge--tasks{background:#e8eaee;color:#3d4657;border-color:#c5cad3;}' +
      '.ge-batch-badge--buffer{background:#dbe8ff;color:#0e42c4;border-color:#9bbcf5;}' +
      '.ge-batch-live-round{' +
      'display:none;margin-top:6px;padding:6px 8px;font-size:11px;line-height:1.5;' +
      'color:#1d2129;background:#f0f9ff;border:1px solid #91caff;border-radius:6px;word-break:break-word;white-space:pre-wrap;' +
      '}' +
      '.ge-batch-live-status{' +
      'display:none;margin-top:6px;padding:6px 8px;font-size:11px;line-height:1.45;' +
      'color:#373c43;background:#fff;border:1px solid #dfe3e8;border-radius:6px;word-break:break-word;white-space:pre-wrap;' +
      '}';
    (document.head || document.documentElement).appendChild(st);
  }

  function addButton() {
    if (document.getElementById('goods-export-btn')) return;
    const panelPrefs = loadExportPanelPrefs();
    const wrap = document.createElement('div');
    wrap.id = 'goods-export-wrap';
    wrap.style.cssText =
      'position:fixed;bottom:80px;right:20px;z-index:2147483647;pointer-events:auto;display:flex;flex-direction:column;gap:4px;align-items:flex-end';
    const toggleRow = document.createElement('div');
    toggleRow.style.cssText =
      'display:flex;justify-content:flex-end;align-items:center;gap:6px;width:100%';
    const dragHandle = document.createElement('span');
    dragHandle.id = 'ge-panel-drag-handle';
    dragHandle.setAttribute('role', 'button');
    dragHandle.title = '拖动面板（折叠时也可拖）';
    dragHandle.textContent = '⠿';
    dragHandle.style.cssText =
      'cursor:grab;padding:3px 7px;font-size:13px;line-height:1.2;color:#86909c;user-select:none;border:1px solid #d0d3d6;border-radius:6px;background:linear-gradient(180deg,#fff,#f2f3f5);flex-shrink:0;box-shadow:0 1px 2px rgba(0,0,0,.06)';
    const togglePanelBtn = document.createElement('button');
    togglePanelBtn.type = 'button';
    togglePanelBtn.id = 'ge-panel-toggle-btn';
    togglePanelBtn.textContent = panelPrefs.panelCollapsed ? '展开面板 ▸' : '收起面板 ▾';
    togglePanelBtn.style.cssText =
      'padding:4px 10px;font-size:11px;border:1px solid #c9cdd4;border-radius:6px;background:#fff;color:#646a73;cursor:pointer;box-shadow:0 1px 3px rgba(0,0,0,.06)';
    const panelBody = document.createElement('div');
    panelBody.id = 'ge-export-panel-body';
    panelBody.style.cssText =
      'display:' +
      (panelPrefs.panelCollapsed ? 'none' : 'flex') +
      ';flex-direction:column;gap:0;align-items:flex-end;width:auto';
    togglePanelBtn.addEventListener(
      'click',
      function (e) {
        e.stopPropagation();
        e.preventDefault();
        const hidden = panelBody.style.display === 'none';
        if (hidden) {
          panelBody.style.display = 'flex';
          togglePanelBtn.textContent = '收起面板 ▾';
          saveExportPanelPrefs({ panelCollapsed: false });
        } else {
          panelBody.style.display = 'none';
          togglePanelBtn.textContent = '展开面板 ▸';
          saveExportPanelPrefs({ panelCollapsed: true });
        }
        return false;
      },
      true
    );
    toggleRow.appendChild(dragHandle);
    toggleRow.appendChild(togglePanelBtn);
    wrap.appendChild(toggleRow);
    wrap.appendChild(panelBody);

    injectGeExportPanelStyles();

    const panelInner = document.createElement('div');
    panelInner.className = 'ge-export-panel-inner';

    function geMkPanelDivider() {
      const d = document.createElement('div');
      d.className = 'ge-panel-divider';
      return d;
    }

    function geMkPanelBtn(classNames, text, fn) {
      const b = document.createElement('button');
      b.type = 'button';
      b.className = 'ge-panel-btn ' + classNames;
      b.textContent = text;
      b.addEventListener(
        'click',
        function (e) {
          e.stopPropagation();
          e.preventDefault();
          fn();
          return false;
        },
        true
      );
      return b;
    }

    const btn = document.createElement('button');
    btn.id = 'goods-export-btn';
    btn.type = 'button';
    btn.textContent = '导出 CSV';
    btn.className = 'ge-panel-btn ge-panel-btn--orange';
    btn.addEventListener(
      'click',
      function (e) {
        e.stopPropagation();
        e.preventDefault();
        doExport().catch(function (err) {
          console.error(err);
        });
        return false;
      },
      true
    );
    const btnFs = document.createElement('button');
    btnFs.id = 'goods-export-feishu-btn';
    btnFs.type = 'button';
    btnFs.textContent = '写入飞书';
    btnFs.className = 'ge-panel-btn ge-panel-btn--blue ge-panel-btn--flex';
    btnFs.addEventListener(
      'click',
      function (e) {
        e.stopPropagation();
        e.preventDefault();
        doExportFeishu().catch(function (err) {
          alert('飞书写入失败：' + (err && err.message ? err.message : err));
          console.error(err);
        });
        return false;
      },
      true
    );
    const linkCfg = document.createElement('button');
    linkCfg.type = 'button';
    linkCfg.id = 'ge-fs-cfg-btn';
    linkCfg.className = 'ge-panel-btn ge-panel-btn--icon';
    linkCfg.textContent = '\u2699';
    linkCfg.title = '飞书多维表格配置';
    linkCfg.setAttribute('aria-label', '飞书配置');
    linkCfg.addEventListener('click', function (e) {
      e.stopPropagation();
      e.preventDefault();
      showFeishuSettingsModal();
      return false;
    }, true);

    const dataGroup = document.createElement('div');
    dataGroup.className = 'ge-panel-group';
    const gridData = document.createElement('div');
    gridData.className = 'ge-panel-grid-2';
    const fsCell = document.createElement('div');
    fsCell.className = 'ge-panel-fs-cell';
    fsCell.appendChild(btnFs);
    fsCell.appendChild(linkCfg);
    gridData.appendChild(btn);
    gridData.appendChild(fsCell);
    dataGroup.appendChild(gridData);
    panelInner.appendChild(dataGroup);

    if (isBuyin()) {
      panelInner.appendChild(geMkPanelDivider());

      const autoG = document.createElement('div');
      autoG.className = 'ge-panel-group ge-panel-group--auto';
      const mainLab = document.createElement('label');
      mainLab.className = 'ge-auto-write-row';
      mainLab.setAttribute('for', 'ge-batch-auto-feishu-cb');
      const autoFsCb = document.createElement('input');
      autoFsCb.type = 'checkbox';
      autoFsCb.className = 'ge-batch-auto-cb';
      autoFsCb.id = 'ge-batch-auto-feishu-cb';
      autoFsCb.checked = !!panelPrefs.batchAutoFeishuOnComplete;
      autoFsCb.addEventListener('change', function () {
        saveExportPanelPrefs({ batchAutoFeishuOnComplete: autoFsCb.checked });
      });
      const titleSpan = document.createElement('span');
      titleSpan.className = 'ge-batch-auto-title';
      titleSpan.textContent = '自动写入飞书';
      const tipEl = document.createElement('span');
      tipEl.className = 'ge-panel-info-tip';
      tipEl.textContent = '?';
      tipEl.title =
        '勾选后：每个店铺每完成一大轮会尝试自动写飞书并清理已写缓冲；全部店结束后若仍有剩余也会再写一次。不勾选请手动点「写飞书」。';
      mainLab.appendChild(autoFsCb);
      mainLab.appendChild(titleSpan);
      mainLab.appendChild(tipEl);

      const badgeRow = document.createElement('div');
      badgeRow.className = 'ge-batch-badge-row';
      const bState = document.createElement('span');
      bState.id = 'ge-batch-badge-state';
      bState.className = 'ge-batch-badge ge-batch-badge--idle';
      bState.textContent = '未运行';
      const bTasks = document.createElement('span');
      bTasks.id = 'ge-batch-badge-tasks';
      bTasks.className = 'ge-batch-badge ge-batch-badge--tasks';
      bTasks.textContent = '任务0';
      const bBuf = document.createElement('span');
      bBuf.id = 'ge-batch-badge-buffer';
      bBuf.className = 'ge-batch-badge ge-batch-badge--buffer';
      bBuf.textContent = '缓冲0';
      const liveRound = document.createElement('div');
      liveRound.id = 'ge-batch-live-round';
      liveRound.className = 'ge-batch-live-round';
      liveRound.style.display = 'none';
      const liveSt = document.createElement('div');
      liveSt.id = 'ge-batch-live-status';
      liveSt.className = 'ge-batch-live-status';
      liveSt.style.display = 'none';
      badgeRow.appendChild(bState);
      badgeRow.appendChild(bTasks);
      badgeRow.appendChild(bBuf);
      autoG.appendChild(mainLab);
      autoG.appendChild(badgeRow);
      autoG.appendChild(liveRound);
      autoG.appendChild(liveSt);
      panelInner.appendChild(autoG);

      panelInner.appendChild(geMkPanelDivider());

      const monG = document.createElement('div');
      monG.className = 'ge-panel-group';
      const gridMon = document.createElement('div');
      gridMon.className = 'ge-panel-grid-2';
      gridMon.appendChild(
        geMkPanelBtn('ge-panel-btn--neutral', '配置任务', function () {
          showBuyinBatchConfigModal();
        })
      );
      gridMon.appendChild(
        geMkPanelBtn('ge-panel-btn--green-outline', '加入当前', function () {
          geAddCurrentShopToTaskList();
        })
      );
      gridMon.appendChild(
        geMkPanelBtn('ge-panel-btn--green', '开始监控', function () {
          geStartBuyinBatch();
        })
      );
      gridMon.appendChild(
        geMkPanelBtn('ge-panel-btn--danger', '停止监控', function () {
          geStopBuyinBatch();
        })
      );
      monG.appendChild(gridMon);
      panelInner.appendChild(monG);

      panelInner.appendChild(geMkPanelDivider());

      const bufG = document.createElement('div');
      bufG.className = 'ge-panel-group';
      const gridBuf = document.createElement('div');
      gridBuf.className = 'ge-panel-grid-3';
      gridBuf.appendChild(
        geMkPanelBtn('ge-panel-btn--neutral', '导出缓冲', function () {
          geExportBatchAccumCsv();
        })
      );
      gridBuf.appendChild(
        geMkPanelBtn('ge-panel-btn--blue-outline', '写飞书', function () {
          geDoExportFeishuBatchAccum().catch(function (err) {
            alert('飞书写入失败：' + (err && err.message ? err.message : err));
            console.error(err);
          });
        })
      );
      gridBuf.appendChild(
        geMkPanelBtn('ge-panel-btn--danger-text', '清空', function () {
          geClearBatchAccumFromPanel();
        })
      );
      bufG.appendChild(gridBuf);
      panelInner.appendChild(bufG);

      geUpdateBatchStatusLine();
      geScheduleBuyinBatchResume();
    }

    panelBody.appendChild(panelInner);
    (document.body || document.documentElement).appendChild(wrap);
    installExportPanelDrag(dragHandle, wrap);
    requestAnimationFrame(function () {
      applySavedExportPanelPosition(wrap);
    });
    if (!window.__geExportPanelResizeBound) {
      window.__geExportPanelResizeBound = true;
      window.addEventListener(
        'resize',
        function () {
          const w = document.getElementById('goods-export-wrap');
          if (!w || !loadExportPanelPosition()) return;
          let r;
          try {
            r = w.getBoundingClientRect();
          } catch (_) {
            return;
          }
          const c = geClampPanelPosition(w, r.left, r.top);
          applyExportPanelPosition(w, c.left, c.top);
          saveExportPanelPosition(c.left, c.top);
        },
        { passive: true }
      );
    }
    const site = isBuyin() ? '百应' : isTaobao() ? '淘宝' : '';
    console.log('[导出] 已就绪 ' + site + '：面板操作 CSV / 写入飞书；⚙ 打开飞书配置');
  }

  function tryInjectIframe() {
    if (window !== window.top || !isBuyin()) return;
    document.querySelectorAll('iframe').forEach(function (f) {
      try {
        if (f.contentDocument && !f.contentDocument.__goodsInjected && f.src) {
          f.contentDocument.__goodsInjected = true;
          const s = f.contentDocument.createElement('script');
          s.textContent =
            '(function(){function L(x){if(!x||typeof x!=="object")return false;if(x.base_model&&x.base_model.product_info&&(x.base_model.product_info.name||x.base_model.product_info.detail_url))return true;if(x.product_info&&(x.product_info.name||x.product_info.detail_url))return true;if((x.name||x.title||x.product_name)&&(x.detail_url||x.url))return true;return false}function G(j){if(!j)return false;if(j.data&&j.data.summary_promotions&&j.data.summary_promotions.length)return true;var d=j.data||j;if(!d||typeof d!=="object")return false;var ks=["product_list","products","items","goods_list","commodity_list","records","list","data_list","sku_list"];for(var i=0;i<ks.length;i++){var a=d[ks[i]];if(Array.isArray(a)&&a.length&&L(a[0]))return true}return false}var o=window.fetch;window.fetch=function(){return o.apply(this,arguments).then(function(r){var c=r.clone();try{c.json().then(function(j){if(G(j))window.parent.postMessage({type:"buyin_data",data:j},"*")})}catch(e){}return r})}})();';
          (f.contentDocument.head || f.contentDocument.documentElement).appendChild(s);
        }
      } catch (_) {}
    });
  }

  function TaobaoDebug() {
    const info = {
      ok: isTaobao(),
      url: location.href,
      hostname: location.hostname,
      isShopPage: /shop\d+\.(taobao|world\.taobao)\.com/.test(location.hostname),
      taobaoDataCount: taobaoData.length,
      /** 三项缓存：sessionStorage / localStorage / #ge-mtop-stash 文本长度（有 mtop 快照时通常 >500） */
      cacheLengths: { sessionStorage_ge_last_shop_mtop: 0, localStorage_ge_last_shop_mtop: 0, ge_mtop_stash_value: 0 },
      iframeCount: document.querySelectorAll('iframe').length,
      fallbackCandidates: 0,
      withPriceAndImg: 0,
      windowVars: {},
      domCards: 0,
      domLinks: 0,
      lastShopMtopSessionLen: 0,
      lastShopMtopLocalLen: 0,
      stashTextareaLen: 0,
      hint: '',
    };
    try {
      info.lastShopMtopSessionLen = (sessionStorage.getItem('ge_last_shop_mtop') || '').length;
      info.cacheLengths.sessionStorage_ge_last_shop_mtop = info.lastShopMtopSessionLen;
    } catch (_) {
      info.lastShopMtopSessionLen = -1;
      info.cacheLengths.sessionStorage_ge_last_shop_mtop = -1;
    }
    try {
      info.lastShopMtopLocalLen = (localStorage.getItem('ge_last_shop_mtop') || '').length;
      info.cacheLengths.localStorage_ge_last_shop_mtop = info.lastShopMtopLocalLen;
    } catch (_) {
      info.lastShopMtopLocalLen = -1;
      info.cacheLengths.localStorage_ge_last_shop_mtop = -1;
    }
    try {
      const st = document.getElementById('ge-mtop-stash');
      info.stashTextareaLen = (st && st.value) ? st.value.length : 0;
      info.cacheLengths.ge_mtop_stash_value = info.stashTextareaLen;
    } catch (_) {
      info.stashTextareaLen = -1;
      info.cacheLengths.ge_mtop_stash_value = -1;
    }
    if (!isTaobao()) {
      info.hint = '当前页面 hostname 不匹配淘宝/天猫。请确认在 *.taobao.com / *.world.taobao.com / *.tmall.com 下打开控制台。若店铺在 iframe 内，请将控制台上下文切换到该 iframe。';
      console.log('[导出] 调试:', info);
      console.table ? console.table(info) : null;
      return info;
    }
    const fallbackCandidates = document.querySelectorAll('div, li, dl, section, article, ul > li');
    const withPrice = [...fallbackCandidates].filter((el) => /[¥￥]\s*\d+(\.\d+)?/.test((el.innerText || el.textContent || '').replace(/\s+/g, ' ')));
    info.fallbackCandidates = fallbackCandidates.length;
    info.withPriceAndImg = withPrice.filter((el) => el.querySelector('img')).length;
    ['__INITIAL_STATE__', 'g_initialData', 'pageData', '__NUXT_DATA__', '__PRELOADED_STATE__'].forEach((k) => {
      const v = window[k];
      info.windowVars[k] = v ? (typeof v === 'object' ? Object.keys(v).slice(0, 5) : typeof v) : 'undefined';
    });
    info.domCards = document.querySelectorAll('[class*="card"], [class*="Card"], [class*="item"], [class*="Item"], [data-item]').length;
    let domLinkCnt = 0;
    document.querySelectorAll('a[href]').forEach((a) => {
      const h = a.href || a.getAttribute('href') || '';
      if (extractTaobaoItemIdFromHref(h) || /item\.(taobao|world\.taobao)|detail\.(tmall|tmall\.hk)|item\.htm/i.test(h)) domLinkCnt++;
    });
    info.domLinks = domLinkCnt;
    if (info.taobaoDataCount === 0 && (info.lastShopMtopSessionLen > 500 || info.lastShopMtopLocalLen > 500 || info.stashTextareaLen > 500)) info.hint = '已有三项缓存（见 cacheLengths）。点「导出」或执行 tryIngestLastShopMtop()；仍为 0 则解析失败，可贴 Network 里 shop.simple.fetch 的 Response 开头给作者。';
    else if (info.taobaoDataCount === 0 && info.domLinks === 0) info.hint = '未捕获到 API 数据且 DOM 中无商品链接。请滚动加载、Disable cache 强刷，或看 Network 是否有 mtop.taobao.shop.simple.fetch。';
    console.log('[导出] 调试信息:', info);
    if (typeof console.table === 'function') console.table(info);
    return info;
  }
  function TaobaoFindCards() {
    if (!isTaobao()) return console.log('当前非淘宝页面');
    const els = document.querySelectorAll('div, li, section, article');
    const found = [];
    let priceAsImg = 0;
    els.forEach((el) => {
      const t = (el.innerText || '').replace(/\s+/g, ' ');
      if (t.length > 20 && /[¥￥]\s*\d+/.test(t) && el.querySelector('img')) {
        const cls = el.className || (el.getAttribute && el.getAttribute('class')) || '';
        const priceArea = el.querySelector('[class*="price"], span.text-price');
        const hasPriceImg = priceArea && priceArea.querySelector('img, canvas');
        if (hasPriceImg) priceAsImg++;
        found.push({ tag: el.tagName, class: cls.slice(0, 80), textLen: t.length, priceIsImg: !!hasPriceImg });
      }
    });
    const unique = [...new Map(found.map((x) => [x.class, x])).values()];
    console.log('[导出] 含价格+图片:', found.length, '去重:', unique.length, '价格疑似图片:', priceAsImg);
    console.table(unique.slice(0, 10));
    return found;
  }
  function exposeDebug() {
    window.TaobaoDebug = TaobaoDebug;
    window.TaobaoFindCards = TaobaoFindCards;
    window.tryIngestLastShopMtop = tryIngestLastShopMtopFromStorage;
    window.openFeishuSettings = showFeishuSettingsModal;
    window.doExportFeishu = doExportFeishu;
    window.doFeishuSyncColumnsOnly = doFeishuSyncColumnsOnly;
    window.showBuyinBatchConfig = showBuyinBatchConfigModal;
    window.geAddCurrentShopToTaskList = geAddCurrentShopToTaskList;
    window.geExportBatchAccumCsv = geExportBatchAccumCsv;
    window.geStartBuyinBatch = geStartBuyinBatch;
    window.geStopBuyinBatch = geStopBuyinBatch;
    window.__goodsExportDebug = TaobaoDebug;
    window.__goodsExportDebugData = { getInfo: TaobaoDebug };
    try { if (window.top !== window) { window.top.TaobaoDebug = TaobaoDebug; window.top.__goodsExportDebug = TaobaoDebug; } } catch (_) {}
    try {
      const s = document.createElement('script');
      s.textContent = '(function(){var d=window.__goodsExportDebugData;if(d&&d.getInfo){window.TaobaoDebug=function(){return d.getInfo();};window.__goodsExportDebug=window.TaobaoDebug;}})();';
      (document.head||document.documentElement).appendChild(s);
      s.remove();
    } catch (_) {}
  }
  if (isTaobao()) {
    exposeDebug();
  } else {
    window.TaobaoDebug = TaobaoDebug;
    window.TaobaoFindCards = TaobaoFindCards;
    window.openFeishuSettings = showFeishuSettingsModal;
    window.doExportFeishu = doExportFeishu;
    window.doFeishuSyncColumnsOnly = doFeishuSyncColumnsOnly;
    window.showBuyinBatchConfig = showBuyinBatchConfigModal;
    window.geAddCurrentShopToTaskList = geAddCurrentShopToTaskList;
    window.geExportBatchAccumCsv = geExportBatchAccumCsv;
    window.geStartBuyinBatch = geStartBuyinBatch;
    window.geStopBuyinBatch = geStopBuyinBatch;
  }

  try {
    if (typeof GM_registerMenuCommand === 'function') {
      GM_registerMenuCommand('百应：配置店铺批量任务', function () {
        if (isBuyin()) showBuyinBatchConfigModal();
        else alert('请在百应 buyin.jinritemai.com 页面使用');
      });
      GM_registerMenuCommand('飞书：多维表格配置', function () {
        showFeishuSettingsModal();
      });
      GM_registerMenuCommand('飞书：仅同步表头（自动建列）', function () {
        doFeishuSyncColumnsOnly();
      });
    } else {
      const g = geGm();
      if (g && typeof g.registerMenuCommand === 'function') {
        g.registerMenuCommand('百应：配置店铺批量任务', function () {
          if (isBuyin()) showBuyinBatchConfigModal();
          else alert('请在百应 buyin.jinritemai.com 页面使用');
        });
        g.registerMenuCommand('飞书：多维表格配置', function () {
          showFeishuSettingsModal();
        });
        g.registerMenuCommand('飞书：仅同步表头（自动建列）', function () {
          doFeishuSyncColumnsOnly();
        });
      }
    }
  } catch (_) {}

  if (document.body) {
    setTimeout(addButton, 800);
    if (isBuyin()) setTimeout(tryInjectIframe, 2000);
    if (isTaobao()) setTimeout(findTaobaoInWindow, 2000);
  } else {
    document.addEventListener('DOMContentLoaded', function () {
      setTimeout(addButton, 1500);
      if (isBuyin()) setTimeout(tryInjectIframe, 3000);
      if (isTaobao()) setTimeout(findTaobaoInWindow, 2500);
    });
  }
})();
