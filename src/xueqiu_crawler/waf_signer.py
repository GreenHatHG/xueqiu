from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


MD5_QUERY_KEY = "md5__1038"
DEFAULT_SIGNER_TIMEOUT_SEC = 25.0
DEFAULT_HARD_EXIT_MS = "12000"
DEFAULT_FINAL_WAIT_MS = "6500"
DEFAULT_INLINE_EVAL_TIMEOUT_MS = "8000"
DEFAULT_EXT_EVAL_TIMEOUT_MS = "6000"
SIGNED_URL_PATTERN = re.compile(r"^SIGNED_URL=(.+)$", re.M)
SIGNED_URL_FALLBACK_PATTERN = re.compile(r"https?://[^\s\"']*md5__1038=[^\s\"']+")
COOKIE_WRITES_PATTERN = re.compile(r"^COOKIE_WRITES=(.+)$", re.M)
CHALLENGE_SCRIPT_SRC_PATTERN = re.compile(r"<script[^>]+src=[\"']([^\"']+)[\"']", re.I)


SIGNER_RUNTIME_JS = r"""
const fs = require('fs');
const vm = require('vm');

const inputUrl = process.argv[2] || '';
const htmlPath = process.argv[3] || '';
const externalPath = process.argv[4] || '';
const html = htmlPath ? fs.readFileSync(htmlPath, 'utf8') : '';
let externalJs = '';
if (externalPath && fs.existsSync(externalPath)) {
  externalJs = fs.readFileSync(externalPath, 'utf8');
}
const wafInlinePattern = new RegExp("<script[^>]*name=[\"']aliyunwaf_[^\"']+[\"'][^>]*>([\\s\\S]*?)</script>", 'i');
let waf = (html.match(wafInlinePattern) || [])[1] || '';
const challengeUrl = inputUrl || 'https://xueqiu.com/';

waf = String(waf || '').replace(/\bdebugger\b/g, 'void 0');
externalJs = String(externalJs || '').replace(/\bdebugger\b/g, 'void 0');

let cookieStore = '';
const signedHits = [];
const cookieWrites = [];
let finalized = false;

function finalizeAndExit(code = 0) {
  if (finalized) return;
  finalized = true;
  if (signedHits.length) {
    const latest = signedHits[signedHits.length - 1].value;
    console.log(`SIGNED_URL=${latest}`);
  }
  if (cookieWrites.length) {
    console.log(`COOKIE_WRITES=${JSON.stringify(cookieWrites)}`);
  }
  process.exit(code);
}

function capture(value, where) {
  try {
    const s = String(value);
    if (s.includes('md5__1038=')) {
      if (!signedHits.length || signedHits[signedHits.length - 1].value !== s) {
        signedHits.push({ where, value: s });
      }
    }
  } catch (_) {}
}

function autoAny(label = 'auto') {
  const target = function autoStub() {};
  target.__label = label;
  target.refresh = () => {};
  target.reload = () => {};
  target.toString = () => `[${label}]`;
  target.valueOf = () => 0;
  return new Proxy(target, {
    get(obj, prop) {
      if (prop in obj) {
        const v = obj[prop];
        return typeof v === 'function' ? v.bind(obj) : v;
      }
      if (prop === Symbol.toPrimitive) return () => '';
      if (prop === 'length') return 0;
      return autoAny(`${label}.${String(prop)}`);
    },
    set(obj, prop, val) {
      obj[prop] = val;
      return true;
    },
    apply() { return autoAny(`${label}()`); },
    construct() { return autoAny(`new ${label}`); },
  });
}

class Anchor {
  constructor(init) {
    this._u = new URL(init || challengeUrl, 'https://xueqiu.com');
    this.style = {};
  }

  _setHref(v) {
    capture(v, 'anchor.href=set');
    try {
      this._u = new URL(String(v), this._u.href || 'https://xueqiu.com');
    } catch (_) {
      try {
        this._u = new URL(String(v), 'https://xueqiu.com');
      } catch (_) {}
    }
  }

  get href() { return this._u.href; }
  set href(v) { this._setHref(v); }

  get protocol() { return this._u.protocol; }
  set protocol(v) { this._u.protocol = v; }

  get host() { return this._u.host; }
  set host(v) { this._u.host = v; }

  get hostname() { return this._u.hostname; }
  set hostname(v) { this._u.hostname = v; }

  get port() { return this._u.port; }
  set port(v) { this._u.port = v; }

  get pathname() { return this._u.pathname || '/'; }
  set pathname(v) { this._u.pathname = v; }

  get search() { return this._u.search; }
  set search(v) { this._u.search = v; }

  get hash() { return this._u.hash; }
  set hash(v) { this._u.hash = v; }

  get origin() { return this._u.origin; }

  assign(v) { this._setHref(v); }
  replace(v) { this._setHref(v); }
  reload() {}
  refresh() {}
  toString() { return this._u.href; }
  valueOf() { return this._u.href; }

  setAttribute(name, value) {
    const k = String(name).toLowerCase();
    if (k === 'href') this.href = value;
    else this[k] = String(value);
  }

  getAttribute(name) {
    const k = String(name).toLowerCase();
    if (k === 'href') return this.href;
    return this[k] == null ? null : String(this[k]);
  }

  cloneNode() {
    return makeAnchor(this.href);
  }
}

function makeAnchor(init) {
  const base = new Anchor(init);
  return new Proxy(base, {
    get(obj, prop) {
      if (prop in obj) {
        const v = obj[prop];
        return typeof v === 'function' ? v.bind(obj) : v;
      }
      if (prop === Symbol.toPrimitive) return () => obj.href;
      if (prop === 'length') return 0;
      return undefined;
    },
    set(obj, prop, value) {
      obj[prop] = value;
      return true;
    },
  });
}

class Element {
  constructor(tagName) {
    this.tagName = String(tagName || '').toUpperCase();
    this.nodeName = this.tagName;
    this.style = {};
    this.children = [];
    this.attributes = {};
    this.firstChild = makeAnchor(challengeUrl);
    this._innerHTML = '';
    this.parentNode = {
      insertBefore: () => {},
      removeChild: () => {},
      appendChild: () => {},
    };
  }

  appendChild(el) {
    this.children.push(el);
    this.firstChild = this.children[0] || null;
    return el;
  }

  insertBefore(el) {
    this.children.unshift(el);
    this.firstChild = this.children[0] || null;
    return el;
  }

  removeChild(el) {
    this.children = this.children.filter((x) => x !== el);
    this.firstChild = this.children[0] || null;
    return el;
  }

  set innerHTML(v) {
    this._innerHTML = String(v || '');
    const m = this._innerHTML.match(/<a[^>]*href=["']?([^"'\s>]+)[^>]*>/i);
    if (m) {
      const a = makeAnchor(m[1]);
      this.children = [a];
      this.firstChild = a;
    }
  }

  get innerHTML() {
    return this._innerHTML;
  }

  setAttribute(name, value) {
    this.attributes[String(name)] = String(value);
  }

  getAttribute(name) {
    return Object.prototype.hasOwnProperty.call(this.attributes, String(name))
      ? this.attributes[String(name)]
      : null;
  }

  addEventListener() {}
  removeEventListener() {}
  querySelector() { return null; }
  querySelectorAll() { return []; }
  cloneNode() { return new Element(this.tagName); }
}

const locationObj = makeAnchor(challengeUrl);
const renderData = (html.match(/<textarea id="renderData"[^>]*>([\s\S]*?)<\/textarea>/i) || [])[1] || '';
let wafRender = {};
try {
  wafRender = JSON.parse(renderData || '{}');
} catch (_) {
  wafRender = {};
}

const headEl = new Element('head');
const bodyEl = new Element('body');
bodyEl.innerHTML = html;
const documentElement = new Element('html');
documentElement.innerHTML = html;
const scriptEl = new Element('script');
scriptEl.parentNode = { insertBefore: () => {}, removeChild: () => {} };

const documentObj = {
  body: bodyEl,
  head: headEl,
  documentElement,
  referrer: '',
  title: 'xq',
  readyState: 'complete',
  URL: challengeUrl,
  location: locationObj,

  getElementById(id) {
    if (id === 'renderData') return { innerHTML: renderData };
    return new Element('div');
  },

  getElementsByTagName(tag) {
    const t = String(tag).toLowerCase();
    if (t === 'script') return [scriptEl];
    if (t === 'head') return [headEl];
    if (t === 'body') return [bodyEl];
    return [new Element(t)];
  },

  createElement(tag) {
    const t = String(tag).toLowerCase();
    if (t === 'a') return makeAnchor(challengeUrl);
    return new Element(t);
  },

  addEventListener() {},
  removeEventListener() {},
  querySelector() { return null; },
  querySelectorAll() { return []; },
};

Object.defineProperty(documentObj, 'cookie', {
  configurable: true,
  get() { return cookieStore; },
  set(v) {
    const sv = String(v);
    cookieStore = cookieStore ? `${cookieStore}; ${sv}` : sv;
    cookieWrites.push(sv);
    capture(sv, 'document.cookie=set');
  },
});

function setGlobal(name, value) {
  try {
    Object.defineProperty(globalThis, name, {
      configurable: true,
      writable: true,
      value,
    });
  } catch (_) {
    globalThis[name] = value;
  }
}

setGlobal('window', globalThis);
setGlobal('self', globalThis);
setGlobal('top', globalThis);
setGlobal('parent', globalThis);
setGlobal('document', documentObj);
setGlobal('location', locationObj);
setGlobal('navigator', {
  userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
  language: 'zh-CN',
  languages: ['zh-CN', 'zh'],
  platform: 'Win32',
  cookieEnabled: true,
  location: locationObj,
});
setGlobal('clientInformation', globalThis.navigator);
setGlobal('screen', { width: 1920, height: 1080 });
setGlobal('history', {
  pushState: (_v, _t, u) => capture(u, 'history.pushState'),
  replaceState: (_v, _t, u) => capture(u, 'history.replaceState'),
  location: locationObj,
  refresh() {},
});
setGlobal('localStorage', {
  _m: new Map(),
  getItem(k) { return this._m.has(k) ? this._m.get(k) : null; },
  setItem(k, v) { this._m.set(k, String(v)); },
  removeItem(k) { this._m.delete(k); },
});
setGlobal('sessionStorage', {
  _m: new Map(),
  getItem(k) { return this._m.has(k) ? this._m.get(k) : null; },
  setItem(k, v) { this._m.set(k, String(v)); },
  removeItem(k) { this._m.delete(k); },
});
setGlobal('performance', { now: () => Date.now() });
const NativeDate = Date;
const useFastTime = String(process.env.WAF_FAST_TIME || '0') === '1';
if (useFastTime) {
  let fakeNow = NativeDate.now();
  function fastNow() {
    fakeNow += 111;
    return fakeNow;
  }
  class FastDate extends NativeDate {
    constructor(...args) {
      if (args.length === 0) super(fastNow());
      else super(...args);
    }
    static now() { return fastNow(); }
    static parse(v) { return NativeDate.parse(v); }
    static UTC(...args) { return NativeDate.UTC(...args); }
  }
  setGlobal('Date', FastDate);
} else {
  setGlobal('Date', NativeDate);
}
setGlobal('crypto', {
  getRandomValues(arr) {
    for (let i = 0; i < arr.length; i += 1) arr[i] = Math.floor(Math.random() * 256);
    return arr;
  },
});
setGlobal('atob', (s) => Buffer.from(s, 'base64').toString('binary'));
setGlobal('btoa', (s) => Buffer.from(s, 'binary').toString('base64'));
setGlobal('Image', function Image() {
  return {
    set src(v) { capture(v, 'Image.src'); },
  };
});
setGlobal('_waf_bd8ce2ce37', wafRender._waf_bd8ce2ce37 || '');
setGlobal('_waf_a86dfdc5f2', Date.now());

function XHR() {
  this.headers = {};
  this.readyState = 0;
  this.status = 200;
  this.responseText = html;
  this.responseURL = challengeUrl;
  this.onreadystatechange = null;
  this.onload = null;
}
XHR.prototype.open = function open(method, url) {
  this.method = method;
  this.url = url;
  capture(url, 'XHR.open');
};
XHR.prototype.setRequestHeader = function setRequestHeader(k, v) {
  this.headers[k] = v;
};
XHR.prototype.send = function send() {
  this.readyState = 4;
  if (typeof this.onreadystatechange === 'function') this.onreadystatechange();
  if (typeof this.onload === 'function') this.onload();
};
XHR.prototype.abort = function abort() {};
XHR.prototype.addEventListener = function addEventListener() {};
XHR.prototype.getAllResponseHeaders = function getAllResponseHeaders() { return ''; };
setGlobal('XMLHttpRequest', XHR);

setGlobal('fetch', async function fetch(input) {
  const url = typeof input === 'string' ? input : (input && input.url);
  capture(url, 'fetch');
  return {
    ok: true,
    status: 200,
    url: url || challengeUrl,
    text: async () => html,
    json: async () => ({}),
    headers: { get: () => null },
  };
});

const realSetTimeout = globalThis.setTimeout.bind(globalThis);
const realSetInterval = globalThis.setInterval.bind(globalThis);
const realClearTimeout = globalThis.clearTimeout.bind(globalThis);
const realClearInterval = globalThis.clearInterval.bind(globalThis);

function normalizeDelay(ms, cap) {
  const n = Number(ms);
  if (!Number.isFinite(n)) return 0;
  if (n < 0) return 0;
  return Math.min(n, cap);
}

function runTimerCallback(fn, args) {
  try {
    if (typeof fn === 'function') {
      fn(...args);
      return;
    }
    const code = String(fn || '');
    if (code) runScriptSafe(code, 'timer_eval.js', 1000);
  } catch (e) {
    console.error(String((e && e.stack) || e || 'timer_error'));
  }
}

setGlobal('setTimeout', (fn, timeoutMs, ...args) => {
  const delay = normalizeDelay(timeoutMs, 3000);
  return realSetTimeout(() => runTimerCallback(fn, args), delay);
});
setGlobal('setInterval', (fn, timeoutMs, ...args) => {
  // Some challenge callbacks are not idempotent in our stub runtime.
  // Execute once to avoid repeated-state corruption.
  const delay = normalizeDelay(timeoutMs, 1500);
  return realSetTimeout(() => runTimerCallback(fn, args), delay);
});
setGlobal('clearInterval', (id) => {
  try { realClearInterval(id); } catch (_) {}
});
setGlobal('clearTimeout', (id) => {
  try { realClearTimeout(id); } catch (_) {}
});

function runScriptSafe(code, filename, timeoutMs) {
  const script = new vm.Script(code, { filename, displayErrors: true });
  return script.runInThisContext({ timeout: timeoutMs, displayErrors: true });
}

const hardExitMs = Number(process.env.WAF_HARD_EXIT_MS || 15000);
realSetTimeout(() => {
  finalizeAndExit(0);
}, hardExitMs);

try {
  if (externalJs) {
    const extEvalTimeout = Number(process.env.WAF_EXT_EVAL_TIMEOUT_MS || 6000);
    runScriptSafe(externalJs, 'challenge_external_runtime.js', extEvalTimeout);
  }
} catch (e) {
  console.error(String((e && e.stack) || e || 'external_js_error'));
}

try {
  const inlineEvalTimeout = Number(process.env.WAF_INLINE_EVAL_TIMEOUT_MS || 8000);
  runScriptSafe(waf, 'challenge_waf_inline.js', inlineEvalTimeout);
} catch (e) {
  console.error(String((e && e.stack) || e || 'inline_js_error'));
}

const finalWaitMs = Number(process.env.WAF_FINAL_WAIT_MS || 6500);
realSetTimeout(() => {
  finalizeAndExit(0);
}, finalWaitMs);
"""


@dataclass(frozen=True)
class WafSignerResult:
    signed_url: str
    cookie_writes: list[str]
    error_text: str


def strip_md5_query_param(url: str) -> str:
    parts = urlsplit(str(url or ""))
    items = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if str(key) != MD5_QUERY_KEY
    ]
    query = urlencode(items, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


def extract_challenge_script_src(challenge_html: str) -> str:
    match = CHALLENGE_SCRIPT_SRC_PATTERN.search(str(challenge_html or ""))
    return str(match.group(1) or "").strip() if match else ""


def _extract_signed_url(stdout: str) -> str:
    match = SIGNED_URL_PATTERN.search(str(stdout or ""))
    if match:
        return str(match.group(1) or "").strip()
    hits = SIGNED_URL_FALLBACK_PATTERN.findall(str(stdout or ""))
    return str(hits[-1] or "").strip() if hits else ""


def _extract_cookie_writes(stdout: str) -> list[str]:
    match = COOKIE_WRITES_PATTERN.search(str(stdout or ""))
    if not match:
        return []
    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    return [str(item) for item in data if str(item or "").strip()]


def run_waf_signer(
    unsigned_url: str,
    challenge_html: str,
    *,
    external_js: str = "",
    timeout_sec: float = DEFAULT_SIGNER_TIMEOUT_SEC,
) -> WafSignerResult:
    node_path = shutil.which("node")
    if not node_path:
        return WafSignerResult("", [], "node not found in PATH")

    html = str(challenge_html or "")
    if not html.strip():
        return WafSignerResult("", [], "challenge html is empty")

    with tempfile.TemporaryDirectory(prefix="xq-waf-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        runtime_path = tmp_path / "run_waf_signer.js"
        challenge_path = tmp_path / "challenge_runtime.html"
        runtime_path.write_text(SIGNER_RUNTIME_JS, encoding="utf-8")
        challenge_path.write_text(html, encoding="utf-8")

        command = [node_path, str(runtime_path), str(unsigned_url), str(challenge_path)]
        if str(external_js or "").strip():
            external_path = tmp_path / "challenge_external_runtime.js"
            external_path.write_text(str(external_js), encoding="utf-8")
            command.append(str(external_path))

        try:
            proc = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=float(timeout_sec),
                cwd=str(tmp_path),
                env={
                    **os.environ,
                    "WAF_HARD_EXIT_MS": DEFAULT_HARD_EXIT_MS,
                    "WAF_FINAL_WAIT_MS": DEFAULT_FINAL_WAIT_MS,
                    "WAF_INLINE_EVAL_TIMEOUT_MS": DEFAULT_INLINE_EVAL_TIMEOUT_MS,
                    "WAF_EXT_EVAL_TIMEOUT_MS": DEFAULT_EXT_EVAL_TIMEOUT_MS,
                },
            )
        except Exception as exc:
            return WafSignerResult("", [], str(exc))

    error_text = (
        str(proc.stderr or "").strip()[:500]
        or str(proc.stdout or "").strip()[:500]
        or f"node exited with code {int(proc.returncode)}"
    )
    return WafSignerResult(
        signed_url=_extract_signed_url(proc.stdout),
        cookie_writes=_extract_cookie_writes(proc.stdout),
        error_text=error_text,
    )
