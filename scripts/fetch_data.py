#!/usr/bin/env python3
"""股票分析软件数据管道：拉取行情/基本面/做空/期权/内幕交易数据，写入 data/。
在 GitHub Actions 定时运行，也可在本地运行：python3 scripts/fetch_data.py
"""
import json, os, re, sys, time, math, urllib.request, urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
UA_YAHOO = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
UA_SEC = "StockAnalysis/1.0 contact@example.com"
NOW_UTC = datetime.now(timezone.utc)
STAMP = NOW_UTC.strftime("%Y-%m-%d %H:%M UTC")

def log(*a):
    print("[fetch]", *a, flush=True)

def sanitize(sym):
    return re.sub(r"[^A-Za-z0-9]", "", sym).upper()

def http_get(url, headers=None, timeout=30, retries=3, binary=False):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers or {})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                data = r.read()
                return data if binary else data.decode("utf-8", "replace")
        except Exception as e:
            last = e
            time.sleep(2 ** i)
    raise RuntimeError(f"GET failed {url}: {last}")

def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)

def load_tickers():
    out = []
    with open(os.path.join(ROOT, "tickers.txt"), encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                out.append(line)
    return out

# ---------------- Yahoo session (cookie + crumb) ----------------
import threading as _th
import urllib.error as _urlerr
class Yahoo:
    def __init__(self):
        self._cj = None
        proc = urllib.request.HTTPCookieProcessor()
        self._cj = proc.cookiejar
        self.opener = urllib.request.build_opener(proc)
        self.crumb = None
        self._lock = _th.Lock()
    def _get(self, url, retries=3):
        last = None
        for i in range(retries):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": UA_YAHOO})
                with self.opener.open(req, timeout=30) as r:
                    return r.read().decode("utf-8", "replace")
            except Exception as e:
                last = e
                time.sleep(2 ** i)
        raise RuntimeError(f"Yahoo GET failed {url}: {last}")
    def ensure_crumb(self):
        with self._lock:
            if self.crumb:
                return self.crumb
            # fc.yahoo.com returns 404 but sets the A3 cookie; extract it from the error response
            req = urllib.request.Request("https://fc.yahoo.com", headers={"User-Agent": UA_YAHOO})
            try:
                with self.opener.open(req, timeout=30):
                    pass
            except _urlerr.HTTPError as e:
                try:
                    self._cj.extract_cookies(e, req)
                except Exception:
                    pass
            except Exception:
                pass
            self.crumb = self._get("https://query2.finance.yahoo.com/v1/test/getcrumb").strip()
            if not self.crumb or self.crumb.startswith("{"):
                raise RuntimeError("could not obtain Yahoo crumb")
            return self.crumb
    def chart(self, sym, rng="2y", interval="1d"):
        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(sym)}"
               f"?interval={interval}&range={rng}")
        return json.loads(self._get(url))
    def quote_summary(self, sym, modules):
        crumb = self.ensure_crumb()
        url = (f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/"
               f"{urllib.parse.quote(sym)}?modules={modules}&crumb={urllib.parse.quote(crumb)}")
        return json.loads(self._get(url))

Y = Yahoo()

def raw(v):
    return v.get("raw") if isinstance(v, dict) else v

# ---------------- 1. prices ----------------
def fetch_prices(sym):
    try:
        d = Y.chart(sym)
        res = d["chart"]["result"][0]
        ts = res["timestamp"]
        q = res["indicators"]["quote"][0]
        rows = []
        for i, t in enumerate(ts):
            c = q["close"][i]
            if c is None:
                continue
            rows.append({
                "d": datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d"),
                "o": round(q["open"][i] or c, 2), "h": round(q["high"][i] or c, 2),
                "l": round(q["low"][i] or c, 2), "c": round(c, 2),
                "v": int(q["volume"][i] or 0),
            })
        meta = res.get("meta", {})
        out = {"symbol": sym, "name": meta.get("longName") or meta.get("shortName") or sym,
               "currency": meta.get("currency"), "rows": rows, "updated": STAMP}
        return sym, out, None
    except Exception as e:
        return sym, None, str(e)

# ---------------- 2. fundamentals ----------------
FUND_MODULES = "price,summaryDetail,defaultKeyStatistics,financialData,calendarEvents,earningsTrend"
def fetch_fundamentals(sym):
    try:
        d = Y.quote_summary(sym, FUND_MODULES)
        r = d["quoteSummary"]["result"][0]
        p, sd, ks, fd = r.get("price", {}), r.get("summaryDetail", {}), \
                        r.get("defaultKeyStatistics", {}), r.get("financialData", {})
        ce = r.get("calendarEvents", {}).get("earnings", {})
        earn_date = None
        if isinstance(ce, dict) and ce.get("earningsDate"):
            eds = ce["earningsDate"]
            if isinstance(eds, list) and eds:
                earn_date = eds[0].get("fmt") if isinstance(eds[0], dict) else str(eds[0])
        trend = None
        try:
            trend = r["earningsTrend"]["trend"][0].get("earningsEstimate", {}).get("avg", {}).get("raw")
        except Exception:
            pass
        return sym, {
            "name": p.get("longName") or p.get("shortName") or sym,
            "price": raw(p.get("regularMarketPrice")),
            "marketCap": raw(p.get("marketCap")),
            "trailingPE": raw(sd.get("trailingPE")),
            "forwardPE": raw(sd.get("forwardPE")),
            "priceToBook": raw(ks.get("priceToBook")),
            "epsTTM": raw(ks.get("trailingEps")),
            "epsForward": raw(ks.get("forwardEps")),
            "revenue": raw(fd.get("totalRevenue")),
            "profitMargin": raw(fd.get("profitMargins")),
            "operatingMargin": raw(fd.get("operatingMargins")),
            "roe": raw(fd.get("returnOnEquity")),
            "dividendYield": raw(sd.get("dividendYield")),
            "fiftyTwoWeekHigh": raw(sd.get("fiftyTwoWeekHigh")),
            "fiftyTwoWeekLow": raw(sd.get("fiftyTwoWeekLow")),
            "targetMeanPrice": raw(fd.get("targetMeanPrice")),
            "earningsDate": earn_date,
            "earningsEst": trend,
        }, None
    except Exception as e:
        return sym, None, str(e)

# ---------------- 3. short data (FINRA) ----------------
FINRA_MARKETS = ["CNMS", "FNYX"]
def finra_dates(n_days=20, lookback=30):
    dates = []
    d = NOW_UTC.date()
    checked = 0
    while len(dates) < n_days and checked < lookback:
        ds = d.strftime("%Y%m%d")
        url = f"https://cdn.finra.org/equity/regsho/daily/CNMSshvol{ds}.txt"
        try:
            urllib.request.Request(url)
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA_YAHOO}), timeout=20) as r:
                if r.status == 200 and len(r.read(100)) > 0:
                    dates.append(ds)
        except Exception:
            pass
        d -= timedelta(days=1)
        checked += 1
    return dates

def fetch_short(tickers):
    log("probing FINRA dates...")
    dates = finra_dates()
    log("FINRA available dates:", len(dates), dates[:3])
    agg = {t: {} for t in tickers}  # t -> {date: [shortVol, totalVol]}
    for ds in dates:
        for m in FINRA_MARKETS:
            url = f"https://cdn.finra.org/equity/regsho/daily/{m}shvol{ds}.txt"
            try:
                txt = http_get(url, headers={"User-Agent": UA_YAHOO}, retries=2)
            except Exception as e:
                log("FINRA skip", url, e)
                continue
            for line in txt.splitlines():
                parts = line.split("|")
                if len(parts) < 5:
                    continue
                sym = parts[1]
                if sym in agg:
                    try:
                        sv = float(parts[2]); tv = float(parts[4])
                        if tv > 0:
                            a = agg[sym].setdefault(ds, [0.0, 0.0])
                            a[0] += sv; a[1] += tv
                    except ValueError:
                        pass
    out = {"updated": STAMP, "dates": dates, "tickers": {}}
    for t, dmap in agg.items():
        rows = [{"d": ds, "pct": round(100 * v[0] / v[1], 2)}
                for ds, v in sorted(dmap.items()) if v[1] > 0]
        out["tickers"][t] = rows
    write_json(os.path.join(DATA, "short.json"), out)
    n = sum(1 for t in agg if agg[t])
    log(f"short data: {n}/{len(tickers)} tickers have data")
    return out

# ---------------- 4. options (CBOE delayed) ----------------
def fetch_options(sym):
    try:
        csym = sym.lstrip("^")
        url = f"https://cdn.cboe.com/api/global/delayed_quotes/options/{urllib.parse.quote(csym)}.json"
        req = urllib.request.Request(url, headers={"User-Agent": UA_YAHOO})
        # follow redirects manually (CBOE issues 307)
        opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler())
        with opener.open(req, timeout=60) as r:
            d = json.loads(r.read().decode("utf-8", "replace"))
        opts = d.get("data", {}).get("options", [])
        cp_vol = cp_oi = 0
        items = []
        for o in opts:
            vol = o.get("volume") or 0
            oi = o.get("open_interest") or 0
            bid, ask = o.get("bid") or 0, o.get("ask") or 0
            mid = (bid + ask) / 2 if (bid or ask) else (o.get("theo") or o.get("last_trade_price") or 0)
            prem = vol * mid * 100
            code = o.get("option", "")
            m = re.match(r"^[A-Z^]+(\d{6})([CP])(\d{8})$", code)
            if not m:
                continue
            exp = f"20{m.group(1)[:2]}-{m.group(1)[2:4]}-{m.group(1)[4:6]}"
            typ = "C" if m.group(2) == "C" else "P"
            strike = int(m.group(3)) / 1000
            if typ == "C":
                cp_vol += vol
            # put/call volume split
            items.append({"exp": exp, "strike": strike, "type": typ, "vol": int(vol),
                          "oi": int(oi), "prem": round(prem), "iv": o.get("iv"),
                          "delta": o.get("delta")})
        put_vol = sum(i["vol"] for i in items if i["type"] == "P")
        call_vol = sum(i["vol"] for i in items if i["type"] == "C")
        put_oi = sum(i["oi"] for i in items if i["type"] == "P")
        call_oi = sum(i["oi"] for i in items if i["type"] == "C")
        unusual = [i for i in items
                   if (i["oi"] > 0 and i["vol"] / i["oi"] >= 2.5) or i["prem"] >= 500000]
        unusual.sort(key=lambda x: x["prem"], reverse=True)
        out = {"symbol": sym, "updated": STAMP,
               "putCallVol": round(put_vol / call_vol, 2) if call_vol else None,
               "putCallOI": round(put_oi / call_oi, 2) if call_oi else None,
               "totalVol": int(put_vol + call_vol),
               "unusual": unusual[:30]}
        return sym, out, None
    except Exception as e:
        return sym, None, str(e)

# ---------------- 5. insider (SEC EDGAR Form 4) ----------------
CODE_CN = {"P": "买入", "S": "卖出", "M": "行权", "F": "缴税扣股", "A": "授予",
           "D": "处置", "G": "赠与", "C": "转换", "X": "行权", "J": "其他"}
def sec_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA_SEC})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()

def insider_for_ticker(sym, cik_map):
    sym_u = sym.upper()
    if sym_u.startswith("^") or sym_u in ("SPY", "QQQ", "DIA", "IWM"):
        return sym, [], "etf/index skipped"
    cik = cik_map.get(sym_u)
    if not cik:
        return sym, [], "no CIK"
    try:
        d = json.loads(sec_get(f"https://data.sec.gov/submissions/CIK{cik}.json"))
        filings = d["filings"]["recent"]
        cutoff = (NOW_UTC.date() - timedelta(days=90)).isoformat()
        cands = []
        for i, f in enumerate(filings["form"]):
            if f == "4" and filings["filingDate"][i] >= cutoff:
                cands.append((filings["filingDate"][i], filings["accessionNumber"][i]))
                if len(cands) >= 8:
                    break
        txs = []
        for fdate, acc in cands:
            try:
                accn = acc.replace("-", "")
                idx = json.loads(sec_get(
                    f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accn}/index.json"))
                xmls = [it["name"] for it in idx["directory"]["item"]
                        if it["name"].endswith(".xml") and "primary" not in it["name"].lower()]
                if not xmls:
                    continue
                x = sec_get(f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accn}/{xmls[0]}")
                r = ET.fromstring(x)
                def t(el, path):
                    e = el.find(path)
                    return e.text.strip() if e is not None and e.text else ""
                ro = r.find("reportingOwner")
                name = t(ro, "reportingOwnerId/rptOwnerName") if ro is not None else ""
                rel = ro.find("reportingOwnerRelationship") if ro is not None else None
                title = t(rel, "officerTitle") if rel is not None else ""
                role = []
                if rel is not None:
                    if t(rel, "isDirector") == "1":
                        role.append("董事")
                    if t(rel, "isOfficer") == "1":
                        role.append("高管")
                    if t(rel, "isTenPercentOwner") == "1":
                        role.append("10%股东")
                for tbl in ("nonDerivativeTable/nonDerivativeTransaction",
                            "derivativeTable/derivativeTransaction"):
                    for nt in r.findall(tbl):
                        code = t(nt, "transactionCoding/transactionCode")
                        shares = t(nt, "transactionAmounts/transactionShares/value")
                        price = t(nt, "transactionAmounts/transactionPricePerShare/value")
                        tdate = t(nt, "transactionDate/value")
                        if code and shares:
                            try:
                                txs.append({"date": tdate or fdate, "name": name,
                                            "title": title or "、".join(role),
                                            "code": code, "action": CODE_CN.get(code, code),
                                            "shares": int(float(shares)),
                                            "price": float(price) if price else None})
                            except ValueError:
                                pass
            except Exception:
                continue
            time.sleep(0.2)
        txs.sort(key=lambda x: x["date"], reverse=True)
        out = {"symbol": sym, "updated": STAMP, "txs": txs[:20]}
        return sym, out, None
    except Exception as e:
        return sym, None, str(e)

def fetch_insider(tickers):
    log("loading SEC ticker map...")
    cmap = {}
    try:
        d = json.loads(sec_get("https://www.sec.gov/files/company_tickers.json").decode("utf-8"))
        for k, v in d.items():
            cmap[v["ticker"].upper()] = str(v["cik_str"]).zfill(10)
    except Exception as e:
        log("SEC ticker map failed:", e)
    ok, fail, outs = [], [], {}
    for t in tickers:
        sym, out, err = insider_for_ticker(t, cmap)
        if err:
            fail.append(sym)
        else:
            ok.append(sym)
            outs[sym] = out
        log(f"insider {sym}: {len(out['txs']) if out else 0} txs" + (f" ({err})" if err else ""))
        time.sleep(0.3)
    write_json(os.path.join(DATA, "insider.json"), {"updated": STAMP, "symbols": outs})
    return ok, fail

# ---------------- 6. market ----------------
def fetch_market(tickers):
    idx = ["SPY", "QQQ", "DIA", "IWM", "^VIX"]
    m = {}
    for s in idx:
        try:
            d = Y.chart(s, rng="5d")
            res = d["chart"]["result"][0]
            closes = [c for c in res["indicators"]["quote"][0]["close"] if c]
            if len(closes) >= 2:
                m[s] = {"price": round(closes[-1], 2),
                        "chg": round(closes[-1] - closes[-2], 2),
                        "chgPct": round(100 * (closes[-1] - closes[-2]) / closes[-2], 2)}
        except Exception as e:
            log("market", s, "failed:", e)
    out = {"updated": STAMP, "indices": m}
    write_json(os.path.join(DATA, "market.json"), out)
    return out

# ---------------- main ----------------
def main():
    tickers = load_tickers()
    log("tickers:", len(tickers))
    pd = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(fetch_prices, t): t for t in tickers}
        pok = perr = 0
        for f in as_completed(futs):
            sym, out, err = f.result()
            if err:
                perr += 1
                log("prices FAIL", sym, err)
            else:
                pok += 1
                pd[sym] = out
    write_json(os.path.join(DATA, "prices.json"), {"updated": STAMP, "symbols": pd})
    log(f"prices done: {pok} ok, {perr} fail")
    funds, ferr = {}, []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(fetch_fundamentals, t): t for t in tickers}
        for f in as_completed(futs):
            sym, fd, err = f.result()
            if err:
                ferr.append(sym)
                log("fundamentals FAIL", sym, err)
            else:
                funds[sym] = fd
    write_json(os.path.join(DATA, "fundamentals.json"),
               {"updated": STAMP, "tickers": funds})
    log(f"fundamentals done: {len(funds)} ok, {len(ferr)} fail")
    try:
        fetch_short(tickers)
    except Exception as e:
        log("short pipeline failed:", e)
    od, ook, oerr = {}, 0, []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(fetch_options, t): t for t in tickers}
        for f in as_completed(futs):
            sym, out, err = f.result()
            if err:
                oerr.append(sym)
                log("options FAIL", sym, err)
            else:
                ook += 1
                od[sym] = out
                if out and out.get("unusual"):
                    log(f"options {sym}: {len(out['unusual'])} unusual")
    write_json(os.path.join(DATA, "options.json"), {"updated": STAMP, "symbols": od})
    log(f"options done: {ook} ok, {len(oerr)} fail")
    iok, ifail = fetch_insider(tickers)
    log(f"insider done: {len(iok)} ok, {len(ifail)} skipped/failed")
    fetch_market(tickers)
    write_json(os.path.join(DATA, "coverage.json"), {
        "updated": STAMP, "tickers": tickers,
        "prices_ok": pok, "fundamentals_fail": ferr,
        "options_fail": oerr, "insider_ok": iok, "insider_skip": ifail,
    })
    log("ALL DONE", STAMP)

if __name__ == "__main__":
    main()
