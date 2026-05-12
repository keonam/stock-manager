import json, math, os, re, sqlite3
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from html import unescape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO
import pandas as pd
from pykrx.website.krx.market import wrap as krx_market_wrap
from main import KST, collect_market_snapshot, load_config, stock, format_date, get_reference_today
B=Path(__file__).resolve().parent; O=B/'output'/'stock_web'; S=O/'snapshots'; D=O/'stock_snapshots_v2.db'; H=os.environ.get('STOCK_WEB_HOST','127.0.0.1'); P=int(os.environ.get('STOCK_WEB_PORT','8060'))

def now(): return datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
def nv(v):
    if v in (None,'','-','None'): return None
    if isinstance(v,(int,float)):
        return None if isinstance(v,float) and (math.isnan(v) or math.isinf(v)) else float(v)
    try: return float(str(v).replace(',','').replace('%','').strip())
    except: return None

def clean(v):
    if isinstance(v,dict): return {k:clean(x) for k,x in v.items()}
    if isinstance(v,list): return [clean(x) for x in v]
    if isinstance(v,float) and (math.isnan(v) or math.isinf(v)): return None
    try:
        if pd.isna(v): return None
    except: pass
    return v

def jd(x): return json.dumps(clean(x),ensure_ascii=False,allow_nan=False)
def rj(p): return json.loads(Path(p).read_text(encoding='utf-8-sig'))
def wj(p,x): Path(p).write_text(jd(x),encoding='utf-8')
def rows(sec,allr): return [r for r in allr if str(r.get('구분'))==sec]
def fmt(v):
    n=nv(v)
    if n is None: return '-'
    return f'{int(round(n)):,}' if abs(n-round(n))<1e-9 else f'{n:,.2f}'
def sgn(v,s=''):
    n=nv(v)
    if n is None: return '-'
    t=f'{int(round(n)):,}' if abs(n-round(n))<1e-9 else f'{n:,.2f}'
    return ('+' if n>0 else '')+t+s

def db(): O.mkdir(parents=True,exist_ok=True); S.mkdir(parents=True,exist_ok=True); c=sqlite3.connect(D); c.row_factory=sqlite3.Row; return c
def q(sql,p=(),one=False,write=False):
    c=db(); cur=c.cursor(); cur.execute(sql,p)
    if write: c.commit(); lid=cur.lastrowid; c.close(); return lid
    r=cur.fetchone() if one else cur.fetchall(); c.close(); return r

def snapshot_json_column():
    cols = [row['name'] for row in q("PRAGMA table_info(snapshots)")]
    for name in ('rows_json', 'data_json', 'payload_json', 'snapshot_json'):
        if name in cols:
            return name
    for name in cols:
        if 'json' in str(name).lower():
            return name
    raise RuntimeError('snapshots 테이블에서 JSON 컬럼을 찾지 못했습니다.')

def ensure_daily_columns(cur):
    cols = [row[1] for row in cur.execute("PRAGMA table_info(daily_price_points)").fetchall()]
    wanted = {
        'open_price': 'REAL',
        'high_price': 'REAL',
        'low_price': 'REAL',
        'personal_amount': 'REAL',
        'foreign_amount': 'REAL',
        'institution_amount': 'REAL',
    }
    for name, data_type in wanted.items():
        if name not in cols:
            cur.execute(f"ALTER TABLE daily_price_points ADD COLUMN {name} {data_type}")


def init():
    c=db(); cur=c.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS snapshots(id INTEGER PRIMARY KEY AUTOINCREMENT,collected_at TEXT,rows_json TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS analyst_notes(id INTEGER PRIMARY KEY AUTOINCREMENT,snapshot_id INTEGER,title TEXT,tags TEXT,body TEXT,created_at TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS portfolio_holdings(code TEXT PRIMARY KEY,name TEXT,quantity REAL,avg_price REAL,target_weight REAL,target_price REAL,stop_price REAL,memo TEXT,created_at TEXT,updated_at TEXT)')
    cur.execute("CREATE TABLE IF NOT EXISTS watchlist_items(code TEXT PRIMARY KEY,name TEXT,priority INTEGER DEFAULT 50,action_state TEXT DEFAULT '관찰',memo TEXT,created_at TEXT,updated_at TEXT)")
    cur.execute('CREATE TABLE IF NOT EXISTS watchlist_notes(id INTEGER PRIMARY KEY AUTOINCREMENT,code TEXT,title TEXT,body TEXT,created_at TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS daily_price_points(code TEXT NOT NULL,name TEXT NOT NULL,trade_date TEXT NOT NULL,close_price REAL,volume REAL,personal_net REAL,foreign_net REAL,institution_net REAL,collected_at TEXT NOT NULL,PRIMARY KEY(code, trade_date))')
    ensure_daily_columns(cur)
    c.commit(); c.close()

def rowdict(r): return {k:r[k] for k in r.keys()}
def snaprow(r):
    d = rowdict(r)
    raw = d.get('rows_json') or d.get('data_json') or d.get('payload_json') or d.get('snapshot_json') or '[]'
    if not raw:
        for v in d.values():
            if isinstance(v, str) and v.lstrip().startswith('['):
                raw = v
                break
    try:
        parsed = json.loads(raw or '[]')
    except Exception:
        parsed = []
    return {
        'id': d.get('id') or d.get('snapshot_id') or d.get('rowid') or 0,
        'collected_at': d.get('collected_at') or d.get('created_at') or d.get('saved_at') or '',
        'rows': parsed,
    }
def snaps(lim=50): return [snaprow(r) for r in q('SELECT * FROM snapshots ORDER BY id DESC LIMIT ?', (lim,))]
def latest():
    r=q('SELECT * FROM snapshots ORDER BY id DESC LIMIT 1',one=True)
    return snaprow(r) if r else None
def snap(i):
    r=q('SELECT * FROM snapshots WHERE id=?',(i,),one=True)
    return snaprow(r) if r else None

def snapshot_items(lim=200):
    items = snaps(lim)
    if items:
        return items
    out = []
    for path in sorted(S.glob('snapshot_*.json'), reverse=True)[:lim]:
        try:
            payload = json.loads(path.read_text(encoding='utf-8-sig'))
        except Exception:
            continue
        out.append({'id': payload.get('id') or 0, 'collected_at': payload.get('collected_at') or '', 'rows': payload.get('rows') or []})
    return out

def collect():
    x=collect_market_snapshot(load_config())
    rs=clean(x['dataframe'].where(pd.notna(x['dataframe']),None).to_dict(orient='records'))
    t=now()
    json_col = snapshot_json_column()
    sql = f"INSERT INTO snapshots(collected_at,{json_col}) VALUES(?,?)"
    i=q(sql,(t,jd(rs)),write=True)
    ts=datetime.now(KST).strftime('%Y%m%d_%H%M%S')
    wj(S/f'snapshot_{ts}.json',{'id':i,'collected_at':t,'rows':rs})
    try: x['dataframe'].to_csv(S/f'snapshot_{ts}.csv',index=False,encoding='utf-8-sig')
    except: pass
    return {'id':i,'collected_at':t,'rows':rs}


def date_like(value):
    try:
        datetime.strptime(str(value)[:10], '%Y-%m-%d')
        return True
    except Exception:
        return False


def normalize_trading_frame(df):
    if df is None or getattr(df, 'empty', True):
        return None
    frame = df.copy()
    idx_sample = [x for x in list(frame.index)[:3]]
    col_sample = [x for x in list(frame.columns)[:3]]
    idx_dates = sum(1 for x in idx_sample if date_like(x))
    col_dates = sum(1 for x in col_sample if date_like(x))
    if col_dates > idx_dates:
        frame = frame.transpose()
    return frame


def pick_series_value(row, candidates, fallback_index=None):
    if row is None:
        return None
    values = {}
    ordered = []
    try:
        for key, value in row.items():
            values[str(key).replace(' ', '').strip()] = value
            number = nv(value)
            if number is not None:
                ordered.append(number)
    except Exception:
        pass
    for name in candidates:
        value = values.get(str(name).replace(' ', '').strip())
        number = nv(value)
        if number is not None:
            return number
    if fallback_index is not None and len(ordered) > fallback_index:
        return ordered[fallback_index]
    return None


def pick_investor_metric(df, investor_aliases, investor_fallback_index=None):
    if df is None or getattr(df, 'empty', True):
        return None
    try:
        labels = [str(x).replace(' ', '').strip() for x in list(df.index)]
        row = None
        for name in investor_aliases:
            key = str(name).replace(' ', '').strip()
            if key in labels:
                row = df.iloc[labels.index(key)]
                break
        if row is None and investor_fallback_index is not None and len(df.index) > investor_fallback_index:
            row = df.iloc[investor_fallback_index]
        return pick_series_value(row, ['순매수', '순매수량', '순매수금액', '순매수거래량', '순매수거래대금'], 2)
    except Exception:
        return None


def build_investor_fallback_map(code, trade_dates, value_mode=False):
    out = {}
    targets = list(trade_dates)[-40:]
    for trade_date in targets:
        target = str(trade_date).replace('-', '')
        try:
            frame = stock.get_market_trading_value_by_investor(target, target, code) if value_mode else stock.get_market_trading_volume_by_investor(target, target, code)
        except Exception:
            frame = None
        if frame is None or getattr(frame, 'empty', True):
            continue
        out[str(trade_date)] = {
            '개인': pick_investor_metric(frame, ['개인', '개인투자자', '개인합계'], 2),
            '외국인합계': pick_investor_metric(frame, ['외국인합계', '외국인', '외인', '외국인계'], 3),
            '기관합계': pick_investor_metric(frame, ['기관합계', '기관', '기관계'], 0),
        }
    return out
def daily_stock_items():
    return [dict(r) for r in q('SELECT code, name, COUNT(*) AS row_count, MIN(trade_date) AS min_date, MAX(trade_date) AS max_date FROM daily_price_points GROUP BY code, name ORDER BY name ASC')]


def normalize_krx_ticker_frame(frame):
    if frame is None or getattr(frame, 'empty', True):
        return frame
    cols = list(frame.columns)
    rename = {}
    if len(cols) >= 1:
        rename[cols[0]] = '시가'
    if len(cols) >= 2:
        rename[cols[1]] = '고가'
    if len(cols) >= 3:
        rename[cols[2]] = '저가'
    if len(cols) >= 4:
        rename[cols[3]] = '종가'
    if len(cols) >= 5:
        rename[cols[4]] = '거래량'
    if len(cols) >= 6:
        rename[cols[5]] = '거래대금'
    return frame.rename(columns=rename)


def build_daily_price_map(start_date, end_date, codes):
    wanted = {str(code) for code in codes}
    out = {str(code): {} for code in wanted}
    errors = []
    current = start_date
    while current <= end_date:
        date_text = format_date(current)
        try:
            frame = normalize_krx_ticker_frame(krx_market_wrap.get_market_ohlcv_by_ticker(date_text, 'ALL'))
        except TypeError:
            try:
                frame = normalize_krx_ticker_frame(krx_market_wrap.get_market_ohlcv_by_ticker(date_text, 'ALL'))
            except Exception as exc:
                errors.append(f'{current.isoformat()} KRX 일자 시세 조회 실패: {exc}')
                current += timedelta(days=1)
                continue
        except Exception as exc:
            errors.append(f'{current.isoformat()} KRX 일자 시세 조회 실패: {exc}')
            current += timedelta(days=1)
            continue
        if frame is not None and not getattr(frame, 'empty', True):
            for ticker, row in frame.iterrows():
                code = str(ticker)
                if code in wanted:
                    out[code][current.isoformat()] = row
        current += timedelta(days=1)
    return out, errors


def build_daily_price_map_from_snapshots(start_date, end_date, codes, limit=600):
    wanted = {str(code) for code in codes}
    out = {str(code): {} for code in wanted}
    source_items = sorted(snapshot_items(limit), key=lambda s: str(s.get('collected_at') or ''))
    for snap_item in source_items:
        collected_at = str(snap_item.get('collected_at') or '')
        trade_text = collected_at[:10]
        try:
            trade_date = datetime.strptime(trade_text, '%Y-%m-%d').date()
        except Exception:
            continue
        if trade_date < start_date or trade_date > end_date:
            continue
        for row in rows('종목', snap_item.get('rows') or []):
            code = str(row.get('코드') or '')
            if code not in wanted:
                continue
            bucket = out[code].setdefault(trade_text, {})
            current_price = nv(row.get('현재가'))
            open_price = nv(row.get('시가')) if nv(row.get('시가')) is not None else current_price
            high_price = nv(row.get('고가')) if nv(row.get('고가')) is not None else current_price
            low_price = nv(row.get('저가')) if nv(row.get('저가')) is not None else current_price
            volume = nv(row.get('거래량'))
            if '시가' not in bucket and open_price is not None:
                bucket['시가'] = open_price
            if high_price is not None:
                bucket['고가'] = max(bucket.get('고가', high_price), high_price)
            if low_price is not None:
                bucket['저가'] = min(bucket.get('저가', low_price), low_price) if bucket.get('저가') is not None else low_price
            if current_price is not None:
                bucket['종가'] = current_price
            if volume is not None:
                bucket['거래량'] = max(bucket.get('거래량', volume), volume)
    return out


def fetch_text(url, encoding='euc-kr'):
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Cache-Control': 'no-cache', 'Pragma': 'no-cache', 'Referer': 'https://finance.naver.com/'})
    with urlopen(req, timeout=10) as resp:
        raw = resp.read()
    return raw.decode(encoding, errors='ignore')


def flatten_columns(columns):
    out = []
    for col in list(columns):
        if isinstance(col, tuple):
            parts = [str(x).strip() for x in col if str(x).strip() and str(x).strip().lower() != 'nan']
            text = ' '.join(parts)
        else:
            text = str(col)
        out.append(re.sub(r'\s+', ' ', text).strip())
    return out


def pick_naver_table(html, required_tokens):
    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        return pd.DataFrame()
    for table in tables:
        frame = table.copy()
        frame.columns = flatten_columns(frame.columns)
        cols = [str(c) for c in frame.columns]
        if all(any(token in col for col in cols) for token in required_tokens):
            return frame
    return pd.DataFrame()


def normalize_naver_daily_table(frame):
    if frame is None or frame.empty:
        return pd.DataFrame()
    rename = {}
    for col in frame.columns:
        name = str(col)
        if '??' in name:
            rename[col] = '??'
        elif '??' in name:
            rename[col] = '??'
        elif '??' in name:
            rename[col] = '??'
        elif '??' in name:
            rename[col] = '??'
        elif '??' in name:
            rename[col] = '??'
        elif '???' in name:
            rename[col] = '???'
    frame = frame.rename(columns=rename)
    keep = [c for c in ['??', '??', '??', '??', '??', '???'] if c in frame.columns]
    if '??' not in keep:
        return pd.DataFrame()
    frame = frame[keep].copy()
    frame['??'] = frame['??'].astype(str).str.strip()
    frame = frame[frame['??'].str.match(r'^\d{4}\.\d{2}\.\d{2}$', na=False)]
    return frame


def normalize_naver_frgn_table(frame):
    if frame is None or frame.empty:
        return pd.DataFrame()
    rename = {}
    for col in frame.columns:
        name = str(col)
        if '??' in name:
            rename[col] = '??'
        elif '??' in name:
            rename[col] = '??'
        elif '???' in name:
            rename[col] = '???'
        elif '??' in name and ('???' in name or name == '??'):
            rename[col] = '??????'
        elif '???' in name and ('???' in name or name == '???'):
            rename[col] = '??????'
    frame = frame.rename(columns=rename)
    keep = [c for c in ['??', '??', '???', '??????', '??????'] if c in frame.columns]
    if '??' not in keep:
        return pd.DataFrame()
    frame = frame[keep].copy()
    frame['??'] = frame['??'].astype(str).str.strip()
    frame = frame[frame['??'].str.match(r'^\d{4}\.\d{2}\.\d{2}$', na=False)]
    return frame


def fetch_naver_daily_stock_data(code, start_date, end_date, max_pages=2):
    day_span = max(1, (end_date - start_date).days + 1)
    pages = min(max(1, int(max_pages or 2)), max(1, math.ceil(day_span / 10) + 1))
    rows_by_date = {}
    errors = []
    for page in range(1, pages + 1):
        try:
            daily_html = fetch_text(f'https://finance.naver.com/item/sise_day.naver?code={code}&page={page}')
            daily_table = normalize_naver_daily_table(pick_naver_table(daily_html, ['??', '??', '??', '??', '??', '???']))
            for _, row in daily_table.iterrows():
                trade_date = str(row.get('??') or '').replace('.', '-')
                try:
                    dt = datetime.strptime(trade_date, '%Y-%m-%d').date()
                except Exception:
                    continue
                if dt < start_date or dt > end_date:
                    continue
                bucket = rows_by_date.setdefault(trade_date, {})
                for key in ['??', '??', '??', '??', '???']:
                    val = nv(row.get(key))
                    if val is not None:
                        bucket[key] = val
        except Exception as exc:
            errors.append(f'{code} ??? ???? {page}p ?? ??: {exc}')
        try:
            frgn_html = fetch_text(f'https://finance.naver.com/item/frgn.naver?code={code}&page={page}')
            frgn_table = normalize_naver_frgn_table(pick_naver_table(frgn_html, ['??', '???', '??', '???']))
            for _, row in frgn_table.iterrows():
                trade_date = str(row.get('??') or '').replace('.', '-')
                try:
                    dt = datetime.strptime(trade_date, '%Y-%m-%d').date()
                except Exception:
                    continue
                if dt < start_date or dt > end_date:
                    continue
                bucket = rows_by_date.setdefault(trade_date, {})
                inst = nv(row.get('??????'))
                foreign = nv(row.get('??????'))
                if inst is not None:
                    bucket['??????'] = inst
                if foreign is not None:
                    bucket['??????'] = foreign
                if inst is not None or foreign is not None:
                    bucket['??????'] = -((foreign or 0) + (inst or 0))
                close_price = nv(bucket.get('??')) if bucket.get('??') is not None else nv(row.get('??'))
                if close_price is not None:
                    if bucket.get('??????') is not None:
                        bucket['???????'] = round(bucket['??????'] * close_price, 2)
                    if foreign is not None:
                        bucket['???????'] = round(foreign * close_price, 2)
                    if inst is not None:
                        bucket['???????'] = round(inst * close_price, 2)
        except Exception as exc:
            errors.append(f'{code} ??? ?? {page}p ?? ??: {exc}')
    return rows_by_date, errors

def existing_daily_last_dates():
    return {str(r['code']): str(r['max_date']) for r in q('SELECT code, MAX(trade_date) AS max_date FROM daily_price_points GROUP BY code') if r['max_date']}


def build_naver_daily_maps(plan_items):
    out = {}
    errors = []
    items = list(plan_items or [])
    if not items:
        return out, errors
    workers = min(16, max(1, len(items)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                fetch_naver_daily_stock_data,
                str(item.get('code')),
                item.get('start_date'),
                item.get('end_date'),
                item.get('max_pages', 2),
            ): item
            for item in items
        }
        for future in as_completed(futures):
            item = futures[future]
            code = str(item.get('code'))
            name = str(item.get('name') or code)
            try:
                rows_by_date, stock_errors = future.result()
                out[code] = rows_by_date
                errors.extend([f'{name} {msg}' for msg in stock_errors])
            except Exception as exc:
                out[code] = {}
                errors.append(f'{name} ??? ?? ??? ?? ?? ??: {exc}')
    return out, errors

def collect_daily_prices(days=365):
    cfg = load_config()
    stocks_cfg = list(cfg.get('stocks', []))
    end_date = get_reference_today()
    requested_days = int(days or 365)
    start_date = end_date - timedelta(days=requested_days)
    fast_window = min(max(requested_days, 5), 30)
    recent_start = end_date - timedelta(days=fast_window)
    existing_last = existing_daily_last_dates()
    plan_items = []
    for item in stocks_cfg:
        code = str(item.get('code'))
        stock_start = recent_start
        max_pages = 2
        last_date = existing_last.get(code)
        if last_date:
            try:
                last_dt = datetime.strptime(last_date, '%Y-%m-%d').date()
                stock_start = max(start_date, last_dt - timedelta(days=3))
                max_pages = 1
            except Exception:
                stock_start = recent_start
        plan_items.append({'code': code, 'name': str(item.get('name') or code), 'start_date': stock_start, 'end_date': end_date, 'max_pages': max_pages})
    saved_rows = 0
    errors = []
    collected_at = now()
    naver_map, naver_errors = build_naver_daily_maps(plan_items)
    snapshot_price_map = build_daily_price_map_from_snapshots(recent_start, end_date, [str(item.get('code')) for item in stocks_cfg], limit=200)
    errors.extend(naver_errors)
    for item in stocks_cfg:
        code = str(item.get('code'))
        name = str(item.get('name') or code)
        merged_rows = dict(snapshot_price_map.get(code, {}))
        for trade_date, row in (naver_map.get(code) or {}).items():
            bucket = dict(merged_rows.get(trade_date, {}))
            for key in ['??', '??', '??', '??', '???']:
                val = nv(row.get(key))
                if val is not None:
                    bucket[key] = val
            merged_rows[trade_date] = bucket
        if not merged_rows:
            errors.append(f'{name} ?? ??? ??')
            continue
        price_df = pd.DataFrame.from_dict(merged_rows, orient='index').sort_index()
        flow_map = {}
        amount_map = {}
        for trade_date, row in (naver_map.get(code) or {}).items():
            personal = nv(row.get('??????'))
            foreign = nv(row.get('??????'))
            inst = nv(row.get('??????'))
            if personal is not None or foreign is not None or inst is not None:
                flow_map[trade_date] = {'??': personal, '?????': foreign, '????': inst}
            personal_amt = nv(row.get('???????'))
            foreign_amt = nv(row.get('???????'))
            inst_amt = nv(row.get('???????'))
            if personal_amt is not None or foreign_amt is not None or inst_amt is not None:
                amount_map[trade_date] = {'??': personal_amt, '?????': foreign_amt, '????': inst_amt}
        if not flow_map:
            errors.append(f'{name} ?? ?? ??? ?? ??')
        if not amount_map:
            errors.append(f'{name} ?? ?? ??? ?? ??')
        for idx, row in price_df.iterrows():
            trade_date = idx.strftime('%Y-%m-%d') if hasattr(idx, 'strftime') else str(idx)
            flow_row = flow_map.get(trade_date)
            amount_row = amount_map.get(trade_date)
            q(
                "INSERT INTO daily_price_points(code,name,trade_date,close_price,open_price,high_price,low_price,volume,personal_net,foreign_net,institution_net,personal_amount,foreign_amount,institution_amount,collected_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(code,trade_date) DO UPDATE SET name=excluded.name,close_price=excluded.close_price,open_price=excluded.open_price,high_price=excluded.high_price,low_price=excluded.low_price,volume=excluded.volume,personal_net=excluded.personal_net,foreign_net=excluded.foreign_net,institution_net=excluded.institution_net,personal_amount=excluded.personal_amount,foreign_amount=excluded.foreign_amount,institution_amount=excluded.institution_amount,collected_at=excluded.collected_at",
                (
                    code,
                    name,
                    trade_date,
                    nv(row.get('??')),
                    nv(row.get('??')),
                    nv(row.get('??')),
                    nv(row.get('??')),
                    nv(row.get('???')),
                    pick_series_value(flow_row, ['??', '?????', '????'], 2),
                    pick_series_value(flow_row, ['?????', '???', '??', '????'], 3),
                    pick_series_value(flow_row, ['????', '??', '???'], 0),
                    pick_series_value(amount_row, ['??', '?????', '????'], 2),
                    pick_series_value(amount_row, ['?????', '???', '??', '????'], 3),
                    pick_series_value(amount_row, ['????', '??', '???'], 0),
                    collected_at,
                ),
                write=True,
            )
            saved_rows += 1
    return {
        'saved_rows': saved_rows,
        'stock_count': len(stocks_cfg),
        'start_date': recent_start.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'errors': errors,
        'collected_at': collected_at,
        'mode': 'fast_incremental',
    }

def daily_stock_payload(code, limit=180):
    rows_desc = [dict(r) for r in q('SELECT code, name, trade_date, close_price, open_price, high_price, low_price, volume, personal_net, foreign_net, institution_net, personal_amount, foreign_amount, institution_amount, collected_at FROM daily_price_points WHERE code=? ORDER BY trade_date DESC LIMIT ?', (str(code), int(limit)))]
    if not rows_desc:
        return {'rows': [], 'chart_rows': [], 'summary': {}, 'analysis': []}
    latest = rows_desc[0]
    previous = rows_desc[1] if len(rows_desc) > 1 else None
    lookback_20 = rows_desc[:20]
    base_5 = rows_desc[min(4, len(rows_desc)-1)] if len(rows_desc) >= 5 else None
    base_20 = rows_desc[min(19, len(rows_desc)-1)] if len(rows_desc) >= 20 else None
    latest_close = latest.get('close_price')
    prev_close = previous.get('close_price') if previous else None
    latest_high = latest.get('high_price')
    latest_low = latest.get('low_price')
    close_position = None
    if latest_high not in (None, 0) and latest_low is not None and latest_close is not None and latest_high != latest_low:
        close_position = round(((latest_close - latest_low) / (latest_high - latest_low)) * 100, 2)
    intraday_range_rate = None
    if latest_high is not None and latest_low is not None and prev_close not in (None, 0):
        intraday_range_rate = round(((latest_high - latest_low) / prev_close) * 100, 2)
    summary = {
        'name': latest.get('name'),
        'code': latest.get('code'),
        'latest_date': latest.get('trade_date'),
        'latest_open': latest.get('open_price'),
        'latest_high': latest.get('high_price'),
        'latest_low': latest.get('low_price'),
        'latest_close': latest_close,
        'latest_volume': latest.get('volume'),
        'one_day_change': round(((latest_close - prev_close) / prev_close) * 100, 2) if prev_close not in (None, 0) and latest_close is not None else None,
        'five_day_change': round(((latest_close - base_5.get('close_price')) / base_5.get('close_price')) * 100, 2) if base_5 and base_5.get('close_price') not in (None, 0) and latest_close is not None else None,
        'twenty_day_change': round(((latest_close - base_20.get('close_price')) / base_20.get('close_price')) * 100, 2) if base_20 and base_20.get('close_price') not in (None, 0) and latest_close is not None else None,
        'avg_volume_20d': round(sum((r.get('volume') or 0) for r in lookback_20) / len(lookback_20), 2) if lookback_20 else None,
        'personal_20d': round(sum((r.get('personal_net') or 0) for r in lookback_20), 2),
        'foreign_20d': round(sum((r.get('foreign_net') or 0) for r in lookback_20), 2),
        'institution_20d': round(sum((r.get('institution_net') or 0) for r in lookback_20), 2),
        'personal_amount_20d': round(sum((r.get('personal_amount') or 0) for r in lookback_20), 2),
        'foreign_amount_20d': round(sum((r.get('foreign_amount') or 0) for r in lookback_20), 2),
        'institution_amount_20d': round(sum((r.get('institution_amount') or 0) for r in lookback_20), 2),
        'intraday_range_rate': intraday_range_rate,
        'close_position': close_position,
        'row_count': len(rows_desc),
    }
    dominant_volume = max(
        [('개인', abs(summary.get('personal_20d') or 0)), ('외인', abs(summary.get('foreign_20d') or 0)), ('기관', abs(summary.get('institution_20d') or 0))],
        key=lambda x: x[1],
    )[0]
    dominant_amount = max(
        [('개인', abs(summary.get('personal_amount_20d') or 0)), ('외인', abs(summary.get('foreign_amount_20d') or 0)), ('기관', abs(summary.get('institution_amount_20d') or 0))],
        key=lambda x: x[1],
    )[0]
    analysis = [
        f"최근 종가는 {fmt(summary.get('latest_close'))}원이며 시가 {fmt(summary.get('latest_open'))}원, 고가 {fmt(summary.get('latest_high'))}원, 저가 {fmt(summary.get('latest_low'))}원입니다.",
        f"전일 대비 {sgn(summary.get('one_day_change'), '%')}, 5거래일 {sgn(summary.get('five_day_change'), '%')}, 20거래일 {sgn(summary.get('twenty_day_change'), '%')} 흐름입니다.",
        f"당일 변동폭은 전일 종가 대비 {sgn(summary.get('intraday_range_rate'), '%')}이며 종가 위치는 장중 범위의 {fmt(summary.get('close_position'))}% 수준입니다.",
        f"최근 20거래일 순매수량 기준 우세 주체는 {dominant_volume}이며 개인 {fmt(summary.get('personal_20d'))}, 외인 {fmt(summary.get('foreign_20d'))}, 기관 {fmt(summary.get('institution_20d'))}입니다.",
        f"최근 20거래일 순매수금액 기준 우세 주체는 {dominant_amount}이며 개인 {fmt(summary.get('personal_amount_20d'))}, 외인 {fmt(summary.get('foreign_amount_20d'))}, 기관 {fmt(summary.get('institution_amount_20d'))}입니다.",
        f"최근 20거래일 평균 거래량은 {fmt(summary.get('avg_volume_20d'))}입니다.",
    ]
    return {'rows': rows_desc, 'chart_rows': list(reversed(rows_desc)), 'summary': summary, 'analysis': analysis}
def known():
    out=[]
    try:
        cfg=rj(B/'config.json')
        for k in ('stocks','indexes','commodities'):
            for it in cfg.get(k,[]):
                if isinstance(it,dict) and it.get('name') and it.get('code'): out.append({'name':str(it['name']),'code':str(it['code'])})
    except: pass
    lt=latest()
    if lt:
        for r in lt['rows']:
            if r.get('이름') and r.get('코드'): out.append({'name':str(r['이름']),'code':str(r['코드'])})
    seen=set(); res=[]
    for it in out:
        k=(it['name'],it['code'])
        if k not in seen: seen.add(k); res.append(it)
    return res

def resolve(name,code):
    name=(name or '').strip(); code=(code or '').strip()
    if code:
        for it in known():
            if it['code']==code: return it['name'],code
        return name or code,code
    if not name: raise ValueError('종목명 또는 종목코드가 필요합니다.')
    for it in known():
        if it['name']==name: return it['name'],it['code']
    raise ValueError(f'종목명으로 코드를 찾지 못했습니다: {name}')

def hist(code,lim=12):
    out=[]
    for s in snaps(lim):
        for r in s['rows']:
            if str(r.get('코드'))==str(code):
                z=dict(r); z['스냅샷시각']=s['collected_at']; out.append(z); break
    return out

def recommendation_daily_context(code, lim=90):
    rr=[dict(r) for r in q('SELECT trade_date, close_price, open_price, high_price, low_price, volume, personal_net, foreign_net, institution_net, personal_amount, foreign_amount, institution_amount FROM daily_price_points WHERE code=? ORDER BY trade_date DESC LIMIT ?', (str(code), int(lim)))]
    if not rr:
        return {'has_daily_data': False}
    latest_row=rr[0]
    latest_close=nv(latest_row.get('close_price'))
    base20=nv(rr[min(19,len(rr)-1)].get('close_price')) if rr else None
    base60=nv(rr[min(59,len(rr)-1)].get('close_price')) if rr else None
    high=nv(latest_row.get('high_price'))
    low=nv(latest_row.get('low_price'))
    close_pos=None
    if latest_close is not None and high is not None and low is not None and high!=low:
        close_pos=round((latest_close-low)/(high-low)*100,2)
    volumes=[nv(x.get('volume')) for x in rr[:20] if nv(x.get('volume')) is not None]
    volume_multiple=None
    if volumes:
        avg20=sum(volumes)/len(volumes)
        if avg20:
            volume_multiple=round((volumes[0] or 0)/avg20,2)
    foreign20=sum((nv(x.get('foreign_net')) or 0) for x in rr[:20])
    institution20=sum((nv(x.get('institution_net')) or 0) for x in rr[:20])
    foreign_amount20=sum((nv(x.get('foreign_amount')) or 0) for x in rr[:20])
    institution_amount20=sum((nv(x.get('institution_amount')) or 0) for x in rr[:20])
    return {
        'has_daily_data': True,
        'daily_latest_date': latest_row.get('trade_date'),
        'daily_20d_change': round(((latest_close-base20)/base20)*100,2) if latest_close is not None and base20 not in (None,0) else None,
        'daily_60d_change': round(((latest_close-base60)/base60)*100,2) if latest_close is not None and base60 not in (None,0) else None,
        'daily_foreign_20d': round(foreign20,2),
        'daily_institution_20d': round(institution20,2),
        'daily_foreign_amount_20d': round(foreign_amount20,2),
        'daily_institution_amount_20d': round(institution_amount20,2),
        'daily_volume_multiple': volume_multiple,
        'daily_close_position': close_pos,
    }
def themes_map(): return {'반도체':['삼성전자','SK하이닉스','오픈엣지테크놀러지','아스플로'],'방산/우주':['한화에어로스페이스','쎄트렉아이','비츠로넥스텍','그린광학'],'로봇/자동화':['우진','우진엔텍','로보티즈'],'에너지/전력':['한화솔루션','HD현대에너지솔루션','LS일렉트릭','일진전기','OCI홀딩스','두산에너빌리티'],'2차전지':['삼성SDI','엘엔에프','에코프로','에코프로비엠','에코프로머티','롯데에너지머티리얼즈','코스모신소재','LS머티리얼즈'],'산업재':['현대차','두산밥캣','HJ중공업','삼성중공업']}
def theme(name):
    for k,v in themes_map().items():
        if name in v: return k
    return '기타'

def theme_table(allr):
    g={}
    for r in rows('종목',allr): g.setdefault(theme(str(r.get('이름'))),[]).append(r)
    out=[]
    for k,its in g.items():
        rr=[nv(x.get('등락률(%)')) for x in its if nv(x.get('등락률(%)')) is not None]; avg=round(sum(rr)/len(rr),2) if rr else None; leaders=sorted(its,key=lambda x:nv(x.get('등락률(%)')) or -999,reverse=True)[:2]
        out.append({'theme':k,'count':len(its),'avg_rate':avg,'leaders':[x.get('이름') for x in leaders]})
    return sorted(out,key=lambda x:nv(x.get('avg_rate')) or -999,reverse=True)

def brief(allr):
    st=rows('종목',allr); idx=rows('지수',allr); cm=rows('상품',allr); fl=rows('수급',allr); out=[]
    for n in ('코스피','코스닥','원달러환율'):
        r=next((x for x in idx if x.get('이름')==n),None)
        if r: out.append(f"{n}는 현재 {fmt(r.get('현재가'))}, 전일 대비 {sgn(r.get('등락률(%)'),'%')}입니다.")
    if st:
        up=len([x for x in st if (nv(x.get('등락률(%)')) or 0)>0]); dn=len([x for x in st if (nv(x.get('등락률(%)')) or 0)<0]); avg=round(sum(nv(x.get('등락률(%)')) or 0 for x in st)/len(st),2)
        out.append(f"종목군은 상승 {up}종목, 하락 {dn}종목이며 평균 등락률은 {sgn(avg,'%')}입니다.")
        vv=sorted(st,key=lambda x:nv(x.get('거래량배수')) or -999,reverse=True)[:3]
        if vv: out.append('거래량 탄력 상위는 '+', '.join(f"{x.get('이름')} {fmt(x.get('거래량배수'))}배" for x in vv)+'입니다.')
    if fl: out.append(f"코스피 수급은 현재 비고: {fl[0].get('비고') or '-'} 상태입니다.")
    if cm:
        o=next((x for x in cm if x.get('이름')=='서부텍사스중질유'),None)
        if o: out.append(f"유가는 {sgn(o.get('등락률(%)'),'%')} 움직임입니다.")
    return out

def notes(): return [{'id':r['id'],'snapshot_id':r['snapshot_id'],'title':r['title'],'tags':[x.strip() for x in (r['tags'] or '').split(',') if x.strip()],'body':r['body'],'created_at':r['created_at']} for r in q('SELECT * FROM analyst_notes ORDER BY id DESC LIMIT 100')]
def add_note(sid,title,tags,body): q('INSERT INTO analyst_notes(snapshot_id,title,tags,body,created_at) VALUES(?,?,?,?,?)',(sid,title.strip(),tags.strip(),body.strip(),now()),write=True)

def holds(): return [dict(r) for r in q('SELECT * FROM portfolio_holdings ORDER BY updated_at DESC,name ASC')]
def up_hold(p):
    n,c=resolve(p.get('name'),p.get('code')); t=now(); q("INSERT INTO portfolio_holdings(code,name,quantity,avg_price,target_weight,target_price,stop_price,memo,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(code) DO UPDATE SET name=excluded.name,quantity=excluded.quantity,avg_price=excluded.avg_price,target_weight=excluded.target_weight,target_price=excluded.target_price,stop_price=excluded.stop_price,memo=excluded.memo,updated_at=excluded.updated_at",(c,n,float(p.get('quantity') or 0),float(p.get('avg_price') or 0),float(p.get('target_weight') or 0),nv(p.get('target_price')),nv(p.get('stop_price')),str(p.get('memo') or ''),t,t),write=True); return {'name':n,'code':c}
def del_hold(code): q('DELETE FROM portfolio_holdings WHERE code=?',(str(code),),write=True)

def port():
    hs=holds(); lt=latest(); by={str(r.get('코드')):r for r in rows('종목',lt['rows'])} if lt else {}; out=[]; tv=0.0; tc=0.0
    for it in hs:
        r=by.get(str(it.get('code'))); cp=nv(r.get('현재가')) if r else None; qty=float(it.get('quantity') or 0); avg=float(it.get('avg_price') or 0); mv=cp*qty if cp is not None else None; cv=avg*qty; pnl=mv-cv if mv is not None else None; pr=round((pnl/cv)*100,2) if pnl is not None and cv else None
        tp=nv(it.get('target_price')); sp=nv(it.get('stop_price')); tg=round(((tp-cp)/cp)*100,2) if cp is not None and tp is not None and cp>0 else None; sg=round(((cp-sp)/cp)*100,2) if cp is not None and sp is not None and cp>0 else None; rr=None
        if cp is not None and tp is not None and sp is not None and tp>cp and sp<cp: rr=round((tp-cp)/(cp-sp),2)
        st='가격미수집' if cp is None else '목표도달' if tp is not None and cp>=tp else '손절경계' if sp is not None and cp<=sp else '목표근접' if tg is not None and tg<=3 else '손절근접' if sg is not None and sg<=3 else '보유'
        if mv is not None: tv+=mv
        tc+=cv
        out.append({**it,'current_price':cp,'change_rate':nv(r.get('등락률(%)')) if r else None,'gap_rate':nv(r.get('시가갭률(%)')) if r else None,'intraday_range_rate':nv(r.get('장중변동률(%)')) if r else None,'close_position':nv(r.get('종가위치(%)')) if r else None,'volume_ratio':nv(r.get('거래량배수')) if r else None,'market_value':mv,'cost_value':cv,'pnl':pnl,'pnl_rate':pr,'target_price':tp,'stop_price':sp,'target_gap_pct':tg,'stop_gap_pct':sg,'risk_reward_ratio':rr,'holding_status':st})
    for it in out: it['weight']=round((it['market_value']/tv)*100,2) if it.get('market_value') is not None and tv>0 else None
    th=len([x for x in out if x['holding_status']=='목표도달']); tn=len([x for x in out if x['holding_status']=='목표근접']); sr=len([x for x in out if x['holding_status']=='손절경계']); sn=len([x for x in out if x['holding_status']=='손절근접']); mp=len([x for x in out if x['holding_status']=='가격미수집'])
    risk=sum((1 if abs(nv(x.get('gap_rate')) or 0)>=3 else 0)+(1 if (nv(x.get('intraday_range_rate')) or 0)>=8 else 0)+(2 if (nv(x.get('close_position')) or 100)<=15 else 0)+(2 if (nv(x.get('pnl_rate')) or 0)<=-10 else 0) for x in out)
    pay={'summary':{'holding_count':len(out),'total_value':round(tv,2),'total_cost':round(tc,2),'total_pnl':round(tv-tc,2) if tv else None,'total_pnl_rate':round(((tv-tc)/tc)*100,2) if tc and tv else None,'latest_collected_at':lt['collected_at'] if lt else '','target_hit_count':th,'target_near_count':tn,'stop_risk_count':sr,'stop_near_count':sn,'missing_price_count':mp,'risk_score':risk,'risk_grade':'경계' if risk>=8 else '주의' if risk>=4 else '안정'},'holdings':out}
    byw=sorted(out,key=lambda x:nv(x.get('weight')) or -1,reverse=True); byp=sorted(out,key=lambda x:nv(x.get('pnl_rate')) or -999,reverse=True); byr=sorted(out,key=lambda x:(nv(x.get('intraday_range_rate')) or 0)+abs(nv(x.get('gap_rate')) or 0),reverse=True)
    pay['insights']=[f"총 {len(out)}종목, 평가금액 {fmt(pay['summary']['total_value'])}, 손익률 {sgn(pay['summary']['total_pnl_rate'],'%')}입니다."] if out else ['아직 등록된 포트 종목이 없습니다.']
    if byw: pay['insights'].append(f"비중 상위는 {byw[0].get('name')} {fmt(byw[0].get('weight'))}%입니다.")
    if byp: pay['insights']+= [f"손익 기여 상위는 {byp[0].get('name')} {fmt(byp[0].get('pnl'))}입니다.",f"점검 우선 종목은 {byp[-1].get('name')} {fmt(byp[-1].get('pnl'))}입니다."]
    pay['report']=[f"현재 포트 손익률은 {sgn(pay['summary']['total_pnl_rate'],'%')}입니다."] if out else ['아직 등록된 포트 종목이 없습니다.']
    pay['alerts']=[{'name':x.get('name'),'message':f"{x.get('holding_status')} 상태입니다.",'level':'warn' if x.get('holding_status') in ('손절경계','손절근접','가격미수집') else 'info'} for x in out if x.get('holding_status') not in ('보유',None)][:8]
    pay['focus_cards']=[
        {'title':'비중 상위','name':byw[0].get('name') if byw else '-','metric':f"{fmt(byw[0].get('weight'))}%" if byw else '-','detail':'현재 포트 내 비중이 가장 큽니다.' if byw else '-'},
        {'title':'수익 상위','name':byp[0].get('name') if byp else '-','metric':sgn(byp[0].get('pnl_rate'),'%') if byp else '-','detail':f"손익 {fmt(byp[0].get('pnl'))}" if byp else '-'},
        {'title':'부진 종목','name':byp[-1].get('name') if byp else '-','metric':sgn(byp[-1].get('pnl_rate'),'%') if byp else '-','detail':f"손익 {fmt(byp[-1].get('pnl'))}" if byp else '-'},
        {'title':'당일 변동','name':byr[0].get('name') if byr else '-','metric':f"{fmt(byr[0].get('intraday_range_rate'))}%" if byr else '-','detail':'장중 변동률 기준 상위 종목입니다.' if byr else '-'}
    ] if out else []
    pay['rebalance']=[{'name':x.get('name'),'action':'비중 축소 검토','reason':f"비중이 목표 대비 {round((nv(x.get('weight')) or 0)-(nv(x.get('target_weight')) or 0),2)}%p 높습니다."} for x in out if nv(x.get('weight')) is not None and nv(x.get('target_weight')) is not None and (nv(x.get('weight'))-nv(x.get('target_weight')))>=3][:6]
    pay['history']=[{'collected_at':s['collected_at'],'total_value':round(sum((float(h.get('quantity') or 0))*(nv(next((r.get('현재가') for r in rows('종목',s['rows']) if str(r.get('코드'))==str(h.get('code'))),None)) or 0) for h in hs),2)} for s in reversed(snaps(20))] if hs else []
    return pay
def witems(): return [dict(r) for r in q('SELECT * FROM watchlist_items ORDER BY priority DESC,updated_at DESC,name ASC')]
def up_watch(name,code,priority,state,memo):
    n,c=resolve(name,code); t=now(); q("INSERT INTO watchlist_items(code,name,priority,action_state,memo,created_at,updated_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(code) DO UPDATE SET name=excluded.name,priority=excluded.priority,action_state=excluded.action_state,memo=excluded.memo,updated_at=excluded.updated_at",(c,n,int(priority),state or '관찰',memo or '',t,t),write=True); return {'name':n,'code':c}
def del_watch(code): q('DELETE FROM watchlist_items WHERE code=?',(str(code),),write=True)
def add_wnote(code,title,body): q('INSERT INTO watchlist_notes(code,title,body,created_at) VALUES(?,?,?,?)',(str(code),title.strip(),body.strip(),now()),write=True)
def wnotes(code): return [dict(r) for r in q('SELECT * FROM watchlist_notes WHERE code=? ORDER BY id DESC',(str(code),))]
def wrisk(it):
    s=(1 if abs(nv(it.get('gap_rate')) or 0)>=3 else 0)+(2 if (nv(it.get('intraday_range_rate')) or 0)>=8 else 0)+(2 if (nv(it.get('close_position')) or 100)<=20 else 0)+(1 if (nv(it.get('change_rate')) or 0)<=-3 else 0)+(1 if (nv(it.get('volume_ratio')) or 0)>=2 else 0)
    return s,('경계' if s>=5 else '주의' if s>=3 else '안정')
def wcheck(it):
    z=[]
    if abs(nv(it.get('gap_rate')) or 0)>=3: z.append('시가갭 배경을 뉴스/공시와 함께 확인합니다.')
    if (nv(it.get('intraday_range_rate')) or 0)>=8: z.append('장중 변동성이 높아 추격 판단을 재점검합니다.')
    if (nv(it.get('close_position')) or 100)<=20: z.append('저가권 마감 여부를 확인합니다.')
    if (nv(it.get('volume_ratio')) or 0)>=1.8: z.append('거래량 증가가 추세 강화인지 확인합니다.')
    return z or ['현재는 기본 추세와 거래량 흐름 위주로 점검하면 됩니다.']
def watch():
    lt=latest(); by={str(r.get('코드')):r for r in rows('종목',lt['rows'])} if lt else {}; out=[]
    for it in witems():
        r=by.get(str(it.get('code'))); x={**it,'current_price':nv(r.get('현재가')) if r else None,'change_rate':nv(r.get('등락률(%)')) if r else None,'volume_ratio':nv(r.get('거래량배수')) if r else None,'gap_rate':nv(r.get('시가갭률(%)')) if r else None,'intraday_range_rate':nv(r.get('장중변동률(%)')) if r else None,'close_position':nv(r.get('종가위치(%)')) if r else None,'latest_collected_at':lt['collected_at'] if lt else '','history':[{'date':h.get('기준일') or h.get('스냅샷시각'),'price':nv(h.get('현재가'))} for h in hist(it.get('code'),10)]}
        sc,gr=wrisk(x); x['risk_score']=sc; x['risk_grade']=gr; x['grade_history']=[{'date':h.get('기준일') or h.get('스냅샷시각'),'risk_score':wrisk({'gap_rate':h.get('시가갭률(%)'),'intraday_range_rate':h.get('장중변동률(%)'),'close_position':h.get('종가위치(%)'),'change_rate':h.get('등락률(%)'),'volume_ratio':h.get('거래량배수')})[0],'risk_grade':wrisk({'gap_rate':h.get('시가갭률(%)'),'intraday_range_rate':h.get('장중변동률(%)'),'close_position':h.get('종가위치(%)'),'change_rate':h.get('등락률(%)'),'volume_ratio':h.get('거래량배수')})[1]} for h in hist(it.get('code'),8)]; x['checklist']=wcheck(x); out.append(x)
    sm={'count':len(out),'positive_count':len([x for x in out if (nv(x.get('change_rate')) or 0)>0]),'high_volume_count':len([x for x in out if (nv(x.get('volume_ratio')) or 0)>=1.8]),'warning_count':len([x for x in out if x.get('risk_grade')=='주의']),'high_risk_count':len([x for x in out if x.get('risk_grade')=='경계'])}
    bf=['우선 점검 종목은 '+', '.join(f"{x.get('name')}({x.get('risk_grade')})" for x in sorted(out,key=lambda x:x.get('risk_score') or 0,reverse=True)[:3])+'입니다.'] if out else ['감시 종목이 아직 없습니다.']
    return {'summary':sm,'brief':bf,'items':out}

def wbrief(code):
    x=next((i for i in watch()['items'] if str(i.get('code'))==str(code)),None)
    if not x: return []
    out=[f"{x.get('name')} 현재 위험등급은 {x.get('risk_grade')}입니다."]
    if x.get('action_state'): out.append(f"현재 액션 상태는 {x.get('action_state')}입니다.")
    if (nv(x.get('volume_ratio')) or 0)>=1.8: out.append('거래량이 평소보다 강하게 붙는 구간입니다.')
    if (nv(x.get('close_position')) or 100)<=20: out.append('저가권 마감으로 종가 구조는 약한 편입니다.')
    return out


def news_text(v):
    return re.sub(r'\s+', ' ', unescape(re.sub(r'<[^>]+>', '', str(v or '')))).strip()


def google_news_search(query, limit=6):
    url = f"https://news.google.com/rss/search?q={quote(query)}&hl=ko&gl=KR&ceid=KR:ko"
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Cache-Control': 'no-cache', 'Pragma': 'no-cache'})
    today = datetime.now(KST).date()
    items = []
    with urlopen(req, timeout=10) as resp:
        raw = resp.read()
    root = ET.fromstring(raw)
    for item in root.findall('./channel/item'):
        title = news_text(item.findtext('title'))
        link = news_text(item.findtext('link'))
        source = news_text(item.findtext('source'))
        pub_text = news_text(item.findtext('pubDate'))
        published_at = ''
        try:
            dt = parsedate_to_datetime(pub_text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KST)
            dt = dt.astimezone(KST)
            if dt.date() != today:
                continue
            published_at = dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            pass
        if not title or not link:
            continue
        items.append({'title': title, 'link': link, 'source': source or '-', 'published_at': published_at})
        if len(items) >= int(limit or 6):
            break
    return items



def fetch_naver_finance_main_news(code, limit=6):
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Cache-Control': 'no-cache', 'Pragma': 'no-cache'})
    today = datetime.now(KST).strftime('%m/%d')
    with urlopen(req, timeout=10) as resp:
        raw = resp.read()
    html = raw.decode('utf-8', errors='ignore')
    block_match = re.search(r'<div class="section new_bbs">.*?<a href="/item/news\.naver\?code=' + re.escape(str(code)) + r'" class="more"', html, re.S)
    if not block_match:
        return []
    block = block_match.group(0)
    pattern = re.compile(r'<a href="(?P<link>/item/news_read\.naver\?article_id=[^"]+&office_id=[^"]+&code=' + re.escape(str(code)) + r'[^"]*)"[^>]*>(?P<title>.*?)</a>.*?<em>\s*(?P<date>\d{2}/\d{2})\s*</em>', re.S)
    items = []
    seen = set()
    for match in pattern.finditer(block):
        date_text = news_text(match.group('date'))
        if date_text != today:
            continue
        title = news_text(match.group('title'))
        link = 'https://finance.naver.com' + news_text(match.group('link')).replace('&amp;', '&')
        key = (title, link)
        if key in seen:
            continue
        seen.add(key)
        items.append({'title': title, 'link': link, 'source': '네이버금융 종목뉴스', 'published_at': f"{datetime.now(KST).strftime('%Y')}-{date_text.replace('/', '-')}"})
        if len(items) >= int(limit or 6):
            break
    return items


def fetch_naver_finance_stock_news(code, limit=6):
    primary = fetch_naver_finance_main_news(code, limit)
    if primary:
        return primary
    url = f"https://finance.naver.com/item/news_news.naver?code={code}&page=1&sm=title_entity_id.basic&clusterId="
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Cache-Control': 'no-cache', 'Pragma': 'no-cache'})
    today = datetime.now(KST).strftime('%Y.%m.%d')
    with urlopen(req, timeout=10) as resp:
        raw = resp.read()
    html = raw.decode('euc-kr', errors='ignore')
    pattern = re.compile(r'<tr[^>]*>.*?<a[^>]+href="(?P<link>/item/news_read\.naver\?[^"]+)"[^>]*title="(?P<title>[^"]+)"[^>]*>.*?</a>.*?<td[^>]*class="date"[^>]*>(?P<date>[^<]+)</td>', re.S)
    items = []
    seen = set()
    for match in pattern.finditer(html):
        date_text = news_text(match.group('date'))
        if not date_text.startswith(today):
            continue
        title = news_text(match.group('title'))
        link = 'https://finance.naver.com' + news_text(match.group('link')).replace('&amp;', '&')
        key = (title, link)
        if key in seen:
            continue
        seen.add(key)
        items.append({'title': title, 'link': link, 'source': '네이버금융 종목뉴스', 'published_at': date_text.replace('.', '-').replace('  ', ' ')})
        if len(items) >= int(limit or 6):
            break
    return items
def summarize_news(name, sector_name, stock_articles, sector_articles, errors=None):
    all_articles = stock_articles + sector_articles
    if not all_articles:
        lines = ['당일 기사 데이터를 찾지 못했습니다.']
        if errors:
            lines.extend(errors)
        return lines
    titles = [str(item.get('title') or '') for item in all_articles]
    positive_words = ['수주','계약','협력','증설','투자','실적 개선','흑자','신제품','호재','강세','확대','선정']
    negative_words = ['급락','적자','하락','리콜','소송','규제','약세','차질','중단','우려','감소','매각']
    topic_map = {
        '실적': ['실적','영업이익','매출','가이던스','흑자','적자'],
        '수주/계약': ['수주','계약','공급','납품','협력'],
        '투자/증설': ['투자','증설','공장','CAPEX','시설'],
        '정책/규제': ['정책','규제','법안','지원','인허가'],
        '기술/제품': ['신제품','개발','기술','플랫폼','양산'],
    }
    pos = sum(1 for title in titles if any(word in title for word in positive_words))
    neg = sum(1 for title in titles if any(word in title for word in negative_words))
    topics = []
    for topic, keywords in topic_map.items():
        count = sum(1 for title in titles if any(word in title for word in keywords))
        if count:
            topics.append((count, topic))
    topics.sort(reverse=True)
    source_names = []
    seen = set()
    for item in all_articles:
        source = str(item.get('source') or '-')
        if source not in seen:
            seen.add(source)
            source_names.append(source)
    lines = [
        f"{name} 관련 당일 기사 {len(stock_articles)}건, {sector_name} 섹터 기사 {len(sector_articles)}건을 기준으로 정리했습니다.",
        f"기사 제목상 긍정 키워드 {pos}건, 부정 키워드 {neg}건으로 {'우호적' if pos > neg else '경계' if neg > pos else '중립'} 흐름입니다.",
        f"기사 출처는 {', '.join(source_names[:4]) if source_names else '-'} 중심입니다.",
    ]
    if topics:
        lines.append('핵심 이슈는 ' + ', '.join(topic for _, topic in topics[:3]) + ' 쪽에 모여 있습니다.')
    if stock_articles:
        lines.append('종목 직접 기사 우선 해석 후, 섹터 기사와 맞물리는지 확인하면 매매 판단에 도움이 됩니다.')
    else:
        lines.append('종목 직접 기사가 적어 섹터 기사 해석 비중이 큰 상태입니다.')
    if errors:
        lines.extend(errors)
    return lines


def portfolio_news(code):
    item = next((x for x in holds() if str(x.get('code')) == str(code)), None)
    if not item:
        raise ValueError('포트 보유 종목을 찾지 못했습니다.')
    name = str(item.get('name') or code)
    code = str(item.get('code') or code)
    sector_name = theme(name)
    errors = []
    try:
        stock_articles = fetch_naver_finance_stock_news(code, 6)
    except Exception as exc:
        stock_articles = []
        errors.append(f'네이버금융 종목 기사 조회 실패: {exc}')
    if not stock_articles:
        finance_include = ['주식', '증시', '코스피', '코스닥', '증권', '공시', '실적', '수주', code]
        finance_exclude = ['배우', '가수', '예능', '드라마', '영화', '앨범', '결혼', '열애', '방송', '공연']
        def is_relevant(article):
            text = ' '.join([str(article.get('title') or ''), str(article.get('source') or '')])
            if name not in text and code not in text:
                return False
            if any(word in text for word in finance_exclude):
                return False
            return any(word in text for word in finance_include)
        stock_queries = [
            f'"{name}" {code} 주식 증권 공시 when:1d',
            f'"{name}" {code} 실적 수주 when:1d',
            f'"{name}" {code} 코스피 OR 코스닥 when:1d',
        ]
        seen = set()
        stock_query_error = None
        for query in stock_queries:
            try:
                candidates = google_news_search(query, 8)
            except Exception as exc:
                stock_query_error = exc
                continue
            for article in candidates:
                key = (article.get('title'), article.get('link'))
                if key in seen:
                    continue
                if not is_relevant(article):
                    continue
                seen.add(key)
                stock_articles.append(article)
                if len(stock_articles) >= 6:
                    break
            if len(stock_articles) >= 6:
                break
        if not stock_articles and stock_query_error:
            errors.append(f'보조 종목 기사 조회 실패: {stock_query_error}')
    seen = {(item.get('title'), item.get('link')) for item in stock_articles}
    try:
        sector_articles = google_news_search(f'"{sector_name}" 주식 증시 when:1d', 6) if sector_name and sector_name != '기타' else []
    except Exception as exc:
        sector_articles = []
        errors.append(f'섹터 기사 조회 실패: {exc}')
    uniq_sector = []
    for article in sector_articles:
        key = (article.get('title'), article.get('link'))
        if key in seen:
            continue
        seen.add(key)
        uniq_sector.append(article)
    analysis = summarize_news(name, sector_name, stock_articles, uniq_sector, errors)
    return {'code': str(code), 'name': name, 'sector': sector_name, 'stock_articles': stock_articles, 'sector_articles': uniq_sector, 'analysis': analysis, 'errors': errors, 'fetched_at': now()}
def screener(s,p):
    rr=[]; th=sorted({theme(str(r.get('이름'))) for r in rows('종목',s['rows'])})
    for r in rows('종목',s['rows']):
        ch=nv(r.get('등락률(%)')); vr=nv(r.get('거래량배수')); gp=abs(nv(r.get('시가갭률(%)')) or 0); ir=nv(r.get('장중변동률(%)')) or 0; thm=theme(str(r.get('이름')))
        if p.get('min_change') is not None and (ch is None or ch<p['min_change']): continue
        if p.get('min_volume_ratio') is not None and (vr is None or vr<p['min_volume_ratio']): continue
        if p.get('min_gap_rate') is not None and gp<p['min_gap_rate']: continue
        if p.get('min_intraday_range_rate') is not None and ir<p['min_intraday_range_rate']: continue
        if p.get('theme') and thm!=p['theme']: continue
        z=dict(r); z['테마']=thm; rr.append(z)
    rr.sort(key=lambda x:((nv(x.get('거래량배수')) or 0)*1.5)+(nv(x.get('등락률(%)')) or 0),reverse=True)
    return {'results':rr,'themes':th}

def recs():
    lt=latest()
    if not lt: return {'items':[],'summary':['저장된 스냅샷이 없습니다.'],'method':[],'themes':[],'styles':[]}
    out=[]
    for r in rows('종목',lt['rows']):
        h=hist(r.get('코드'),6)
        ch=[nv(x.get('등락률(%)')) for x in h if nv(x.get('등락률(%)')) is not None]
        avg=round(sum(ch)/len(ch),2) if ch else 0
        pos=len([x for x in ch if x>0])
        vr=nv(r.get('거래량배수')) or 0
        cp=nv(r.get('종가위치(%)')) or 0
        gp=abs(nv(r.get('시가갭률(%)')) or 0)
        ir=nv(r.get('장중변동률(%)')) or 0
        daily=recommendation_daily_context(r.get('코드'))
        d20=nv(daily.get('daily_20d_change')) or 0
        d60=nv(daily.get('daily_60d_change')) or 0
        dvr=nv(daily.get('daily_volume_multiple')) or 0
        dcp=nv(daily.get('daily_close_position')) or 0
        ff=nv(daily.get('daily_foreign_20d')) or 0
        ii=nv(daily.get('daily_institution_20d')) or 0
        flow_bonus=(1.5 if ff>0 else -1 if ff<0 else 0)+(1.5 if ii>0 else -1 if ii<0 else 0)
        sc=round(50+(nv(r.get('등락률(%)')) or 0)*2+vr*8+avg*2+pos*3+max(cp-50,0)*0.15-max(gp-3,0)*2-max(ir-8,0)*1.5+d20*1.2+d60*0.6+dvr*3+max(dcp-55,0)*0.08+flow_bonus,2)
        st='공격형' if sc>=78 else '균형형' if sc>=58 else '관찰형'
        rs=[]
        if vr>=1.5: rs.append('거래량 확인')
        if (nv(r.get('등락률(%)')) or 0)>0: rs.append('모멘텀')
        if cp>=70: rs.append('고가권 마감')
        if avg>0: rs.append('저장소 추세 양호')
        if d20>0: rs.append('20일 종가 추세 양호')
        if d60>0: rs.append('60일 종가 추세 유지')
        if ff>0: rs.append('외인 순매수 우위')
        if ii>0: rs.append('기관 순매수 우위')
        if dvr>=1.2: rs.append('일별 거래량 확대')
        out.append({'name':r.get('이름'),'code':r.get('코드'),'theme':theme(str(r.get('이름'))),'score':sc,'style':st,'current_price':nv(r.get('현재가')),'change_rate':nv(r.get('등락률(%)')),'volume_ratio':vr,'gap_rate':nv(r.get('시가갭률(%)')),'intraday_range_rate':ir,'close_position':cp,'avg_change':avg,'positive_days':pos,'daily_latest_date':daily.get('daily_latest_date'),'daily_20d_change':daily.get('daily_20d_change'),'daily_60d_change':daily.get('daily_60d_change'),'daily_foreign_20d':daily.get('daily_foreign_20d'),'daily_institution_20d':daily.get('daily_institution_20d'),'daily_foreign_amount_20d':daily.get('daily_foreign_amount_20d'),'daily_institution_amount_20d':daily.get('daily_institution_amount_20d'),'daily_volume_multiple':daily.get('daily_volume_multiple'),'daily_close_position':daily.get('daily_close_position'),'has_daily_data':daily.get('has_daily_data',False),'reasons':rs or ['추가 관찰 필요']})
    out.sort(key=lambda x:x['score'],reverse=True)
    daily_count=len([x for x in out if x.get('has_daily_data')])
    return {'items':out,'summary':[f"추천 후보는 총 {len(out)}종목입니다.",f"이 중 {daily_count}종목은 일별 종가/거래량/외인·기관 수급 데이터까지 연결해 분석했습니다.",'모멘텀, 거래량, 종가 구조, 최근 저장 히스토리와 일별 추세를 함께 반영했습니다.'],'method':['현재 등락률, 거래량배수, 종가위치, 최근 저장 히스토리 평균을 함께 반영했습니다.','일별 종가 기준 20일/60일 추세, 최근 거래량 배수, 외인/기관 20일 순매수 흐름을 추가 반영했습니다.','과도한 갭과 과한 장중 변동성에는 패널티를 적용했습니다.','설명 가능한 규칙형 하이브리드 점수입니다.'],'themes':sorted({x['theme'] for x in out}),'styles':['공격형','균형형','관찰형']}
def dashboard(): ss=snaps(); lt=ss[0] if ss else None; return {'snapshots':[{'id':x['id'],'collected_at':x['collected_at']} for x in ss],'latest_snapshot':lt,'analysis':brief(lt['rows']) if lt else ['저장된 스냅샷이 없습니다.'],'notes_count':len(notes())}

def ai(s,cmp,qst):
    rr=s['rows']; th=theme_table(rr); lines=[f'질문: {qst}','','[핵심 요약]']+[f'- {x}' for x in brief(rr)[:6]]; st=rows('종목',rr)
    for r in st:
        n=str(r.get('이름')); c=str(r.get('코드'))
        if qst and (n in qst or c in qst): lines+=['',f'[종목 해설: {n}]',f"- 현재가 {fmt(r.get('현재가'))}, 등락률 {sgn(r.get('등락률(%)'),'%')}입니다.",f"- 거래량배수 {fmt(r.get('거래량배수'))}, 시가갭률 {sgn(r.get('시가갭률(%)'),'%')}입니다.",f"- 장중변동률 {fmt(r.get('장중변동률(%)'))}%, 종가위치 {fmt(r.get('종가위치(%)'))}%입니다.",f"- 테마는 {theme(n)}로 분류됩니다."]; break
    lines+=['','[테마 강도]']+[f"- {x['theme']} 평균 등락률 {sgn(x.get('avg_rate'),'%')} / 선도 종목 {', '.join(x.get('leaders') or ['-'])}" for x in th[:5]]
    if cmp:
        cur={str(r.get('코드')):r for r in st}; pre={str(r.get('코드')):r for r in rows('종목',cmp['rows'])}; ds=[]
        for c,r in cur.items():
            p=pre.get(c); cr=nv(r.get('등락률(%)')); pr=nv(p.get('등락률(%)')) if p else None
            if cr is not None and pr is not None: ds.append((cr-pr,r.get('이름')))
        ds.sort(reverse=True); lines+=['','[비교 스냅샷 변화]']+[f"- {n} 등락률 변화 {sgn(d,'%')}p" for d,n in ds[:5]]
    return {'answer':'\n'.join(lines),'themes':th}

def cfg(): return rj(B/'config.json')
def wcfg(x): wj(B/'config.json',x)
def add_stock(name,code):
    c=cfg(); st=c.setdefault('stocks',[])
    if not any(str(i.get('code'))==str(code) for i in st if isinstance(i,dict)): st.append({'name':name,'code':code}); wcfg(c)
    return {'stock':{'name':name,'code':code},'stocks':c.get('stocks',[])}
def move_stock(code,d):
    c=cfg(); st=c.get('stocks',[]); i=next((n for n,x in enumerate(st) if str(x.get('code'))==str(code)),None)
    if i is not None:
        j=i-1 if d=='up' else i+1
        if 0<=j<len(st): st[i],st[j]=st[j],st[i]; wcfg(c)
    return st
def del_stock(name,code):
    c=cfg(); st=[x for x in c.get('stocks',[]) if not (str(x.get('code'))==str(code) or str(x.get('name'))==str(name))]; c['stocks']=st; wcfg(c); return {'deleted':{'name':name,'code':code},'stocks':st}

def page(title,body): return f'<!doctype html><html lang="ko"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{title}</title><style>:root{{--bg:#f7f2e8;--card:#fffdf8;--ink:#132238;--muted:#6b7280;--line:#dcc7a5;--accent:#1e5c57;--bad:#c2410c;--good:#0f766e;}}*{{box-sizing:border-box}}body{{margin:0;background:radial-gradient(circle at top left,#fff7e7 0,#f5efe4 40%,#efe8db 100%);color:var(--ink);font:16px/1.5 Segoe UI,sans-serif}}.wrap{{width:min(90vw,1600px);margin:0 auto;padding:24px 0}}.hero{{background:linear-gradient(135deg,#19324a,#1e5c57);color:#fff;border-radius:28px;padding:28px}}.hero h1{{margin:0 0 8px;font-size:44px}}.nav{{display:flex;gap:10px;flex-wrap:wrap;margin-top:18px}}.nav a{{text-decoration:none;color:#fff;padding:10px 14px;border:1px solid rgba(255,255,255,.28);border-radius:999px;background:rgba(255,255,255,.08)}}.card{{background:#fffdf8;border:1px solid #dcc7a5;border-radius:28px;padding:22px;margin-top:18px}}.metric{{background:#fff;border:1px solid #dcc7a5;border-radius:18px;padding:14px}}.metric .k{{font-size:13px;color:#6b7280}}.metric .v{{font-size:24px;font-weight:700;margin-top:6px}}.metric-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}}.controls{{display:flex;gap:10px;flex-wrap:wrap;margin:14px 0}}.btn{{border:1px solid #19324a;border-radius:999px;background:#ffe08a;padding:10px 16px;font-weight:700;cursor:pointer}}.btn.secondary{{background:#fff}}.table{{overflow:auto;border:1px solid #dcc7a5;border-radius:20px;background:#fff}}table{{width:100%;border-collapse:collapse}}th,td{{padding:12px 14px;border-bottom:1px solid #eee;white-space:nowrap;text-align:left}}th{{background:#faf3e5}}.num{{text-align:right}}.good{{color:#0f766e;font-weight:700}}.bad{{color:#c2410c;font-weight:700}}.empty{{padding:16px;color:#6b7280}}</style></head><body><div class="wrap">{body}</div></body></html>'

def home():
    body = """<section class=\"hero\"><h1>Stock Signal Desk</h1><div>수집, 저장, 분석, 포트/감시/추천까지 한 흐름으로 다루는 로컬 웹 대시보드입니다.</div><div class=\"nav\"><a href=\"/local-ai\">로컬 AI 랩</a><a href=\"/portfolio\">나의포트</a><a href=\"/watchlist\">감시목록</a><a href=\"/recommendations\">추천 종목</a><a href=\"/daily-analysis\">일별 종가 분석</a><a href=\"/collection-settings\">수집 종목 관리</a></div></section><section class=\"card\"><h2>수집과 저장</h2><div class=\"controls\"><button class=\"btn\" id=\"collectBtn\">수집하고 저장</button><button class=\"btn secondary\" id=\"refreshBtn\">다시 불러오기</button></div><div id=\"status\"></div></section><section class=\"card\"><h2>저장소 활용</h2><select id=\"snap\"></select><div id=\"analysis\"></div></section><section class=\"card\"><h2>참고 데이터</h2><div class=\"table\"><table><thead id=\"thead\"></thead><tbody id=\"tbody\"></tbody></table></div></section><script>const num=v=>{if(v===null||v===undefined||v==="")return null;const n=Number(String(v).replaceAll(",","").replace("%",""));return Number.isFinite(n)?n:null};const fmt=v=>v===null||v===undefined||v===""?"-":(typeof v==="number"?new Intl.NumberFormat("ko-KR",{maximumFractionDigits:2}).format(v):String(v));async function j(u,o){const r=await fetch(u,o);const t=await r.text();const d=t?JSON.parse(t):{};if(!r.ok)throw new Error(d.error||t||"요청 실패");return d}function ana(ls){document.getElementById("analysis").innerHTML=(ls||[]).length?`<ul>${ls.map(x=>`<li>${x}</li>`).join("")}</ul>`:'<div class="empty">분석 데이터가 없습니다.</div>'}function tbl(s){const thead=document.getElementById('thead');const tbody=document.getElementById('tbody');const rs=(s&&s.rows)||[];if(!rs.length){thead.innerHTML='<tr><th>데이터가 없습니다.</th></tr>';tbody.innerHTML='';return}const cs=Object.keys(rs[0]);thead.innerHTML=`<tr>${cs.map(c=>`<th class="${rs.some(r=>num(r[c])!==null)?'num':''}">${c}</th>`).join('')}</tr>`;tbody.innerHTML=rs.map(r=>`<tr>${cs.map(c=>{const n=num(r[c]);const cc=[n!==null?'num':''];if(String(c).includes('등락률')||String(c).includes('전일대비')){if(n>0)cc.push('good');else if(n<0)cc.push('bad')}return `<td class="${cc.join(' ')}">${fmt(r[c])}</td>`}).join('')}</tr>`).join('')}async function load(){const status=document.getElementById('status');const snap=document.getElementById('snap');const d=await j('/api/dashboard');snap.innerHTML=(d.snapshots||[]).map(x=>`<option value="${x.id}">${x.collected_at}</option>`).join('');ana(d.analysis||[]);tbl(d.latest_snapshot);status.textContent=''}document.addEventListener('DOMContentLoaded',()=>{const status=document.getElementById('status');const snap=document.getElementById('snap');const collectBtn=document.getElementById('collectBtn');const refreshBtn=document.getElementById('refreshBtn');refreshBtn.addEventListener('click',()=>load().catch(e=>status.textContent=e.message));collectBtn.addEventListener('click',async()=>{status.textContent='수집 중...';collectBtn.disabled=true;try{const d=await j('/api/collect',{method:'POST'});status.textContent=`저장 완료: ${d.snapshot.collected_at}`;await load()}catch(e){status.textContent=e.message}finally{collectBtn.disabled=false}});snap.addEventListener('change',async e=>{try{const d=await j(`/api/snapshots/${e.target.value}`);tbl(d.snapshot)}catch(err){status.textContent=err.message}});load().catch(e=>status.textContent=e.message)});</script>"""
    return page('Stock Signal Desk', body)
class X(BaseHTTPRequestHandler):
    def out(self,c,b,t): self.send_response(c); self.send_header('Content-Type',t); self.send_header('Content-Length',str(len(b))); self.end_headers(); self.wfile.write(b)
    def js(self,x,c=200): self.out(c,jd(x).encode('utf-8'),'application/json; charset=utf-8')
    def html(self,x,c=200): self.out(c,x.encode('utf-8'),'text/html; charset=utf-8')
    def err(self,m,c=400): self.js({'error':m},c)
    def body(self):
        n=int(self.headers.get('Content-Length','0') or '0'); return json.loads((self.rfile.read(n) if n>0 else b'{}').decode('utf-8') or '{}')
    def file(self,p):
        p=Path(p)
        if not p.exists(): return self.err('파일을 찾지 못했습니다.',404)
        self.out(200,p.read_bytes(),'text/html; charset=utf-8')
    def do_GET(self):
        try:
            u=urlparse(self.path); p=u.path; a={k:v[0] for k,v in parse_qs(u.query).items()}
            if p=='/': return self.html(home())
            if p=='/local-ai': return self.file(B/'stock_local_ai.html')
            if p=='/portfolio': return self.file(B/'stock_portfolio.html')
            if p=='/watchlist': return self.file(B/'stock_watchlist.html')
            if p=='/recommendations': return self.file(B/'stock_recommendations.html')
            if p=='/daily-analysis': return self.file(B/'stock_daily_analysis.html')
            if p=='/collection-settings': return self.file(B/'stock_collection_settings.html')
            if p=='/api/dashboard': return self.js(dashboard())
            if p.startswith('/api/snapshots/'): return self.js({'snapshot':snap(int(p.rsplit('/',1)[-1]))})
            if p=='/api/history': return self.js({'history':hist(a.get('code',''),int(a.get('limit','12') or '12'))})
            if p=='/api/themes':
                s=snap(int(a.get('snapshot_id') or (latest() or {}).get('id') or 0)) if (a.get('snapshot_id') or latest()) else None
                return self.js({'themes':theme_table(s['rows'] if s else [])})
            if p=='/api/notes': return self.js({'notes':notes()})
            if p=='/api/screener':
                s=snap(int(a.get('snapshot_id') or (latest() or {}).get('id') or 0)) if (a.get('snapshot_id') or latest()) else None
                return self.js({'results':[],'themes':[]} if not s else screener(s,{'min_change':nv(a.get('min_change')),'min_volume_ratio':nv(a.get('min_volume_ratio')),'min_gap_rate':nv(a.get('min_gap_rate')),'min_intraday_range_rate':nv(a.get('min_intraday_range_rate')),'theme':a.get('theme') or ''}))
            if p=='/api/portfolio': return self.js(port())
            if p=='/api/portfolio/news': return self.js(portfolio_news(a.get('code','')))
            if p=='/api/watchlist': return self.js(watch())
            if p=='/api/watchlist/brief': return self.js({'brief':wbrief(a.get('code',''))})
            if p=='/api/watchlist/notes': return self.js({'notes':wnotes(a.get('code',''))})
            if p=='/api/daily-analysis/stocks': return self.js({'stocks':daily_stock_items()})
            if p=='/api/daily-analysis/data': return self.js(daily_stock_payload(a.get('code',''), int(a.get('limit','180') or '180')))
            if p=='/api/recommendations':
                d=recs(); its=d['items'];
                if a.get('style'): its=[x for x in its if x.get('style')==a['style']]
                if a.get('theme'): its=[x for x in its if x.get('theme')==a['theme']]
                if a.get('min_score'): its=[x for x in its if (nv(x.get('score')) or 0)>=(nv(a.get('min_score')) or 0)]
                d['items']=its; return self.js(d)
            if p=='/api/config/stocks': return self.js({'stocks':cfg().get('stocks',[])})
            return self.err('지원하지 않는 경로입니다.',404)
        except Exception as e: return self.err(str(e),500)
    def do_POST(self):
        try:
            p=urlparse(self.path).path; b=self.body()
            if p=='/api/collect': return self.js({'snapshot':collect()})
            if p=='/api/daily-analysis/collect': return self.js(collect_daily_prices(int(b.get('days') or 365)))
            if p=='/api/notes': add_note(int(b.get('snapshot_id')) if b.get('snapshot_id') else None,str(b.get('title') or '메모'),str(b.get('tags') or ''),str(b.get('body') or '')); return self.js({'ok':True,'notes':notes()})
            if p=='/api/local-ai/query':
                s=snap(int(b.get('snapshot_id') or (latest() or {}).get('id') or 0)) if (b.get('snapshot_id') or latest()) else None; c=snap(int(b.get('compare_snapshot_id') or 0)) if b.get('compare_snapshot_id') else None
                return self.js({'answer':'저장된 스냅샷이 없습니다.','themes':[]} if not s else ai(s,c,str(b.get('question') or '')))
            if p=='/api/portfolio':
                if b.get('action')=='delete': del_hold(str(b.get('code') or '')); return self.js({'ok':True})
                return self.js({'ok':True,'item':up_hold(b)})
            if p=='/api/watchlist':
                if b.get('action')=='delete': del_watch(str(b.get('code') or '')); return self.js({'ok':True})
                return self.js({'ok':True,'item':up_watch(str(b.get('name') or ''),str(b.get('code') or ''),int(float(b.get('priority') or 50)),str(b.get('action_state') or '관찰'),str(b.get('memo') or ''))})
            if p=='/api/watchlist/notes': add_wnote(str(b.get('code') or ''),str(b.get('title') or '메모'),str(b.get('body') or '')); return self.js({'ok':True})
            if p=='/api/config/stocks':
                ac=str(b.get('action') or '')
                if ac=='move': return self.js({'stocks':move_stock(str(b.get('code') or ''),str(b.get('direction') or 'up'))})
                if ac=='delete': return self.js(del_stock(str(b.get('name') or ''),str(b.get('code') or '')))
                n,c=resolve(b.get('name'),b.get('code')); return self.js(add_stock(n,c))
            return self.err('지원하지 않는 경로입니다.',404)
        except Exception as e: return self.err(str(e),500)

def run(): init(); s=ThreadingHTTPServer((H,P),X); print(f'Stock web server running on http://{H}:{P}'); s.serve_forever()
if __name__=='__main__': run()

















