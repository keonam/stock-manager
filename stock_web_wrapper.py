import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from io import StringIO
from urllib.request import Request, urlopen

import pandas as pd

import stock_web as base

K_OPEN = '\uc2dc\uac00'
K_HIGH = '\uace0\uac00'
K_LOW = '\uc800\uac00'
K_CLOSE = '\uc885\uac00'
K_VOLUME = '\uac70\ub798\ub7c9'


def fetch_text(url, encoding='euc-kr'):
    req = Request(url, headers={
        'User-Agent': 'Mozilla/5.0',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Referer': 'https://finance.naver.com/',
    })
    with urlopen(req, timeout=8) as resp:
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
        out.append(' '.join(text.split()))
    return out


def pick_naver_table(html):
    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        return pd.DataFrame()
    for table in tables:
        frame = table.copy()
        frame.columns = flatten_columns(frame.columns)
        if len(frame.columns) >= 5:
            return frame
    return pd.DataFrame()


def normalize_naver_daily_table(frame):
    if frame is None or getattr(frame, 'empty', True):
        return pd.DataFrame()
    frame = frame.copy()
    frame.columns = flatten_columns(frame.columns)
    cols = list(frame.columns)
    if len(cols) >= 7:
        keep = [cols[0], cols[1], cols[3], cols[4], cols[5], cols[6]]
    elif len(cols) >= 6:
        keep = [cols[0], cols[1], cols[2], cols[3], cols[4], cols[5]]
    else:
        return pd.DataFrame()
    frame = frame[keep].copy()
    frame.columns = ['date', 'close', 'open', 'high', 'low', 'volume']
    frame['date'] = frame['date'].astype(str).str.strip()
    frame = frame[frame['date'].str.match(r'^\d{4}\.\d{2}\.\d{2}$', na=False)]
    return frame


def normalize_naver_frgn_table(frame):
    if frame is None or getattr(frame, 'empty', True):
        return pd.DataFrame()
    frame = frame.copy()
    frame.columns = flatten_columns(frame.columns)
    cols = list(frame.columns)
    if len(cols) >= 6:
        frame = frame[[cols[0], cols[1], cols[3], cols[4], cols[5]]].copy()
    elif len(cols) >= 5:
        frame = frame[[cols[0], cols[1], cols[2], cols[3], cols[4]]].copy()
    else:
        return pd.DataFrame()
    frame.columns = ['date', 'close', 'volume', 'institution', 'foreign']
    frame['date'] = frame['date'].astype(str).str.strip()
    frame = frame[frame['date'].str.match(r'^\d{4}\.\d{2}\.\d{2}$', na=False)]
    return frame


def fetch_naver_daily_stock_data(code, start_date, end_date, max_pages=2):
    day_span = max(1, (end_date - start_date).days + 1)
    pages = min(max(1, int(max_pages or 2)), max(1, math.ceil(day_span / 10) + 1))
    rows_by_date = {}
    errors = []
    for page in range(1, pages + 1):
        try:
            daily_html = fetch_text(f'https://finance.naver.com/item/sise_day.naver?code={code}&page={page}')
            daily_table = normalize_naver_daily_table(pick_naver_table(daily_html))
            for _, row in daily_table.iterrows():
                trade_date = str(row.get('date') or '').replace('.', '-')
                try:
                    dt = datetime.strptime(trade_date, '%Y-%m-%d').date()
                except Exception:
                    continue
                if dt < start_date or dt > end_date:
                    continue
                bucket = rows_by_date.setdefault(trade_date, {})
                for key in ['close', 'open', 'high', 'low', 'volume']:
                    val = base.nv(row.get(key))
                    if val is not None:
                        bucket[key] = val
        except Exception as exc:
            errors.append(f'{code} daily page {page} failed: {exc}')
        try:
            frgn_html = fetch_text(f'https://finance.naver.com/item/frgn.naver?code={code}&page={page}')
            frgn_table = normalize_naver_frgn_table(pick_naver_table(frgn_html))
            for _, row in frgn_table.iterrows():
                trade_date = str(row.get('date') or '').replace('.', '-')
                try:
                    dt = datetime.strptime(trade_date, '%Y-%m-%d').date()
                except Exception:
                    continue
                if dt < start_date or dt > end_date:
                    continue
                bucket = rows_by_date.setdefault(trade_date, {})
                inst = base.nv(row.get('institution'))
                foreign = base.nv(row.get('foreign'))
                if inst is not None:
                    bucket['institution'] = inst
                if foreign is not None:
                    bucket['foreign'] = foreign
                if inst is not None or foreign is not None:
                    bucket['personal'] = -((foreign or 0) + (inst or 0))
                close_price = base.nv(bucket.get('close')) if bucket.get('close') is not None else base.nv(row.get('close'))
                if close_price is not None:
                    if bucket.get('personal') is not None:
                        bucket['personal_amount'] = round(bucket['personal'] * close_price, 2)
                    if foreign is not None:
                        bucket['foreign_amount'] = round(foreign * close_price, 2)
                    if inst is not None:
                        bucket['institution_amount'] = round(inst * close_price, 2)
        except Exception as exc:
            errors.append(f'{code} investor page {page} failed: {exc}')
    return rows_by_date, errors


def existing_daily_last_dates():
    return {
        str(r['code']): str(r['max_date'])
        for r in base.q('SELECT code, MAX(trade_date) AS max_date FROM daily_price_points GROUP BY code')
        if r['max_date']
    }


def existing_daily_missing_investor_dates(start_date, end_date):
    rows = base.q(
        'SELECT code, MIN(trade_date) AS min_missing_date FROM daily_price_points WHERE trade_date>=? AND trade_date<=? AND (personal_net IS NULL OR foreign_net IS NULL OR institution_net IS NULL) GROUP BY code',
        (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
    )
    return {str(r['code']): str(r['min_missing_date']) for r in rows if r['min_missing_date']}

def build_naver_daily_maps(plan_items):
    out = {}
    errors = []
    items = list(plan_items or [])
    if not items:
        return out, errors
    workers = min(12, max(1, len(items)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(fetch_naver_daily_stock_data, str(item.get('code')), item.get('start_date'), item.get('end_date'), item.get('max_pages', 2)): item
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
                errors.append(f'{name} collection failed: {exc}')
    return out, errors


def collect_daily_prices(days=365):
    cfg = base.load_config()
    stocks_cfg = list(cfg.get('stocks', []))
    end_date = base.get_reference_today()
    requested_days = int(days or 365)
    start_date = end_date - timedelta(days=requested_days)
    fast_window = min(max(requested_days, 5), 30)
    recent_start = end_date - timedelta(days=fast_window)
    existing_last = existing_daily_last_dates()
    missing_investor = existing_daily_missing_investor_dates(recent_start, end_date)
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
        missing_date = missing_investor.get(code)
        if missing_date:
            try:
                missing_dt = datetime.strptime(missing_date, '%Y-%m-%d').date()
                stock_start = min(stock_start, missing_dt)
                days_needed = max(1, (end_date - stock_start).days + 1)
                max_pages = max(max_pages, min(6, math.ceil(days_needed / 10) + 1))
            except Exception:
                pass
        plan_items.append({'code': code, 'name': str(item.get('name') or code), 'start_date': stock_start, 'end_date': end_date, 'max_pages': max_pages})
    saved_rows = 0
    errors = []
    collected_at = base.now()
    naver_map, naver_errors = build_naver_daily_maps(plan_items)
    snapshot_price_map = base.build_daily_price_map_from_snapshots(recent_start, end_date, [str(item.get('code')) for item in stocks_cfg], limit=200)
    errors.extend(naver_errors)
    for item in stocks_cfg:
        code = str(item.get('code'))
        name = str(item.get('name') or code)
        merged_rows = dict(snapshot_price_map.get(code, {}))
        for trade_date, row in (naver_map.get(code) or {}).items():
            bucket = dict(merged_rows.get(trade_date, {}))
            mapping = {'open': K_OPEN, 'high': K_HIGH, 'low': K_LOW, 'close': K_CLOSE, 'volume': K_VOLUME}
            for src_key, dst_key in mapping.items():
                val = base.nv(row.get(src_key))
                if val is not None:
                    bucket[dst_key] = val
            merged_rows[trade_date] = bucket
        if not merged_rows:
            errors.append(f'{name} price data missing')
            continue
        price_df = pd.DataFrame.from_dict(merged_rows, orient='index').sort_index()
        missing_flow_count = 0
        for idx, row in price_df.iterrows():
            trade_date = idx.strftime('%Y-%m-%d') if hasattr(idx, 'strftime') else str(idx)
            flow_row = (naver_map.get(code) or {}).get(trade_date, {})
            if not flow_row:
                missing_flow_count += 1
            base.q(
                "INSERT INTO daily_price_points(code,name,trade_date,close_price,open_price,high_price,low_price,volume,personal_net,foreign_net,institution_net,personal_amount,foreign_amount,institution_amount,collected_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(code,trade_date) DO UPDATE SET name=excluded.name,close_price=excluded.close_price,open_price=excluded.open_price,high_price=excluded.high_price,low_price=excluded.low_price,volume=excluded.volume,personal_net=excluded.personal_net,foreign_net=excluded.foreign_net,institution_net=excluded.institution_net,personal_amount=excluded.personal_amount,foreign_amount=excluded.foreign_amount,institution_amount=excluded.institution_amount,collected_at=excluded.collected_at",
                (
                    code,
                    name,
                    trade_date,
                    base.nv(row.get(K_CLOSE)),
                    base.nv(row.get(K_OPEN)),
                    base.nv(row.get(K_HIGH)),
                    base.nv(row.get(K_LOW)),
                    base.nv(row.get(K_VOLUME)),
                    base.nv(flow_row.get('personal')),
                    base.nv(flow_row.get('foreign')),
                    base.nv(flow_row.get('institution')),
                    base.nv(flow_row.get('personal_amount')),
                    base.nv(flow_row.get('foreign_amount')),
                    base.nv(flow_row.get('institution_amount')),
                    collected_at,
                ),
                write=True,
            )
            saved_rows += 1
        if missing_flow_count and missing_flow_count == len(price_df.index):
            errors.append(f'{name} investor data missing')
    return {
        'saved_rows': saved_rows,
        'stock_count': len(stocks_cfg),
        'start_date': recent_start.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'errors': errors,
        'collected_at': collected_at,
        'mode': 'fast_incremental',
    }



def _sum_window(rows, key, start, size):
    window = rows[start:start + size]
    values = [base.nv(r.get(key)) for r in window]
    nums = [value for value in values if value is not None]
    return round(sum(nums), 2) if nums else None


def _price_change_from(rows, offset):
    if not rows or len(rows) <= offset:
        return None
    latest_close = base.nv(rows[0].get('close_price'))
    base_close = base.nv(rows[offset].get('close_price'))
    if latest_close is None or base_close in (None, 0):
        return None
    return round(((latest_close - base_close) / base_close) * 100, 2)


def _investor_flow_text(foreign_5d, institution_5d, foreign_prev5d, institution_prev5d):
    recent_foreign = foreign_5d or 0
    recent_inst = institution_5d or 0
    prev_foreign = foreign_prev5d or 0
    prev_inst = institution_prev5d or 0
    if recent_foreign > 0 and recent_inst > 0:
        if prev_foreign <= 0 or prev_inst <= 0:
            return '외인/기관 동반 순매수 전환'
        return '외인/기관 동반 순매수 지속'
    if recent_foreign < 0 and recent_inst < 0:
        if prev_foreign >= 0 or prev_inst >= 0:
            return '외인/기관 동반 순매도 전환'
        return '외인/기관 동반 순매도 지속'
    if recent_foreign > 0 and recent_inst < 0:
        return '외인 매수 우위, 기관은 차익 실현'
    if recent_foreign < 0 and recent_inst > 0:
        return '기관 매수 우위, 외인은 차익 실현'
    return '수급 우위가 뚜렷하지 않음'


def _flow_grade(foreign_5d, institution_5d, foreign_prev5d, institution_prev5d, five_day_change):
    recent_foreign = foreign_5d or 0
    recent_inst = institution_5d or 0
    prev_foreign = foreign_prev5d or 0
    prev_inst = institution_prev5d or 0
    price_5d = five_day_change or 0
    if recent_foreign > 0 and recent_inst > 0 and price_5d > 0:
        return '매수 우세'
    if recent_foreign < 0 and recent_inst < 0 and price_5d < 0:
        return '경계'
    if recent_foreign < 0 and recent_inst < 0:
        return '주의'
    if (recent_foreign > 0 and recent_inst <= 0) or (recent_foreign <= 0 and recent_inst > 0):
        if (prev_foreign <= 0 and recent_foreign > 0) or (prev_inst <= 0 and recent_inst > 0):
            return '전환 관찰'
        return '혼조'
    return '중립'


def _flow_grade_rank(flow_grade):
    return {
        '매수 우세': 4,
        '전환 관찰': 3,
        '혼조': 2,
        '중립': 1,
        '주의': -1,
        '경계': -2,
    }.get(flow_grade or '', 0)

def _nearest_levels(current_price, values, side):
    clean = sorted({round(v, 2) for v in values if v is not None and v > 0})
    if not clean or current_price in (None, 0):
        return None, None
    if side == 'support':
        supports = [v for v in clean if v <= current_price]
        if not supports:
            return None, None
        support_1 = supports[-1]
        support_2 = supports[-2] if len(supports) >= 2 else None
        return support_1, support_2
    resistances = [v for v in clean if v >= current_price]
    if not resistances:
        return None, None
    resistance_1 = resistances[0]
    resistance_2 = resistances[1] if len(resistances) >= 2 else None
    return resistance_1, resistance_2

def _support_resistance_context(items, foreign_5d, institution_5d):
    if not items:
        return {}
    latest_close = base.nv(items[0].get('close_price'))
    lookback_20 = items[:20]
    lookback_60 = items[:60]
    lows = [base.nv(x.get('low_price')) for x in lookback_60]
    highs = [base.nv(x.get('high_price')) for x in lookback_60]
    closes = [base.nv(x.get('close_price')) for x in lookback_60]
    support_candidates = [base.nv(x.get('low_price')) for x in lookback_20] + lows + closes
    resistance_candidates = [base.nv(x.get('high_price')) for x in lookback_20] + highs + closes
    support_1, support_2 = _nearest_levels(latest_close, support_candidates, 'support')
    resistance_1, resistance_2 = _nearest_levels(latest_close, resistance_candidates, 'resistance')
    support_gap_1 = round(((latest_close - support_1) / latest_close) * 100, 2) if latest_close not in (None, 0) and support_1 not in (None, 0) else None
    resistance_gap_1 = round(((resistance_1 - latest_close) / latest_close) * 100, 2) if latest_close not in (None, 0) and resistance_1 not in (None, 0) else None
    if (foreign_5d or 0) > 0 and (institution_5d or 0) > 0:
        level_signal = '지지 신뢰도 높음'
    elif (foreign_5d or 0) < 0 and (institution_5d or 0) < 0:
        level_signal = '저항 압력 유의'
    elif (foreign_5d or 0) > 0 or (institution_5d or 0) > 0:
        level_signal = '지지 확인 중'
    else:
        level_signal = '수급 중립'
    return {
        'support_1': support_1,
        'support_2': support_2,
        'resistance_1': resistance_1,
        'resistance_2': resistance_2,
        'support_gap_1': support_gap_1,
        'resistance_gap_1': resistance_gap_1,
        'level_signal': level_signal,
    }


def _portfolio_health_diagnosis(payload, holdings):
    summary = dict(payload.get('summary') or {})
    score = 100
    strengths = []
    risks = []
    checklist = []
    steps = []
    support_count = 0
    resistance_count = 0
    missing_price = int(summary.get('missing_price_count') or 0)
    stop_risk = int(summary.get('stop_risk_count') or 0)
    target_hit = int(summary.get('target_hit_count') or 0)
    flow_support = int(summary.get('flow_support_count') or 0)
    flow_warning = int(summary.get('flow_warning_count') or 0)
    total = max(1, len(holdings or []))
    history_points = [h for h in (payload.get('history') or []) if base.nv(h.get('total_value')) is not None]
    history_points = sorted(history_points, key=lambda x: str(x.get('collected_at') or ''))
    value_delta = None
    value_delta_rate = None
    recent_trend = '변화 미확인'
    recent_streak = '중립'
    if len(history_points) >= 2:
        latest_value = base.nv(history_points[-1].get('total_value'))
        prev_value = base.nv(history_points[-2].get('total_value'))
        if latest_value is not None and prev_value not in (None, 0):
            value_delta = latest_value - prev_value
            value_delta_rate = round((value_delta / prev_value) * 100, 2)
            if value_delta > 0:
                recent_trend = '직전 대비 개선'
            elif value_delta < 0:
                recent_trend = '직전 대비 악화'
            else:
                recent_trend = '직전 대비 보합'
    recent_window = history_points[-5:]
    recent_up_count = 0
    recent_down_count = 0
    recent_flat_count = 0
    recent_bias = '혼조'
    history_rows = []
    if len(history_points) >= 3:
        v1 = base.nv(history_points[-3].get('total_value'))
        v2 = base.nv(history_points[-2].get('total_value'))
        v3 = base.nv(history_points[-1].get('total_value'))
        if None not in (v1, v2, v3):
            if v1 < v2 < v3:
                recent_streak = '3회 연속 개선'
            elif v1 > v2 > v3:
                recent_streak = '3회 연속 악화'
            else:
                recent_streak = '혼조'
    if len(recent_window) >= 2:
        for idx in range(1, len(recent_window)):
            prev_v = base.nv(recent_window[idx-1].get('total_value'))
            curr_v = base.nv(recent_window[idx].get('total_value'))
            if prev_v is None or curr_v is None:
                continue
            if curr_v > prev_v:
                recent_up_count += 1
            elif curr_v < prev_v:
                recent_down_count += 1
            else:
                recent_flat_count += 1
        if recent_up_count > recent_down_count:
            recent_bias = '개선 우세'
        elif recent_down_count > recent_up_count:
            recent_bias = '악화 우세'
        else:
            recent_bias = '혼조'
    history_rows = []
    for idx, row in enumerate(recent_window):
        curr_v = base.nv(row.get('total_value'))
        direction = '시작점'
        if idx > 0:
            prev_v = base.nv(recent_window[idx - 1].get('total_value'))
            if prev_v is not None and curr_v is not None:
                if curr_v > prev_v:
                    direction = '상승'
                elif curr_v < prev_v:
                    direction = '하락'
                else:
                    direction = '보합'
        history_rows.append({
            'date': str(row.get('collected_at') or '')[:16],
            'value': curr_v,
            'direction': direction,
        })
    for item in holdings or []:
        support_gap = base.nv(item.get('support_gap_1'))
        resistance_gap = base.nv(item.get('resistance_gap_1'))
        if support_gap is not None and support_gap <= 3:
            support_count += 1
        if resistance_gap is not None and resistance_gap <= 3:
            resistance_count += 1
    best_flow_item = None
    caution_item = None
    improving_item = None
    weakening_item = None
    if holdings:
        best_flow_item = sorted(
            holdings,
            key=lambda x: (_flow_grade_rank(x.get('flow_grade')), (base.nv(x.get('foreign_5d')) or 0) + (base.nv(x.get('institution_5d')) or 0)),
            reverse=True,
        )[0]
        caution_item = sorted(
            holdings,
            key=lambda x: (
                1 if (x.get('flow_grade') in ('경계', '주의')) else 0,
                1 if ((base.nv(x.get('stop_gap_pct')) is not None) and (base.nv(x.get('stop_gap_pct')) <= 5)) else 0,
                -((base.nv(x.get('foreign_5d')) or 0) + (base.nv(x.get('institution_5d')) or 0)),
            ),
            reverse=True,
        )[0]
        improving_item = sorted(
            holdings,
            key=lambda x: (
                (base.nv(x.get('foreign_5d')) or 0) - (base.nv(x.get('foreign_prev5d')) or 0)
                + (base.nv(x.get('institution_5d')) or 0) - (base.nv(x.get('institution_prev5d')) or 0),
                _flow_grade_rank(x.get('flow_grade')),
            ),
            reverse=True,
        )[0]
        weakening_item = sorted(
            holdings,
            key=lambda x: (
                (base.nv(x.get('foreign_5d')) or 0) - (base.nv(x.get('foreign_prev5d')) or 0)
                + (base.nv(x.get('institution_5d')) or 0) - (base.nv(x.get('institution_prev5d')) or 0),
                _flow_grade_rank(x.get('flow_grade')),
            ),
        )[0]
    score -= min(18, missing_price * 6)
    score -= min(20, stop_risk * 7)
    score -= min(15, flow_warning * 4)
    score -= min(10, resistance_count * 2)
    score += min(12, flow_support * 3)
    score += min(8, support_count * 2)
    score = max(0, min(100, int(round(score))))
    if value_delta is not None and value_delta_rate is not None:
        if value_delta > 0:
            strengths.append(f"최근 포트 평가금액이 직전 저장 대비 {base.fmt(value_delta)}원 ({value_delta_rate:+.2f}%) 개선됐습니다.")
        elif value_delta < 0:
            risks.append(f"최근 포트 평가금액이 직전 저장 대비 {base.fmt(value_delta)}원 ({value_delta_rate:+.2f}%) 감소했습니다.")
    if recent_streak == '3회 연속 개선':
        strengths.append('최근 3회 저장 기준 포트 가치가 연속 개선 흐름입니다.')
    elif recent_streak == '3회 연속 악화':
        risks.append('최근 3회 저장 기준 포트 가치가 연속 악화 흐름입니다.')
    if len(recent_window) >= 2:
        history_line = f"최근 5회 저장 기준 상승 {recent_up_count}회, 하락 {recent_down_count}회, 보합 {recent_flat_count}회로 {recent_bias}입니다."
        if recent_bias == '개선 우세':
            strengths.append(history_line)
        elif recent_bias == '악화 우세':
            risks.append(history_line)
        else:
            checklist.append(history_line)
        if history_rows:
            flow_line = '최근 진단 흐름은 ' + ' → '.join(f"{row.get('date')} {row.get('direction')}" for row in history_rows)
            checklist.append(flow_line)
    if best_flow_item is not None:
        strengths.append(f"가장 건강한 흐름 종목은 {best_flow_item.get('name')}이며 수급 등급은 {best_flow_item.get('flow_grade') or '-'}입니다.")
    if improving_item is not None:
        strengths.append(f"최근 수급이 가장 개선된 종목은 {improving_item.get('name')}입니다.")
    if caution_item is not None and caution_item.get('flow_grade') in ('경계', '주의'):
        risks.append(f"가장 주의할 종목은 {caution_item.get('name')}이며 수급 등급은 {caution_item.get('flow_grade') or '-'}입니다.")
    if weakening_item is not None and weakening_item.get('name') != (improving_item.get('name') if improving_item else None):
        risks.append(f"최근 수급이 가장 약해진 종목은 {weakening_item.get('name')}입니다.")
    if flow_support >= max(1, math.ceil(total / 3)):
        strengths.append(f"외인/기관 수급 우세 종목이 {flow_support}개로 방어력이 있습니다.")
    if support_count >= max(1, math.ceil(total / 4)):
        strengths.append(f"지지 구간 근처 종목이 {support_count}개 있어 눌림 대응 여지가 있습니다.")
    if stop_risk > 0:
        risks.append(f"손절가 점검 종목이 {stop_risk}개 있어 손실 관리 우선 확인이 필요합니다.")
    if flow_warning > 0:
        risks.append(f"수급 경계/주의 종목이 {flow_warning}개라 외인/기관 이탈 여부를 다시 봐야 합니다.")
    if resistance_count > 0:
        risks.append(f"1차 저항 근접 종목이 {resistance_count}개라 추격 대응보다 확인 매매가 유리합니다.")
    if missing_price > 0:
        risks.append(f"가격 미수집 종목이 {missing_price}개 있어 일부 진단 신뢰도가 낮습니다.")
    if target_hit > 0:
        checklist.append(f"목표가 도달 종목 {target_hit}개는 분할 차익 또는 목표 재설정 여부를 결정합니다.")
    if stop_risk > 0:
        checklist.append('손절가 인접 종목은 대응 기준과 주문 계획을 먼저 확인합니다.')
    if flow_warning > 0:
        checklist.append('수급 경계 종목은 오늘 기사/공시와 거래량 둔화 여부를 같이 점검합니다.')
    if support_count > 0:
        checklist.append('지지 근접 종목은 지지 이탈 여부보다 외인/기관 동행 여부를 우선 확인합니다.')
    if resistance_count > 0:
        checklist.append('저항 근접 종목은 추격 매수보다 비중 조절과 익절 시나리오를 먼저 검토합니다.')
    if not checklist:
        checklist.append('현재 포트는 큰 이상 신호가 적어 기존 계획 점검 중심으로 보면 됩니다.')
    if score >= 80:
        status = '양호'
        headline = f'포트 건강 상태가 안정적입니다. 현재 흐름은 {recent_trend}이며 기존 계획을 유지하되 수급 강도만 점검하면 좋습니다.'
    elif score >= 60:
        status = '점검'
        headline = f'포트는 전반적으로 유지 가능하지만 {recent_trend} 흐름이라 일부 종목의 수급과 저항 구간을 점검할 필요가 있습니다.'
    elif score >= 40:
        status = '주의'
        headline = f'포트에 경고 신호가 누적되고 있고 {recent_trend} 상태라 손절 기준과 비중 관리를 먼저 보는 편이 좋습니다.'
    else:
        status = '경계'
        headline = f'포트 방어가 우선인 상태입니다. 현재 {recent_trend} 흐름으로 손실 확대 가능성이 있는 종목부터 즉시 점검이 필요합니다.'
    if caution_item is not None:
        steps.append(f"1단계: {caution_item.get('name')}부터 점검합니다. 수급 등급은 {caution_item.get('flow_grade') or '-'}입니다.")
    else:
        steps.append('1단계: 손절가 점검 종목과 수급 경계 종목을 먼저 확인합니다.')
    steps.append('2단계: 지지 근접 종목은 외인/기관 동행 여부를 확인해 보유/추가 대응을 나눕니다.')
    if best_flow_item is not None:
        steps.append(f"3단계: {best_flow_item.get('name')}는 가장 건강한 흐름 후보로, 눌림 시 대응 우선순위를 높게 둡니다.")
    else:
        steps.append('3단계: 저항 근접 종목은 차익실현, 비중 축소, 목표가 재설정 중 하나를 선택합니다.')
    return {
        'health_score': score,
        'health_status': status,
        'headline': headline,
        'strengths': strengths,
        'risks': risks,
        'checklist': checklist,
        'steps': steps,
        'counts': {
            'flow_support': flow_support,
            'flow_warning': flow_warning,
            'support_near': support_count,
            'resistance_near': resistance_count,
            'stop_risk': stop_risk,
            'missing_price': missing_price,
        },
        'trend': {
            'value_delta': value_delta,
            'value_delta_rate': value_delta_rate,
            'recent_trend': recent_trend,
            'recent_streak': recent_streak,
            'recent_bias': recent_bias,
            'recent_up_count': recent_up_count,
            'recent_down_count': recent_down_count,
            'recent_flat_count': recent_flat_count,
            'improving_name': improving_item.get('name') if improving_item else None,
            'weakening_name': weakening_item.get('name') if weakening_item else None,
            'history_summary': f"최근 5회 기준 {recent_bias} · 상승 {recent_up_count}회 / 하락 {recent_down_count}회 / 보합 {recent_flat_count}회",
            'history_rows': history_rows,
        },
    }

def daily_stock_payload(code, limit=180):
    rows_desc = [dict(r) for r in base.q('SELECT code, name, trade_date, close_price, open_price, high_price, low_price, volume, personal_net, foreign_net, institution_net, personal_amount, foreign_amount, institution_amount, collected_at FROM daily_price_points WHERE code=? ORDER BY trade_date DESC LIMIT ?', (str(code), int(limit)))]
    if not rows_desc:
        return {'rows': [], 'chart_rows': [], 'summary': {}, 'analysis': []}
    latest = rows_desc[0]
    previous = rows_desc[1] if len(rows_desc) > 1 else None
    lookback_20 = rows_desc[:20]
    latest_close = base.nv(latest.get('close_price'))
    latest_open = base.nv(latest.get('open_price'))
    latest_high = base.nv(latest.get('high_price'))
    latest_low = base.nv(latest.get('low_price'))
    prev_close = base.nv(previous.get('close_price')) if previous else None
    close_position = None
    if latest_close is not None and latest_high is not None and latest_low is not None and latest_high != latest_low:
        close_position = round(((latest_close - latest_low) / (latest_high - latest_low)) * 100, 2)
    intraday_range_rate = None
    if latest_high is not None and latest_low is not None and prev_close not in (None, 0):
        intraday_range_rate = round(((latest_high - latest_low) / prev_close) * 100, 2)
    personal_5d = _sum_window(rows_desc, 'personal_net', 0, 5)
    personal_5d = _sum_window(rows_desc, 'personal_net', 0, 5)
    foreign_5d = _sum_window(rows_desc, 'foreign_net', 0, 5)
    institution_5d = _sum_window(rows_desc, 'institution_net', 0, 5)
    personal_prev5d = _sum_window(rows_desc, 'personal_net', 5, 5)
    foreign_prev5d = _sum_window(rows_desc, 'foreign_net', 5, 5)
    institution_prev5d = _sum_window(rows_desc, 'institution_net', 5, 5)
    level_ctx = _support_resistance_context(rows_desc, foreign_5d, institution_5d)
    summary = {
        'name': latest.get('name'), 'code': latest.get('code'), 'latest_date': latest.get('trade_date'),
        'latest_open': latest_open, 'latest_high': latest_high, 'latest_low': latest_low, 'latest_close': latest_close,
        'latest_volume': base.nv(latest.get('volume')),
        'one_day_change': round(((latest_close - prev_close) / prev_close) * 100, 2) if prev_close not in (None, 0) and latest_close is not None else None,
        'five_day_change': _price_change_from(rows_desc, 4), 'twenty_day_change': _price_change_from(rows_desc, 19),
        'avg_volume_20d': round(sum((base.nv(r.get('volume')) or 0) for r in lookback_20) / len(lookback_20), 2) if lookback_20 else None,
        'personal_5d': personal_5d, 'foreign_5d': foreign_5d, 'institution_5d': institution_5d,
        'personal_prev5d': personal_prev5d, 'foreign_prev5d': foreign_prev5d, 'institution_prev5d': institution_prev5d,
        'personal_20d': _sum_window(rows_desc, 'personal_net', 0, 20), 'foreign_20d': _sum_window(rows_desc, 'foreign_net', 0, 20), 'institution_20d': _sum_window(rows_desc, 'institution_net', 0, 20),
        'personal_amount_20d': _sum_window(rows_desc, 'personal_amount', 0, 20), 'foreign_amount_20d': _sum_window(rows_desc, 'foreign_amount', 0, 20), 'institution_amount_20d': _sum_window(rows_desc, 'institution_amount', 0, 20),
        'intraday_range_rate': intraday_range_rate, 'close_position': close_position,
        'flow_signal': _investor_flow_text(foreign_5d, institution_5d, foreign_prev5d, institution_prev5d), 'row_count': len(rows_desc),
        'support_1': level_ctx.get('support_1'), 'support_2': level_ctx.get('support_2'), 'resistance_1': level_ctx.get('resistance_1'), 'resistance_2': level_ctx.get('resistance_2'),
        'support_gap_1': level_ctx.get('support_gap_1'), 'resistance_gap_1': level_ctx.get('resistance_gap_1'), 'level_signal': level_ctx.get('level_signal'),
    }
    analysis = [
        f"최신 종가는 {base.fmt(summary.get('latest_close'))}원이며 시가 {base.fmt(summary.get('latest_open'))}, 고가 {base.fmt(summary.get('latest_high'))}, 저가 {base.fmt(summary.get('latest_low'))}입니다.",
        f"전일 대비 {base.sgn(summary.get('one_day_change'), '%')}, 5거래일 {base.sgn(summary.get('five_day_change'), '%')}, 20거래일 {base.sgn(summary.get('twenty_day_change'), '%')} 흐름입니다.",
        f"최근 5일 수급은 개인 {base.fmt(personal_5d)}, 외인 {base.fmt(foreign_5d)}, 기관 {base.fmt(institution_5d)}이며, 직전 5일 대비 신호는 '{summary.get('flow_signal')}' 입니다.",
        f"최근 20일 누적 수급은 개인 {base.fmt(summary.get('personal_20d'))}, 외인 {base.fmt(summary.get('foreign_20d'))}, 기관 {base.fmt(summary.get('institution_20d'))}입니다.",
        f"장중 변동률은 {base.fmt(summary.get('intraday_range_rate'))}%이고, 종가 위치는 당일 범위 대비 {base.fmt(summary.get('close_position'))}%입니다.",
        f"1차 지지는 {base.fmt(summary.get('support_1'))}원, 1차 저항은 {base.fmt(summary.get('resistance_1'))}원이며 수급 기준 신뢰도는 '{summary.get('level_signal')}' 입니다.",
    ]
    if (summary.get('five_day_change') or 0) > 0 and (foreign_5d or 0) <= 0 and (institution_5d or 0) <= 0:
        analysis.append('가격은 반등했지만 외인/기관이 동행하지 않아 단기 반등의 지속성은 한 번 더 확인하는 편이 좋습니다.')
    elif (summary.get('five_day_change') or 0) > 0 and (foreign_5d or 0) > 0 and (institution_5d or 0) > 0:
        analysis.append('가격 상승과 외인/기관 동반 순매수가 함께 나타나 추세 신뢰도가 상대적으로 높습니다.')
    elif (summary.get('five_day_change') or 0) < 0 and (foreign_5d or 0) < 0 and (institution_5d or 0) < 0:
        analysis.append('가격 약세와 외인/기관 동반 순매도가 겹쳐 방어적 접근이 유리한 구간입니다.')
    return {'rows': rows_desc, 'chart_rows': list(reversed(rows_desc)), 'summary': summary, 'analysis': analysis}


def _daily_flow_context_map(codes, lookback=25):
    codes = [str(code) for code in codes if code]
    if not codes:
        return {}
    placeholders = ','.join('?' for _ in codes)
    rows = [dict(r) for r in base.q(f'SELECT code, trade_date, close_price, high_price, low_price, personal_net, foreign_net, institution_net FROM daily_price_points WHERE code IN ({placeholders}) ORDER BY trade_date DESC', tuple(codes))]
    by_code = {}
    for row in rows:
        by_code.setdefault(str(row.get('code')), []).append(row)
    out = {}
    for code, items in by_code.items():
        items = items[:lookback]
        foreign_5d = _sum_window(items, 'foreign_net', 0, 5)
        institution_5d = _sum_window(items, 'institution_net', 0, 5)
        foreign_prev5d = _sum_window(items, 'foreign_net', 5, 5)
        institution_prev5d = _sum_window(items, 'institution_net', 5, 5)
        five_day_change = _price_change_from(items, 4)
        level_ctx = _support_resistance_context(items, foreign_5d, institution_5d)
        out[code] = {
            'personal_5d': _sum_window(items, 'personal_net', 0, 5), 'foreign_5d': foreign_5d, 'institution_5d': institution_5d,
            'personal_prev5d': _sum_window(items, 'personal_net', 5, 5), 'foreign_prev5d': foreign_prev5d, 'institution_prev5d': institution_prev5d,
            'personal_20d': _sum_window(items, 'personal_net', 0, 20), 'foreign_20d': _sum_window(items, 'foreign_net', 0, 20), 'institution_20d': _sum_window(items, 'institution_net', 0, 20),
            'five_day_change': five_day_change, 'twenty_day_change': _price_change_from(items, 19),
            'flow_signal': _investor_flow_text(foreign_5d, institution_5d, foreign_prev5d, institution_prev5d),
            'flow_grade': _flow_grade(foreign_5d, institution_5d, foreign_prev5d, institution_prev5d, five_day_change),
            'support_1': level_ctx.get('support_1'), 'support_2': level_ctx.get('support_2'), 'resistance_1': level_ctx.get('resistance_1'), 'resistance_2': level_ctx.get('resistance_2'),
            'support_gap_1': level_ctx.get('support_gap_1'), 'resistance_gap_1': level_ctx.get('resistance_gap_1'), 'level_signal': level_ctx.get('level_signal'),
        }
    return out


_original_port = base.port
_original_recs = base.recs


def port():
    payload = _original_port()
    holdings = list(payload.get('holdings') or [])
    flow_map = _daily_flow_context_map([item.get('code') for item in holdings])
    extra_alerts = []
    extra_report = []
    for item in holdings:
        ctx = flow_map.get(str(item.get('code')), {})
        item.update(ctx)
        price_5d = ctx.get('five_day_change') or 0
        foreign_5d = ctx.get('foreign_5d') or 0
        institution_5d = ctx.get('institution_5d') or 0
        foreign_prev5d = ctx.get('foreign_prev5d') or 0
        institution_prev5d = ctx.get('institution_prev5d') or 0
        support_gap_1 = base.nv(ctx.get('support_gap_1'))
        resistance_gap_1 = base.nv(ctx.get('resistance_gap_1'))
        level_signal = ctx.get('level_signal') or ''
        if item.get('flow_grade') == '매수 우세' and support_gap_1 is not None and support_gap_1 <= 3:
            item['diagnosis_note'] = f"지지 인근에서 외인/기관 수급이 받쳐주는 구간입니다. ({support_gap_1:.2f}% 거리)"
        elif item.get('flow_grade') in ('경계', '주의') and support_gap_1 is not None and support_gap_1 <= 3:
            item['diagnosis_note'] = f"지지 가격은 가깝지만 수급 약화가 동반돼 보수적 대응이 필요합니다. ({support_gap_1:.2f}% 거리)"
        elif resistance_gap_1 is not None and resistance_gap_1 <= 3:
            item['diagnosis_note'] = f"저항권에 근접해 추격보다 익절/비중조절 시나리오가 유리합니다. ({resistance_gap_1:.2f}% 거리)"
        else:
            item['diagnosis_note'] = level_signal or '현재 구조상 특별한 경고보다 계획 점검 중심으로 보면 됩니다.'
        if price_5d < 0 and foreign_5d < 0 and institution_5d < 0:
            extra_alerts.append({'name': item.get('name'), 'message': '최근 5일 가격 약세와 외인/기관 동반 순매도', 'level': 'warn'})
        elif price_5d > 0 and foreign_5d > 0 and institution_5d > 0:
            extra_alerts.append({'name': item.get('name'), 'message': '최근 5일 가격 상승과 외인/기관 동반 순매수', 'level': 'info'})
        elif (foreign_5d > 0 and institution_5d > 0) and (foreign_prev5d <= 0 or institution_prev5d <= 0):
            extra_alerts.append({'name': item.get('name'), 'message': '외인/기관 수급이 최근 5일 순매수로 전환', 'level': 'info'})
        elif (foreign_5d < 0 and institution_5d < 0) and (foreign_prev5d >= 0 or institution_prev5d >= 0):
            extra_alerts.append({'name': item.get('name'), 'message': '외인/기관 수급이 최근 5일 순매도로 전환', 'level': 'warn'})
        if support_gap_1 is not None and support_gap_1 <= 3:
            extra_alerts.append({'name': item.get('name'), 'message': f"1차 지지 근접 ({support_gap_1:.2f}%) · {level_signal or '지지 구간 확인 필요'}", 'level': 'info' if item.get('flow_grade') in ('매수 우세', '전환 관찰') else 'warn'})
        if resistance_gap_1 is not None and resistance_gap_1 <= 3:
            extra_alerts.append({'name': item.get('name'), 'message': f"1차 저항 근접 ({resistance_gap_1:.2f}%) · {level_signal or '저항 구간 확인 필요'}", 'level': 'warn'})
    leaders = [h for h in holdings if (h.get('foreign_5d') or 0) > 0 and (h.get('institution_5d') or 0) > 0]
    laggards = [h for h in holdings if (h.get('foreign_5d') or 0) < 0 and (h.get('institution_5d') or 0) < 0]
    if leaders:
        leaders = sorted(leaders, key=lambda h: ((h.get('foreign_5d') or 0) + (h.get('institution_5d') or 0)), reverse=True)
        extra_report.append('최근 5일 외인/기관 동반 순매수 상위는 ' + ', '.join(f"{h.get('name')}({base.fmt((h.get('foreign_5d') or 0) + (h.get('institution_5d') or 0))})" for h in leaders[:3]) + '입니다.')
        extra_report.append(f"수급이 가장 강한 종목은 {leaders[0].get('name')}이며 현재 등급은 {leaders[0].get('flow_grade') or '-'}입니다.")
    if laggards:
        laggards = sorted(laggards, key=lambda h: ((h.get('foreign_5d') or 0) + (h.get('institution_5d') or 0)))
        extra_report.append('최근 5일 외인/기관 동반 순매도 상위는 ' + ', '.join(f"{h.get('name')}({base.fmt((h.get('foreign_5d') or 0) + (h.get('institution_5d') or 0))})" for h in laggards[:3]) + '입니다.')
        extra_report.append(f"수급이 가장 약한 종목은 {laggards[0].get('name')}이며 현재 등급은 {laggards[0].get('flow_grade') or '-'}입니다.")
    near_support = [h for h in holdings if base.nv(h.get('support_gap_1')) is not None]
    near_support = sorted(near_support, key=lambda h: base.nv(h.get('support_gap_1')) or 9999)
    if near_support:
        h = near_support[0]
        extra_report.append(f"{h.get('name')}는 1차 지지 {base.fmt(h.get('support_1'))}원까지 {base.fmt(h.get('support_gap_1'))}% 거리이며, 해석은 {h.get('level_signal') or '-'}입니다.")
    near_resistance = [h for h in holdings if base.nv(h.get('resistance_gap_1')) is not None]
    near_resistance = sorted(near_resistance, key=lambda h: base.nv(h.get('resistance_gap_1')) or 9999)
    if near_resistance:
        h = near_resistance[0]
        extra_report.append(f"{h.get('name')}는 1차 저항 {base.fmt(h.get('resistance_1'))}원까지 {base.fmt(h.get('resistance_gap_1'))}% 거리이며, 해석은 {h.get('level_signal') or '-'}입니다.")
    flow_support_count = len([h for h in holdings if h.get('flow_grade') == '매수 우세'])
    flow_warning_count = len([h for h in holdings if h.get('flow_grade') in ('경계', '주의')])
    if extra_report:
        payload['report'] = list(payload.get('report') or []) + extra_report
    payload.setdefault('summary', {})['flow_support_count'] = flow_support_count
    payload['summary']['flow_warning_count'] = flow_warning_count
    if extra_alerts:
        payload['alerts'] = (list(payload.get('alerts') or []) + extra_alerts)[:10]
    base_rebalance = list(payload.get('rebalance') or [])
    flow_rebalance = []
    for item in holdings:
        grade = item.get('flow_grade') or ''
        weight = base.nv(item.get('weight')) or 0
        target_weight = base.nv(item.get('target_weight')) or 0
        pnl_rate = base.nv(item.get('pnl_rate')) or 0
        gap = round(weight - target_weight, 2) if item.get('weight') is not None and item.get('target_weight') is not None else None
        if grade == '\ub9e4\uc218 \uc6b0\uc138' and gap is not None and gap <= -2:
            flow_rebalance.append({'name': item.get('name'), 'action': '\uc218\uae09 \uae30\ubc18 \ube44\uc911 \ud655\ub300 \uac80\ud1a0', 'reason': f"\uc678\uc778/\uae30\uad00 \ub3d9\ubc18 \uc21c\ub9e4\uc218\uc774\uba70 \ubaa9\ud45c \ub300\ube44 {abs(gap):.2f}%p \ub0ae\uc544 \ubd84\ud560 \ud655\ub300 \ud6c4\ubcf4\uc785\ub2c8\ub2e4."})
        elif grade in ('\uacbd\uacc4', '\uc8fc\uc758') and (gap is None or gap >= 0):
            flow_rebalance.append({'name': item.get('name'), 'action': '\uc218\uae09 \uc57d\ud654 \ube44\uc911 \uc810\uac80', 'reason': f"\ucd5c\uadfc \uc218\uae09 \ub4f1\uae09\uc774 {grade}\uc774\uace0 \uc190\uc775\ub960 {base.sgn(pnl_rate, '%')}\ub85c \ucd94\uac00 \uc57d\uc138 \uc804\uac1c \uc804 \ube44\uc911 \uc810\uac80\uc774 \ud544\uc694\ud569\ub2c8\ub2e4."})
        elif grade == '\uc804\ud658 \uad00\ucc30':
            flow_rebalance.append({'name': item.get('name'), 'action': '\uc218\uae09 \uc804\ud658 \ud655\uc778', 'reason': '\uc678\uc778 \ub610\ub294 \uae30\uad00 \uc218\uae09\uc774 \uac1c\uc120\ub418\ub294 \ucd08\uc785\uc774\ub77c \ud558\ub8e8 \uc774\ud2c0 \ub354 \ud655\uc778 \ud6c4 \ub300\uc751\ud558\ub294 \ud3b8\uc774 \uc88b\uc2b5\ub2c8\ub2e4.'})
    priority = {'\uc218\uae09 \uae30\ubc18 \ube44\uc911 \ud655\ub300 \uac80\ud1a0': 3, '\uc218\uae09 \uc57d\ud654 \ube44\uc911 \uc810\uac80': 2, '\uc218\uae09 \uc804\ud658 \ud655\uc778': 1}
    payload['rebalance'] = sorted(base_rebalance + flow_rebalance, key=lambda x: priority.get(x.get('action') or '', 0), reverse=True)[:8]
    payload['diagnosis'] = _portfolio_health_diagnosis(payload, holdings)
    payload['holdings'] = holdings
    return payload

def recs():
    payload = _original_recs()
    items = list(payload.get('items') or [])
    flow_map = _daily_flow_context_map([item.get('code') for item in items])
    for item in items:
        ctx = flow_map.get(str(item.get('code')), {})
        item['flow_signal'] = ctx.get('flow_signal')
        item['five_day_foreign'] = ctx.get('foreign_5d')
        item['five_day_institution'] = ctx.get('institution_5d')
        item['five_day_personal'] = ctx.get('personal_5d')
        item['flow_grade'] = ctx.get('flow_grade')
        score_adjust = 0
        if ctx.get('foreign_5d') is not None and ctx.get('institution_5d') is not None:
            if (ctx.get('foreign_5d') or 0) > 0 and (ctx.get('institution_5d') or 0) > 0:
                item.setdefault('reasons', []).append('5일 외인/기관 동반 순매수')
                score_adjust += 4
            elif (ctx.get('foreign_5d') or 0) < 0 and (ctx.get('institution_5d') or 0) < 0:
                item.setdefault('reasons', []).append('5일 외인/기관 동반 순매도')
                score_adjust -= 4
        if (ctx.get('foreign_5d') or 0) > 0 and (ctx.get('institution_5d') or 0) <= 0:
            item.setdefault('reasons', []).append('5일 외인 우위')
            score_adjust += 1.5
        elif (ctx.get('foreign_5d') or 0) <= 0 and (ctx.get('institution_5d') or 0) > 0:
            item.setdefault('reasons', []).append('5일 기관 우위')
            score_adjust += 1.5
        if score_adjust:
            item['score'] = round((item.get('score') or 0) + score_adjust, 2)
        score = item.get('score') or 0
        item['style'] = '공격형' if score >= 78 else '균형형' if score >= 58 else '관찰형'
    items.sort(key=lambda x: ((x.get('score') or 0), _flow_grade_rank(x.get('flow_grade'))), reverse=True)
    payload['items'] = items
    payload['items'] = items
    summary = list(payload.get('summary') or [])
    if items:
        supported = [x for x in items if (x.get('five_day_foreign') or 0) > 0 and (x.get('five_day_institution') or 0) > 0]
        pressured = [x for x in items if (x.get('five_day_foreign') or 0) < 0 and (x.get('five_day_institution') or 0) < 0]
        if supported:
            summary.append('최근 5일 외인/기관 동반 순매수 종목이 추천 우선순위에서 가산점을 받습니다.')
            strongest = sorted(supported, key=lambda x: ((x.get('five_day_foreign') or 0) + (x.get('five_day_institution') or 0)), reverse=True)[0]
            summary.append(f"가장 강한 수급 후보는 {strongest.get('name')}({strongest.get('flow_grade') or '-'})입니다.")
        if pressured:
            summary.append('최근 5일 외인/기관 동반 순매도 종목은 추천은 되더라도 관찰형 비중이 높아질 수 있습니다.')
            weakest = sorted(pressured, key=lambda x: ((x.get('five_day_foreign') or 0) + (x.get('five_day_institution') or 0)))[0]
            summary.append(f"가장 약한 수급 후보는 {weakest.get('name')}({weakest.get('flow_grade') or '-'})입니다.")
    if items:
        grade_counts = {}
        for item in items:
            grade = item.get('flow_grade') or '미분류'
            grade_counts[grade] = grade_counts.get(grade, 0) + 1
        ordered = ['매수 우세', '전환 관찰', '혼조', '중립', '주의', '경계', '미분류']
        parts = [f"{grade} {grade_counts[grade]}종목" for grade in ordered if grade in grade_counts]
        if parts:
            summary.append('수급 등급 분포는 ' + ', '.join(parts) + '입니다.')
    payload['summary'] = summary
    method = list(payload.get('method') or [])
    method.append('최근 5일 외인/기관 수급 방향과 직전 5일 대비 전환 여부를 보조 판단으로 추가합니다.')
    payload['method'] = method
    return payload

def _portfolio_html():
    text = (base.B / 'stock_portfolio.html').read_text(encoding='utf-8', errors='ignore')
    old = """<div class="metric"><div class="k">거래량배수</div><div class="v">${fmt(item.volume_ratio)}</div></div><div class="metric"><div class="k">목표가 여유</div>"""
    new = """<div class="metric"><div class="k">거래량배수</div><div class="v">${fmt(item.volume_ratio)}</div></div><div class="metric"><div class="k">5일 외인 순매수</div><div class="v ${((num(item.foreign_5d)||0)>0)?'good':((num(item.foreign_5d)||0)<0)?'bad':''}">${fmt(item.foreign_5d)}</div></div><div class="metric"><div class="k">5일 기관 순매수</div><div class="v ${((num(item.institution_5d)||0)>0)?'good':((num(item.institution_5d)||0)<0)?'bad':''}">${fmt(item.institution_5d)}</div></div><div class="metric"><div class="k">20일 외인 순매수</div><div class="v ${((num(item.foreign_20d)||0)>0)?'good':((num(item.foreign_20d)||0)<0)?'bad':''}">${fmt(item.foreign_20d)}</div></div><div class="metric"><div class="k">20일 기관 순매수</div><div class="v ${((num(item.institution_20d)||0)>0)?'good':((num(item.institution_20d)||0)<0)?'bad':''}">${fmt(item.institution_20d)}</div></div><div class="metric"><div class="k">수급 신호</div><div class="v" style="font-size:16px">${item.flow_signal||'-'}</div></div><div class="metric"><div class="k">수급 등급</div><div class="v ${item.flow_grade==='매수 우세'?'good':(item.flow_grade==='경계'||item.flow_grade==='주의')?'bad':''}">${item.flow_grade||'-'}</div></div><div class="metric"><div class="k">1차 지지</div><div class="v">${fmt(item.support_1)}</div></div><div class="metric"><div class="k">2차 지지</div><div class="v">${fmt(item.support_2)}</div></div><div class="metric"><div class="k">1차 저항</div><div class="v">${fmt(item.resistance_1)}</div></div><div class="metric"><div class="k">2차 저항</div><div class="v">${fmt(item.resistance_2)}</div></div><div class="metric"><div class="k">지지까지 거리</div><div class="v ${((num(item.support_gap_1)||0)<=3)?'good':''}">${fmt(item.support_gap_1)}</div></div><div class="metric"><div class="k">저항까지 거리</div><div class="v ${((num(item.resistance_gap_1)||0)<=3)?'warn':''}">${fmt(item.resistance_gap_1)}</div></div><div class="metric"><div class="k">지지/저항 해석</div><div class="v" style="font-size:16px">${item.level_signal||'-'}</div></div><div class="metric"><div class="k">목표가 여유</div>"""
    if old in text:
        text = text.replace(old, new)

    text = text.replace("['위험도 점수',fmt(summary.risk_score)],['위험도 등급',summary.risk_grade||'-']", "['위험도 점수',fmt(summary.risk_score)],['위험도 등급',summary.risk_grade||'-'],['수급 우세',fmt(summary.flow_support_count)],['수급 경계',fmt(summary.flow_warning_count)]")

    diagnosis_section = """<section class=\"card\" style=\"margin-top:18px\"><h2>포트 진단형 AI</h2><p class=\"lead\">수익률 예측이 아니라 현재 포트의 건강 상태와 위험 신호를 매일 진단하는 보드입니다.</p><div id=\"portfolioDiagnosis\"></div></section>\r\n\r\n    <section class=\"grid\">"""
    text = text.replace('<section class="grid">', diagnosis_section, 1)

    diagnosis_js = """let latestDiagnosis={};
function renderDiagnosis(data){
  const box=document.getElementById('portfolioDiagnosis');
  if(!box){return}
  if(!data||Object.keys(data).length===0){
    box.innerHTML='<div class="empty">진단 데이터가 없습니다.</div>';
    return;
  }
  const counts=data.counts||{};
  const trend=data.trend||{};
  const statusClass=(data.health_status==='양호')?'good':(data.health_status==='주의'||data.health_status==='경계')?'bad':'warn';
  const trendRows=(trend.history_rows&&trend.history_rows.length)
    ? `<div style="margin-top:8px;font-size:13px;color:#5b6472">${trend.history_rows.map(row=>`${row.date} ${fmt(row.value)}원 ${row.direction}`).join(' | ')}</div>`
    : '';
  box.innerHTML=`<div class="dual"><div><div class="metric-grid"><div class="metric"><div class="k">건강 점수</div><div class="v ${statusClass}">${fmt(data.health_score)}</div></div><div class="metric"><div class="k">진단 상태</div><div class="v ${statusClass}">${data.health_status||'-'}</div></div><div class="metric"><div class="k">수급 우세</div><div class="v">${fmt(counts.flow_support)}</div></div><div class="metric"><div class="k">수급 경계</div><div class="v">${fmt(counts.flow_warning)}</div></div><div class="metric"><div class="k">지지 근접</div><div class="v">${fmt(counts.support_near)}</div></div><div class="metric"><div class="k">저항 근접</div><div class="v">${fmt(counts.resistance_near)}</div></div><div class="metric"><div class="k">손절 점검</div><div class="v">${fmt(counts.stop_risk)}</div></div><div class="metric"><div class="k">가격 미수집</div><div class="v">${fmt(counts.missing_price)}</div></div></div><div class="item" style="margin-top:12px"><strong>한줄 진단</strong><div style="margin-top:8px">${data.headline||'-'}</div></div><div class="item" style="margin-top:12px"><strong>최근 진단 흐름</strong><div style="margin-top:8px">${trend.history_summary||'최근 흐름 데이터가 부족합니다.'}</div>${trendRows}</div></div><div><div class="item"><strong>위험 신호</strong>${(data.risks||[]).length?(data.risks||[]).map(line=>`<div style="margin-top:8px">- ${line}</div>`).join(''):'<div class="empty">뚜렷한 위험 신호가 없습니다.</div>'}</div><div class="item"><strong>강점</strong>${(data.strengths||[]).length?(data.strengths||[]).map(line=>`<div style="margin-top:8px">- ${line}</div>`).join(''):'<div class="empty">강점 데이터가 없습니다.</div>'}</div></div></div><div class="triple" style="margin-top:14px"><div class="item"><strong>오늘의 체크리스트</strong>${(data.checklist||[]).map(line=>`<div style="margin-top:8px">- ${line}</div>`).join('')}</div><div class="item" style="grid-column:span 2"><strong>단계별 진행</strong>${(data.steps||[]).map(line=>`<div style="margin-top:8px">${line}</div>`).join('')}</div></div>`;
}
"""
    text = text.replace('let latestNews=null;', 'let latestNews=null;\n' + diagnosis_js, 1)
    text = text.replace('renderReport(d.report||[]);renderTable();renderDetail();await loadPortfolioNews()', "latestDiagnosis=d.diagnosis||{};if(typeof renderDiagnosis==='function'){renderDiagnosis(latestDiagnosis)};renderReport(d.report||[]);renderTable();renderDetail();await loadPortfolioNews()", 1)
    text = text.replace('<div class="item"><strong>메모</strong>', '<div class="item"><strong>종목 진단</strong><div style="margin-top:8px">${item.diagnosis_note||\'-\'}</div></div><div class="item"><strong>메모</strong>', 1)
    return text


def _recommendations_html():
    text = (base.B / 'stock_recommendations.html').read_text(encoding='utf-8', errors='ignore')
    text = text.replace('<select id="themeFilter"><option value="">전체 테마</option></select>\n        <input id="scoreFilter" type="number" step="0.1" placeholder="최소 추천점수 예: 20">', '<select id="themeFilter"><option value="">전체 테마</option></select>\n        <select id="flowFilter"><option value="">전체 수급</option><option value="support">매수 우세</option><option value="warn">경계/주의</option><option value="turn">전환 관찰</option><option value="mixed">혼조</option></select>\n        <select id="sortFilter"><option value="score">추천점수순</option><option value="flow">수급강도순</option><option value="foreign5">5일 외인수급순</option><option value="institution5">5일 기관수급순</option></select>\n        <input id="scoreFilter" type="number" step="0.1" placeholder="최소 추천점수 예: 20">')
    text = text.replace('<button class="btn" id="applyFilterBtn">필터 적용</button>', '<button class="btn" id="applyFilterBtn">필터 적용</button><button class="btn secondary" id="flowSupportBtn">매수 우세만</button><button class="btn secondary" id="flowWarnBtn">경계/주의</button><button class="btn secondary" id="flowTurnBtn">전환 관찰</button><button class="btn secondary" id="flowResetBtn">수급 초기화</button>')
    text = text.replace("const params=new URLSearchParams(); const style=document.getElementById('styleFilter').value; const theme=document.getElementById('themeFilter').value; const minScore=document.getElementById('scoreFilter').value; if(style) params.set('style',style); if(theme) params.set('theme',theme); if(minScore!=='') params.set('min_score',minScore);", "const params=new URLSearchParams(); const style=document.getElementById('styleFilter').value; const theme=document.getElementById('themeFilter').value; const flow=document.getElementById('flowFilter').value; const sort=document.getElementById('sortFilter').value; const minScore=document.getElementById('scoreFilter').value; if(style) params.set('style',style); if(theme) params.set('theme',theme); if(flow) params.set('flow',flow); if(sort) params.set('sort',sort); if(minScore!=='') params.set('min_score',minScore);")
    text = text.replace("document.getElementById('applyFilterBtn').addEventListener('click',()=>load().catch(err=>alert(err.message)));", "document.getElementById('applyFilterBtn').addEventListener('click',()=>load().catch(err=>alert(err.message)));document.getElementById('flowSupportBtn').addEventListener('click',()=>{document.getElementById('flowFilter').value='support';load().catch(err=>alert(err.message));});document.getElementById('flowWarnBtn').addEventListener('click',()=>{document.getElementById('flowFilter').value='warn';load().catch(err=>alert(err.message));});document.getElementById('flowTurnBtn').addEventListener('click',()=>{document.getElementById('flowFilter').value='turn';load().catch(err=>alert(err.message));});document.getElementById('flowResetBtn').addEventListener('click',()=>{document.getElementById('flowFilter').value='';document.getElementById('sortFilter').value='score';load().catch(err=>alert(err.message));});")
    return text
_original_do_get = base.X.do_GET


def do_GET(self):
    try:
        u = base.urlparse(self.path)
        p = u.path
        a = {k: v[0] for k, v in base.parse_qs(u.query).items()}
        if p == '/portfolio':
            return self.out(200, _portfolio_html().encode('utf-8'), 'text/html; charset=utf-8')
        if p == '/recommendations':
            return self.out(200, _recommendations_html().encode('utf-8'), 'text/html; charset=utf-8')
        if p == '/api/recommendations':
            d = recs()
            its = list(d.get('items') or [])
            if a.get('style'):
                its = [x for x in its if x.get('style') == a['style']]
            if a.get('theme'):
                its = [x for x in its if x.get('theme') == a['theme']]
            if a.get('min_score'):
                its = [x for x in its if (base.nv(x.get('score')) or 0) >= (base.nv(a.get('min_score')) or 0)]
            flow = a.get('flow') or ''
            if flow == 'support':
                its = [x for x in its if x.get('flow_grade') == '\ub9e4\uc218 \uc6b0\uc138']
            elif flow == 'warn':
                its = [x for x in its if x.get('flow_grade') in ('\uacbd\uacc4', '\uc8fc\uc758')]
            elif flow == 'turn':
                its = [x for x in its if x.get('flow_grade') == '\uc804\ud658 \uad00\ucc30']
            elif flow == 'mixed':
                its = [x for x in its if x.get('flow_grade') == '\ud63c\uc870']
            sort_key = a.get('sort') or 'score'
            if sort_key == 'flow':
                its = sorted(its, key=lambda x: (_flow_grade_rank(x.get('flow_grade')), base.nv(x.get('score')) or 0), reverse=True)
            elif sort_key == 'foreign5':
                its = sorted(its, key=lambda x: (base.nv(x.get('five_day_foreign')) or 0, base.nv(x.get('score')) or 0), reverse=True)
            elif sort_key == 'institution5':
                its = sorted(its, key=lambda x: (base.nv(x.get('five_day_institution')) or 0, base.nv(x.get('score')) or 0), reverse=True)
            else:
                its = sorted(its, key=lambda x: (base.nv(x.get('score')) or 0, _flow_grade_rank(x.get('flow_grade'))), reverse=True)
            d['items'] = its
            return self.js(d)
    except Exception:
        pass
    return _original_do_get(self)

base.normalize_naver_daily_table = normalize_naver_daily_table
base.normalize_naver_frgn_table = normalize_naver_frgn_table
base.fetch_naver_daily_stock_data = fetch_naver_daily_stock_data
base.existing_daily_last_dates = existing_daily_last_dates
base.build_naver_daily_maps = build_naver_daily_maps
base.collect_daily_prices = collect_daily_prices
base.daily_stock_payload = daily_stock_payload
base.port = port
base.recs = recs
base.X.do_GET = do_GET

if __name__ == '__main__':
    base.run()























































