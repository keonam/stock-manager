import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yfinance as yf
from pykrx import stock


CONFIG_FILE = Path('config.json')
OUTPUT_DIR = Path('output')
KST = timezone(timedelta(hours=9))
INITIAL_LOOKBACK_DAYS = 14
MAX_LOOKBACK_DAYS = 180
SEARCH_STEP_DAYS = 14
MIN_PYTHON = (3, 11)
TESTED_PYTHON = (3, 13)
YAHOO_LOOKBACK_DAYS = 30
FLOW_LOOKBACK_DAYS = 10


class AppError(Exception):
    pass


def ensure_supported_python() -> None:
    if sys.version_info < MIN_PYTHON:
        required = '.'.join(str(part) for part in MIN_PYTHON)
        current = '.'.join(str(part) for part in sys.version_info[:3])
        raise AppError(f'Python {required} 이상이 필요합니다. 현재 버전: {current}')


def read_json_file(path: Path) -> Any:
    if not path.exists():
        raise AppError(f'파일을 찾을 수 없습니다: {path.resolve()}')

    try:
        return json.loads(path.read_text(encoding='utf-8-sig'))
    except json.JSONDecodeError as exc:
        raise AppError(f'{path.name} JSON 파싱 오류: {exc}') from exc


def write_json_file(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')


def normalize_items(raw_items: Any, field_name: str, default_type: str) -> List[Dict[str, str]]:
    if not isinstance(raw_items, list):
        raise AppError(f'config.json의 {field_name}는 배열이어야 합니다.')

    normalized_items: List[Dict[str, str]] = []
    for index, item in enumerate(raw_items, start=1):
        if not isinstance(item, dict):
            raise AppError(f'config.json {field_name}[{index}] 형식이 올바르지 않습니다.')

        code = str(item.get('code', '')).strip()
        name = str(item.get('name', '')).strip()
        if not code:
            raise AppError(f'config.json {field_name}[{index}]의 code 값이 비어 있습니다.')

        normalized_items.append({'code': code, 'name': name, 'type': default_type})

    return normalized_items


def load_config() -> Dict[str, Any]:
    config = read_json_file(CONFIG_FILE)
    if not isinstance(config, dict):
        raise AppError('config.json은 객체 형식이어야 합니다.')

    return {
        'indexes': normalize_items(config.get('indexes', []), 'indexes', 'index'),
        'commodities': normalize_items(config.get('commodities', []), 'commodities', 'commodity'),
        'stocks': normalize_items(config.get('stocks', []), 'stocks', 'stock'),
        'save_csv': bool(config.get('save_csv', True)),
        'save_json': bool(config.get('save_json', True)),
    }


def format_date(value: date) -> str:
    return value.strftime('%Y%m%d')


def to_python_number(value: Any) -> Optional[float]:
    if isinstance(value, pd.Series):
        value = value.iloc[0] if not value.empty else None
    if value is None or pd.isna(value):
        return None
    return float(value)


def to_int(value: Any) -> Optional[int]:
    number = to_python_number(value)
    if number is None:
        return None
    return int(round(number))


def to_float(value: Any) -> Optional[float]:
    number = to_python_number(value)
    if number is None:
        return None
    return round(float(number), 2)


def calculate_change_rate(current_close: Optional[float], previous_close: Optional[float]) -> Optional[float]:
    if current_close is None or previous_close in (None, 0):
        return None
    return round(((current_close - previous_close) / previous_close) * 100, 2)


def calculate_ratio(numerator: Optional[float], denominator: Optional[float], digits: int = 2) -> Optional[float]:
    if numerator is None or denominator in (None, 0):
        return None
    return round(numerator / denominator, digits)


def calculate_intraday_range_rate(high_price: Optional[float], low_price: Optional[float], reference_price: Optional[float]) -> Optional[float]:
    if high_price is None or low_price is None or reference_price in (None, 0):
        return None
    return round(((high_price - low_price) / reference_price) * 100, 2)


def calculate_position_percent(value: Optional[float], low_price: Optional[float], high_price: Optional[float]) -> Optional[float]:
    if value is None or low_price is None or high_price is None:
        return None
    if high_price == low_price:
        return 100.0 if value == high_price else None
    return round(((value - low_price) / (high_price - low_price)) * 100, 2)


def build_analysis_metrics(
    current_price: Optional[float],
    previous_close: Optional[float],
    open_price: Optional[float],
    high_price: Optional[float],
    low_price: Optional[float],
    current_volume: Optional[float],
    previous_volume: Optional[float],
) -> Dict[str, Optional[float]]:
    gap_amount = None if open_price is None or previous_close is None else round(open_price - previous_close, 2)
    intraday_change = None if current_price is None or open_price is None else round(current_price - open_price, 2)
    return {
        '시가갭': gap_amount,
        '시가갭률(%)': calculate_change_rate(open_price, previous_close),
        '장중등락': intraday_change,
        '시가대비등락률(%)': calculate_change_rate(current_price, open_price),
        '장중변동폭': None if high_price is None or low_price is None else round(high_price - low_price, 2),
        '장중변동률(%)': calculate_intraday_range_rate(high_price, low_price, previous_close or current_price),
        '종가위치(%)': calculate_position_percent(current_price, low_price, high_price),
        '거래량배수': calculate_ratio(current_volume, previous_volume),
    }


def get_reference_today() -> date:
    return datetime.now(KST).date()


def fetch_stock_ohlcv(code: str) -> pd.DataFrame:
    end_date = get_reference_today()
    attempted_ranges: List[str] = []

    for lookback_days in range(INITIAL_LOOKBACK_DAYS, MAX_LOOKBACK_DAYS + 1, SEARCH_STEP_DAYS):
        start_date = end_date - timedelta(days=lookback_days)
        attempted_ranges.append(f'{start_date.isoformat()}~{end_date.isoformat()}')

        try:
            df = stock.get_market_ohlcv_by_date(format_date(start_date), format_date(end_date), code)
        except Exception as exc:
            raise AppError(f'{code} 시세 조회 중 오류가 발생했습니다: {exc}') from exc

        if df is not None and not df.empty:
            return df.sort_index()

    tried = ', '.join(attempted_ranges)
    raise AppError(f'{code} 최근 시세 데이터를 찾지 못했습니다. 조회 범위: {tried}')


def normalize_yahoo_df(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [str(col[0]) for col in df.columns]
    return df


def fetch_yahoo_history(item: Dict[str, str]) -> pd.DataFrame:
    end_date = get_reference_today() + timedelta(days=1)
    start_date = get_reference_today() - timedelta(days=YAHOO_LOOKBACK_DAYS)

    try:
        df = yf.download(
            item['code'],
            start=start_date.isoformat(),
            end=end_date.isoformat(),
            interval='1d',
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception as exc:
        raise AppError(f"{item['name']} 조회 중 오류가 발생했습니다: {exc}") from exc

    if df is None or df.empty:
        raise AppError(f"{item['name']} 최근 데이터를 찾지 못했습니다.")

    return normalize_yahoo_df(df.sort_index())


def build_stock_result(item: Dict[str, str], latest_date_str: str, latest_row: Dict[str, Any], previous_row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    current_close = to_int(latest_row.get('종가'))
    previous_close = to_int(previous_row.get('종가')) if previous_row is not None else None
    previous_volume = to_int(previous_row.get('거래량')) if previous_row is not None else None
    open_price = to_int(latest_row.get('시가'))
    high_price = to_int(latest_row.get('고가'))
    low_price = to_int(latest_row.get('저가'))
    current_volume = to_int(latest_row.get('거래량'))
    change_amount = None if current_close is None or previous_close is None else current_close - previous_close
    change_rate = calculate_change_rate(current_close, previous_close)
    metrics = build_analysis_metrics(current_close, previous_close, open_price, high_price, low_price, current_volume, previous_volume)

    return {
        '구분': '종목',
        '이름': item['name'] or item['code'],
        '코드': item['code'],
        '기준일': latest_date_str,
        '현재가': current_close,
        '전일종가': previous_close,
        '전일대비': change_amount,
        '등락률(%)': change_rate,
        '시가': open_price,
        '고가': high_price,
        '저가': low_price,
        '거래량': current_volume,
        '전일거래량': previous_volume,
        '거래대금': to_int(latest_row.get('거래대금')),
        **metrics,
        '조회시각': datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'),
    }


def build_yahoo_result(item: Dict[str, str], latest_date_str: str, latest_row: pd.Series, previous_row: Optional[pd.Series]) -> Dict[str, Any]:
    current_close = to_float(latest_row.get('Close'))
    previous_close = to_float(previous_row.get('Close')) if previous_row is not None else None
    previous_volume = to_int(previous_row.get('Volume')) if previous_row is not None else None
    open_price = to_float(latest_row.get('Open'))
    high_price = to_float(latest_row.get('High'))
    low_price = to_float(latest_row.get('Low'))
    current_volume = to_int(latest_row.get('Volume'))
    change_amount = None if current_close is None or previous_close is None else round(current_close - previous_close, 2)
    change_rate = calculate_change_rate(current_close, previous_close)
    metrics = build_analysis_metrics(current_close, previous_close, open_price, high_price, low_price, current_volume, previous_volume)

    kind = '지수' if item['type'] == 'index' else '상품'
    return {
        '구분': kind,
        '이름': item['name'] or item['code'],
        '코드': item['code'],
        '기준일': latest_date_str,
        '현재가': current_close,
        '전일종가': previous_close,
        '전일대비': change_amount,
        '등락률(%)': change_rate,
        '시가': open_price,
        '고가': high_price,
        '저가': low_price,
        '거래량': current_volume,
        '전일거래량': previous_volume,
        '거래대금': None,
        **metrics,
        '조회시각': datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S'),
    }


def fetch_latest_price(item: Dict[str, str]) -> Dict[str, Any]:
    if item['type'] == 'stock':
        df = fetch_stock_ohlcv(item['code'])
        latest = df.iloc[-1]
        previous = df.iloc[-2] if len(df) >= 2 else None
        latest_date = df.index[-1]
        latest_date_str = latest_date.strftime('%Y-%m-%d') if hasattr(latest_date, 'strftime') else str(latest_date)

        latest_row = {
            '종가': latest.get('종가'),
            '시가': latest.get('시가'),
            '고가': latest.get('고가'),
            '저가': latest.get('저가'),
            '거래량': latest.get('거래량'),
            '거래대금': latest.get('거래대금'),
        }
        previous_row = None
        if previous is not None:
            previous_row = {
                '종가': previous.get('종가'),
                '거래량': previous.get('거래량'),
            }
        return build_stock_result(item, latest_date_str, latest_row, previous_row)

    df = fetch_yahoo_history(item)
    latest = df.iloc[-1]
    previous = df.iloc[-2] if len(df) >= 2 else None
    latest_date = df.index[-1]
    latest_date_str = latest_date.strftime('%Y-%m-%d') if hasattr(latest_date, 'strftime') else str(latest_date)
    return build_yahoo_result(item, latest_date_str, latest, previous)


def extract_net_buy_volume(df: pd.DataFrame, investor_name: str) -> Optional[int]:
    if df is None or df.empty or investor_name not in df.index:
        return None
    row = df.loc[investor_name]
    if isinstance(row, pd.DataFrame):
        row = row.iloc[0]
    for candidate in ('순매수', '순매수량', '순매도수량'):
        value = row.get(candidate)
        if value is not None:
            return to_int(value)
    return None


def try_market_trading_volume(target: str, **kwargs: Any) -> pd.DataFrame:
    return stock.get_market_trading_volume_by_investor(target, target, 'KOSPI', **kwargs)


def fetch_kospi_investor_flow() -> Dict[str, Any]:
    end_date = get_reference_today()
    errors: List[str] = []

    for offset in range(FLOW_LOOKBACK_DAYS + 1):
        target_date = end_date - timedelta(days=offset)
        target = format_date(target_date)
        attempts = [
            {'etf': True, 'etn': True, 'elw': True},
            {'etf': False, 'etn': False, 'elw': False},
            {},
        ]
        for kwargs in attempts:
            try:
                df = try_market_trading_volume(target, **kwargs)
            except Exception as exc:
                errors.append(f"{target_date.isoformat()} {kwargs}: {exc}")
                continue

            result = {
                '구분': '수급',
                '이름': '코스피 현물',
                '코드': 'KOSPI',
                '기준일': target_date.strftime('%Y-%m-%d'),
                '개인 순매수량': extract_net_buy_volume(df, '개인'),
                '외인 순매수량': extract_net_buy_volume(df, '외국인'),
                '기관 순매수량': extract_net_buy_volume(df, '기관합계'),
                '비고': '최근 영업일 기준',
            }
            if any(result[key] is not None for key in ('개인 순매수량', '외인 순매수량', '기관 순매수량')):
                return result
            errors.append(f'{target_date.isoformat()} {kwargs}: 데이터 없음')

    return {
        '구분': '수급',
        '이름': '코스피 현물',
        '코드': 'KOSPI',
        '기준일': end_date.strftime('%Y-%m-%d'),
        '개인 순매수량': None,
        '외인 순매수량': None,
        '기관 순매수량': None,
        '비고': 'pykrx 수급 API 응답 불안정',
    }


def fetch_futures_investor_flow() -> Dict[str, Any]:
    return {
        '구분': '수급',
        '이름': '선물',
        '코드': 'FUTURES',
        '기준일': get_reference_today().strftime('%Y-%m-%d'),
        '개인 순매수량': None,
        '외인 순매수량': None,
        '기관 순매수량': None,
        '비고': '공개 데이터 소스 미확인',
    }


def save_outputs(df: pd.DataFrame, save_csv: bool, save_json: bool) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(KST).strftime('%Y%m%d_%H%M%S')

    if save_csv:
        csv_path = OUTPUT_DIR / f'stock_prices_{timestamp}.csv'
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f'CSV 저장 완료: {csv_path.resolve()}')

    if save_json:
        json_path = OUTPUT_DIR / f'stock_prices_{timestamp}.json'
        records = df.where(pd.notna(df), None).to_dict(orient='records')
        write_json_file(json_path, records)
        print(f'JSON 저장 완료: {json_path.resolve()}')


def print_section(title: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows)
    print(f'[{title}]')
    print(df.to_string(index=False))
    print()


def collect_market_snapshot(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if config is None:
        config = load_config()

    flow_results: List[Dict[str, Any]] = []
    index_results: List[Dict[str, Any]] = []
    commodity_results: List[Dict[str, Any]] = []
    stock_results: List[Dict[str, Any]] = []
    flow_errors: List[str] = []
    index_errors: List[str] = []
    commodity_errors: List[str] = []
    stock_errors: List[str] = []

    try:
        flow_results.append(fetch_kospi_investor_flow())
    except AppError as exc:
        flow_errors.append(str(exc))

    flow_results.append(fetch_futures_investor_flow())

    for item in config['indexes']:
        try:
            index_results.append(fetch_latest_price(item))
        except AppError as exc:
            index_errors.append(str(exc))

    for item in config['commodities']:
        try:
            commodity_results.append(fetch_latest_price(item))
        except AppError as exc:
            commodity_errors.append(str(exc))

    for item in config['stocks']:
        try:
            stock_results.append(fetch_latest_price(item))
        except AppError as exc:
            stock_errors.append(str(exc))

    results = flow_results + index_results + commodity_results + stock_results
    if not results:
        all_errors = flow_errors + index_errors + commodity_errors + stock_errors
        raise AppError('모든 항목 조회에 실패했습니다.\\n' + '\\n'.join(all_errors))


    return {
        'flow_results': flow_results,
        'index_results': index_results,
        'commodity_results': commodity_results,
        'stock_results': stock_results,
        'errors': {
            'flow': flow_errors,
            'index': index_errors,
            'commodity': commodity_errors,
            'stock': stock_errors,
        },
        'dataframe': pd.DataFrame(results),
    }

def main() -> int:
    try:
        ensure_supported_python()
        current_python = '.'.join(str(part) for part in sys.version_info[:3])
        tested_python = '.'.join(str(part) for part in TESTED_PYTHON)
        print(f'Python 런타임: {current_python} (권장 확인 버전 계열: {tested_python}.x)')

        config = load_config()
        snapshot = collect_market_snapshot(config)
        flow_results = snapshot['flow_results']
        index_results = snapshot['index_results']
        commodity_results = snapshot['commodity_results']
        stock_results = snapshot['stock_results']
        result_df = snapshot['dataframe']
        flow_errors = snapshot['errors']['flow']
        index_errors = snapshot['errors']['index']
        commodity_errors = snapshot['errors']['commodity']
        stock_errors = snapshot['errors']['stock']

        print_section('수급', flow_results)
        print_section('지수', index_results)
        print_section('상품', commodity_results)
        print_section('종목', stock_results)

        save_outputs(result_df, save_csv=config['save_csv'], save_json=config['save_json'])

        if flow_errors:
            print('[수급 조회 실패]')
            for message in flow_errors:
                print(f'- {message}')
            print()

        if index_errors:
            print('[지수 조회 실패]')
            for message in index_errors:
                print(f'- {message}')
            print()

        if commodity_errors:
            print('[상품 조회 실패]')
            for message in commodity_errors:
                print(f'- {message}')
            print()

        if stock_errors:
            print('[종목 조회 실패]')
            for message in stock_errors:
                print(f'- {message}')

        return 0
    except AppError as exc:
        print(f'오류: {exc}', file=sys.stderr)
        return 1
    except Exception as exc:
        print(f'예상치 못한 오류: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())






