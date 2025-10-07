#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
occ_memo_daily_gcal.py
- OCC Information Memos 수집
- FLEX 관련 메모 제외
- (옵션) 조사일(KST) 기준 이미 지난 Effective Date 제외
- CSV/XLSX 저장
- (옵션) Google Calendar에 Effective Date 날짜로 09:30~10:30 일정 생성
사용 예:
  # 수집만
  python occ_memo_daily_gcal.py --out out --exclude-past-effective --since-posted-days 3
  # 수집 후 즉시 캘린더 등록 (GitHub Actions에서 사용)
  GCAL_SERVICE_JSON="...json..." GCAL_CALENDAR_ID="primary" \
  python occ_memo_daily_gcal.py --out out --exclude-past-effective --since-posted-days 3 --insert-calendar-now
"""
import argparse, csv, datetime as dt, io, os, re, sys
from dataclasses import dataclass
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd
from pdfminer.high_level import extract_text as pdf_extract_text
from dateutil import parser as dateparser

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

try:
    from zoneinfo import ZoneInfo
except Exception:
    from backports.zoneinfo import ZoneInfo  # type: ignore

BASE = "https://infomemo.theocc.com"
SEARCH_URL = f"{BASE}/infomemo/search"

EVENT_KEYWORDS = {
    "reverse split": ["reverse split"],
    "split": ["stock split", "split "],
    "name/symbol change": ["name/symbol change", "symbol change", "name change"],
    "merger": ["merger", "combination", "acquisition"],
    "tender": ["tender offer"],
    "liquidation": ["liquidation"],
}

RE_MEMO_NUMBER = re.compile(r"/infomemos\?number=(\d+)")
RE_EFFECTIVE_DATE_LINE = re.compile(r"Effective Date:\s*([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})", re.I)
RE_DATE_FIELD = re.compile(r"^Date:\s*(\d{1,2}/\d{1,2}/\d{4})\s*$", re.M)
RE_MARKET_OPEN_LINE = re.compile(r"effective (?:at|before) the (?:open|opening) (?:of (?:the )?business )?(?:on )?([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})", re.I)
RE_SUBJECT = re.compile(r"^Subject:\s*(.+)$", re.M | re.I)
RE_OPT_SYM = re.compile(r"^Option Symbol[s]?:\s*(.+)$", re.M | re.I)
RE_NEW_SYM = re.compile(r"^New Symbol[s]?:\s*(.+)$", re.M | re.I)
RE_TITLED_NEW_SYM = re.compile(r"New Symbols?:\s*([A-Za-z0-9/]+)", re.I)
RE_ADJUSTED_SYM = re.compile(r"Adjusted Option Symbol[s]?:\s*([A-Za-z0-9/]+)", re.I)

def _to_iso(date_str: str) -> Optional[str]:
    try:
        d = dateparser.parse(date_str, fuzzy=True)
        return d.strftime("%Y-%m-%d")
    except Exception:
        return None

@dataclass
class MemoRow:
    memo_number: int
    title: str
    url: str
    post_date: Optional[str] = None
    effective_date: Optional[str] = None
    event_type: Optional[str] = None
    option_symbols: Optional[str] = None
    new_symbols: Optional[str] = None
    subject: Optional[str] = None
    details: Optional[str] = None

def fetch_search_html(session: requests.Session) -> str:
    r = session.get(SEARCH_URL, timeout=30)
    r.raise_for_status()
    return r.text

def parse_search_listing(html: str) -> List[MemoRow]:
    soup = BeautifulSoup(html, "html.parser")
    rows: List[MemoRow] = []
    for a in soup.find_all("a", href=True):
        m = RE_MEMO_NUMBER.search(a["href"])
        if not m:
            continue
        num = int(m.group(1))
        title = a.get_text(strip=True)
        url = BASE + a["href"] if a["href"].startswith("/infomemos") else a["href"]

        post_date = None
        effective_date = None
        try:
            parent = a.parent
            texts_back = []
            sib = parent.previous_sibling
            cnt = 0
            while sib and cnt < 12:
                if hasattr(sib, "get_text"):
                    texts_back.append(sib.get_text(" ", strip=True))
                elif isinstance(sib, str):
                    texts_back.append(sib.strip())
                sib = sib.previous_sibling
                cnt += 1
            candidates = [t for t in texts_back if t]
            candidates = list(reversed(candidates))
            dates = [t for t in candidates if re.match(r"\d{2}/\d{2}/\d{4}", t)]
            if len(dates) >= 1:
                post_date = _to_iso(dates[0])
            if len(dates) >= 2:
                effective_date = _to_iso(dates[1])
        except Exception:
            pass
        rows.append(MemoRow(memo_number=num, title=title, url=url,
                            post_date=post_date, effective_date=effective_date))
    uniq = {}
    for r in rows:
        if r.memo_number not in uniq:
            uniq[r.memo_number] = r
    return sorted(uniq.values(), key=lambda x: x.memo_number, reverse=True)

def fetch_pdf_text(session: requests.Session, url: str) -> str:
    r = session.get(url, timeout=60)
    r.raise_for_status()
    if "application/pdf" in r.headers.get("Content-Type", "") or url.lower().endswith(".pdf") or "infomemos?number=" in url:
        data = io.BytesIO(r.content)
        return pdf_extract_text(data)
    return r.text

def classify_event(title: str, subject: Optional[str]) -> Optional[str]:
    text = (title + " " + (subject or "")).lower()
    for label, keys in EVENT_KEYWORDS.items():
        for k in keys:
            if k in text:
                return label
    if "reverse" in text and "split" in text:
        return "reverse split"
    if "stock split" in text or re.search(r"\b\d+\s*for\s*\d+\b", text):
        return "split"
    return None

def parse_pdf_fields(text: str) -> Dict[str, Optional[str]]:
    subject = None
    opt_syms = None
    new_syms = None
    eff_iso = None

    m = RE_SUBJECT.search(text)
    if m: subject = m.group(1).strip()
    m = RE_OPT_SYM.search(text)
    if m: opt_syms = m.group(1).strip()
    m = RE_NEW_SYM.search(text)
    if m: new_syms = m.group(1).strip()
    else:
        m = RE_TITLED_NEW_SYM.search(text)
        if m: new_syms = m.group(1).strip()
        else:
            m = RE_ADJUSTED_SYM.search(text)
            if m: new_syms = m.group(1).strip()

    m = RE_EFFECTIVE_DATE_LINE.search(text)
    if m:
        eff_iso = _to_iso(m.group(1))
    else:
        m = RE_DATE_FIELD.search(text)
        if m:
            eff_iso = _to_iso(m.group(1))
        else:
            m = RE_MARKET_OPEN_LINE.search(text)
            if m:
                eff_iso = _to_iso(m.group(1))
    return dict(subject=subject, option_symbols=opt_syms, new_symbols=new_syms, effective_date=eff_iso)

def exclude_flex_rows(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("title","subject"):
        if col in df.columns:
            df = df[df[col].fillna("").str.contains(r"\bflex\b", flags=re.I, regex=True) == False]
    return df

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="./out")
    p.add_argument("--state", default="./occ_last_number.txt")
    p.add_argument("--since-posted-days", type=int, default=3)
    p.add_argument("--exclude-past-effective", action="store_true")
    p.add_argument("--insert-calendar-now", action="store_true", help="Insert events immediately using GCAL_* envs")
    args = p.parse_args()

    os.makedirs(args.out, exist_ok=True)

    session = requests.Session()
    html = fetch_search_html(session)
    listing = parse_search_listing(html)

    today = dt.datetime.now(ZoneInfo("Asia/Seoul")).date()
    picked: List[MemoRow] = []

    if args.since_posted_days is not None:
        cutoff = today - dt.timedelta(days=args.since_posted_days)
        for r in listing:
            if r.post_date:
                try:
                    pd_ = dt.date.fromisoformat(r.post_date)
                except Exception:
                    continue
                if pd_ >= cutoff:
                    picked.append(r)
    else:
        try:
            last_n = 0
            if os.path.exists(args.state):
                with open(args.state, "r", encoding="utf-8") as f:
                    last_n = int(f.read().strip())
        except Exception:
            last_n = 0
        for r in listing:
            if r.memo_number > last_n:
                picked.append(r)

    if not picked:
        print("No new memos found on search page.")
        return

    results: List[MemoRow] = []
    for r in picked:
        try:
            text = fetch_pdf_text(session, r.url)
            fields = parse_pdf_fields(text)
            r.subject = fields.get("subject")
            r.option_symbols = fields.get("option_symbols") or r.option_symbols
            r.new_symbols = fields.get("new_symbols") or r.new_symbols
            r.effective_date = fields.get("effective_date") or r.effective_date
            r.event_type = classify_event(r.title, r.subject)
            results.append(r)
        except Exception as e:
            r.details = f"parse_error: {e}"
            results.append(r)

    df = pd.DataFrame([{
        "memo_number": r.memo_number,
        "post_date": r.post_date,
        "effective_date": r.effective_date,
        "event_type": r.event_type,
        "title": r.title,
        "subject": r.subject,
        "option_symbols": r.option_symbols,
        "new_symbols": r.new_symbols,
        "url": r.url,
    } for r in results])

    if not df.empty:
        df = exclude_flex_rows(df)

    if args.exclude_past_effective and not df.empty:
        def iso_to_date(x):
            try:
                return dt.date.fromisoformat(x)
            except Exception:
                return None
        df["eff_d"] = df["effective_date"].apply(lambda x: iso_to_date(x) if isinstance(x, str) else None)
        df = df[df["eff_d"].isna() | (df["eff_d"] >= today)]
        df = df.drop(columns=["eff_d"])

    if not df.empty:
        df = df.sort_values(by=["memo_number"], ascending=False)

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    xlsx_path = os.path.join(args.out, f"OCC_memos_{ts}.xlsx")
    csv_path  = os.path.join(args.out, f"OCC_memos_{ts}.csv")
    df.to_excel(xlsx_path, index=False)
    df.to_csv(csv_path, index=False, quoting=csv.QUOTE_MINIMAL)

    latest_csv = os.path.join(args.out, "latest.csv")
    df.to_csv(latest_csv, index=False, quoting=csv.QUOTE_MINIMAL)

    # state 업데이트 (검색 페이지 상 최대 번호)
    max_num = max([r.memo_number for r in listing]) if listing else 0
    try:
        with open(args.state, "w", encoding="utf-8") as f:
            f.write(str(max_num))
    except Exception as e:
        print(f"[warn] cannot write state file: {e}", file=sys.stderr)

    # 즉시 캘린더 등록 옵션
    if args.insert_calendar_now and not df.empty:
        cal_json = os.environ.get("GCAL_SERVICE_JSON")
        cal_id = os.environ.get("GCAL_CALENDAR_ID", "primary")
        if cal_json:
            import json
            data = json.loads(cal_json)
            creds = Credentials.from_service_account_info(
                data, scopes=["https://www.googleapis.com/auth/calendar"]
            )
            svc = build("calendar", "v3", credentials=creds)
            created = 0
            for _, row in df.iterrows():
                eff = row.get("effective_date")
                if not eff: 
                    continue
                y,m,d = map(int, eff.split("-"))
                start = dt.datetime(y,m,d, 9,30, tzinfo=ZoneInfo("Asia/Seoul"))
                end   = dt.datetime(y,m,d,10,30, tzinfo=ZoneInfo("Asia/Seoul"))
                sym = row.get("option_symbols") or ""
                new = row.get("new_symbols")
                evt = (row.get("event_type") or "").title()
                title = f"[OCC] {sym}" + (f" → {new}" if new else "") + (f" ({evt})" if evt else "")
                desc = []
                if "memo_number" in row: desc.append(f"OCC Memo #{int(row['memo_number'])}")
                if "post_date" in row and pd.notna(row["post_date"]): desc.append(f"게시일: {row['post_date']}")
                if "url" in row and pd.notna(row["url"]): desc.append(f"링크: {row['url']}")
                if evt: desc.append(f"변경내용: {evt}")
                body = "\n".join(desc)
                event = {
                    "summary": title,
                    "description": body,
                    "start": {"dateTime": start.isoformat()},
                    "end":   {"dateTime": end.isoformat()},
                    "timeZone": "Asia/Seoul",
                }
                svc.events().insert(calendarId=cal_id, body=event).execute()
                created += 1
            print(f"Created {created} calendar events.")
        else:
            print("GCAL_SERVICE_JSON not set; skip calendar insert.")
    else:
        print("Calendar insertion skipped (flag off or empty result).")

if __name__ == "__main__":
    main()
