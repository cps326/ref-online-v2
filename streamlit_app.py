from bs4 import BeautifulSoup
import pandas as pd
import re
import requests
from openai import OpenAI
import os
from collections import OrderedDict
import json
import streamlit as st
import io
import openai
import time
from urllib.parse import urljoin, urlparse

# =========================
# OpenAI API Key (Cloud 중심)
# =========================
api_key = st.secrets.get("OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY")
if not api_key:
    st.error("OPENAI_API_KEY가 설정되지 않았습니다. Streamlit Cloud > Secrets에 추가해주세요.")
    st.stop()

client = OpenAI(api_key=api_key)
openai.api_key = api_key

st.set_page_config(layout="wide", page_title="KEI 참고문헌 온라인자료 검증도구")

# =========================
# 최종 출력 컬럼 순서(요청 반영)
# =========================
FINAL_COL_ORDER = [
    "최종_URL_상태",
    "최종_URL_메모",
    "URL_수정안",
    "작성기관_작성자",
    "제목",
    "URL_보고서기준",
    "search_date",
    "원문",
    "참고문헌_작성양식_체크(규칙기반)",
    "참고문헌_작성양식_체크(GPT기반)",
    "URL_내용일치여부(GPT)",
    "URL_상태",
    "URL_메모",
    "URL_상태코드",
    "URL_수동검증_결과",
    "수동검증_메모",
]


def reorder_columns(df: pd.DataFrame, order: list[str]) -> pd.DataFrame:
    front = [c for c in order if c in df.columns]
    tail = [c for c in df.columns if c not in front]
    return df[front + tail]


def ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return df
    for c in FINAL_COL_ORDER:
        if c not in df.columns:
            df[c] = ""
    return df


# =========================
# URL 상태 체크 (정상/오류/확인불가/정상(보안주의) + 메모)
# =========================
def check_url_status(url: str, timeout: int = 15) -> dict:
    if not isinstance(url, str) or not url.strip():
        return {"URL_상태": "오류", "URL_상태코드": "", "URL_최종URL": "", "URL_메모": "URL 없음"}

    url = url.strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        return {"URL_상태": "오류", "URL_상태코드": "", "URL_최종URL": "", "URL_메모": "http/https로 시작하지 않음"}

    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        status_code = r.status_code
        final_url = r.url

        if 200 <= status_code < 300:
            return {"URL_상태": "정상", "URL_상태코드": status_code, "URL_최종URL": final_url, "URL_메모": ""}
        return {"URL_상태": "오류", "URL_상태코드": status_code, "URL_최종URL": final_url, "URL_메모": f"HTTP {status_code}"}

    except requests.exceptions.SSLError:
        # SSL 검증 실패지만 verify=False로 1회 재시도
        try:
            r2 = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True, verify=False)
            status_code = r2.status_code
            final_url = r2.url

            if 200 <= status_code < 300:
                memo = "SSL 검증 실패(보안주의): verify=False로는 접속됨"
                return {"URL_상태": "정상(보안주의)", "URL_상태코드": status_code, "URL_최종URL": final_url, "URL_메모": memo}

            memo = f"SSL 검증 실패 + HTTP {status_code}(verify=False)"
            return {"URL_상태": "오류", "URL_상태코드": status_code, "URL_최종URL": final_url, "URL_메모": memo}

        except Exception as e2:
            msg = f"{type(e2).__name__}: {str(e2)[:120]}"
            return {
                "URL_상태": "확인불가",
                "URL_상태코드": "",
                "URL_최종URL": "",
                "URL_메모": f"SSL 핸드셰이크 실패(verify=False도 실패) - {msg}",
            }

    except requests.exceptions.Timeout:
        return {"URL_상태": "확인불가", "URL_상태코드": "", "URL_최종URL": "", "URL_메모": "Timeout"}
    except requests.exceptions.ConnectionError:
        return {"URL_상태": "확인불가", "URL_상태코드": "", "URL_최종URL": "", "URL_메모": "Connection error"}
    except requests.exceptions.InvalidURL:
        return {"URL_상태": "오류", "URL_상태코드": "", "URL_최종URL": "", "URL_메모": "Invalid URL"}
    except requests.exceptions.MissingSchema:
        return {"URL_상태": "오류", "URL_상태코드": "", "URL_최종URL": "", "URL_메모": "URL 스키마 누락(http/https)"}
    except Exception as e:
        return {"URL_상태": "확인불가", "URL_상태코드": "", "URL_최종URL": "", "URL_메모": f"예외: {type(e).__name__}"}


# =========================
# crawling: URL에서 페이지 텍스트 가져오기
# =========================
def crawling(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }
    doc_exts = [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt", ".csv", ".rtf"]

    if not isinstance(url, str) or not url.strip():
        return "확인불가"

    if any(ext in url for ext in doc_exts):
        try:
            response = requests.head(url, allow_redirects=True, timeout=5)
            return "파일다운가능" if response.status_code == 200 else "파일다운불가"
        except requests.exceptions.RequestException:
            return "파일다운불가"

    try:
        response = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        response_text = response.text

        if "You need to enable JavaScript to run this app" in response_text:
            soup2 = BeautifulSoup(response_text, "html.parser")
            text = soup2.get_text(separator=" ", strip=True)
            if len(text) < 200:
                return "확인불가"

        match = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", response_text)
        if match:
            redirect_url = match.group(1)
            if "javascript:" not in redirect_url.lower():
                redirect_url = urljoin(url, redirect_url)
                response2 = requests.get(redirect_url, headers=headers, timeout=30, allow_redirects=True)
                response_text = response_text + response2.text

        if response.status_code != 200:
            return "확인불가"

        soup = BeautifulSoup(response_text, "html.parser")
        content = soup.get_text(strip=True)

        iframes = soup.find_all("iframe")
        iframe_contents = []
        for iframe in iframes:
            iframe_src = iframe.get("src")
            if not iframe_src or not iframe_src.strip():
                continue
            iframe_url = urljoin(url, iframe_src)
            parsed = urlparse(iframe_url)
            if parsed.scheme not in ("http", "https"):
                continue
            try:
                iframe_response = requests.get(iframe_url, headers=headers, timeout=30, allow_redirects=True)
                if iframe_response.status_code == 200:
                    iframe_soup = BeautifulSoup(iframe_response.content, "html.parser")
                    iframe_contents.append(iframe_soup.get_text(strip=True))
            except Exception:
                pass

        if iframe_contents:
            content += "\n\n" + "\n\n".join(iframe_contents)

        return content

    except Exception:
        return "확인불가"


# =========================
# GPT 기반 URL 내용 판별
# =========================
MAX_LEN = 50000


def GPTclass(x, y):
    y = crawling(y)
    if isinstance(y, str) and len(y) > MAX_LEN:
        y = y[:MAX_LEN]

    if y == "확인불가":
        return "확인불가"
    if y == "파일다운가능":
        return "파일다운가능(내용확인불가)"
    if y == "파일다운불가":
        return "파일다운불가"
    if isinstance(x, str) and "확인필요" in x:
        return "O(형식오류)"

    retries = 0
    while retries < 5:
        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "[[웹자료]]에서 내용이 주어진 [[정보]] 관련내용이 대략적으로 포함되어있으면 X, 관련내용이 아니거나, 빈페이지 또는 없는 페이지면 O 출력"},
                    {"role": "user", "content": f"[[정보]]: {x}, [[웹자료]] : {y}"},
                ],
            )
            return response.choices[0].message.content
        except openai.RateLimitError as e:
            time.sleep(getattr(e, "retry_after", 2) + 2)
            retries += 1
        except Exception:
            return "확인불가"


def map_gpt_url_result(v):
    if v is None or not isinstance(v, str):
        return "확인불가"
    s = v.strip()

    if s == "확인불가":
        return "확인불가"
    if "파일다운가능" in s:
        return "파일(내용확인불가)"
    if "파일다운불가" in s:
        return "확인불가"

    if s == "X" or s.startswith("X"):
        return "일치(유효)"
    if s == "O" or s.startswith("O"):
        return "불일치(오류)"

    return s


# =========================
# 참고문헌 분리 + 규칙 기반 형식 체크
# =========================
def separator(entry):
    parts = [""] * 4
    if "http" in entry:
        pattern_http = r",\s+(?=http)"
    else:
        pattern_http = r",\s+(?=검색일)"

    parts_http = re.split(pattern_http, entry)
    doc_info = parts_http[0]
    ref_info = parts_http[1] if len(parts_http) > 1 else ""

    if "“" in doc_info and "”" in doc_info:
        match = re.match(r"(.+?),\s*?“(.*)”", doc_info)
        if match:
            parts[0] = match.group(1).strip()
            parts[1] = f"“{match.group(2)}”"
        else:
            parts[0] = doc_info.strip()
            parts[1] = ""
    else:
        parts[0] = doc_info.strip()
        parts[1] = ""

    if "http" in ref_info:
        pattern_ref = r",\s+(?=검색일)"
        parts_ref = re.split(pattern_ref, ref_info)
        parts[2] = parts_ref[0].strip()
        parts[3] = parts_ref[1].strip() if len(parts_ref) > 1 else ""
    else:
        parts[3] = ref_info.strip()

    return parts


def check_format(text):
    # 제목(" ") 존재 여부만 간단 체크(규칙은 점진 강화 추천)
    title_match = re.search(r'"[^"]*"', text)
    return bool(title_match)


# =========================
# GPT 형식 검증
# =========================
def GPTcheck(doc):
    query = """
    당신은 각 줄마다 아래 형식에 맞는 문헌 정보가 정확히 입력되었는지 검토합니다.
    1. 출처
    2. 제목: 반드시 큰따옴표(" ")로 감쌈
    3. URL
    4. 검색일: "검색일: yyyy.m.d." 형식
    출력: JSON {"오류여부":"X"} 또는 {"오류여부":"O(이유)"}
    """
    retries = 0
    while retries < 5:
        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": query},
                    {"role": "user", "content": f"문서:{doc}"},
                ],
            )
            raw = response.choices[0].message.content
            result_dict = json.loads(raw)
            err = result_dict.get("오류여부") or "O(오류여부 누락)"
            return {"오류여부": err, "원문": doc}
        except openai.RateLimitError as e:
            time.sleep(getattr(e, "retry_after", 2) + 2)
            retries += 1
        except Exception as e:
            return {"오류여부": f"O(GPTcheck 실패:{type(e).__name__})", "원문": doc}


# =========================
# entries -> DataFrame
# =========================
def process_entries(entries):
    articles = []
    for entry in entries:
        rule_note = "" if check_format(entry) else "확인필요"

        s = separator(entry)
        s = ["확인필요" if item in ("NA", "", None) else item for item in s]

        작성기관_작성자 = s[0]
        제목 = s[1]
        URL_보고서기준 = s[2]

        search_date = s[3].replace("검색일: ", "").strip()
        if not re.search(r"\b\d{4}\.([1-9]|1[0-2])\.([1-9]|[12][0-9]|3[01])\b", search_date):
            search_date = "확인필요"

        url_result = check_url_status(URL_보고서기준)

        articles.append({
            "URL_상태": url_result["URL_상태"],
            "URL_메모": url_result["URL_메모"],
            "URL_상태코드": url_result["URL_상태코드"],
            "URL_수정안": url_result["URL_최종URL"],

            "작성기관_작성자": 작성기관_작성자,
            "제목": 제목,
            "URL_보고서기준": URL_보고서기준,
            "search_date": search_date,

            "원문": entry,
            "참고문헌_작성양식_체크(규칙기반)": rule_note,
        })

    df = pd.DataFrame(articles)
    return df


# =========================
# 화면/엑셀 색칠 기준(최종_URL_상태)
# =========================
def highlight_url_status(val):
    if val == "오류":
        return "background-color: #f8d7da"
    if val == "확인불가":
        return "background-color: #fff3cd"
    if val == "정상(보안주의)":
        return "background-color: #ffe5b4"
    return ""


def write_excel_with_conditional_format(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
        workbook = writer.book
        worksheet = writer.sheets["Sheet1"]

        if "최종_URL_상태" in df.columns:
            status_col = df.columns.get_loc("최종_URL_상태")

            fmt_red = workbook.add_format({"bg_color": "#F8D7DA"})
            fmt_yel = workbook.add_format({"bg_color": "#FFF3CD"})
            fmt_org = workbook.add_format({"bg_color": "#FFE5B4"})

            start_row = 1
            end_row = len(df)

            worksheet.conditional_format(start_row, status_col, end_row, status_col, {
                "type": "text", "criteria": "containing", "value": "오류", "format": fmt_red
            })
            worksheet.conditional_format(start_row, status_col, end_row, status_col, {
                "type": "text", "criteria": "containing", "value": "확인불가", "format": fmt_yel
            })
            worksheet.conditional_format(start_row, status_col, end_row, status_col, {
                "type": "text", "criteria": "containing", "value": "정상(보안주의)", "format": fmt_org
            })

    output.seek(0)
    return output.read()


# =========================
# Streamlit UI
# =========================
def main():
    st.title("KEI 참고문헌 온라인자료 검증도구")

    # 세션 상태 초기화
    if "processed_data" not in st.session_state:
        st.session_state["processed_data"] = None
    if "result_df" not in st.session_state:
        st.session_state["result_df"] = None

    # ✅ 옵션(선택형 실행)
    st.subheader("✅ 실행 옵션(선택)")
    col_opt1, col_opt2 = st.columns([1, 1])
    with col_opt1:
        do_gpt_format = st.checkbox("GPT로 참고문헌 작성양식 검토하기", value=True)
    with col_opt2:
        do_gpt_urlmatch = st.checkbox("GPT로 URL 내용일치(유효정보) 검토하기", value=True)

    st.caption("기본 실행은 항상: 규칙기반 작성양식 체크 + URL 상태(접속/코드/SSL) 확인")

    uploaded_file = st.file_uploader(
        "보고서 참고문헌 중 온라인자료에 해당하는 텍스트 파일(txt)를 업로드 하거나 ",
        type=["txt"],
    )
    text_data = st.text_area(
        "또는 아래에 온라인자료에 해당하는 텍스트를 입력하세요",
        "",
        height=300,
    )

    col_run, col_reset = st.columns([1, 1])
    with col_run:
        run_clicked = st.button("👉여기를 눌러, 검증을 실행해 주세요.")
    with col_reset:
        reset_clicked = st.button("🔃(검증 후)수동 입력/결과 초기화 버튼")

    if reset_clicked:
        st.session_state["processed_data"] = None
        st.session_state["result_df"] = None
        st.success("초기화 완료! 다시 실행하세요.")
        st.stop()

    # ✅ Expander 헤더 배경색(수동 확인 영역만)
    st.markdown(
        """
        <style>
        div.manual-expander-marker + div[data-testid="stExpander"] details summary {
            background: #e8f0fe !important;
            border: 1px solid #8ab4f8 !important;
            border-radius: 12px !important;
            padding: 12px 14px !important;
            font-weight: 800 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if run_clicked:
        progress_bar = st.progress(0)
        status_text = st.empty()

        if not (uploaded_file or text_data.strip()):
            st.warning("텍스트 파일 업로드 또는 텍스트 입력이 필요합니다.")
            st.stop()

        progress_bar.progress(5)
        status_text.text("1단계: 입력 데이터 로딩 중...")

        data = uploaded_file.read().decode("utf-8") if uploaded_file else text_data
        entries = data.strip().splitlines()

        progress_bar.progress(15)
        status_text.text("2단계: 규칙기반 작성양식 + URL 상태 체크 중...")

        result_df = process_entries(entries)

        # 기본 컬럼 세팅(옵션 off여도 컬럼 유지)
        result_df["참고문헌_작성양식_체크(GPT기반)"] = ""
        result_df["URL_내용일치여부(GPT)"] = ""

        # ===== GPT 형식검증(선택)
        if do_gpt_format:
            status_text.text("3단계: GPT 작성양식 검증 수행 중...")
            gpt_list = []
            n3 = len(entries)
            for idx, doc in enumerate(entries):
                gpt_list.append(GPTcheck(doc))
                progress = 15 + int(25 * (idx + 1) / max(n3, 1))  # 15~40
                progress_bar.progress(progress)

            result_df["참고문헌_작성양식_체크(GPT기반)"] = [
                r.get("오류여부", "O(오류여부 없음)") if isinstance(r, dict) else "O(GPTcheck None)"
                for r in gpt_list
            ]
        else:
            progress_bar.progress(40)

        # ===== GPT URL 내용일치(선택)
        if do_gpt_urlmatch:
            status_text.text("4단계: GPT URL 내용일치 검증 수행 중...")
            n4 = len(result_df)
            url_results = []
            for i, (title_source, url) in enumerate(
                zip(
                    result_df["제목"].astype(str) + " + " + result_df["작성기관_작성자"].astype(str),
                    result_df["URL_보고서기준"].astype(str),
                )
            ):
                url_results.append(GPTclass(title_source, url))
                progress = 40 + int(45 * (i + 1) / max(n4, 1))  # 40~85
                progress_bar.progress(progress)

            result_df["URL_내용일치여부(GPT)"] = [map_gpt_url_result(x) for x in url_results]
        else:
            progress_bar.progress(85)

        # ===== 수동/최종 컬럼 준비
        result_df["URL_수동검증_결과"] = ""
        result_df["수동검증_메모"] = ""
        result_df["최종_URL_상태"] = result_df["URL_상태"]
        result_df["최종_URL_메모"] = result_df["URL_메모"]

        result_df = ensure_required_columns(result_df)
        result_df = reorder_columns(result_df, FINAL_COL_ORDER)
        st.session_state["result_df"] = result_df

        progress_bar.progress(100)
        status_text.text("✅ 완료되었습니다! 아래에서 수동 확인 후 다운로드하세요.")

    # =========================
    # 결과 표시(세션 기반)
    # =========================
    if st.session_state["result_df"] is not None:
        result_df = ensure_required_columns(st.session_state["result_df"])
        result_df = reorder_columns(result_df, FINAL_COL_ORDER)

        # 마커(다음 expander 스타일 적용용)
        st.markdown('<div class="manual-expander-marker"></div>', unsafe_allow_html=True)

        with st.expander(
            "🔎 담당자의 수동 확인(오류/확인불가)이 필요합니다. 여기를 눌러주세요! 아래 표가 활성화되면, URL(클릭)에 접속하여 최종 판정 결과를 입력해주세요.🤗",
            expanded=False,
        ):
            issue_mask = result_df["URL_상태"].isin(["오류", "확인불가"])
            issues_cols = [
                "URL_상태", "URL_메모", "URL_보고서기준",
                "작성기관_작성자", "제목",
                "URL_수동검증_결과", "수동검증_메모"
            ]
            issues_df = result_df.loc[issue_mask, [c for c in issues_cols if c in result_df.columns]].copy()

            if len(issues_df) == 0:
                st.info("수동 확인이 필요한(오류/확인불가) 항목이 없습니다.")
            else:
                edited = st.data_editor(
                    issues_df,
                    use_container_width=True,
                    column_config={
                        "URL_보고서기준": st.column_config.LinkColumn("URL(클릭)", display_text="열기"),
                        "URL_수동검증_결과": st.column_config.SelectboxColumn(
                            "URL_수동검증_결과(선택)",
                            options=["", "정상", "정상(보안주의)", "오류", "확인불가"],
                        ),
                        "수동검증_메모": st.column_config.TextColumn("수동검증_메모"),
                    },
                    disabled=[c for c in ["URL_상태", "URL_메모", "작성기관_작성자", "제목"] if c in issues_df.columns],
                    key="manual_editor",
                )

                if st.button("✅ 수동 판정 적용"):
                    result_df.loc[edited.index, "URL_수동검증_결과"] = edited.get("URL_수동검증_결과", "")
                    result_df.loc[edited.index, "수동검증_메모"] = edited.get("수동검증_메모", "")

                    has_manual = result_df["URL_수동검증_결과"].astype(str).str.strip().ne("")
                    result_df.loc[has_manual, "최종_URL_상태"] = result_df.loc[has_manual, "URL_수동검증_결과"]

                    has_manual_memo = result_df["수동검증_메모"].astype(str).str.strip().ne("")
                    result_df.loc[has_manual_memo, "최종_URL_메모"] = result_df.loc[has_manual_memo, "수동검증_메모"]

                    result_df = reorder_columns(result_df, FINAL_COL_ORDER)
                    st.session_state["result_df"] = result_df
                    st.success("수동 판정을 최종 값에 반영했습니다.")

        styled = result_df.style.applymap(highlight_url_status, subset=["최종_URL_상태"])
        st.dataframe(styled, use_container_width=True)

        excel_bytes = write_excel_with_conditional_format(result_df)
        st.session_state["processed_data"] = excel_bytes

        st.download_button(
            label="엑셀로 다운로드",
            data=st.session_state["processed_data"],
            file_name="result.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


if __name__ == "__main__":
    main()
