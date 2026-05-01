#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WebCrawler - 同一ドメイン変更検知ツール
使い方: python crawler.py
"""

import json
import os
import re
import hashlib
import difflib
import time
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from jinja2 import Template

# ── ログ設定 ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("crawler.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ── 設定読み込み ──────────────────────────────────────────
def load_config(path="config.json"):
    with open(path, encoding="utf-8") as f:
        return json.load(f)

# ── URLの正規化 ───────────────────────────────────────────
def normalize_url(url):
    parsed = urlparse(url)
    # フラグメント除去、末尾スラッシュ統一
    return parsed._replace(fragment="").geturl().rstrip("/")

# ── 同一ドメイン判定 ──────────────────────────────────────
def is_same_domain(base_url, target_url):
    return urlparse(base_url).netloc == urlparse(target_url).netloc

# ── 除外パターン判定 ──────────────────────────────────────
def is_excluded(url, patterns):
    for pat in patterns:
        if re.search(pat, url):
            return True
    return False

# ── ページ取得 ────────────────────────────────────────────
def fetch_page(url, timeout=10, headers=None):
    """
    戻り値: (html_text, status_code)
      - 正常: (html文字列, 200)
      - 404 : (None, 404)
      - その他エラー: (None, -1)
    """
    try:
        resp = requests.get(url, timeout=timeout, headers=headers or {})
        status = resp.status_code
        if status == 404:
            log.warning(f"404 Not Found: {url}")
            return None, 404
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding
        return resp.text, status
    except requests.exceptions.HTTPError as e:
        code = e.response.status_code if e.response is not None else -1
        log.warning(f"HTTPエラー({code}): {url} → {e}")
        return None, code
    except Exception as e:
        log.warning(f"取得失敗: {url} → {e}")
        return None, -1

# ── テキスト抽出（ノイズ除去） ────────────────────────────
def extract_text(html):
    soup = BeautifulSoup(html, "html.parser")
    # script/style/nav/footer などノイズ除去
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    # 空行を整理
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    return "\n".join(lines)

# ── リンク収集 ────────────────────────────────────────────
def collect_links(html, base_url, exclude_patterns):
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        full_url = normalize_url(urljoin(base_url, href))
        parsed = urlparse(full_url)
        # http/httpsのみ、同一ドメイン、除外パターン外
        if parsed.scheme not in ("http", "https"):
            continue
        if not is_same_domain(base_url, full_url):
            continue
        if is_excluded(full_url, exclude_patterns):
            continue
        links.add(full_url)
    return links

# ── スナップショット保存・読み込み ────────────────────────
def snapshot_path(url, snapshot_dir):
    url_hash = hashlib.md5(url.encode()).hexdigest()
    return os.path.join(snapshot_dir, f"{url_hash}.txt")

def load_snapshot(url, snapshot_dir):
    path = snapshot_path(url, snapshot_dir)
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return f.read()
    return None

def save_snapshot(url, text, snapshot_dir):
    os.makedirs(snapshot_dir, exist_ok=True)
    path = snapshot_path(url, snapshot_dir)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

# ── 404URLをconfig.jsonの除外リストに自動追記 ────────────
def append_404_to_exclude(url, config_path="config.json"):
    """
    404になったURLを正規表現エスケープして exclude_patterns に追加する。
    同じパターンが既にあれば追記しない。
    """
    try:
        with open(config_path, encoding="utf-8") as f:
            cfg = json.load(f)

        pattern = re.escape(url)   # URLの特殊文字をエスケープ
        if pattern not in cfg.get("exclude_patterns", []):
            cfg.setdefault("exclude_patterns", []).append(pattern)
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False, indent=2)
            log.info(f"除外リストに追加: {url}")
    except Exception as e:
        log.warning(f"除外リスト更新失敗: {e}")


def compute_diff(old_text, new_text, url):
    if old_text is None:
        return {"url": url, "status": "new", "diff_html": "", "old_lines": 0, "new_lines": len(new_text.splitlines())}
    
    old_lines = old_text.splitlines(keepends=True)
    new_lines = new_text.splitlines(keepends=True)
    
    diff = list(difflib.unified_diff(old_lines, new_lines, lineterm=""))
    
    if not diff:
        return {"url": url, "status": "unchanged", "diff_html": "", "old_lines": len(old_lines), "new_lines": len(new_lines)}
    
    # 差分をHTML用に整形
    diff_lines = []
    for line in diff:
        if line.startswith("+++") or line.startswith("---") or line.startswith("@@"):
            diff_lines.append({"type": "info", "text": line.rstrip()})
        elif line.startswith("+"):
            diff_lines.append({"type": "add", "text": line.rstrip()})
        elif line.startswith("-"):
            diff_lines.append({"type": "del", "text": line.rstrip()})
        else:
            diff_lines.append({"type": "ctx", "text": line.rstrip()})
    
    return {
        "url": url,
        "status": "changed",
        "diff_lines": diff_lines,
        "old_lines": len(old_lines),
        "new_lines": len(new_lines),
        "added": sum(1 for d in diff_lines if d["type"] == "add"),
        "deleted": sum(1 for d in diff_lines if d["type"] == "del"),
    }

# ── BFS クロール ──────────────────────────────────────────
def crawl(config, config_path="config.json"):
    start_url = normalize_url(config["start_url"])
    max_depth = config.get("max_depth", 2)
    delay = config.get("delay_seconds", 1)
    exclude = config.get("exclude_patterns", [])
    snapshot_dir = config.get("snapshot_dir", "snapshots")
    headers = config.get("headers", {"User-Agent": "Mozilla/5.0 WebCrawler-Monitor/1.0"})

    visited = set()
    # queue の要素: (url, depth, referrer_url)
    queue = [(start_url, 0, None)]
    results = []

    while queue:
        url, depth, referrer = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        log.info(f"[depth={depth}] クロール中: {url}")
        html, status_code = fetch_page(url, headers=headers)

        if status_code == 404:
            log.warning(f"404検出: {url}  ← リンク元: {referrer or '(起点URL)'}")
            results.append({
                "url": url,
                "status": "error_404",
                "referrer": referrer or start_url,
                "diff_html": "",
            })
            # config.json の除外リストに自動追記
            append_404_to_exclude(url, config_path)
            # 実行中の除外リストにも即時反映
            pattern = re.escape(url)
            if pattern not in exclude:
                exclude.append(pattern)
            continue

        if html is None:
            results.append({
                "url": url,
                "status": "error",
                "referrer": referrer or start_url,
                "diff_html": "",
            })
            continue

        new_text = extract_text(html)
        old_text = load_snapshot(url, snapshot_dir)
        diff_result = compute_diff(old_text, new_text, url)
        diff_result["referrer"] = referrer or start_url
        results.append(diff_result)
        save_snapshot(url, new_text, snapshot_dir)

        # 深さ制限内ならリンクを追加（リファラとして現在URLを渡す）
        if depth < max_depth:
            for link in collect_links(html, start_url, exclude):
                if link not in visited:
                    queue.append((link, depth + 1, url))

        time.sleep(delay)

    return results

# ── HTMLレポート生成 ───────────────────────────────────────
REPORT_TEMPLATE = """
<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>変更レポート {{ timestamp }}</title>
<style>
  :root {
    --bg: #f8fafc; --card: #ffffff; --border: #e2e8f0;
    --add: #dcfce7; --add-border: #16a34a;
    --del: #fee2e2; --del-border: #dc2626;
    --info: #f1f5f9; --info-text: #64748b;
    --changed: #fef9c3; --new: #dbeafe; --error: #fee2e2;
    --text: #1e293b; --muted: #64748b;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: "Segoe UI", sans-serif; padding: 24px; }
  h1 { font-size: 1.5rem; margin-bottom: 4px; }
  .meta { color: var(--muted); font-size: 0.875rem; margin-bottom: 24px; }
  .summary { display: flex; gap: 16px; margin-bottom: 32px; flex-wrap: wrap; }
  .badge { padding: 8px 20px; border-radius: 9999px; font-weight: 600; font-size: 0.875rem; }
  .badge.changed { background: #fef9c3; color: #854d0e; border: 1px solid #fde047; }
  .badge.new     { background: #dbeafe; color: #1e40af; border: 1px solid #93c5fd; }
  .badge.error   { background: #fee2e2; color: #991b1b; border: 1px solid #fca5a5; }
  .badge.ok      { background: #dcfce7; color: #166534; border: 1px solid #86efac; }
  .page-card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; margin-bottom: 20px; overflow: hidden; }
  .page-header { display: flex; align-items: center; gap: 12px; padding: 16px 20px; border-bottom: 1px solid var(--border); }
  .status-dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
  .dot-changed { background: #eab308; }
  .dot-new     { background: #3b82f6; }
  .dot-error   { background: #ef4444; }
  .dot-unchanged { background: #22c55e; }
  .page-url { font-size: 0.9rem; font-weight: 600; word-break: break-all; }
  .page-url a { color: var(--text); text-decoration: none; }
  .page-url a:hover { text-decoration: underline; }
  .page-stats { margin-left: auto; font-size: 0.78rem; color: var(--muted); white-space: nowrap; }
  .diff-block { font-family: "Consolas","Courier New", monospace; font-size: 0.78rem; line-height: 1.6; overflow-x: auto; }
  .diff-line { padding: 1px 16px; white-space: pre; }
  .diff-add  { background: var(--add);  border-left: 3px solid var(--add-border); }
  .diff-del  { background: var(--del);  border-left: 3px solid var(--del-border); }
  .diff-info { background: var(--info); color: var(--info-text); border-left: 3px solid #cbd5e1; }
  .diff-ctx  { color: var(--muted); }
  .no-change { padding: 16px 20px; color: var(--muted); font-size: 0.875rem; }
  .tag { font-size: 0.7rem; padding: 2px 8px; border-radius: 4px; margin-left: 8px; }
  .tag-changed   { background: #fef9c3; color: #854d0e; }
  .tag-new       { background: #dbeafe; color: #1e40af; }
  .tag-error     { background: #fee2e2; color: #991b1b; }
  .tag-unchanged { background: #dcfce7; color: #166534; }
  .dot-error404{ background: #f97316; }
  .tag-error_404 { background: #ffedd5; color: #9a3412; }
  .referrer-row { padding: 10px 20px; font-size: 0.8rem; color: var(--muted); background: #fff7ed; border-top: 1px solid #fed7aa; }
  .referrer-row a { color: #c2410c; }
</style>
</head>
<body>
<h1>🔍 サイト変更レポート</h1>
<p class="meta">生成日時: {{ timestamp }} ／ 起点URL: <a href="{{ start_url }}">{{ start_url }}</a></p>

<div class="summary">
  {% if counts.changed %}<span class="badge changed">📝 変更 {{ counts.changed }}件</span>{% endif %}
  {% if counts.new %}<span class="badge new">🆕 新規 {{ counts.new }}件</span>{% endif %}
  {% if counts.error_404 %}<span class="badge error">🚫 404エラー {{ counts.error_404 }}件</span>{% endif %}
  {% if counts.error %}<span class="badge error">❌ その他エラー {{ counts.error }}件</span>{% endif %}
  <span class="badge ok">✅ 変更なし {{ counts.unchanged }}件</span>
</div>

{% if pages_404 %}
<h2>🚫 404 Not Found（次回から自動除外）</h2>
{% for page in pages_404 %}
<div class="page-card">
  <div class="page-header">
    <div class="status-dot dot-error404"></div>
    <div class="page-url">
      <a href="{{ page.url }}" target="_blank">{{ page.url }}</a>
      <span class="tag tag-error_404">404</span>
    </div>
  </div>
  <div class="referrer-row">
    🔗 リンク元: <a href="{{ page.referrer }}" target="_blank">{{ page.referrer }}</a>
    　※ このURLは config.json の除外リストに自動追加されました
  </div>
</div>
{% endfor %}
{% endif %}

{% if changed_pages %}
<h2>変更・新規ページ</h2>
{% for page in changed_pages %}
<div class="page-card">
  <div class="page-header">
    <div class="status-dot dot-{{ page.status }}"></div>
    <div class="page-url">
      <a href="{{ page.url }}" target="_blank">{{ page.url }}</a>
      <span class="tag tag-{{ page.status }}">
        {% if page.status == 'changed' %}変更{% elif page.status == 'new' %}新規{% elif page.status == 'error' %}エラー{% endif %}
      </span>
    </div>
    {% if page.status == 'changed' %}
    <div class="page-stats">+{{ page.added }} / -{{ page.deleted }} 行</div>
    {% endif %}
  </div>
  {% if page.status == 'changed' %}
  <div class="diff-block">
    {% for line in page.diff_lines %}
    <div class="diff-line diff-{{ line.type }}">{{ line.text }}</div>
    {% endfor %}
  </div>
  {% elif page.status == 'new' %}
  <div class="no-change">新規ページが追加されました（{{ page.new_lines }}行）</div>
  {% else %}
  <div class="no-change">取得エラー</div>
  {% endif %}
</div>
{% endfor %}
{% endif %}

{% if unchanged_pages %}
<h2>変更なし（{{ unchanged_pages|length }}件）</h2>
{% for page in unchanged_pages %}
<div class="page-card">
  <div class="page-header">
    <div class="status-dot dot-unchanged"></div>
    <div class="page-url"><a href="{{ page.url }}" target="_blank">{{ page.url }}</a></div>
    <span class="tag tag-unchanged" style="margin-left:auto">変更なし</span>
  </div>
</div>
{% endfor %}
{% endif %}

</body>
</html>
"""

def generate_report(results, config, report_dir="reports"):
    os.makedirs(report_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(report_dir, f"report_{timestamp}.html")

    changed_pages  = [r for r in results if r["status"] in ("changed", "new", "error")]
    unchanged_pages = [r for r in results if r["status"] == "unchanged"]
    pages_404       = [r for r in results if r["status"] == "error_404"]

    counts = {
        "changed":   sum(1 for r in results if r["status"] == "changed"),
        "new":       sum(1 for r in results if r["status"] == "new"),
        "error":     sum(1 for r in results if r["status"] == "error"),
        "error_404": sum(1 for r in results if r["status"] == "error_404"),
        "unchanged": sum(1 for r in results if r["status"] == "unchanged"),
    }

    html = Template(REPORT_TEMPLATE).render(
        timestamp=datetime.now().strftime("%Y年%m月%d日 %H:%M:%S"),
        start_url=config["start_url"],
        changed_pages=changed_pages,
        unchanged_pages=unchanged_pages,
        pages_404=pages_404,
        counts=counts,
    )

    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)

    log.info(f"レポート生成完了: {filename}")
    return filename

# ── メール送信 ────────────────────────────────────────────
def send_mail(config, results, report_url):
    """
    定時実行通知メールを送信する。
    変更があった場合はレポートのURLをメール本文に記載する。
    Gmail の認証情報は環境変数 GMAIL_USER / GMAIL_APP_PASSWORD から取得。
    """
    mail_cfg = config.get("mail", {})
    to_addresses = mail_cfg.get("to", [])
    if not to_addresses:
        log.info("mail.to が未設定のためメール送信をスキップ")
        return

    gmail_user = os.environ.get("GMAIL_USER", "")
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not gmail_user or not gmail_pass:
        log.warning("GMAIL_USER / GMAIL_APP_PASSWORD が未設定のためメール送信をスキップ")
        return

    # 集計
    changed  = sum(1 for r in results if r["status"] == "changed")
    new_page = sum(1 for r in results if r["status"] == "new")
    err_404  = sum(1 for r in results if r["status"] == "error_404")
    total    = len(results)
    now_str  = datetime.now().strftime("%Y年%m月%d日 %H:%M")

    has_change = (changed + new_page + err_404) > 0

    # 件名
    if has_change:
        subject = f"【WebCrawler】変更検知あり ({changed}件変更 / {new_page}件新規 / {err_404}件404) {now_str}"
    else:
        subject = f"【WebCrawler】定時巡回完了・変更なし {now_str}"

    # 本文（HTML）
    report_link = (
        f'<p>📊 <a href="{report_url}">最新レポートを開く</a></p>'
        if has_change else ""
    )

    changed_list = ""
    if has_change:
        items = []
        for r in results:
            if r["status"] == "changed":
                items.append(f'<li>📝 変更: <a href="{r["url"]}">{r["url"]}</a></li>')
            elif r["status"] == "new":
                items.append(f'<li>🆕 新規: <a href="{r["url"]}">{r["url"]}</a></li>')
            elif r["status"] == "error_404":
                items.append(f'<li>🚫 404: <a href="{r["url"]}">{r["url"]}</a>　← リンク元: {r["referrer"]}</li>')
        changed_list = "<ul>" + "".join(items) + "</ul>"

    html_body = f"""
<html><body style="font-family:sans-serif; color:#1e293b;">
<h2>🔍 WebCrawler 定時巡回レポート</h2>
<p>実行日時: {now_str}</p>
<p>監視対象: <a href="{config['start_url']}">{config['start_url']}</a></p>
<hr>
<table style="border-collapse:collapse;">
  <tr><td style="padding:4px 12px;">巡回ページ数</td><td><b>{total}</b></td></tr>
  <tr><td style="padding:4px 12px;">📝 変更</td><td><b>{changed} 件</b></td></tr>
  <tr><td style="padding:4px 12px;">🆕 新規</td><td><b>{new_page} 件</b></td></tr>
  <tr><td style="padding:4px 12px;">🚫 404エラー</td><td><b>{err_404} 件</b></td></tr>
</table>
<hr>
{report_link}
{changed_list}
<p style="color:#64748b; font-size:0.85em;">このメールはWebCrawler Monitorにより自動送信されています。</p>
</body></html>
"""

    # メール組み立て
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = gmail_user
    msg["To"]      = ", ".join(to_addresses)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    # 送信
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(gmail_user, gmail_pass)
            smtp.sendmail(gmail_user, to_addresses, msg.as_string())
        log.info(f"メール送信完了 → {to_addresses}")
    except Exception as e:
        log.error(f"メール送信失敗: {e}")


# ── メイン ────────────────────────────────────────────────
def main():
    config = load_config("config.json")
    log.info(f"=== クロール開始: {config['start_url']} ===")
    results = crawl(config, config_path="config.json")
    report_file = generate_report(results, config)

    changed = sum(1 for r in results if r["status"] in ("changed", "new"))
    log.info(f"=== 完了: {len(results)}ページ確認、{changed}件の変更を検出 ===")

    # GitHub Pages のレポートURL（環境変数から取得、なければローカルパス）
    pages_url = os.environ.get("PAGES_URL", "").rstrip("/")
    report_url = f"{pages_url}/index.html" if pages_url else os.path.abspath(report_file)

    # メール送信（定時実行通知・変更時はレポートリンク付き）
    send_mail(config, results, report_url)

    # 変更があればブラウザで自動表示（ローカル実行時のみ）
    if changed > 0 and config.get("auto_open_browser", True) and not pages_url:
        import webbrowser
        webbrowser.open(os.path.abspath(report_file))

if __name__ == "__main__":
    main()
