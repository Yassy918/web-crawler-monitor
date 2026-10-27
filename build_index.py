#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
reports/summary_*.json と docs/report_*.html から docs/index.html を生成する。

GitHub Actions の "Prepare Pages report" ステップから
    python3 build_index.py
で呼び出す（リポジトリ直下で実行すること）。
"""

import glob
import json
import os
import re

from jinja2 import Environment, FileSystemLoader, select_autoescape

BASE = os.path.dirname(os.path.abspath(__file__))
REPORT_DIR = os.path.join(BASE, "reports")
DOCS_DIR = os.path.join(BASE, "docs")

env = Environment(
    loader=FileSystemLoader(os.path.join(BASE, "templates")),
    autoescape=select_autoescape(["html"]),
)


def load_summaries():
    """summary_*.json を {レポートのファイル名: サマリー} で返す"""
    result = {}
    for path in glob.glob(os.path.join(REPORT_DIR, "summary_*.json")):
        with open(path, encoding="utf-8") as f:
            s = json.load(f)
        result[s["filename"]] = s
    return result


def build_badges(summary):
    """一覧の1行に出すバッジ。summary が無い（過去分）場合は「不明」"""
    if summary is None:
        return [{"kind": "unknown", "label": "不明"}]
    if not summary.get("has_change"):
        return [{"kind": "ok", "label": "変更なし"}]
    c = summary["counts"]
    badges = []
    if c.get("changed"):
        badges.append({"kind": "changed", "label": f"変更 {c['changed']}件"})
    if c.get("new"):
        badges.append({"kind": "new", "label": f"新規 {c['new']}件"})
    if c.get("error_404"):
        badges.append({"kind": "error404", "label": f"404 {c['error_404']}件"})
    if c.get("error"):
        badges.append({"kind": "error", "label": f"エラー {c['error']}件"})
    return badges


def build_status(summary):
    """絞り込み用の種別（スペース区切り）。判定は従来の index.html と同じ"""
    if summary is None:
        return "unknown"
    if not summary.get("has_change"):
        return "ok"
    c = summary["counts"]
    keys = []
    if c.get("changed"):
        keys.append("changed")
    if c.get("new"):
        keys.append("new")
    if c.get("error_404"):
        keys.append("error404")
    if c.get("error"):
        keys.append("error")
    return " ".join(keys)


def main():
    os.makedirs(DOCS_DIR, exist_ok=True)
    summaries = load_summaries()

    files = sorted(glob.glob(os.path.join(DOCS_DIR, "report_*.html")), reverse=True)
    rows = []
    for path in files:
        name = os.path.basename(path)
        m = re.search(r"report_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})\.html", name)
        if not m:
            continue
        y, mo, d, hh, mm, _ = m.groups()
        summary = summaries.get(name)
        rows.append({
            "file": name,
            "date": f"{y}/{mo}/{d}",
            "time": f"{hh}:{mm}",
            "status": build_status(summary),
            "badges": build_badges(summary),
        })

    html = env.get_template("index.html").render(rows=rows)
    with open(os.path.join(DOCS_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)
    print(f"index.html 生成完了（{len(rows)}件）")


if __name__ == "__main__":
    main()
