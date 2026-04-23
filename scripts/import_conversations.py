#!/usr/bin/env python3
"""Simple importer: converts raw .txt chat dumps into Markdown files with YAML frontmatter."""
import os
import sys
from datetime import datetime
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(ROOT, "conversations", "import_raw")
OUT_DIR = os.path.join(ROOT, "conversations")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)


def slugify(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^a-z0-9_-]+", "-", name)
    name = re.sub(r"-+", "-", name)
    return name.strip("-")


def convert_file(path: str):
    with open(path, "r", encoding="utf-8") as f:
        content = f.read().strip()
    if not content:
        return None
    base = os.path.basename(path)
    name, _ = os.path.splitext(base)
    slug = slugify(name)
    now = datetime.utcnow().isoformat() + "Z"
    out_name = f"{slug}.md"
    out_path = os.path.join(OUT_DIR, out_name)
    front = ["---",
             f"title: \"{name}\"",
             f"date: {now}",
             "source: claude-copied",
             "---",
             "\n"]
    with open(out_path, "w", encoding="utf-8") as out:
        out.write("\n".join(front))
        out.write(content)
    return out_path


def main():
    files = [os.path.join(RAW_DIR, p) for p in os.listdir(RAW_DIR) if p.lower().endswith(".txt")]
    if not files:
        print("No .txt files found in conversations/import_raw/. Paste your copied chats there first.")
        return
    for f in files:
        out = convert_file(f)
        if out:
            print(f"Converted: {f} -> {out}")
        else:
            print(f"Skipped empty file: {f}")


if __name__ == "__main__":
    main()
