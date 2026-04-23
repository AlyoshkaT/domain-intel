Instructions for storing Claude conversations locally

Place raw conversation text files into `conversations/import_raw/`.
Each file should be a plain `.txt` file containing one saved chat (copy-paste from Claude).

Run the import script to convert raw `.txt` files into Markdown with metadata:

```bash
python3 scripts/import_conversations.py
```

Output: `conversations/<original-filename>.md`.

Recommended workflow:
- Keep `conversations/` inside the project root.
- Version locally as you like (Git optional). Do not push if you prefer local-only.
- Use filenames with dates or short descriptive titles, e.g. `2026-04-09_project-design.txt`.
