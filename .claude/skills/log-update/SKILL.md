---
name: log-update
description: Record finished work the way this project requires - one line in PROGRESS_LOG.md, a detailed row in PROGRESS_LOG.full-history.md, bugs/lessons in docs/lessons_learned.md, done items removed from "Resume here", then commit/push. Use after any change, incident, eval or decision worth keeping.
---

# Log an update

Every update touches a **pair** of logs, plus the lessons file when something broke or
surprised us. Both PROGRESS_LOG files are git-ignored (local only); `docs/` is committed.

## 1. PROGRESS_LOG.md (the short log)

- Add **one** row to the change-log table, after the newest row, in the existing style:
  `| YYYY-MM-DD | **Bold headline** (`commit`): what changed + the key numbers. |`
  One or two sentences; numbers over adjectives.
- **"Resume here" holds open work only.** When an item is done, delete it (don't tick it);
  its record lives in the change log. Update the item's text if it changed shape (e.g. parked).
- Bump "Last reviewed" if the file has it and you re-read the whole section.

## 2. PROGRESS_LOG.full-history.md (the detailed log)

Append one row at the end: `| YYYY-MM-DD | **Headline.** ... |` with:
- what the user asked, quoted in their own words;
- what was done, the decisions and why, alternatives rejected;
- numbers: counts, costs ($ and tokens / GB), timings, commit hashes;
- what was NOT done or left open.

## 3. docs/lessons_learned.md (bugs, incidents, gotchas)

For any bug, incident or non-obvious trap, add an entry in the right section (read the
golden rules and the index at the top first) **and** a row in the index. Entry shape: what
happened (dates, impact), root cause, fix (commit), how to avoid it next time.

## 4. Commit and push

- Push straight to `main` for routine work (no PR unless really needed).
- End the message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Never commit secrets or `.env`. Leave the unrelated `install.ps1` untracked.

## Checklist

- [ ] PROGRESS_LOG.md: 1 change-log row; "Resume here" pruned / updated
- [ ] PROGRESS_LOG.full-history.md: 1 detailed row with the user's words
- [ ] docs/lessons_learned.md entry + index row (only if something broke or surprised)
- [ ] committed + pushed (if tracked files changed)
