# Plan: repo layout Phase B (component folders)

**Status:** planned 2026-10-24, **deferred until after the GCP trial decision (2026-12-09)**.
Phase A (`c195d5b`) did the low-risk part: `ai_agents/jobs/`, `legacy/` folders,
`deploy/local/`, tests, Makefile, MIT license. Phase B regroups the repo by pipeline
component, the layout most well-known data / ML repos use.

**Why wait for the trial decision:** if production is upgraded, Phase B includes the
production switch (step 4). If the data is exported and production shut down, steps 4-5
disappear and Phase B is a ~1 h local rename.

## Decisions (agreed 2026-10-24)

| Question | Decision |
|---|---|
| Timing | After the trial decision (2026-12-09) |
| Folder names | As in the target layout below (`agent/`, `transform/`, ...) |
| Installable `src/` package (`import news_agent.core`) | No. Keep the `core` package name, so no import line changes. Revisit as a separate step |
| Pause `news-pipeline-trigger` during the switch | Yes (confirm with the user at execution time: production change) |

## What couples to today's paths (inventory 2026-10-24)

- **Cloud Run Jobs:** only `vn-ingestion` overrides its command
  (`sh scripts/batch_data/run_vn_ingestion_job.sh`); `guardian-ingestion`, `dbt-build`,
  `embed-sync` and `cost-report` run their image's `CMD`.
- **Crawlers use cwd-relative paths** (`data/batch/...`): `data/` and the containers'
  `WORKDIR /app` stay as they are; only the scripts move.
- **`capstone_dbt_project`** is referenced in 20+ places: CI (6), the uv workspace member
  + `uv.lock`, the Airflow compose file (absolute `D:/...` mounts), the dbt DAG
  (`DBT_PROJECT_DIR`), the deploy skill, README / CLAUDE.md. It has an untracked nested
  `.venv` (stale; the root `.venv` is the one in use).
- **`sys.path`:** the evals and jobs add their parent folder to find `core`, so `core/`,
  `jobs/` and `eval/` must move together.
- **Imports by module name** between neighbouring scripts (e.g. the loader and
  `raw_archive.py`): move whole folders, never split them.
- The Cloud Workflows DAG names jobs only (no paths): unaffected.

## Target layout

```
├── ingestion/            ← scripts/batch_data/ (crawlers, loader, Bronze, job entrypoints)
│   ├── legacy/           ← scripts/batch_data/legacy/
│   └── streaming/        ← scripts/real_time_data/ (Kafka)
├── transform/            ← capstone_dbt_project/ (the dbt project name inside stays capstone_dbt_project)
├── agent/                ← ai_agents/ (core/, jobs/, eval/, legacy/, app.py, cli_agent.py)
├── orchestration/
│   ├── workflows/        ← workflows/ (Cloud Workflows DAG + deploy.sh)
│   └── airflow/dags/     ← dags/
├── monitoring/           ← scripts/monitoring/ (cost report)
├── infra/
│   ├── docker/           ← Dockerfile.ingestion / .embed / .cost
│   └── local/            ← deploy/local/ + sql/init.sql
├── docs/ (+ docs/images/ ← images/)
├── tests/
└── data/                 (stays: cwd-relative paths)
```

## Steps

### 1. Branch and move (local only, ~1 h)
- Branch `refactor/layout-b`; `git mv` each folder (history kept, `git log --follow`).
- Delete the stale `capstone_dbt_project/.venv` before renaming its folder.
- Fix every coupling:
  - the 3 root Dockerfiles' `COPY` / `CMD` (they move to `infra/docker/`; build contexts
    stay the repo-relative minimal copies), the dbt Dockerfile's context (`transform/`);
  - CI: the 6 `--project-dir` / `working-directory` / `DBT_PROFILES_DIR` paths;
  - `pyproject.toml`: workspace member → `uv lock`; pytest `pythonpath`; pyright paths;
  - Makefile; compose mounts (make the dbt ones relative: `./transform`);
  - DAG `bash_command` paths and `DBT_PROJECT_DIR`;
  - `scripts/monitoring/deploy_cost_report.sh`, `workflows/deploy.sh`;
  - the 3 skills (`check-pipeline` + `check.py`, `deploy-job-image`, `log-update`).
- Verify: `make test`; `dbt parse`; `docker compose config` diffed old vs new (mask the
  output: it prints secrets); every `.py` parses.

### 2. Docs (~30 min)
- Update the living docs: README, CLAUDE.md, skills, PROGRESS_LOG "Resume here", memory.
- Leave historical reports in `docs/` unchanged; add `docs/repo_layout.md` with an
  old → new path table so old references still resolve.

### 3. Pull request (not a direct push)
- PR_CI must pass on the branch before merging. Merging alone is safe: production runs
  baked images, and the old `vn-ingestion` command path still exists inside the old image.

### 4. Switch production in a quiet window (~45 min, right after a run ends, e.g. 04:07 UTC)
1. Record the current image digest of all 4 jobs (the rollback point).
2. Pause `news-pipeline-trigger` (user's go first).
3. Build the 3 images from minimal contexts, tagged `:latest` **and** the commit SHA.
4. `gcloud run jobs update` each job. For `vn-ingestion`, change the image and the command
   (`sh ingestion/run_vn_ingestion_job.sh`) in **one** update: either one alone breaks it.
5. Run the full workflow once by hand (`{"transform": true}`): all 4 steps in order,
   ~2.5 GB of BigQuery.
6. `check-pipeline`: rows per source, vectors vs OBT, Pinecone; not exit codes.
7. Resume the trigger.

### 5. Rollback (at any step)
`gcloud run jobs update <job> --image <old digest>` (plus the old `--args` for
`vn-ingestion`), `git revert` the merge commit, resume the trigger.

### 6. Record it
`log-update` skill: the new digests and the commit now live; a lessons entry if anything
surprised.

## Cost and time
~3 h in one session; ≈ $0 (Cloud Build inside its free minutes, one extra pipeline run
≈ 2.5 GB of BigQuery inside the free tier).
