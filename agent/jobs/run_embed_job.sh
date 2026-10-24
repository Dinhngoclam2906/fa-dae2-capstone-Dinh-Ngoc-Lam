#!/bin/sh
# Entrypoint for the embed-sync Cloud Run Job (step 4 of the news-pipeline workflow, after dbt-build).
#
# 1. embed: vectors for every article in marts.obt_articles that has none for the current
#    (article_id, content_hash). --full looks at ALL months (~0.1 GB when nothing is
#    pending), so a newly added news site's whole backlog is picked up automatically;
#    --max-tokens caps ONE RUN (6 runs/day): 1M tokens = 5x the busiest run seen (~0.2M,
#    2026-10-24), ~$0.20; worst case ~$1.20/day. A bigger backlog (a new source, a mass
#    re-embed) just continues over the next runs.
# 2. sync: copy rows embedded since the last sync into the Pinecone index.
# The sync runs even if embed failed part-way (it pushes whatever was saved), and the
# exit code still reports a failure, so the email alert fires.
set -u

rc=0
python agent/jobs/embed_articles.py embed --full --max-tokens 1000000 || rc=1
python agent/jobs/sync_vector_index.py sync || rc=1

exit "$rc"
