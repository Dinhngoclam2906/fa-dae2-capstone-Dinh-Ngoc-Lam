#!/bin/sh
# Entrypoint for the embed-sync Cloud Run Job (daily, after dbt-build).
#
# 1. embed: vectors for every article in marts.obt_articles that has none for the current
#    (article_id, content_hash). --full looks at ALL months (~0.1 GB when nothing is
#    pending), so a newly added news site's whole backlog is picked up automatically;
#    --max-tokens caps one day's spend (~$6), and anything left continues the next day.
# 2. sync: copy rows embedded since the last sync into the Pinecone index.
# The sync runs even if embed failed part-way (it pushes whatever was saved), and the
# exit code still reports a failure, so the email alert fires.
set -u

rc=0
python ai_agents/embed_articles.py embed --full --max-tokens 30000000 || rc=1
python ai_agents/sync_vector_index.py sync || rc=1

exit "$rc"
