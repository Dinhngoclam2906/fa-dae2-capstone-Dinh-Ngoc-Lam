---
name: deploy-job-image
description: Rebuild and deploy a Cloud Run Job image (guardian-ingestion, vn-ingestion, dbt-build, embed-sync) after its code changed - minimal build context, Cloud Build, job update, then verify with a real run. Use whenever a commit touches code that one of the jobs runs.
---

# Rebuild and deploy a job image

Committing code does NOT change production: each job runs a baked image. The 10-21 sync bug
went live only when an image was rebuilt, so check what each image contains.

| Job | Dockerfile | Build context | Image |
|---|---|---|---|
| `guardian-ingestion`, `vn-ingestion` (same image) | `infra/docker/Dockerfile.ingestion` | minimal copy (below) | `.../guardian-ingestion/guardian-ingestion:latest` |
| `dbt-build` | `transform/Dockerfile` | `transform/` (has its own `.gcloudignore`) | `.../dbt-build/dbt-build:latest` |
| `embed-sync` | `infra/docker/Dockerfile.embed` | minimal copy (below) | `.../embed-sync/embed-sync:latest` |

Registry prefix: `asia-southeast1-docker.pkg.dev/$GCP_PROJECT/`. Region `asia-southeast1`.

## 1. Which jobs does the change affect?

Read the Dockerfile's `COPY` lines. The agent (`agent/core/retrieval.py`, `agent.py`,
`tools.py`, `app.py`) is in **no** job image; `embed-sync` copies only `core/__init__.py`,
`config.py`, `bq.py`, `articles.py` plus the two pipeline scripts. Don't rebuild what
the change doesn't touch.

## 2. Build from a minimal context (never the repo root)

The repo root has no `.gcloudignore`, so `gcloud builds submit .` would upload
`transform/.venv` and more. Copy only what the Dockerfile `COPY`s into a fresh
folder in the scratchpad, keeping the relative paths:

```bash
CTX=<scratchpad>/embed_ctx; rm -rf "$CTX"; mkdir -p "$CTX"
# copy the Dockerfile + every file it COPYs, e.g. for embed-sync:
cp --parents infra/docker/Dockerfile.embed agent/jobs/requirements-embed.txt agent/core/__init__.py \
  agent/core/config.py agent/core/bq.py agent/core/articles.py \
  agent/jobs/embed_articles.py agent/jobs/sync_vector_index.py agent/jobs/run_embed_job.sh "$CTX"/
cat > "$CTX/cloudbuild.yaml" <<EOF
steps:
- name: gcr.io/cloud-builders/docker
  args: [build, -f, infra/docker/Dockerfile.embed, -t, asia-southeast1-docker.pkg.dev/$GCP_PROJECT/embed-sync/embed-sync:latest, .]
images: [asia-southeast1-docker.pkg.dev/$GCP_PROJECT/embed-sync/embed-sync:latest]
EOF
gcloud builds submit "$CTX" --config "$CTX/cloudbuild.yaml" --project $GCP_PROJECT
```

Compare the file list with the Dockerfile's `COPY` lines before building: a missing file
fails the build, an extra one only wastes upload.
For dbt: `gcloud builds submit transform --tag <image> --project $GCP_PROJECT`.

## 3. Point the job at the new image

```bash
gcloud run jobs update <job> --image <image>:latest --region asia-southeast1 --project $GCP_PROJECT
```

(`:latest` is resolved at update time; without this step the job may keep the old digest.)
**`vn-ingestion` overrides the image command** (`sh ingestion/run_vn_ingestion_job.sh`; the image's
`CMD` runs Guardian). When that path changes, update image and command in ONE call:
`gcloud run jobs update vn-ingestion --image <image>:latest --command sh --args ingestion/run_vn_ingestion_job.sh ...`
(either alone breaks the job). Before any rebuild, note each job's current digest
(`gcloud run jobs describe <job> --format='value(spec.template.spec.template.spec.containers[0].image)'`)
as the rollback point; tagging builds with the commit SHA as well as `latest` makes rollback a one-liner.
For the ingestion image, update **both** `guardian-ingestion` and `vn-ingestion`.

## 4. Verify with a real run

Ask the user before an extra run if it costs money or writes data they didn't expect
(embed-sync spends Gemini tokens only on pending articles; usually ~$0).

```bash
gcloud run jobs execute <job> --region asia-southeast1 --project $GCP_PROJECT --wait
```

Then run the `check-pipeline` skill: judge by rows, not by the exit code.

## 5. Record it

Log the rebuild with the `log-update` skill (the image change is a production change: say
which commit is now live).
