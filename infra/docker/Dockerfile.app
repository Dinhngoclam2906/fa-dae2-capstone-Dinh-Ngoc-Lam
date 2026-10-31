# infra/docker/Dockerfile.app - the published Streamlit app (Cloud Run service `news-agent-app`).
# Minimal: agent/app.py + agent/core/, not the jobs, evals or legacy code.
# Build from a minimal context (like the job images): the repo root has no .gcloudignore.
# Deploy + settings: infra/app/deploy_app.sh.
FROM python:3.13-slim

WORKDIR /app

COPY agent/requirements-app.txt ./
RUN pip install --no-cache-dir -r requirements-app.txt

COPY agent/__init__.py agent/app.py ./agent/
COPY agent/core/ ./agent/core/

ENV PYTHONUNBUFFERED=1
# Cloud Run sets PORT (8080). The app runs from agent/ on sys.path (it imports `core`).
CMD ["sh", "-c", "exec streamlit run agent/app.py --server.port=${PORT:-8080} --server.address=0.0.0.0 --server.headless=true --browser.gatherUsageStats=false"]
