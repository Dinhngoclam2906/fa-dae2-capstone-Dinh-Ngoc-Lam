"""
e5_gpu_embed.py - embed the bake-off sample with multilingual-e5-base on a free GPU
(Google Colab or Kaggle). On this laptop's CPU the same job ran for 55+ minutes without
finishing; on a T4 GPU it takes a few minutes.

Colab: Runtime > Change runtime type > T4 GPU, then in one cell:
    from google.colab import files
    files.upload()                      # pick data/eval/bakeoff/sample.parquet
    !pip -q install sentence-transformers polars
    # paste this whole file into the next cell (or upload it and `%run e5_gpu_embed.py`)
    files.download("docs_e5.npy")       # after it finishes

Kaggle: New Notebook > Settings > Accelerator: GPU T4 x2; upload sample.parquet as a
Dataset (or via the input panel), set SAMPLE below to its path, run, then download
docs_e5.npy from /kaggle/working.

Back on the laptop: put docs_e5.npy in data/eval/bakeoff/ and run
    uv run --env-file .env python ai_agents/eval/embedding_bakeoff.py embed --model e5
(it finds the cached docs and only embeds the 75 questions, on CPU, which also gives the
CPU query latency the app would see), then `... evaluate`.

Must match embedding_bakeoff.py exactly: text = title + "\n\n" + lead, prefix "passage: ",
max_seq_length 512, normalised vectors, rows in sample.parquet order.
"""
import time

import numpy as np
import polars as pl
import torch
from sentence_transformers import SentenceTransformer

SAMPLE = "sample.parquet"  # Kaggle: e.g. "/kaggle/input/<your-dataset>/sample.parquet"

df = pl.read_parquet(SAMPLE)
texts = ["passage: " + f"{r['web_title']}\n\n{r['lead']}" for r in df.iter_rows(named=True)]
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"{len(texts)} docs on {device} ({torch.cuda.get_device_name(0) if device == 'cuda' else 'no GPU!'})")

model = SentenceTransformer("intfloat/multilingual-e5-base", device=device)
model.max_seq_length = 512
if device == "cuda":
    model.half()  # fp16: ~2x faster on a T4, same ranking quality for retrieval
t = time.time()
vecs = model.encode(texts, batch_size=64, normalize_embeddings=True, show_progress_bar=True)
np.save("docs_e5.npy", vecs.astype(np.float32))
print(f"done: {vecs.shape} in {time.time() - t:.0f}s -> docs_e5.npy")
