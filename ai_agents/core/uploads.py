"""PDF upload into the vector store, scoped to one session.

Chunks go to the shared `user_documents` namespace tagged with `session_id`, and the
retriever filters on it, so one user's upload never shows up in another user's answers.
(A namespace per session would hit the free plan's 100-namespaces-per-index limit.)
Size caps keep a public upload from running up embedding costs.
"""

from __future__ import annotations

import logging
import os
import tempfile

from core.config import settings

logger = logging.getLogger(__name__)

MAX_BYTES = 10 * 1024 * 1024
MAX_CHUNKS = 200


def upload_pdf(data: bytes, file_name: str, session_id: str) -> int:
    """Chunk, embed and store a PDF for this session. Returns the number of chunks."""
    if len(data) > MAX_BYTES:
        raise ValueError(f"PDF is larger than {MAX_BYTES // (1024 * 1024)} MB")
    from langchain_core.documents import Document
    from langchain_pinecone import PineconeVectorStore
    from services.document_processor import DocumentProcessor

    from core.retrieval import _embeddings

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(data)
        path = tmp.name
    try:
        chunks = DocumentProcessor(
            chunk_size=int(os.getenv("CHUNK_SIZE", "1000")),
            chunk_overlap=int(os.getenv("CHUNK_OVERLAP", "200")),
            chunking_strategy="hybrid",
        ).process_document(path)
    finally:
        os.unlink(path)
    if not chunks:
        raise ValueError("No text could be extracted from this PDF")
    if len(chunks) > MAX_CHUNKS:
        raise ValueError(f"PDF is too long ({len(chunks)} chunks; limit {MAX_CHUNKS})")
    docs = []
    for c in chunks:
        md = dict(c["metadata"], file_name=file_name, source_type="uploaded_document",
                  session_id=session_id)
        md.pop("source", None)  # a local temp path, meaningless to others
        docs.append(Document(page_content=c["text"], metadata=md))
    # Ids start with the session id, so a session's uploads can be listed and deleted
    # by prefix (delete_session_uploads) without a metadata-filtered delete, which
    # serverless indexes don't support.
    ids = [f"{session_id}#{file_name}#{i}" for i in range(len(docs))]
    store = PineconeVectorStore(index_name=settings.pinecone_index, embedding=_embeddings(),
                                namespace=settings.user_docs_namespace)
    store.add_documents(docs, ids=ids)
    logger.info("pdf_uploaded file=%s chunks=%d", file_name, len(docs))
    return len(docs)


def delete_session_uploads(session_id: str) -> int:
    """Delete every chunk this session uploaded. Returns how many were deleted."""
    from pinecone import Pinecone
    index = Pinecone(api_key=os.environ["PINECONE_API_KEY"]).Index(settings.pinecone_index)
    ids = [i for page in index.list(prefix=f"{session_id}#", namespace=settings.user_docs_namespace)
           for i in page]
    if ids:
        index.delete(ids=ids, namespace=settings.user_docs_namespace)
    return len(ids)
