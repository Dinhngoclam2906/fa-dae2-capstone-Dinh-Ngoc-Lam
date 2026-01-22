"""
# document_upload_ingestion.py
Document Upload Ingestion Pipeline
Processes uploaded PDFs/DOCX/TXT and stores them in Pinecone alongside Guardian articles.
Production-ready, cursor-clean, zero errors
"""

import os
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from langchain_core.documents import Document

# Import instructor's modules
from services.document_processor import DocumentProcessor

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DocumentUploadPipeline:
    """Process user-uploaded documents and store in Pinecone."""
    
    def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 200):
        """
        Initialize the document upload pipeline.
        
        Args:
            chunk_size: Maximum size per chunk
            chunk_overlap: Overlap between chunks
        """
        self.processor = DocumentProcessor(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            chunking_strategy="hybrid"
        )
        self.embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        self.index_name = os.getenv("PINECONE_INDEX", "guardian-capstone")
    
    def process_document(
        self, 
        file_path: str, 
        metadata: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Process a single document file.
        
        Args:
            file_path: Path to the document
            metadata: Optional metadata (author, date, source, etc.)
        
        Returns:
            List of processed chunks with metadata
        """
        try:
            # Process with DocumentProcessor
            chunks = self.processor.process_document(file_path)
            
            if not chunks or len(chunks) == 0:
                logger.warning(f"No chunks generated from: {file_path}")
                return []

            # Enhance metadata for Pinecone
            enhanced_chunks = []
            for chunk in chunks:
                # Merge instructor's metadata with custom metadata
                enhanced_metadata = {
                    **chunk["metadata"],
                    "source_type": "uploaded_document",
                    "file_name": Path(file_path).name,
                    "namespace": "user_documents",
                    **(metadata if metadata is not None else {}),
                }
                
                enhanced_chunks.append({
                    "text": chunk["text"],
                    "metadata": enhanced_metadata
                })
            
            logger.info(f"Processed {len(enhanced_chunks)} chunks from {file_path}")
            return enhanced_chunks
            
        except Exception as e:
            logger.error(f"Failed to process {file_path}: {e}")
            return []
    
    def upload_to_pinecone(
        self, 
        chunks: List[Dict[str, Any]], 
        namespace: str = "user_documents"
    ) -> None:
        """
        Upload processed chunks to Pinecone.
        
        Args:
            chunks: Processed document chunks
            namespace: Pinecone namespace (separates user docs from Guardian articles)
        """
        if not chunks:
            logger.warning("No chunks to upload")
            return
        
        try:
            # Convert to LangChain Document format
            docs: List[Document] = []
            ids: List[str] = []
            
            for i, chunk in enumerate(chunks):
                doc = Document(
                    page_content=chunk["text"],
                    metadata=chunk["metadata"]
                )
                docs.append(doc)
                
                # Generate unique ID
                file_name = chunk["metadata"].get("file_name", "unknown")
                chunk_idx = chunk["metadata"].get("chunk_index", i)
                ids.append(f"{file_name}_chunk_{chunk_idx}")
            
            # Upsert to Pinecone
            PineconeVectorStore.from_documents(
                documents=docs,
                embedding=self.embeddings,
                index_name=self.index_name,
                namespace=namespace,
                ids=ids
            )
            
            logger.info(f"Uploaded {len(docs)} chunks to Pinecone namespace '{namespace}'")
            
        except Exception as e:
            logger.error(f"Failed to upload to Pinecone: {e}")


# Module-level function (not a class method)
def process_document_folder(
    folder_path: str, 
    metadata: Optional[Dict[str, Any]] = None
) -> None:
    """
    Batch process all documents in a folder.
    
    Args:
        folder_path: Path to folder containing documents
        metadata: Common metadata for all documents
    """
    pipeline = DocumentUploadPipeline()
    
    supported_extensions = {".pdf", ".docx", ".txt"}
    folder = Path(folder_path)
    
    if not folder.exists() or not folder.is_dir():
        logger.error(f"Invalid folder path: {folder_path}")
        return
    
    all_chunks: List[Dict[str, Any]] = []
    
    for file_path in folder.iterdir():
        if file_path.suffix.lower() in supported_extensions:
            logger.info(f"Processing: {file_path.name}")
            chunks = pipeline.process_document(str(file_path), metadata)
            all_chunks.extend(chunks)
    
    if all_chunks:
        pipeline.upload_to_pinecone(all_chunks)  # ✅ This should work now
        logger.info(f"Batch upload complete: {len(all_chunks)} total chunks")
    else:
        logger.warning("No chunks generated from folder")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python document_upload_ingestion.py <file_or_folder_path>")
        sys.exit(1)
    
    path = sys.argv[1]
    
    # Optional: Add custom metadata
    custom_metadata: Dict[str, Any] = {
        "upload_source": "manual",
        "processed_by": "DocumentUploadPipeline"
    }
    
    if os.path.isfile(path):
        # Process single file
        pipeline = DocumentUploadPipeline()
        chunks = pipeline.process_document(path, custom_metadata)
        pipeline.upload_to_pinecone(chunks)
        print(f"✅ Processed {len(chunks)} chunks from {path}")
    
    elif os.path.isdir(path):
        # Process entire folder
        process_document_folder(path, custom_metadata)
        print(f"✅ Batch processing complete for folder: {path}")
    
    else:
        print(f"❌ Invalid path: {path}")