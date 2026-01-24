# """
# # document_upload_ingestion.py
# Document Upload Ingestion Pipeline
# Processes uploaded PDFs/DOCX/TXT and stores them in Pinecone alongside Guardian articles.
# Production-ready, cursor-clean, zero errors
# """

# import os
# import logging
# from typing import List, Dict, Any, Optional
# from pathlib import Path

# from dotenv import load_dotenv
# from langchain_openai import OpenAIEmbeddings
# from langchain_pinecone import PineconeVectorStore
# from langchain_core.documents import Document

# # Import instructor's modules
# from services.document_processor import DocumentProcessor

# load_dotenv()
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


# class DocumentUploadPipeline:
#     """Process user-uploaded documents and store in Pinecone."""
    
#     def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 200):
#         """
#         Initialize the document upload pipeline.
        
#         Args:
#             chunk_size: Maximum size per chunk
#             chunk_overlap: Overlap between chunks
#         """
#         self.processor = DocumentProcessor(
#             chunk_size=chunk_size,
#             chunk_overlap=chunk_overlap,
#             chunking_strategy="hybrid"
#         )
#         self.embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
#         self.index_name = os.getenv("PINECONE_INDEX", "guardian-capstone")
    
#     def process_document(
#         self, 
#         file_path: str, 
#         metadata: Optional[Dict[str, Any]] = None
#     ) -> List[Dict[str, Any]]:
#         """
#         Process a single document file.
        
#         Args:
#             file_path: Path to the document
#             metadata: Optional metadata (author, date, source, etc.)
        
#         Returns:
#             List of processed chunks with metadata
#         """
#         try:
#             # Process with DocumentProcessor
#             chunks = self.processor.process_document(file_path)
            
#             if not chunks or len(chunks) == 0:
#                 logger.warning(f"No chunks generated from: {file_path}")
#                 return []

#             # Enhance metadata for Pinecone
#             enhanced_chunks = []
#             for chunk in chunks:
#                 # Merge instructor's metadata with custom metadata
#                 enhanced_metadata = {
#                     **chunk["metadata"],
#                     "source_type": "uploaded_document",
#                     "file_name": Path(file_path).name,
#                     "namespace": "user_documents",
#                     **(metadata if metadata is not None else {}),
#                 }
                
#                 enhanced_chunks.append({
#                     "text": chunk["text"],
#                     "metadata": enhanced_metadata
#                 })
            
#             logger.info(f"Processed {len(enhanced_chunks)} chunks from {file_path}")
#             return enhanced_chunks
            
#         except Exception as e:
#             logger.error(f"Failed to process {file_path}: {e}")
#             return []
    
#     def upload_to_pinecone(
#         self, 
#         chunks: List[Dict[str, Any]], 
#         namespace: str = "user_documents"
#     ) -> None:
#         """
#         Upload processed chunks to Pinecone.
        
#         Args:
#             chunks: Processed document chunks
#             namespace: Pinecone namespace (separates user docs from Guardian articles)
#         """
#         if not chunks:
#             logger.warning("No chunks to upload")
#             return
        
#         try:
#             # Convert to LangChain Document format
#             docs: List[Document] = []
#             ids: List[str] = []
            
#             for i, chunk in enumerate(chunks):
#                 doc = Document(
#                     page_content=chunk["text"],
#                     metadata=chunk["metadata"]
#                 )
#                 docs.append(doc)
                
#                 # Generate unique ID
#                 file_name = chunk["metadata"].get("file_name", "unknown")
#                 chunk_idx = chunk["metadata"].get("chunk_index", i)
#                 ids.append(f"{file_name}_chunk_{chunk_idx}")
            
#             # Upsert to Pinecone
#             PineconeVectorStore.from_documents(
#                 documents=docs,
#                 embedding=self.embeddings,
#                 index_name=self.index_name,
#                 namespace=namespace,
#                 ids=ids
#             )
            
#             logger.info(f"Uploaded {len(docs)} chunks to Pinecone namespace '{namespace}'")
            
#         except Exception as e:
#             logger.error(f"Failed to upload to Pinecone: {e}")


# # Module-level function (not a class method)
# def process_document_folder(
#     folder_path: str, 
#     metadata: Optional[Dict[str, Any]] = None
# ) -> None:
#     """
#     Batch process all documents in a folder.
    
#     Args:
#         folder_path: Path to folder containing documents
#         metadata: Common metadata for all documents
#     """
#     pipeline = DocumentUploadPipeline()
    
#     supported_extensions = {".pdf", ".docx", ".txt"}
#     folder = Path(folder_path)
    
#     if not folder.exists() or not folder.is_dir():
#         logger.error(f"Invalid folder path: {folder_path}")
#         return
    
#     all_chunks: List[Dict[str, Any]] = []
    
#     for file_path in folder.iterdir():
#         if file_path.suffix.lower() in supported_extensions:
#             logger.info(f"Processing: {file_path.name}")
#             chunks = pipeline.process_document(str(file_path), metadata)
#             all_chunks.extend(chunks)
    
#     if all_chunks:
#         pipeline.upload_to_pinecone(all_chunks)
#         logger.info(f"Batch upload complete: {len(all_chunks)} total chunks")
#     else:
#         logger.warning("No chunks generated from folder")


# if __name__ == "__main__":
#     import sys
    
#     if len(sys.argv) < 2:
#         print("Usage: python document_upload_ingestion.py <file_or_folder_path>")
#         sys.exit(1)
    
#     path = sys.argv[1]
    
#     # Optional: Add custom metadata
#     custom_metadata: Dict[str, Any] = {
#         "upload_source": "manual",
#         "processed_by": "DocumentUploadPipeline"
#     }
    
#     if os.path.isfile(path):
#         # Process single file
#         pipeline = DocumentUploadPipeline()
#         chunks = pipeline.process_document(path, custom_metadata)
#         pipeline.upload_to_pinecone(chunks)
#         print(f"✅ Processed {len(chunks)} chunks from {path}")
    
#     elif os.path.isdir(path):
#         # Process entire folder
#         process_document_folder(path, custom_metadata)
#         print(f"✅ Batch processing complete for folder: {path}")
    
#     else:
#         print(f"❌ Invalid path: {path}")

"""
# document_upload_ingestion.py
Document Upload Ingestion Pipeline
Processes uploaded PDFs/DOCX/TXT and stores them in Pinecone alongside Guardian articles.
Saves metadata locally for tracking and retrieval.
Production-ready, cursor-clean, zero errors
"""

import os
import json
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime

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
    
    def __init__(
        self, 
        chunk_size: int = 1000, 
        chunk_overlap: int = 200,
        metadata_dir: str = "./metadata"
    ):
        """
        Initialize the document upload pipeline.
        
        Args:
            chunk_size: Maximum size per chunk
            chunk_overlap: Overlap between chunks
            metadata_dir: Directory to store local metadata files
        """
        self.processor = DocumentProcessor(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            chunking_strategy="hybrid"
        )
        self.embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        self.index_name = os.getenv("PINECONE_INDEX", "guardian-capstone")
        
        # Create metadata directory if it doesn't exist
        self.metadata_dir = Path(metadata_dir)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Metadata will be saved to: {self.metadata_dir}")
    
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
                    "file_path": str(Path(file_path).absolute()),
                    "namespace": "user_documents",
                    "upload_timestamp": datetime.now().isoformat(),
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
    ) -> Optional[Dict[str, Any]]:
        """
        Upload processed chunks to Pinecone and save metadata locally.
        
        Args:
            chunks: Processed document chunks
            namespace: Pinecone namespace (separates user docs from Guardian articles)
        
        Returns:
            Dictionary containing upload results and metadata file path
        """
        if not chunks:
            logger.warning("No chunks to upload")
            return None
        
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
                chunk_id = f"{file_name}_chunk_{chunk_idx}"
                ids.append(chunk_id)
            
            # Upsert to Pinecone
            PineconeVectorStore.from_documents(
                documents=docs,
                embedding=self.embeddings,
                index_name=self.index_name,
                namespace=namespace,
                ids=ids
            )
            
            logger.info(f"Uploaded {len(docs)} chunks to Pinecone namespace '{namespace}'")
            
            # Save metadata locally
            metadata_result = self._save_metadata_locally(chunks, ids, namespace)
            
            return {
                "chunks_uploaded": len(docs),
                "namespace": namespace,
                "index_name": self.index_name,
                "metadata_file": metadata_result["metadata_file"],
                "upload_timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to upload to Pinecone: {e}")
            return None
    
    def _save_metadata_locally(
        self, 
        chunks: List[Dict[str, Any]], 
        pinecone_ids: List[str],
        namespace: str
    ) -> Dict[str, Any]:
        """
        Save metadata to local JSON file for tracking.
        
        Args:
            chunks: Processed chunks with metadata
            pinecone_ids: List of Pinecone vector IDs
            namespace: Pinecone namespace
        
        Returns:
            Dictionary with metadata file path and summary
        """
        # Group chunks by source file
        files_metadata: Dict[str, Dict[str, Any]] = {}
        
        for chunk, pinecone_id in zip(chunks, pinecone_ids):
            file_name = chunk["metadata"].get("file_name", "unknown")
            
            if file_name not in files_metadata:
                files_metadata[file_name] = {
                    "file_name": file_name,
                    "file_path": chunk["metadata"].get("file_path", ""),
                    "upload_timestamp": chunk["metadata"].get("upload_timestamp", ""),
                    "namespace": namespace,
                    "index_name": self.index_name,
                    "total_chunks": 0,
                    "chunks": []
                }
            
            # Add chunk info
            files_metadata[file_name]["chunks"].append({
                "pinecone_id": pinecone_id,
                "chunk_index": chunk["metadata"].get("chunk_index", 0),
                "chunk_text_preview": chunk["text"][:200] + "..." if len(chunk["text"]) > 200 else chunk["text"],
                "chunk_size": chunk["metadata"].get("chunk_size", 0),
                "metadata": chunk["metadata"]
            })
            files_metadata[file_name]["total_chunks"] += 1
        
        # Create metadata object
        metadata_obj = {
            "upload_session": {
                "timestamp": datetime.now().isoformat(),
                "namespace": namespace,
                "index_name": self.index_name,
                "total_files": len(files_metadata),
                "total_chunks": len(chunks)
            },
            "files": list(files_metadata.values())
        }
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        metadata_filename = f"upload_metadata_{timestamp}.json"
        metadata_path = self.metadata_dir / metadata_filename
        
        # Save to JSON
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata_obj, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved metadata to: {metadata_path}")
        
        # Also save/update a master index file
        self._update_master_index(metadata_obj, timestamp)
        
        return {
            "metadata_file": str(metadata_path),
            "files_processed": len(files_metadata),
            "total_chunks": len(chunks)
        }
    
    def _update_master_index(self, upload_metadata: Dict[str, Any], timestamp: str) -> None:
        """
        Update a master index file that tracks all uploads.
        
        Args:
            upload_metadata: Metadata from current upload
            timestamp: Upload timestamp
        """
        master_index_path = self.metadata_dir / "master_index.json"
        
        # Load existing master index or create new one
        if master_index_path.exists():
            with open(master_index_path, 'r', encoding='utf-8') as f:
                master_index = json.load(f)
        else:
            master_index = {
                "created_at": datetime.now().isoformat(),
                "total_uploads": 0,
                "total_files": 0,
                "total_chunks": 0,
                "uploads": []
            }
        
        # Add current upload to master index
        master_index["uploads"].append({
            "timestamp": timestamp,
            "upload_timestamp": upload_metadata["upload_session"]["timestamp"],
            "namespace": upload_metadata["upload_session"]["namespace"],
            "files_count": upload_metadata["upload_session"]["total_files"],
            "chunks_count": upload_metadata["upload_session"]["total_chunks"],
            "files": [f["file_name"] for f in upload_metadata["files"]],
            "metadata_file": f"upload_metadata_{timestamp}.json"
        })
        
        # Update totals
        master_index["total_uploads"] += 1
        master_index["total_files"] += upload_metadata["upload_session"]["total_files"]
        master_index["total_chunks"] += upload_metadata["upload_session"]["total_chunks"]
        master_index["last_updated"] = datetime.now().isoformat()
        
        # Save master index
        with open(master_index_path, 'w', encoding='utf-8') as f:
            json.dump(master_index, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Updated master index: {master_index_path}")
    
    def search_metadata(self, file_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Search local metadata for uploaded documents.
        
        Args:
            file_name: Optional file name to filter by
        
        Returns:
            List of matching metadata entries
        """
        master_index_path = self.metadata_dir / "master_index.json"
        
        if not master_index_path.exists():
            logger.warning("No master index found")
            return []
        
        with open(master_index_path, 'r', encoding='utf-8') as f:
            master_index = json.load(f)
        
        results = []
        
        for upload in master_index["uploads"]:
            # If file_name specified, filter
            if file_name and file_name not in upload["files"]:
                continue
            
            # Load detailed metadata
            metadata_file = self.metadata_dir / upload["metadata_file"]
            if metadata_file.exists():
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    detailed_metadata = json.load(f)
                    results.append(detailed_metadata)
        
        return results


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
        result = pipeline.upload_to_pinecone(all_chunks)
        if result:
            logger.info(f"Batch upload complete: {len(all_chunks)} total chunks")
            logger.info(f"Metadata saved to: {result['metadata_file']}")
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
        result = pipeline.upload_to_pinecone(chunks)
        
        if result:
            print(f"✅ Processed {result['chunks_uploaded']} chunks from {path}")
            print(f"📁 Metadata saved to: {result['metadata_file']}")
        else:
            print(f"❌ Failed to process {path}")
    
    elif os.path.isdir(path):
        # Process entire folder
        process_document_folder(path, custom_metadata)
        print(f"✅ Batch processing complete for folder: {path}")
    
    else:
        print(f"❌ Invalid path: {path}")