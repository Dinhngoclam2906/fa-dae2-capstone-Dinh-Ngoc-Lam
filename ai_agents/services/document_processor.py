"""
Main document processor that combines extraction and chunking.
"""

import re
from datetime import datetime
from typing import List, Dict, Any

from services.text_extractors import extract_text
from services.chunking_strategies import chunk_text


class DocumentProcessor:
    """Process documents through extraction, cleaning, and chunking."""
    
    def __init__(
        self,
        chunk_size: int = 1000,
        chunk_overlap: int = 200,
        chunking_strategy: str = "hybrid"  # ✅ Changed default
    ):
        """
        Initialize document processor.
        
        Args:
            chunk_size: Maximum size per chunk
            chunk_overlap: Overlap between chunks
            chunking_strategy: Strategy for chunking
                - 'hybrid': Best (spaCy + regex fallback) - RECOMMENDED
                - 'semantic': Smart regex-based
                - 'spacy': spaCy with fallback
                - 'recursive': LangChain recursive
                - 'sentence': Simple sentence-based
        """
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.chunking_strategy = chunking_strategy
    
    def preprocess_text(self, text: str) -> str:
        """Clean and normalize text."""
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove multiple newlines (keep paragraph structure)
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        # Strip leading/trailing whitespace
        text = text.strip()
        
        return text
    
    def create_metadata(
        self,
        chunk: str,
        source_doc: str,
        chunk_index: int,
        total_chunks: int
    ) -> Dict[str, Any]:
        """Create metadata for a chunk."""
        return {
            "source": source_doc,
            "chunk_index": chunk_index,
            "total_chunks": total_chunks,
            "chunk_size": len(chunk),
            "chunk_size_tokens": len(chunk.split()),
            "timestamp": datetime.now().isoformat(),
            "chunking_strategy": self.chunking_strategy
        }
    
    def process_document(self, file_path: str) -> List[Dict[str, Any]]:
        """Process a document through the complete pipeline."""
        # Extract text
        if not hasattr(self, '_cached_text'):
            raw_text = extract_text(file_path, method="auto")
            self._cached_text = raw_text
        else:
            raw_text = self._cached_text
        
        # Preprocess
        cleaned_text = self.preprocess_text(raw_text)
        
        if not cleaned_text:
            return []
        
        # Chunk with hybrid strategy
        chunks = chunk_text(
            cleaned_text,
            strategy=self.chunking_strategy,
            chunk_size=self.chunk_size,
            overlap=self.chunk_overlap
        )
        
        # Create structured output
        result = []
        for i, chunk in enumerate(chunks):
            result.append({
                "text": chunk,
                "metadata": self.create_metadata(
                    chunk,
                    file_path,
                    i,
                    len(chunks)
                )
            })
        
        return result
    
    def get_processing_stats(self, chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate statistics about processed chunks."""
        if not chunks:
            return {
                "total_chunks": 0,
                "avg_chunk_size": 0,
                "min_chunk_size": 0,
                "max_chunk_size": 0,
                "avg_token_count": 0,
                "sources": []
            }
        
        chunk_sizes = [c["metadata"]["chunk_size"] for c in chunks]
        token_counts = [c["metadata"]["chunk_size_tokens"] for c in chunks]
        sources = list(set(c["metadata"]["source"] for c in chunks))
        
        return {
            "total_chunks": len(chunks),
            "avg_chunk_size": sum(chunk_sizes) / len(chunk_sizes),
            "min_chunk_size": min(chunk_sizes),
            "max_chunk_size": max(chunk_sizes),
            "avg_token_count": sum(token_counts) / len(token_counts),
            "sources": sources
        }