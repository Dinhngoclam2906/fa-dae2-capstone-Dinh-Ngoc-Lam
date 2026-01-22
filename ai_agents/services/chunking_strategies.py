"""
Text chunking strategies for document processing.
Production-ready semantic chunking without experimental dependencies.
"""

from abc import ABC, abstractmethod
from typing import List
import re

from langchain_text_splitters import RecursiveCharacterTextSplitter

# Try to import spaCy for advanced semantic chunking
try:
    import spacy
    SPACY_AVAILABLE = True
    try:
        nlp = spacy.load("en_core_web_sm")
    except OSError:
        SPACY_AVAILABLE = False
        nlp = None
except ImportError:
    SPACY_AVAILABLE = False
    nlp = None


class ChunkingStrategy(ABC):
    """Base class for chunking strategies."""
    
    @abstractmethod
    def chunk(self, text: str, chunk_size: int, overlap: int) -> List[str]:
        """Split text into chunks."""
        pass


class SentenceChunking(ChunkingStrategy):
    """Sentence-based chunking strategy."""
    
    def chunk(self, text: str, chunk_size: int, overlap: int) -> List[str]:
        """Split text by sentences while respecting chunk size."""
        sentences = re.split(r'(?<=[.!?])\s+', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        chunks = []
        current_chunk = []
        current_size = 0
        
        for sentence in sentences:
            sent_size = len(sentence)
            
            if current_size + sent_size > chunk_size and current_chunk:
                chunks.append(' '.join(current_chunk))
                
                # Calculate overlap
                overlap_text = ' '.join(current_chunk)
                if len(overlap_text) > overlap:
                    overlap_text = overlap_text[-overlap:]
                
                if overlap > 0:
                    current_chunk = [overlap_text, sentence]
                    current_size = len(overlap_text) + sent_size
                else:
                    current_chunk = [sentence]
                    current_size = sent_size
            else:
                current_chunk.append(sentence)
                current_size += sent_size
        
        if current_chunk:
            chunks.append(' '.join(current_chunk))
        
        return chunks


class RecursiveChunking(ChunkingStrategy):
    """Enhanced recursive chunking with semantic awareness."""
    
    def chunk(self, text: str, chunk_size: int, overlap: int) -> List[str]:
        """Split text recursively with semantic-aware separators."""
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=overlap,
            length_function=len,
            separators=[
                "\n\n\n",  # Section breaks
                "\n\n",    # Paragraph breaks
                "\n",      # Line breaks
                ". ",      # Sentences
                "! ",      # Exclamations
                "? ",      # Questions
                "; ",      # Semicolons
                ", ",      # Commas
                " ",       # Spaces
                ""         # Characters (last resort)
            ],
            is_separator_regex=False,
        )
        
        return splitter.split_text(text)


class SmartSemanticChunking(ChunkingStrategy):
    """
    Smart semantic chunking without experimental dependencies.
    Prioritizes: Paragraphs > Sentences > Words
    Uses regex-based sentence detection.
    """
    
    def chunk(self, text: str, chunk_size: int, overlap: int) -> List[str]:
        """Chunk by semantic units (paragraphs and sentences)."""
        
        # Step 1: Split by paragraphs
        paragraphs = self._split_paragraphs(text)
        
        chunks = []
        current_chunk = []
        current_size = 0
        
        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
            
            para_size = len(para)
            
            # If paragraph is too large, split it by sentences
            if para_size > chunk_size:
                sentences = self._split_sentences(para)
                for sentence in sentences:
                    sent_size = len(sentence)
                    
                    if current_size + sent_size > chunk_size and current_chunk:
                        # Save current chunk
                        chunks.append("\n\n".join(current_chunk))
                        
                        # Apply overlap
                        overlap_paras = self._get_overlap_content(current_chunk, overlap)
                        current_chunk = overlap_paras + [sentence]
                        current_size = sum(len(p) for p in current_chunk)
                    else:
                        current_chunk.append(sentence)
                        current_size += sent_size
            
            # If paragraph fits in current chunk
            elif current_size + para_size <= chunk_size:
                current_chunk.append(para)
                current_size += para_size
            
            # If paragraph doesn't fit, start new chunk
            else:
                if current_chunk:
                    chunks.append("\n\n".join(current_chunk))
                
                # Apply overlap
                overlap_paras = self._get_overlap_content(current_chunk, overlap)
                current_chunk = overlap_paras + [para]
                current_size = sum(len(p) for p in current_chunk)
        
        # Add final chunk
        if current_chunk:
            chunks.append("\n\n".join(current_chunk))
        
        return chunks
    
    def _split_paragraphs(self, text: str) -> List[str]:
        """Split by double newlines (paragraph breaks)."""
        paragraphs = re.split(r'\n\n+', text)
        return [p.strip() for p in paragraphs if p.strip()]
    
    def _split_sentences(self, text: str) -> List[str]:
        """Split paragraph into sentences using regex."""
        # Advanced regex for sentence detection
        sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', text)
        return [s.strip() for s in sentences if s.strip()]
    
    def _get_overlap_content(self, content_list: List[str], overlap_size: int) -> List[str]:
        """Get last content that fits in overlap size."""
        if overlap_size == 0:
            return []
        
        overlap_content = []
        current_overlap = 0
        
        for content in reversed(content_list):
            if current_overlap + len(content) <= overlap_size:
                overlap_content.insert(0, content)
                current_overlap += len(content)
            else:
                break
        
        return overlap_content


class SpacySemanticChunking(ChunkingStrategy):
    """
    Production-quality semantic chunking using spaCy.
    Falls back to SmartSemanticChunking if spaCy is unavailable.
    """
    
    def __init__(self):
        """Initialize with spaCy or fallback."""
        global nlp  # Reference module-level nlp
        
        self.use_spacy = SPACY_AVAILABLE and nlp is not None
        self.nlp = nlp if self.use_spacy else None  # ✅ Type: Optional[Language]
        
        if not self.use_spacy:
            print("⚠️  spaCy not available, falling back to regex-based chunking")
            print("   For better quality, install spaCy:")
            print("   pip install spacy")
            print("   python -m spacy download en_core_web_sm\n")
            self.fallback = SmartSemanticChunking()
        else:
            self.fallback = SmartSemanticChunking()  # Always have fallback ready
    
    def chunk(self, text: str, chunk_size: int, overlap: int) -> List[str]:
        """Chunk text using spaCy or fallback to regex."""
        
        # Use spaCy if available
        if self.use_spacy and self.nlp is not None:
            try:
                return self._chunk_with_spacy(text, chunk_size, overlap)
            except Exception as e:
                print(f"⚠️  spaCy chunking failed: {e}, using fallback")
                return self.fallback.chunk(text, chunk_size, overlap)
        else:
            # Fallback to SmartSemanticChunking
            return self.fallback.chunk(text, chunk_size, overlap)
    
    def _chunk_with_spacy(self, text: str, chunk_size: int, overlap: int) -> List[str]:
        """Chunk using spaCy's advanced sentence detection."""
        
        # Type guard for safety
        assert self.nlp is not None, "nlp should be checked before calling this method"
        
        # Process text with spaCy (limit size for performance)
        doc = self.nlp(text[:1000000])  # ✅ Now type-safe!
        
        # Extract sentences
        sentences = [sent.text.strip() for sent in doc.sents]
        
        # Group sentences into chunks
        chunks = []
        current_chunk = []
        current_size = 0
        
        for sentence in sentences:
            sent_size = len(sentence)
            
            if current_size + sent_size > chunk_size and current_chunk:
                chunks.append(" ".join(current_chunk))
                
                # Calculate overlap
                overlap_sents = []
                overlap_size = 0
                for s in reversed(current_chunk):
                    if overlap_size + len(s) <= overlap:
                        overlap_sents.insert(0, s)
                        overlap_size += len(s)
                    else:
                        break
                
                current_chunk = overlap_sents + [sentence]
                current_size = sum(len(s) for s in current_chunk)
            else:
                current_chunk.append(sentence)
                current_size += sent_size
        
        if current_chunk:
            chunks.append(" ".join(current_chunk))
        
        return chunks

class HybridSemanticChunking(ChunkingStrategy):
    """
    Hybrid approach combining spaCy (when available) with smart regex fallback.
    Best of both worlds - uses spaCy for precision, regex for reliability.
    """

    # ✅ Add class-level flag to print only once
    _printed_warning = False
    
    def __init__(self):
        """Initialize hybrid chunker."""
        self.spacy_chunker = SpacySemanticChunking()
        self.smart_chunker = SmartSemanticChunking()
        self.use_spacy = SPACY_AVAILABLE and nlp is not None

        # ✅ Only print once per Python session
        if not HybridSemanticChunking._printed_warning:
            if self.use_spacy:
                print("✅ Using spaCy for advanced semantic chunking")
            else:
                print("✅ Using regex-based smart semantic chunking")
            HybridSemanticChunking._printed_warning = True
    
    def chunk(self, text: str, chunk_size: int, overlap: int) -> List[str]:
        """
        Intelligent chunking strategy:
        1. Try spaCy for documents < 500KB
        2. Fall back to regex for large documents or if spaCy unavailable
        3. Post-process to ensure quality
        """
        
        text_size_kb = len(text) / 1024
        
        # Use spaCy for smaller documents (better accuracy)
        if self.use_spacy and text_size_kb < 500:
            try:
                chunks = self.spacy_chunker.chunk(text, chunk_size, overlap)
            except Exception as e:
                print(f"⚠️  spaCy chunking failed ({e}), using regex fallback")
                chunks = self.smart_chunker.chunk(text, chunk_size, overlap)
        else:
            # Use regex-based for large documents (better performance)
            chunks = self.smart_chunker.chunk(text, chunk_size, overlap)
        
        # Post-process: ensure quality
        chunks = self._post_process_chunks(chunks)
        
        return chunks
    
    def _post_process_chunks(self, chunks: List[str]) -> List[str]:
        """Clean up and validate chunks."""
        cleaned_chunks = []
        
        for chunk in chunks:
            # Remove excessive whitespace
            chunk = re.sub(r'\s+', ' ', chunk).strip()
            
            # Skip very short chunks (likely noise)
            if len(chunk) < 50:
                continue
            
            # Ensure chunk ends with sentence punctuation if possible
            if chunk and chunk[-1] not in '.!?':
                # Try to find last sentence boundary
                last_punct = max(
                    chunk.rfind('.'),
                    chunk.rfind('!'),
                    chunk.rfind('?')
                )
                if last_punct > len(chunk) * 0.7:  # If found in last 30%
                    chunk = chunk[:last_punct + 1]
            
            cleaned_chunks.append(chunk)
        
        return cleaned_chunks

def get_chunking_strategy(strategy_name: str) -> ChunkingStrategy:
    """
    Get chunking strategy by name.
    
    Available strategies:
    - 'sentence': Basic sentence-based chunking
    - 'recursive': LangChain recursive chunking with semantic separators
    - 'semantic': Smart regex-based semantic chunking (no dependencies)
    - 'spacy': spaCy-based chunking with regex fallback
    - 'hybrid': Best approach - combines spaCy + regex (RECOMMENDED)
    """
    strategies = {
        'sentence': SentenceChunking,
        'recursive': RecursiveChunking,
        'semantic': SmartSemanticChunking,
        'spacy': SpacySemanticChunking,
        'hybrid': HybridSemanticChunking,  # ✅ RECOMMENDED
    }
    
    if strategy_name not in strategies:
        raise ValueError(
            f"Invalid chunking strategy: {strategy_name}. "
            f"Choose from: {list(strategies.keys())}"
        )
    
    return strategies[strategy_name]()


def chunk_text(
    text: str,
    strategy: str = "hybrid",  # ✅ Changed default to hybrid
    chunk_size: int = 1000,
    overlap: int = 200
) -> List[str]:
    """
    Convenience function to chunk text.
    
    Args:
        text: Input text to chunk
        strategy: Chunking strategy ('hybrid' recommended)
        chunk_size: Maximum characters per chunk
        overlap: Overlap between consecutive chunks
    
    Returns:
        List of text chunks
    """
    chunking_strategy = get_chunking_strategy(strategy)
    return chunking_strategy.chunk(text, chunk_size, overlap)