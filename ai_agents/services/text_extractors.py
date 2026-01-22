"""
Text extraction utilities for various document formats.
"""

import os
from abc import ABC, abstractmethod
from typing import Optional, List

# PDF extraction
try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    pdfplumber = None
    PDFPLUMBER_AVAILABLE = False

# OCR support
try:
    from pdf2image import convert_from_path
    import pytesseract
    OCR_AVAILABLE = True
    
    # Set Tesseract path for Windows (adjust if needed)
    if os.name == 'nt':  # Windows
        possible_paths = [
            r'C:\Program Files\Tesseract-OCR\tesseract.exe',
            r'C:\Program Files (x86)\Tesseract-OCR\tesseract.exe',
            r'C:\Users\{}\AppData\Local\Programs\Tesseract-OCR\tesseract.exe'.format(os.getenv('USERNAME'))
        ]
        for path in possible_paths:
            if os.path.exists(path):
                pytesseract.pytesseract.tesseract_cmd = path
                print(f"✅ Found Tesseract at: {path}")
                break
except ImportError:
    OCR_AVAILABLE = False
    pytesseract = None
    convert_from_path = None

# DOCX extraction
try:
    from docx import Document as DocxDocument  # type: ignore
    DOCX_AVAILABLE = True
except ImportError:
    DocxDocument = None
    DOCX_AVAILABLE = False


class TextExtractor(ABC):
    """Base class for text extractors."""
    
    @abstractmethod
    def extract(self, file_path: str) -> str:
        """Extract text from a file."""
        pass


class TxtExtractor(TextExtractor):
    """Extract text from plain text files."""
    
    def extract(self, file_path: str) -> str:
        """Read plain text file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except UnicodeDecodeError:
            # Fallback for different encodings
            with open(file_path, 'r', encoding='latin-1') as f:
                return f.read()


class PDFPlumberExtractor(TextExtractor):
    """Extract text from PDF files using pdfplumber with OCR fallback."""
    
    def extract(self, file_path: str) -> str:
        """Extract text from PDF, using OCR for image-based PDFs."""
        if not PDFPLUMBER_AVAILABLE or pdfplumber is None:
            raise ImportError(
                "pdfplumber not installed. Install with: pip install pdfplumber"
            )
        
        text_parts = []
        image_based = False
        
        try:
            # First, try regular text extraction
            with pdfplumber.open(file_path) as pdf:
                print(f"📄 Processing {len(pdf.pages)} pages...")
                
                for i, page in enumerate(pdf.pages, 1):
                    text = page.extract_text()
                    
                    if text and len(text.strip()) > 50:
                        # Successfully extracted text
                        clean_text = self._clean_text(text)
                        if clean_text:
                            text_parts.append(clean_text)
                        print(f"  ✅ Page {i}: Extracted {len(text.strip())} chars")
                    else:
                        # No text found - likely image-based
                        image_based = True
                        print(f"  ⚠️  Page {i}: No text found (image-based)")
            
            # If no text was extracted, use OCR
            if not text_parts and image_based:
                print(f"\n🔍 PDF appears to be image-based. Attempting OCR...")
                
                if not OCR_AVAILABLE:
                    raise ImportError(
                        "OCR required but dependencies missing.\n"
                        "Install with: pip install pytesseract pdf2image pillow\n"
                        "Also install Tesseract: https://github.com/UB-Mannheim/tesseract/wiki"
                    )
                
                text_parts = self._extract_with_ocr(file_path)
        
        except Exception as e:
            raise RuntimeError(f"Failed to extract text from PDF: {e}")
        
        if not text_parts:
            raise RuntimeError(
                "No text could be extracted from this PDF. "
                "The document may be empty or corrupted."
            )
        
        return "\n\n".join(text_parts)
    
    def _extract_with_ocr(self, file_path: str) -> List[str]:
        """Extract text using OCR."""
        if not OCR_AVAILABLE or convert_from_path is None or pytesseract is None:
            return []
        
        text_parts = []
        
        try:
            # Convert PDF pages to images
            print("  Converting PDF to images...")
            
            # Set poppler path for Windows
            poppler_path = None
            if os.name == 'nt':  # Windows
                possible_poppler_paths = [
                    r'C:\poppler\Library\bin',                      # ✅ Your current setup
                    r'C:\Program Files\poppler\Library\bin',
                    r'C:\poppler-25.12.0\Library\bin',              # Fallback
                ]
                for path in possible_poppler_paths:
                    if os.path.exists(path):
                        poppler_path = path
                        print(f"  ✅ Using Poppler from: {poppler_path}")
                        break
                
                if not poppler_path:
                    # Try to find pdftoppm.exe in system PATH
                    import shutil
                    pdftoppm = shutil.which('pdftoppm')
                    if pdftoppm:
                        poppler_path = os.path.dirname(pdftoppm)
                        print(f"  ✅ Found Poppler in system PATH: {poppler_path}")
            
            # Convert with poppler path
            if poppler_path:
                images = convert_from_path(file_path, dpi=300, poppler_path=poppler_path)
            else:
                print("  ⚠️  Poppler path not specified, trying system PATH...")
                images = convert_from_path(file_path, dpi=300)
            
            print(f"  Processing {len(images)} pages with OCR...")
            
            for i, image in enumerate(images, 1):
                print(f"    Page {i}...", end=" ")
                try:
                    # Perform OCR
                    text = pytesseract.image_to_string(image, lang='eng')
                    
                    if text and len(text.strip()) > 50:
                        clean_text = self._clean_text(text)
                        if clean_text:
                            text_parts.append(clean_text)
                            print(f"✅ {len(text.strip())} chars")
                        else:
                            print("⚠️ Text too short after cleaning")
                    else:
                        print("⚠️ No text extracted")
                        
                except Exception as e:
                    print(f"❌ Error: {e}")
                    continue
            
            print(f"\n  ✅ OCR complete: {len(text_parts)} pages processed")
            
        except Exception as e:
            print(f"  ❌ OCR failed: {e}")
            # Provide helpful error message
            if "poppler" in str(e).lower():
                print(f"\n  💡 Poppler installation help:")
                print(f"     Current check paths:")
                print(f"     - C:\\poppler\\Library\\bin")
                print(f"     - C:\\Program Files\\poppler\\Library\\bin")
                print(f"\n     To fix:")
                print(f"     1. Download: https://github.com/oschwartz10612/poppler-windows/releases")
                print(f"     2. Extract to C:\\poppler")
                print(f"     3. Verify C:\\poppler\\Library\\bin\\pdftoppm.exe exists")
            return []
        
        return text_parts
    
    def _clean_text(self, text: str) -> str:
        """Clean extracted text."""
        if not text:
            return ""
        
        # Remove common Guardian paywall/navigation text
        unwanted_phrases = [
            'continue', 'remind me in february', 'support the guardian',
            'visa', 'paypal', 'mastercard', 'american express',
            'guardian columnist', 'tim dowling', 'if you appreciate what the guardian does',
            'please consider supporting us on a monthly basis'
        ]
        
        lines = []
        for line in text.split('\n'):
            line = line.strip()
            if line and not any(phrase in line.lower() for phrase in unwanted_phrases):
                lines.append(line)
        
        return '\n'.join(lines)


class DocxExtractor(TextExtractor):
    """Extract text from DOCX files."""
    
    def extract(self, file_path: str) -> str:
        """Extract text from DOCX."""
        if not DOCX_AVAILABLE or DocxDocument is None:
            raise ImportError(
                "python-docx not installed. Install with: pip install python-docx"
            )
        
        try:
            doc = DocxDocument(file_path)
            text_parts = []
            
            for paragraph in doc.paragraphs:
                if paragraph.text.strip():
                    text_parts.append(paragraph.text)
            
            return "\n\n".join(text_parts)
        except Exception as e:
            raise RuntimeError(f"Failed to extract text from DOCX: {e}")


def get_extractor(file_path: str) -> TextExtractor:
    """Get the appropriate extractor based on file extension."""
    ext = os.path.splitext(file_path)[1].lower()
    
    extractors = {
        '.txt': TxtExtractor,
        '.pdf': PDFPlumberExtractor,
        '.docx': DocxExtractor,
        '.doc': DocxExtractor,
    }
    
    if ext not in extractors:
        raise ValueError(
            f"Unsupported file type: {ext}. "
            f"Supported types: {', '.join(extractors.keys())}"
        )
    
    extractor_class = extractors[ext]
    
    # Check if required library is available
    if ext == '.pdf' and not PDFPLUMBER_AVAILABLE:
        raise ImportError(
            "PDF support requires pdfplumber. Install with: pip install pdfplumber"
        )
    if ext in ['.docx', '.doc'] and not DOCX_AVAILABLE:
        raise ImportError(
            "DOCX support requires python-docx. Install with: pip install python-docx"
        )
    
    return extractor_class()


def extract_text(file_path: str, method: str = "auto") -> str:
    """Extract text from a file using automatic extractor selection."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if method == "auto":
        extractor = get_extractor(file_path)
    else:
        raise ValueError(f"Unknown extraction method: {method}. Use 'auto'.")
    
    return extractor.extract(file_path)