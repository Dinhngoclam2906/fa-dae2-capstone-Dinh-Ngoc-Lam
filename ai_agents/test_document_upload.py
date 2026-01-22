"""
# test_document_upload.py
Test document processing pipeline before uploading to Pinecone.
Production-ready, cursor-clean, zero errors
"""

import os
import json
from pathlib import Path
from document_upload_ingestion import DocumentUploadPipeline

def test_document_processing(file_path: str):
    """Test document processing without uploading to Pinecone."""
    
    print(f"\n{'='*60}")
    print(f"Testing Document Processing")
    print(f"{'='*60}\n")
    
    # Check file exists
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False
    
    file_name = Path(file_path).name
    file_size = os.path.getsize(file_path) / 1024  # KB
    print(f"📄 File: {file_name}")
    print(f"📊 Size: {file_size:.2f} KB")
    print(f"📂 Type: {Path(file_path).suffix}\n")
    
    try:
        # Initialize pipeline
        print("🔧 Initializing pipeline...")
        pipeline = DocumentUploadPipeline(
            chunk_size=1000,
            chunk_overlap=200
        )
        print("✅ Pipeline initialized\n")
        
        # Add custom metadata
        custom_metadata = {
            "uploaded_by": "test_user",
            "test_run": True,
            "original_size_kb": round(file_size, 2)
        }
        
        # Process document
        print("🔄 Processing document...")
        chunks = pipeline.process_document(file_path, custom_metadata)
        
        if not chunks:
            print("❌ No chunks generated!")
            return False
        
        print(f"✅ Generated {len(chunks)} chunks\n")
        
        # Analyze chunks
        print(f"{'='*60}")
        print("Chunk Analysis:")
        print(f"{'='*60}\n")
        
        total_chars = sum(len(chunk["text"]) for chunk in chunks)
        avg_chars = total_chars / len(chunks) if chunks else 0
        
        print(f"📊 Total chunks: {len(chunks)}")
        print(f"📊 Total characters: {total_chars:,}")
        print(f"📊 Average chunk size: {avg_chars:.0f} characters")
        print(f"📊 Min chunk size: {min(len(c['text']) for c in chunks)} characters")
        print(f"📊 Max chunk size: {max(len(c['text']) for c in chunks)} characters\n")
        
        # Show first 3 chunks
        print(f"{'='*60}")
        print("Sample Chunks (First 3):")
        print(f"{'='*60}\n")
        
        for i, chunk in enumerate(chunks[:3], 1):
            text_preview = chunk["text"][:200].replace("\n", " ")
            print(f"Chunk {i}:")
            print(f"  Size: {len(chunk['text'])} chars")
            print(f"  Preview: {text_preview}...")
            print(f"  Metadata: {chunk['metadata']}\n")
        
        # Save chunks to JSON for inspection
        output_file = f"test_chunks_{Path(file_path).stem}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(chunks, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Chunks saved to: {output_file}")
        print(f"   (You can inspect this file to see all chunks)\n")
        
        print(f"{'='*60}")
        print("✅ Document processing successful!")
        print(f"{'='*60}\n")
        
        return chunks
        
    except Exception as e:
        print(f"\n❌ Error during processing:")
        print(f"   {type(e).__name__}: {str(e)}\n")
        import traceback
        traceback.print_exc()
        return False


def test_pinecone_upload(chunks):
    """Test uploading chunks to Pinecone."""
    
    print(f"\n{'='*60}")
    print(f"Testing Pinecone Upload")
    print(f"{'='*60}\n")
    
    try:
        pipeline = DocumentUploadPipeline()
        
        print(f"📤 Uploading {len(chunks)} chunks to Pinecone...")
        print(f"   Index: {pipeline.index_name}")
        print(f"   Namespace: user_documents\n")
        
        pipeline.upload_to_pinecone(chunks, namespace="user_documents")
        
        print(f"✅ Upload successful!\n")
        print(f"{'='*60}")
        print("Next Steps:")
        print(f"{'='*60}")
        print("1. Check Pinecone dashboard to verify vectors")
        print("2. Test search/retrieval in your Streamlit app")
        print("3. Try asking questions about the uploaded document\n")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Upload failed:")
        print(f"   {type(e).__name__}: {str(e)}\n")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("\n❌ Usage: python test_document_upload.py <path_to_pdf>")
        print("   Example: python test_document_upload.py sample.pdf\n")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    # Step 1: Test processing
    chunks = test_document_processing(file_path)
    
    if not chunks:
        print("❌ Processing failed. Fix errors before uploading.")
        sys.exit(1)
    
    # Step 2: Ask user if they want to upload
    print("\n" + "="*60)
    response = input("Upload these chunks to Pinecone? (yes/no): ").strip().lower()
    
    if response in ['yes', 'y']:
        success = test_pinecone_upload(chunks)
        if success:
            print("🎉 All tests passed!\n")
        else:
            print("❌ Upload failed. Check errors above.\n")
    else:
        print("\n✅ Processing test complete. Skipped upload.")
        print(f"   To upload later, run: python document_upload_ingestion.py {file_path}\n")