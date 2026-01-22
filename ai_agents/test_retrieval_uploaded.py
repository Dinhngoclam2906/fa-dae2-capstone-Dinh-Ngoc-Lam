"""
# test_retrieval_uploaded.py
Test searching the uploaded Guardian article.
Production-ready, cursor-clean, zero errors
"""

import os
from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore

load_dotenv()

def test_uploaded_doc_search():
    """Search the uploaded document."""
    
    print("\n" + "="*60)
    print("Testing Uploaded Document Retrieval")
    print("="*60 + "\n")
    
    embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
    index_name = os.getenv("PINECONE_INDEX", "guardian-capstone")
    
    vector_store = PineconeVectorStore(
        index_name=index_name,
        embedding=embeddings,
        namespace="user_documents"
    )
    
    # Test queries based on your Trump article
    queries = [
        "What does Trump say about aspirin?",
        "What is Trump's daily routine?",
        "Trump's health habits"
    ]
    
    for query in queries:
        print(f"🔍 Query: '{query}'")
        print("-" * 60)
        
        results = vector_store.similarity_search_with_score(query, k=2)
        
        if results:
            for i, (doc, score) in enumerate(results, 1):
                print(f"\n✅ Result {i} (Relevance: {score:.4f}):")
                print(f"   File: {doc.metadata.get('file_name', 'Unknown')}")
                print(f"   Chunk: {doc.metadata.get('chunk_index', '?')}/{doc.metadata.get('total_chunks', '?')}")
                print(f"   Strategy: {doc.metadata.get('chunking_strategy', 'unknown')}")
                print(f"   Content: {doc.page_content[:150]}...")
        else:
            print("  ❌ No results found")
        
        print("\n" + "="*60 + "\n")


if __name__ == "__main__":
    test_uploaded_doc_search()