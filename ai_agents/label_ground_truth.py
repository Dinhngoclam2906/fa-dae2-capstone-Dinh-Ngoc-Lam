# ai_agents/label_ground_truth.py

import json
from typing import List, Dict
import sys
import os
sys.path.append('..')

from capstone_tool_calling_agent import search_articles_by_multiple_keywords
from langchain_pinecone import PineconeVectorStore
from langchain_openai import OpenAIEmbeddings

def extract_keywords(query: str) -> List[str]:
    """
    Context-aware keyword extraction for SQL search.
    Preserves named entities and filters noise words.
    """
    import re
    
    # Comprehensive noise words (questions, common verbs, determiners)
    noise_words = {
        'what', 'how', 'why', 'when', 'where', 'who', 'which', 'whom',
        'tell', 'show', 'find', 'get', 'give', 'about',
        'happened', 'occurring', 'going', 'related',
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to',
        'of', 'for', 'with', 'from', 'by', 'as', 'into',
        'is', 'are', 'was', 'were', 'been', 'being',
        'have', 'has', 'had', 'do', 'does', 'did',
        'will', 'would', 'could', 'should', 'may', 'might',
    }
    
    # Important abbreviations/terms (keep short forms)
    important_terms = {
        'ai', 'uk', 'us', 'eu', 'un', 'nhs', 'fbi', 'cia',
        'cop', 'cop26', 'cop27', 'cop28',
        'g7', 'g20', 'gdp', 'ceo', 'pm',
        'war', 'ban', 'tax', 'law',
    }
    
    keywords = []
    
    # 1. Capture multi-word named entities (COP26, Donald Trump, etc.)
    # Pattern: Capitalized words next to each other or numbers
    entities = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b|\b[A-Z]+\d+\b', query)
    for entity in entities:
        # Split entity and add individual words
        for word in entity.split():
            word_lower = word.lower()
            if word_lower not in noise_words:
                keywords.append(word_lower)
    
    # 2. Extract significant content words
    all_words = re.findall(r'\b\w+\b', query.lower())
    
    for word in all_words:
        # Skip if already added or is noise
        if word in keywords or word in noise_words:
            continue
        
        # Include if: important term OR length >= 4
        if word in important_terms or len(word) >= 4:
            keywords.append(word)
    
    # 3. Remove redundant words (keep most specific)
    # Example: If we have both "trump" and "donald", keep "trump"
    final_keywords = []
    for kw in keywords:
        # Skip if it's a substring of another keyword
        if not any(kw != other and kw in other for other in keywords):
            final_keywords.append(kw)
    
    # Return top 4 (SQL performs better with fewer, more specific terms)
    return final_keywords[:4]

def load_existing_progress(output_file: str = "ground_truth.json") -> List[Dict]:
    """Load existing ground truth labels if they exist."""
    if os.path.exists(output_file):
        with open(output_file, "r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return []
    return []

def get_labeled_query_ids(labeled_results: List[Dict]) -> set:
    """Get set of query IDs that have already been labeled."""
    return {result["query_id"] for result in labeled_results}

def label_relevance_for_query(query_data: Dict):
    """
    For a given query, retrieve candidates and manually label relevance.
    
    Returns labeled data or None if user wants to quit.
    """
    query = query_data["query"]
    print(f"\n{'='*80}")
    print(f"Query {query_data['id']}: {query}")
    print(f"Category: {query_data['category']}")
    print(f"Description: {query_data['description']}")
    print(f"{'='*80}\n")
    
    # Get candidates from your hybrid system
    all_candidates = []
    
    # 1. Dense search
    print("🔍 Fetching candidates from dense vector search...")
    try:
        embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
        vector_store = PineconeVectorStore(
            index_name=os.getenv("PINECONE_INDEX", "guardian-capstone"),
            embedding=embeddings,
            namespace="guardian_articles"
        )
        
        dense_docs = vector_store.similarity_search(query, k=10)
        for doc in dense_docs:
            all_candidates.append({
                "article_id": doc.metadata.get("article_id"),
                "title": doc.metadata.get("title", "Unknown"),
                "preview": doc.page_content[:200],
                "source": "dense"
            })
        print(f"✅ Found {len(dense_docs)} candidates from dense search")
    except Exception as e:
        print(f"⚠️ Dense search failed: {e}")
    
    # 2. SQL search (extract keywords first)
    print("🗄️ Fetching candidates from SQL search...")
    keywords = extract_keywords(query)
    print(f"🔍 Keywords extracted: {keywords}")
    
    if keywords:
        try:
            sql_response = search_articles_by_multiple_keywords.invoke({
                "keywords": keywords,
                "limit": 10
            })
            
            if "results" in sql_response:
                for article in sql_response["results"]:
                    all_candidates.append({
                        "article_id": article.get("article_id"),
                        "title": article.get("web_title"),
                        "preview": article.get("preview", "")[:200],
                        "source": "sql"
                    })
                print(f"✅ Found {len(sql_response['results'])} candidates from SQL search")
        except Exception as e:
            print(f"⚠️ SQL search failed: {e}")
    
    # Deduplicate by article_id
    seen_ids = set()
    unique_candidates = []
    for candidate in all_candidates:
        article_id = candidate["article_id"]
        if article_id and article_id not in seen_ids:
            seen_ids.add(article_id)
            unique_candidates.append(candidate)
    
    print(f"\n📊 Total unique candidates: {len(unique_candidates)}\n")
    
    # Manual labeling
    relevance_scores = {}
    
    for i, candidate in enumerate(unique_candidates, 1):
        print(f"\n{'─'*80}")
        print(f"[Article {i}/{len(unique_candidates)}]")
        print(f"{'─'*80}")
        print(f"ID:      {candidate['article_id']}")
        print(f"Title:   {candidate['title']}")
        print(f"Preview: {candidate['preview']}...")
        print(f"Source:  {candidate['source']}")
        print(f"{'─'*80}")
        
        while True:
            print("\n💡 Relevance Score:")
            print("  0 = Not relevant")
            print("  1 = Somewhat relevant") 
            print("  2 = Highly relevant")
            print("  s = Skip this article")
            print("  q = Quit and save progress")
            
            score = input("\nYour choice (0/1/2/s/q): ").strip().lower()
            
            if score == 'q':
                print("\n⏸️ Quitting and saving progress...")
                return None  # Signal to quit
            
            if score == 's':
                print("⏭️ Skipping article")
                break
            
            if score in ['0', '1', '2']:
                relevance_scores[candidate['article_id']] = int(score)
                if score == '2':
                    print("✅ Marked as highly relevant")
                elif score == '1':
                    print("✅ Marked as somewhat relevant")
                else:
                    print("✅ Marked as not relevant")
                break
            else:
                print("❌ Invalid input. Please enter 0, 1, 2, s, or q")
    
    # Create result
    return {
        "query_id": query_data["id"],
        "query": query,
        "category": query_data["category"],
        "relevance_scores": relevance_scores,
        "relevant_article_ids": [aid for aid, score in relevance_scores.items() if score >= 1],
        "highly_relevant_ids": [aid for aid, score in relevance_scores.items() if score == 2],
        "num_labeled": len(relevance_scores),
        "num_candidates": len(unique_candidates)
    }


def main():
    """Main labeling workflow with resume capability."""
    
    # Load test queries
    if not os.path.exists("ai_agents/test_queries.json"):
        print("❌ Error: test_queries.json not found!")
        print("Please create test_queries.json first.")
        return
    
    with open("ai_agents/test_queries.json", "r") as f:
        test_queries = json.load(f)
    
    # Load existing progress
    output_file = "ai_agents/ground_truth.json"
    labeled_results = load_existing_progress(output_file)
    labeled_ids = get_labeled_query_ids(labeled_results)
    
    print("\n" + "="*80)
    print("📝 GROUND TRUTH LABELING TOOL")
    print("="*80)
    print(f"\nTotal queries: {len(test_queries)}")
    print(f"Already labeled: {len(labeled_ids)}")
    print(f"Remaining: {len(test_queries) - len(labeled_ids)}")
    
    if labeled_ids:
        print(f"\n✅ Labeled query IDs: {sorted(labeled_ids)}")
        
        # ✅ ADD THIS SECTION
        print("\n" + "="*80)
        print("⚠️  EXISTING LABELS FOUND")
        print("="*80)
        print("\nWhat would you like to do?")
        print("  1 = Resume from where you left off (keep existing labels)")
        print("  2 = Start fresh (delete all labels and re-label everything)")
        print("  3 = Re-label specific queries (keep others)")
        print("  q = Quit")
        
        choice = input("\nYour choice (1/2/3/q): ").strip().lower()
        
        if choice == 'q':
            print("👋 Exiting...")
            return
        
        elif choice == '2':
            # Start fresh
            confirm = input("\n⚠️  This will DELETE all existing labels. Are you sure? (yes/no): ").strip().lower()
            if confirm == 'yes':
                labeled_results = []
                labeled_ids = set()
                # Create backup
                backup_file = output_file.replace('.json', '_backup.json')
                if os.path.exists(output_file):
                    import shutil
                    shutil.copy(output_file, backup_file)
                    print(f"✅ Backup saved to {backup_file}")
                print("✅ Starting fresh - all labels cleared!")
            else:
                print("❌ Cancelled. Keeping existing labels.")
                return
        
        elif choice == '3':
            # Re-label specific queries
            print(f"\nCurrently labeled query IDs: {sorted(labeled_ids)}")
            relabel_input = input("Enter query IDs to re-label (comma-separated, e.g. '1,2,5'): ").strip()
            
            if relabel_input:
                try:
                    relabel_ids = {int(x.strip()) for x in relabel_input.split(',')}
                    # Remove these from labeled_results
                    labeled_results = [r for r in labeled_results if r["query_id"] not in relabel_ids]
                    labeled_ids = get_labeled_query_ids(labeled_results)
                    print(f"✅ Cleared labels for queries: {sorted(relabel_ids)}")
                except ValueError:
                    print("❌ Invalid input. Keeping all labels.")
                    return
        
        elif choice != '1':
            print("❌ Invalid choice. Exiting.")
            return
    
    print("\n" + "="*80)
    print("INSTRUCTIONS:")
    print("- Label each article as 0 (not relevant), 1 (somewhat), or 2 (highly relevant)")
    print("- Press 's' to skip an article")
    print("- Press 'q' at any time to quit and save progress")
    print("- You can resume later from where you left off")
    print("="*80)
    
    input("\n👉 Press Enter to start labeling...")
    
    # Label each query
    for query_data in test_queries:
        # Skip if already labeled
        if query_data["id"] in labeled_ids:
            print(f"\n⏭️ Skipping Query {query_data['id']} (already labeled)")
            continue
        
        # Label this query
        result = label_relevance_for_query(query_data)
        
        # Check if user wants to quit
        if result is None:
            print("\n💾 Saving progress before exit...")
            with open(output_file, "w") as f:
                json.dump(labeled_results, f, indent=2)
            print(f"✅ Progress saved to {output_file}")
            print(f"\n📊 Summary:")
            print(f"   Total labeled: {len(labeled_results)}/{len(test_queries)}")
            print(f"   Remaining: {len(test_queries) - len(labeled_results)}")
            print("\n👋 You can resume later by running this script again!")
            return
        
        # Add to results
        labeled_results.append(result)
        
        # Save after each query
        with open(output_file, "w") as f:
            json.dump(labeled_results, f, indent=2)
        
        print(f"\n💾 Progress saved: {len(labeled_results)}/{len(test_queries)} queries labeled")
    
    # All done
    print("\n" + "="*80)
    print("🎉 ALL QUERIES LABELED!")
    print("="*80)
    print(f"\n✅ Ground truth saved to {output_file}")
    print(f"📊 Total queries labeled: {len(labeled_results)}")
    
    # Show summary
    print("\n📈 Summary by category:")
    category_counts = {}
    for result in labeled_results:
        category = result.get("category", "unknown")
        category_counts[category] = category_counts.get(category, 0) + 1
    
    for category, count in category_counts.items():
        print(f"   {category}: {count} queries")
    
    print(f"\n👉 Next step: Run evaluation.py to see results!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user (Ctrl+C)")
        print("💾 Progress should be saved in ground_truth.json")
        print("👋 Run the script again to resume!")