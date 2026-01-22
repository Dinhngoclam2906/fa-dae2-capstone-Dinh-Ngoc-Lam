# label_ground_truth_llm.py
from openai import OpenAI
import json
import os
from dotenv import load_dotenv
import time

load_dotenv()

client = OpenAI()

def llm_label_relevance(
    query: str, 
    article_title: str, 
    article_preview: str,
    max_retries: int = 3
) -> int:
    """
    Use LLM to label relevance with retry logic.
    
    Returns:
        0 (not relevant), 1 (somewhat), 2 (highly relevant)
    """
    
    prompt = f"""You are an expert relevance judge for a news retrieval system.

    Query: "{query}"

    Article to evaluate:
    Title: {article_title}
    Preview: {article_preview[:1000]}

    Instructions - BE STRICT:

    Score 2 (highly relevant): Article's MAIN TOPIC directly addresses the query.
    - The headline/title clearly indicates the article is ABOUT the query topic
    - Most of the article content focuses on the query topic
    - Example: Query "Brexit referendum" → Article "Brexit Vote: UK Decides to Leave EU" = 2

    Score 1 (somewhat relevant): Article mentions the query topic but it's not the main focus.
    - Query topic appears in the article but is secondary or contextual
    - Article provides background/context for the query topic
    - Example: Query "Brexit referendum" → Article "EU Trade Policy Post-Brexit" = 1

    Score 0 (not relevant): Article doesn't meaningfully address the query.
    - Query terms appear but in unrelated context
    - Article is about a different topic entirely
    - Example: Query "Brexit referendum" → Article "Climate Change Summit 2023" = 0

    IMPORTANT for news articles:
    - Recent, breaking news about the topic = score 2
    - Historical context or tangential mentions = score 1
    - Same topic but different specific event = score 1 (not 2)
    Example: Query "Trump 2024 policies" → Article "Trump 2016 campaign" = 1

    When in doubt:
    - Between 1 and 2 → choose 1 (be conservative)
    - Between 0 and 1 → choose 0 (be strict)

    Respond with ONLY a single digit: 0, 1, or 2
    No explanation, no punctuation, just the number.
    """

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=10,
                timeout=30
            )
            
            # Parse response
            response_text = response.choices[0].message.content.strip()
            
            # Extract first digit
            import re
            match = re.search(r'[012]', response_text)
            
            if match:
                score = int(match.group())
                return score
            else:
                print(f"⚠️  Attempt {attempt+1}: Invalid response '{response_text}'")
                if attempt < max_retries - 1:
                    time.sleep(1)  # Brief delay before retry
                    continue
                else:
                    return 0  # Give up, return conservative default
                    
        except Exception as e:
            print(f"⚠️  Attempt {attempt+1}: Error {e}")
            if attempt < max_retries - 1:
                time.sleep(2)  # Longer delay for errors
                continue
            else:
                print(f"❌ All retries failed, defaulting to 0")
                return 0
    
    return 0  # Fallback


def auto_label_ground_truth(test_queries_file: str, output_file: str, manual_ground_truth_file: str = None):
    """Auto-label ground truth using LLM."""
    
    with open(test_queries_file) as f:
        test_queries = json.load(f)

    if manual_ground_truth_file and os.path.exists(manual_ground_truth_file):
        with open(manual_ground_truth_file) as f:
            manual_gt = json.load(f)
        
        manually_labeled_ids = {q["query_id"] for q in manual_gt}
        test_queries = [q for q in test_queries if q["id"] in manually_labeled_ids]
        
        print(f"\n✅ Filtering to {len(test_queries)} queries with manual labels")
        print(f"   (Skipping {len([q for q in test_queries if q['id'] not in manually_labeled_ids])} unlabeled queries)")
    
    ground_truth = []
    
    for query_data in test_queries:
        query = query_data["query"]
        print(f"\n{'='*80}")
        print(f"Query {query_data['id']}: {query}")
        print(f"{'='*80}")
        
        # Get candidates (same as your manual labeling)
        all_candidates = []
        
        # Dense search
        print("🔍 Fetching from dense vector search...")
        try:
            from langchain_pinecone import PineconeVectorStore
            from langchain_openai import OpenAIEmbeddings
            
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
                    "preview": doc.page_content[:500],
                })
            print(f"✅ Found {len(dense_docs)} from dense search")
        except Exception as e:
            print(f"⚠️ Dense search failed: {e}")
        
        # SQL search
        print("🗄️ Fetching from SQL search...")
        try:
            from baseline_systems import extract_keywords
            from capstone_tool_calling_agent import search_articles_by_multiple_keywords
            
            keywords = extract_keywords(query)
            print(f"   Keywords: {keywords}")
            
            if keywords:
                sql_response = search_articles_by_multiple_keywords.invoke({
                    "keywords": keywords,
                    "limit": 10
                })
                
                for article in sql_response.get("results", []):
                    all_candidates.append({
                        "article_id": article.get("article_id"),
                        "title": article.get("web_title"),
                        "preview": article.get("preview", "")[:500],
                    })
                print(f"✅ Found {len(sql_response.get('results', []))} from SQL")
        except Exception as e:
            print(f"⚠️ SQL search failed: {e}")
        
        # Deduplicate
        seen_ids = set()
        unique_candidates = []
        for c in all_candidates:
            if c["article_id"] not in seen_ids:
                seen_ids.add(c["article_id"])
                unique_candidates.append(c)
        
        print(f"\n📊 Labeling {len(unique_candidates)} unique candidates with GPT-4o-mini...")
        
        # LLM labeling
        relevance_scores = {}
        for i, candidate in enumerate(unique_candidates, 1):
            title_preview = candidate['title'][:60] + "..." if len(candidate['title']) > 60 else candidate['title']
            print(f"  [{i}/{len(unique_candidates)}] {title_preview}", end=" ")
            
            score = llm_label_relevance(
                query=query,
                article_title=candidate["title"],
                article_preview=candidate["preview"]
            )
            
            relevance_scores[candidate["article_id"]] = score
            print(f"→ {score}")
        
        # Save results
        relevant_count = len([s for s in relevance_scores.values() if s >= 1])
        highly_relevant_count = len([s for s in relevance_scores.values() if s == 2])
        
        ground_truth.append({
            "query_id": query_data["id"],
            "query": query,
            "category": query_data["category"],
            "relevance_scores": relevance_scores,
            "relevant_article_ids": [aid for aid, score in relevance_scores.items() if score >= 1],
            "highly_relevant_ids": [aid for aid, score in relevance_scores.items() if score == 2],
            "num_labeled": len(relevance_scores),
            "labeling_method": "llm_gpt4o_mini"
        })
        
        print(f"\n✅ Results: {relevant_count} relevant ({highly_relevant_count} highly relevant)")
    
    # Save
    with open(output_file, "w") as f:
        json.dump(ground_truth, f, indent=2)
    
    print(f"\n{'='*80}")
    print(f"✅ Auto-labeled ground truth saved to {output_file}")
    print(f"{'='*80}")
    
    # Summary
    total_labeled = sum(g["num_labeled"] for g in ground_truth)
    total_relevant = sum(len(g["relevant_article_ids"]) for g in ground_truth)
    total_highly = sum(len(g["highly_relevant_ids"]) for g in ground_truth)
    
    print(f"\n📊 Summary:")
    print(f"   Queries: {len(ground_truth)}")
    print(f"   Total articles labeled: {total_labeled}")
    print(f"   Relevant (score ≥1): {total_relevant}")
    print(f"   Highly relevant (score=2): {total_highly}")
    print(f"   Avg relevant per query: {total_relevant/len(ground_truth):.1f}")
    
    return ground_truth


def compare_labels(manual_file: str, llm_file: str):
    """Compare manual labels vs LLM labels."""
    
    print(f"\n{'='*80}")
    print("COMPARING MANUAL vs LLM LABELS")
    print(f"{'='*80}\n")
    
    # Load both
    with open(manual_file) as f:
        manual = json.load(f)
    
    with open(llm_file) as f:
        llm = json.load(f)
    
    # Compare
    agreements = []
    disagreements = []
    
    for m_query in manual:
        # Find matching LLM query
        l_query = next((q for q in llm if q["query_id"] == m_query["query_id"]), None)
        if not l_query:
            continue
        
        # Compare all articles that appear in both
        all_article_ids = set(m_query["relevance_scores"].keys()) | set(l_query["relevance_scores"].keys())
        
        for article_id in all_article_ids:
            m_score = m_query["relevance_scores"].get(article_id, 0)
            l_score = l_query["relevance_scores"].get(article_id, 0)
            
            if m_score == l_score:
                agreements.append((article_id, m_score))
            else:
                disagreements.append({
                    "query": m_query["query"],
                    "article_id": article_id,
                    "manual": m_score,
                    "llm": l_score,
                    "diff": abs(m_score - l_score)
                })
    
    # Calculate metrics
    total = len(agreements) + len(disagreements)
    if total == 0:
        print("❌ No overlapping articles found!")
        return
    
    agreement_rate = len(agreements) / total
    
    # Cohen's Kappa (more robust than simple agreement)
    from sklearn.metrics import cohen_kappa_score
    
    manual_scores = [m_score for _, m_score in agreements] + [d["manual"] for d in disagreements]
    llm_scores = [l_score for _, l_score in agreements] + [d["llm"] for d in disagreements]
    
    kappa = cohen_kappa_score(manual_scores, llm_scores)
    
    # Results
    print(f"📊 Agreement Statistics:")
    print(f"   Total compared: {total} article-query pairs")
    print(f"   Agreements: {len(agreements)} ({agreement_rate:.1%})")
    print(f"   Disagreements: {len(disagreements)} ({(1-agreement_rate):.1%})")
    print(f"   Cohen's Kappa: {kappa:.3f}", end="")
    
    if kappa > 0.8:
        print(" (🌟 Excellent agreement)")
    elif kappa > 0.6:
        print(" (✅ Good agreement)")
    elif kappa > 0.4:
        print(" (⚠️ Moderate agreement)")
    else:
        print(" (❌ Poor agreement)")
    
    # Show disagreements
    if disagreements:
        print(f"\n📋 Top 10 Disagreements (sorted by difference):")
        sorted_disagreements = sorted(disagreements, key=lambda x: -x["diff"])
        
        for i, d in enumerate(sorted_disagreements[:10], 1):
            print(f"\n{i}. Query: {d['query']}")
            print(f"   Article: {d['article_id'][:70]}...")
            print(f"   Manual: {d['manual']} | LLM: {d['llm']} (diff: {d['diff']})")
    
    print(f"\n{'='*80}")


def main():
    """Main execution function."""
    
    print("\n" + "="*80)
    print("🤖 LLM AUTO-LABELING FOR GROUND TRUTH")
    print("="*80)
    
    print("\nOptions:")
    print("  1. Auto-label queries with LLM (GPT-4o-mini)")
    print("  2. Compare manual vs LLM labels")
    print("  3. Both (auto-label + compare)")
    
    choice = input("\nYour choice (1/2/3): ").strip()
    
    if choice == "1":
        # Auto-label only
        print("\n⚠️ This will use GPT-4o-mini to label all candidates.")
        print("Estimated cost: ~$0.10-0.30 for 10 queries")
        
        confirm = input("\nContinue? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("❌ Cancelled")
            return
        
        auto_label_ground_truth(
            test_queries_file="ai_agents/test_queries.json",
            output_file="ai_agents/ground_truth_llm.json",
            manual_ground_truth_file="ai_agents/ground_truth.json"
        )
        
        print("\n💡 Next step: Run option 2 to compare with manual labels!")
    
    elif choice == "2":
        # Compare only
        if not os.path.exists("ai_agents/ground_truth_llm.json"):
            print("❌ Error: ground_truth_llm.json not found!")
            print("   Run option 1 first to generate LLM labels.")
            return
        
        compare_labels(
            manual_file="ai_agents/ground_truth.json",
            llm_file="ai_agents/ground_truth_llm.json"
        )
    
    elif choice == "3":
        # Both
        print("\n⚠️ This will:")
        print("  1. Auto-label with GPT-4o-mini")
        print("  2. Compare with your manual labels")
        print("\nEstimated cost: ~$0.10-0.30")
        
        confirm = input("\nContinue? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("❌ Cancelled")
            return
        
        # Step 1: Auto-label
        auto_label_ground_truth(
            test_queries_file="ai_agents/test_queries.json",
            output_file="ai_agents/ground_truth_llm.json"
        )
        
        # Step 2: Compare
        compare_labels(
            manual_file="ai_agents/ground_truth.json",
            llm_file="ai_agents/ground_truth_llm.json"
        )
    
    else:
        print("❌ Invalid choice")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user (Ctrl+C)")
        print("👋 Goodbye!")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()