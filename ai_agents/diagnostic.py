# diagnostic.py
import json
from baseline_systems import BaselineRetriever

# Load ground truth
with open("ai_agents/ground_truth.json") as f:
    ground_truth = json.load(f)

# Test query 1 (Brexit)
query_1 = ground_truth[0]
query = query_1["query"]

# UPDATED: Use 'relevant_article_ids' instead of 'relevance_scores'
gt_ids = set(query_1["relevant_article_ids"])  # ← Changed!

print(f"Query: {query}")
print(f"\nGround truth has {len(gt_ids)} relevant articles")
print("Sample relevant IDs:")
for aid in list(gt_ids)[:3]:
    print(f"  - {aid}")

# Also show dates from relevance_scores
print("\nGround truth article dates (first 3):")
for aid in list(gt_ids)[:3]:
    # Extract year from article ID (usually in format: section/YYYY/...)
    parts = aid.split('/')
    year = "Unknown"
    for part in parts:
        if part.isdigit() and len(part) == 4:
            year = part
            break
    print(f"  - {aid[:50]}... ({year})")

# Run SQL search
print("\n" + "="*80)
print("RUNNING SQL SEARCH...")
print("="*80)

retriever = BaselineRetriever()
sql_results, latency = retriever.retrieve_sql_only(query, k=10)  # Get 10 to see more

print(f"\nSQL returned {len(sql_results)} results in {latency:.0f}ms:")
print("\nFirst 5 results:")
for i, r in enumerate(sql_results[:5], 1):
    article_id = r['article_id']
    title = r['title'][:60] + "..." if len(r['title']) > 60 else r['title']
    date = r['metadata'].get('published_date', 'N/A')
    
    # Check if in ground truth
    in_gt = "✅ IN GROUND TRUTH" if article_id in gt_ids else "❌ NOT in GT"
    
    print(f"\n{i}. {in_gt}")
    print(f"   ID: {article_id}")
    print(f"   Title: {title}")
    print(f"   Date: {date}")

# Check overlap
print("\n" + "="*80)
print("OVERLAP ANALYSIS")
print("="*80)

sql_ids = {r['article_id'] for r in sql_results}
overlap = gt_ids & sql_ids

print(f"\n✅ Overlap: {len(overlap)}/{len(sql_results)} SQL results are relevant")
print(f"   SQL precision: {len(overlap)/len(sql_results)*100:.1f}%")

if overlap:
    print(f"\n🎉 SQL found these relevant articles:")
    for aid in overlap:
        print(f"   - {aid}")
else:
    print(f"\n❌ SQL found ZERO relevant articles!")

print(f"\n❌ SQL returned {len(sql_ids - gt_ids)} irrelevant articles")
print(f"❌ SQL missed {len(gt_ids - sql_ids)} relevant articles from ground truth")

# Show what SQL missed
print(f"\nSample articles SQL missed:")
for aid in list(gt_ids - sql_ids)[:3]:
    parts = aid.split('/')
    year = "Unknown"
    for part in parts:
        if part.isdigit() and len(part) == 4:
            year = part
            break
    print(f"   - {aid} ({year})")

print("\n" + "="*80)
print("KEYWORD ANALYSIS")
print("="*80)

from baseline_systems import extract_keywords

keywords = extract_keywords(query)
print(f"\nExtracted keywords: {keywords}")

# Check if ground truth articles contain these keywords
print(f"\n🔍 Checking if ground truth article IDs contain keywords:")

for aid in list(gt_ids)[:5]:  # Check first 5
    aid_lower = aid.lower()
    title_lower = ""  # We don't have titles in ground truth
    
    print(f"\n   Article ID: {aid}")
    
    # Check keyword presence in article ID
    found_keywords = [kw for kw in keywords if kw in aid_lower]
    
    if found_keywords:
        print(f"      ✅ Contains: {found_keywords}")
    else:
        print(f"      ❌ Contains NONE of the keywords!")
        print(f"      💡 This explains why SQL can't find it")

print("\n" + "="*80)
print("💡 INSIGHT")
print("="*80)
print("""
If ground truth articles DON'T contain the keywords:
→ This is EXPECTED behavior - Dense search uses semantic matching
→ SQL can only find lexical matches
→ Your hybrid system should rely more on Dense than SQL for these queries

If ground truth articles DO contain keywords:
→ There's a bug in the SQL query or Snowflake view
→ Need to investigate the SQL LIKE patterns
""")

print("\n" + "="*80)
print("COMPARING DENSE vs SQL")
print("="*80)

# Run dense search
dense_results, _ = retriever.retrieve_naive_rag(query, k=10)
dense_ids = {r['article_id'] for r in dense_results}

print(f"\nDense found {len(dense_ids)} articles")
print(f"SQL found {len(sql_ids)} articles")

# Check overlap
overlap_dense_sql = dense_ids & sql_ids
print(f"\n✅ Overlap: {len(overlap_dense_sql)} articles found by BOTH")

# Articles ONLY found by SQL (SQL's unique contribution)
sql_only = sql_ids - dense_ids
print(f"\n🔍 SQL found {len(sql_only)} articles that dense MISSED:")
for aid in list(sql_only)[:3]:
    print(f"   - {aid}")

# Articles ONLY found by dense
dense_only = dense_ids - sql_ids
print(f"\n🧠 Dense found {len(dense_only)} articles that SQL MISSED:")
for aid in list(dense_only)[:3]:
    print(f"   - {aid}")

# Check if SQL's unique contributions are relevant
sql_unique_relevant = sql_only & gt_ids
if sql_unique_relevant:
    print(f"\n🎉 SQL found {len(sql_unique_relevant)} unique relevant articles:")
    for aid in sql_unique_relevant:
        print(f"   ✅ {aid}")
else:
    print(f"\n⚠️  SQL's unique articles ({len(sql_only)}) are not in ground truth")
    print(f"   This suggests SQL is finding different (possibly irrelevant) articles")