# ai_agents/evaluation.py
"""
Comprehensive evaluation framework for RAG systems.

Evaluates 4 retrieval methods:
1. Naive RAG (Dense only)
2. SQL-only (Keyword only)
3. Hybrid without re-ranking
4. Hybrid with re-ranking (your system)

Metrics:
- Precision@K
- Recall@K
- Mean Reciprocal Rank (MRR)
- Normalized Discounted Cumulative Gain (NDCG@K)
- Average Latency

FIXED: NDCG calculation now properly normalized to max 1.0
"""

import json
import os
from typing import List, Dict, Tuple, Callable
import numpy as np
from baseline_systems import BaselineRetriever

class RAGEvaluator:
    """Evaluate RAG system performance against ground truth labels."""
    
    def __init__(self, ground_truth_file: str = "ai_agents/ground_truth.json"):
        """
        Initialize evaluator with ground truth labels.
        
        Args:
            ground_truth_file: Path to ground truth JSON file
        """
        self.ground_truth = self.load_ground_truth(ground_truth_file)
        self.retriever = BaselineRetriever()
        
        print(f"✅ Loaded {len(self.ground_truth)} labeled queries")
    
    def load_ground_truth(self, filepath: str) -> List[Dict]:
        """Load ground truth labels from JSON file."""
        if not os.path.exists(filepath):
            raise FileNotFoundError(
                f"Ground truth file not found: {filepath}\n"
                f"Please run label_ground_truth.py first to create labels."
            )
        
        with open(filepath, "r") as f:
            data = json.load(f)
        
        # Validate structure
        for item in data:
            required_fields = ["query_id", "query", "relevance_scores"]
            for field in required_fields:
                if field not in item:
                    raise ValueError(f"Missing required field: {field}")
        
        return data
    
    def calculate_precision_at_k(
        self, 
        retrieved_ids: List[str], 
        relevant_ids: List[str], 
        k: int = 5
    ) -> float:
        """
        Calculate Precision@K.
        
        Precision@K = (# relevant docs in top K) / K
        
        Args:
            retrieved_ids: List of retrieved article IDs (in rank order)
            relevant_ids: List of ground truth relevant article IDs
            k: Number of top results to consider
        
        Returns:
            Precision@K score (0-1)
        """
        if k == 0:
            return 0.0
        
        top_k = retrieved_ids[:k]
        relevant_in_top_k = len(set(top_k) & set(relevant_ids))
        
        return relevant_in_top_k / k
    
    def calculate_recall_at_k(
        self, 
        retrieved_ids: List[str], 
        relevant_ids: List[str], 
        k: int = 5
    ) -> float:
        """
        Calculate Recall@K.
        
        Recall@K = (# relevant docs in top K) / (total # relevant docs)
        
        Args:
            retrieved_ids: List of retrieved article IDs (in rank order)
            relevant_ids: List of ground truth relevant article IDs
            k: Number of top results to consider
        
        Returns:
            Recall@K score (0-1)
        """
        if len(relevant_ids) == 0:
            return 0.0
        
        top_k = retrieved_ids[:k]
        relevant_in_top_k = len(set(top_k) & set(relevant_ids))
        
        return relevant_in_top_k / len(relevant_ids)
    
    def calculate_mrr(
        self, 
        retrieved_ids: List[str], 
        relevant_ids: List[str]
    ) -> float:
        """
        Calculate Mean Reciprocal Rank.
        
        MRR = 1 / (rank of first relevant document)
        
        This measures how quickly users find a relevant result.
        
        Args:
            retrieved_ids: List of retrieved article IDs (in rank order)
            relevant_ids: List of ground truth relevant article IDs
        
        Returns:
            MRR score (0-1)
        """
        for i, doc_id in enumerate(retrieved_ids, 1):
            if doc_id in relevant_ids:
                return 1.0 / i
        
        return 0.0

    def calculate_ndcg_at_k(
        self, 
        retrieved_ids: List[str], 
        relevance_scores: Dict[str, int], 
        k: int = 5
    ) -> float:
        """NDCG for sparse ground truth - only uses labeled relevant docs."""
        
        # Get relevances for retrieved (unlabeled = 0)
        retrieved_relevances = []
        for doc_id in retrieved_ids[:k]:
            relevance = relevance_scores.get(doc_id, 0)
            retrieved_relevances.append(relevance)
        
        # Pad to K
        while len(retrieved_relevances) < k:
            retrieved_relevances.append(0)
        
        # Calculate DCG
        dcg = 0.0
        for i, relevance in enumerate(retrieved_relevances, 1):
            dcg += (2**relevance - 1) / np.log2(i + 1)
        
        # CRITICAL: Only use RELEVANT docs (score > 0) for ideal
        relevant_scores = [s for s in relevance_scores.values() if s > 0]
        
        if not relevant_scores:
            return 0.0
        
        # Ideal ranking from relevant docs only
        ideal_relevances = sorted(relevant_scores, reverse=True)[:k]
        while len(ideal_relevances) < k:
            ideal_relevances.append(0)
        
        # Calculate iDCG
        idcg = 0.0
        for i, relevance in enumerate(ideal_relevances, 1):
            idcg += (2**relevance - 1) / np.log2(i + 1)
        
        if idcg == 0:
            return 0.0
        
        return min(dcg / idcg, 1.0)  # Cap at 1.0 just in case
    
    def evaluate_single_query(
        self, 
        query: str,
        retrieved_ids: List[str],
        relevance_scores: Dict[str, int],
        k: int = 5
    ) -> Dict[str, float]:
        """
        Evaluate a single query across all metrics.
        
        Args:
            query: Query string
            retrieved_ids: Retrieved article IDs
            relevance_scores: Ground truth relevance scores
            k: Number of top results to evaluate
        
        Returns:
            Dict of metric scores
        """
        # Get list of relevant IDs (score >= 1)
        relevant_ids = [aid for aid, score in relevance_scores.items() if score >= 1]
        
        metrics = {
            "precision@k": self.calculate_precision_at_k(retrieved_ids, relevant_ids, k),
            "recall@k": self.calculate_recall_at_k(retrieved_ids, relevant_ids, k),
            "mrr": self.calculate_mrr(retrieved_ids, relevant_ids),
            "ndcg@k": self.calculate_ndcg_at_k(retrieved_ids, relevance_scores, k)
        }
        
        return metrics
    
    def evaluate_retrieval_method(
        self, 
        method_name: str,
        retrieval_func: Callable,
        k: int = 5,
        verbose: bool = True
    ) -> Dict:
        """
        Evaluate a retrieval method across all ground truth queries.
        
        Args:
            method_name: Name of method (for display)
            retrieval_func: Function that takes (query, k) and returns (results, latency)
            k: Number of top results to evaluate
            verbose: Whether to print progress
        
        Returns:
            Dictionary of aggregated metrics
        """
        if verbose:
            print(f"\n{'='*80}")
            print(f"Evaluating: {method_name}")
            print(f"{'='*80}")
        
        all_metrics = {
            "precision@k": [],
            "recall@k": [],
            "mrr": [],
            "ndcg@k": [],
            "latency_ms": []
        }
        
        query_results = []
        
        for i, test_case in enumerate(self.ground_truth, 1):
            query = test_case["query"]
            relevance_scores = test_case["relevance_scores"]
            
            if verbose and i % 5 == 0:
                print(f"  Progress: {i}/{len(self.ground_truth)} queries...")
            
            # Run retrieval
            try:
                results, latency = retrieval_func(query, k=k)
                
                # Extract article IDs
                retrieved_ids = [r["article_id"] for r in results if r.get("article_id")]
                
                # Calculate metrics for this query
                query_metrics = self.evaluate_single_query(
                    query, 
                    retrieved_ids, 
                    relevance_scores, 
                    k
                )
                
                # Store metrics
                for metric_name, score in query_metrics.items():
                    all_metrics[metric_name].append(score)
                
                all_metrics["latency_ms"].append(latency)
                
                # Store detailed results
                query_results.append({
                    "query": query,
                    "query_id": test_case.get("query_id"),
                    "retrieved_ids": retrieved_ids,
                    "metrics": query_metrics,
                    "latency_ms": latency
                })
                
            except Exception as e:
                print(f"  ⚠️ Error on query '{query}': {e}")
                # Add zeros for failed queries
                for metric_name in all_metrics:
                    all_metrics[metric_name].append(0.0)
        
        # Calculate aggregated metrics
        aggregated = {
            f"precision@{k}": np.mean(all_metrics["precision@k"]),
            f"recall@{k}": np.mean(all_metrics["recall@k"]),
            "mrr": np.mean(all_metrics["mrr"]),
            f"ndcg@{k}": np.mean(all_metrics["ndcg@k"]),
            "avg_latency_ms": np.mean(all_metrics["latency_ms"]),
            "std_latency_ms": np.std(all_metrics["latency_ms"]),
            "num_queries": len(self.ground_truth),
            "method": method_name
        }
        
        if verbose:
            self._print_results(aggregated, k)
        
        return {
            "aggregated": aggregated,
            "per_query": query_results
        }
    
    def _print_results(self, results: Dict, k: int):
        """Print formatted evaluation results with metric explanations."""
        print(f"\n📊 Results:")
    
        # Precision
        precision = results[f'precision@{k}']
        print(f"  Precision@{k}:      {precision:.3f}", end="")
        if precision >= 0.9:
            print(" ✅ Excellent - nearly all results are relevant")
        elif precision >= 0.7:
            print(" ✅ Good - most results are relevant")
        elif precision >= 0.5:
            print(" ⚠️  Fair - half of results are relevant")
        else:
            print(" ❌ Poor - many irrelevant results")
    
        # Recall
        recall = results[f'recall@{k}']
        print(f"  Recall@{k}:         {recall:.3f}", end="")
        if recall >= 0.7:
            print(f" ✅ Excellent - found {recall*100:.0f}% of all relevant docs")
        elif recall >= 0.5:
            print(f" ✅ Good - found {recall*100:.0f}% of all relevant docs")
        elif recall >= 0.3:
            print(f" ⚠️  Fair - found {recall*100:.0f}% of all relevant docs")
        else:
            print(f" ❌ Poor - found only {recall*100:.0f}% of all relevant docs")
    
        # MRR
        mrr = results['mrr']
        print(f"  MRR:               {mrr:.3f}", end="")
        if mrr >= 0.9:
            print(" ✅ Excellent - first result almost always relevant")
        elif mrr >= 0.7:
            print(" ✅ Good - first result usually relevant")
        elif mrr >= 0.5:
            print(" ⚠️  Fair - first relevant result around position 2")
        else:
            avg_position = int(1/mrr) if mrr > 0 else 0
            print(f" ❌ Poor - first relevant result around position {avg_position}")
    
        # NDCG
        ndcg = results[f'ndcg@{k}']
        print(f"  NDCG@{k}:          {ndcg:.3f}", end="")
        if ndcg > 1.0:
            print(" ❌ ERROR - NDCG cannot exceed 1.0! Bug in calculation.")
        elif ndcg >= 0.9:
            print(f" ✅ Excellent - ranking is {ndcg*100:.0f}% of perfect")
        elif ndcg >= 0.7:
            print(f" ✅ Good - ranking is {ndcg*100:.0f}% of perfect")
        elif ndcg >= 0.5:
            print(f" ⚠️  Fair - ranking is {ndcg*100:.0f}% of perfect")
        else:
            print(f" ❌ Poor - ranking needs improvement")
    
        # Latency
        print(f"  Avg Latency:       {results['avg_latency_ms']:.1f}ms (±{results['std_latency_ms']:.1f}ms)")
    
        # Query count
        print(f"  Queries Evaluated: {results['num_queries']}")

    def print_metric_glossary(self, k: int = 5):
        """Print explanation of all metrics."""
        print("\n" + "="*80)
        print("📚 METRIC GLOSSARY")
        print("="*80)
    
        print(f"\n🎯 Precision@{k}:")
        print(f"   → What: Of the top {k} results, how many are relevant?")
        print(f"   → Formula: (# relevant in top-{k}) / {k}")
        print(f"   → Perfect: 1.000 (100% of results are relevant)")
        print(f"   → User Impact: High precision = users trust your system")
    
        print(f"\n🔍 Recall@{k}:")
        print(f"   → What: Of all relevant documents, how many did we find in top-{k}?")
        print(f"   → Formula: (# relevant in top-{k}) / (total # relevant docs)")
        print(f"   → Perfect: 1.000 (found all relevant documents)")
        print(f"   → User Impact: High recall = comprehensive coverage")
        
        print(f"\n⚡ MRR (Mean Reciprocal Rank):")
        print(f"   → What: How quickly does user find first relevant result?")
        print(f"   → Formula: Average of (1 / position of first relevant result)")
        print(f"   → Perfect: 1.000 (first result always relevant)")
        print(f"   → User Impact: High MRR = users get answers immediately")
        
        print(f"\n🏆 NDCG@{k} (Normalized Discounted Cumulative Gain):")
        print(f"   → What: Are highly relevant results ranked higher than somewhat relevant?")
        print(f"   → Formula: Quality-weighted ranking score / ideal ranking")
        print(f"   → Perfect: 1.000 (optimal ranking of results)")
        print(f"   → Range: 0.0 to 1.0 (cannot exceed 1.0)")
        print(f"   → User Impact: High NDCG = best results appear first")
        
        print(f"\n⏱️  Latency:")
        print(f"   → What: How long does retrieval take?")
        print(f"   → Target: <2000ms for good UX, <5000ms acceptable")
        print(f"   → Trade-off: Higher quality often means higher latency")

    def _print_metric_summary(self, results: List[Dict], k: int):
        """Print human-readable summary of what metrics mean for best system."""
        # Find best system (should be last one - Hybrid + Rerank)
        best = results[-1]
        
        print("\n" + "="*80)
        print("💡 WHAT THESE METRICS MEAN FOR YOUR SYSTEM")
        print("="*80)
        
        precision = best[f'precision@{k}']
        recall = best[f'recall@{k}']
        mrr = best['mrr']
        ndcg = best[f'ndcg@{k}']
        
        print(f"\n✅ Precision@{k}: {precision:.3f} ({precision*100:.0f}%)")
        if precision == 1.0:
            print(f"   → Perfect! Every result shown to users is relevant")
        elif precision >= 0.9:
            print(f"   → Excellent! {precision*100:.0f}% of results are relevant")
        else:
            print(f"   → {precision*100:.0f}% of top-{k} results are relevant")
        print(f"   → User experience: {'🌟 Users trust your results' if precision >= 0.9 else '⚠️  Some irrelevant results'}")
        
        print(f"\n✅ Recall@{k}: {recall:.3f} ({recall*100:.0f}%)")
        print(f"   → Found {recall*100:.0f}% of all relevant documents in just top-{k} results")
        if recall >= 0.6:
            print(f"   → Excellent coverage for K={k}")
        else:
            print(f"   → Could improve by returning more results (K={k*2})")
        print(f"   → User experience: {'🌟 Comprehensive answers' if recall >= 0.5 else '⚠️  Might miss some relevant docs'}")
        
        print(f"\n✅ MRR: {mrr:.3f}")
        if mrr == 1.0:
            print(f"   → Perfect! First result is always relevant")
        elif mrr >= 0.8:
            print(f"   → First relevant result typically at position 1-2")
        else:
            avg_pos = int(1/mrr) if mrr > 0 else 0
            print(f"   → First relevant result typically at position {avg_pos}")
        print(f"   → User experience: {'🌟 Immediate answers' if mrr >= 0.9 else '⚠️  Users must scroll'}")
        
        print(f"\n✅ NDCG@{k}: {ndcg:.3f} ({ndcg*100:.0f}% of perfect)")
        print(f"   → Ranking quality is {ndcg*100:.0f}% as good as theoretically optimal")
        if ndcg >= 0.95:
            print(f"   → Nearly perfect! Highly relevant results ranked first")
        elif ndcg >= 0.8:
            print(f"   → Very good ranking - best results near the top")
        else:
            print(f"   → Ranking could be improved with better re-ranking")
        print(f"   → User experience: {'🌟 Best results appear first' if ndcg >= 0.9 else '⚠️  Some shuffling needed'}")
        
        print(f"\n⏱️  Latency: {best['avg_latency_ms']:.0f}ms")
        if best['avg_latency_ms'] < 2000:
            print(f"   → Excellent! Fast enough for real-time use")
        elif best['avg_latency_ms'] < 5000:
            print(f"   → Good! Acceptable for production")
        else:
            print(f"   → Slow - consider optimization")
        
        print("\n" + "="*80)
        print("🎯 OVERALL ASSESSMENT")
        print("="*80)
        
        if precision >= 0.95 and mrr >= 0.95 and ndcg >= 0.9:
            print("🌟 EXCELLENT: Production-ready system with near-perfect performance!")
        elif precision >= 0.8 and recall >= 0.5:
            print("✅ GOOD: Strong performance suitable for most use cases")
        elif precision >= 0.6:
            print("⚠️  FAIR: Acceptable but could benefit from improvements")
        else:
            print("❌ NEEDS WORK: Significant improvements needed")
    
    def compare_all_methods(self, k: int = 5, save_results: bool = True) -> List[Dict]:
        """
        Evaluate all 4 baseline methods and compare.
        
        Args:
            k: Number of top results to evaluate
            save_results: Whether to save results to JSON file
        
        Returns:
            List of aggregated results for each method
        """
        print("\n" + "="*80)
        print("COMPREHENSIVE RAG SYSTEM EVALUATION")
        print("="*80)
        print(f"Ground truth queries: {len(self.ground_truth)}")
        print(f"Evaluation metric: Top-{k} results")
        print("="*80)
        
        methods = [
            ("Naive RAG (Dense Search)", self.retriever.retrieve_naive_rag),
            ("SQL Only (Lexical Search)", self.retriever.retrieve_sql_only),
            ("Hybrid (No Rerank)", self.retriever.retrieve_hybrid_no_rerank),
            ("Hybrid + Rerank (Your System)", self.retriever.retrieve_hybrid_with_rerank),
        ]
        
        all_results = []
        
        for method_name, method_func in methods:
            result = self.evaluate_retrieval_method(
                method_name,
                method_func,
                k=k,
                verbose=True
            )
            all_results.append(result["aggregated"])
        
        # Save results
        if save_results:
            output_file = "ai_agents/evaluation_results.json"
            with open(output_file, "w") as f:
                json.dump(all_results, f, indent=2)
            print(f"\n✅ Results saved to {output_file}")
        
        # Print comparison summary
        self._print_comparison_table(all_results, k)

        self.print_metric_glossary(k)

        self._print_metric_summary(all_results, k)
        
        return all_results
    
    def _print_comparison_table(self, results: List[Dict], k: int):
        """Print formatted comparison table."""
        print("\n" + "="*80)
        print("COMPARISON TABLE")
        print("="*80)
        
        # Table header
        metrics = [f"precision@{k}", f"recall@{k}", "mrr", f"ndcg@{k}", "avg_latency_ms"]
        metric_headers = [f"P@{k}", f"R@{k}", "MRR", f"NDCG@{k}", "Latency(ms)"]
        
        # Print header
        print(f"\n{'Method':<30} {' '.join(f'{h:>12}' for h in metric_headers)}")
        print("-" * 80)
        
        # Print each method
        for result in results:
            method_name = result["method"][:28]  # Truncate long names
            
            values = []
            for metric in metrics:
                if metric == "avg_latency_ms":
                    values.append(f"{result[metric]:>12.1f}")
                else:
                    values.append(f"{result[metric]:>12.3f}")
            
            print(f"{method_name:<30} {' '.join(values)}")
        
        # Print improvements
        if len(results) >= 4:
            print("\n" + "="*80)
            print("IMPROVEMENTS vs Naive RAG")
            print("="*80)
            
            baseline = results[0]  # Naive RAG
            your_system = results[3]  # Hybrid + Rerank
            
            for metric in metrics:
                if metric == "avg_latency_ms":
                    continue  # Skip latency for improvement
                
                baseline_val = baseline[metric]
                your_val = your_system[metric]
                
                if baseline_val > 0:
                    improvement = ((your_val - baseline_val) / baseline_val) * 100
                    
                    display_name = metric.replace("@", " @ ").upper()
                    print(f"{display_name:<20} {baseline_val:.3f} → {your_val:.3f} ({improvement:+.1f}%)")


# ==================== MAIN EXECUTION ====================

def main():
    """Main evaluation workflow."""
    
    # Check if ground truth exists
    if not os.path.exists("ai_agents/ground_truth_llm.json"):
        print("\n❌ Error: Ground truth file not found!")
        print("\n💡 Next steps:")
        print("  1. Run: python label_ground_truth.py")
        print("  2. Label at least 10-15 queries")
        print("  3. Then run this evaluation script")
        return
    
    # Initialize evaluator
    evaluator = RAGEvaluator(ground_truth_file="ai_agents/ground_truth_llm.json")
    
    # Run comparison
    results = evaluator.compare_all_methods(k=5, save_results=True)
    
    print("\n" + "="*80)
    print("EVALUATION COMPLETE")
    print("="*80)
    print("\n💡 Next steps:")
    print("  1. Review evaluation_results.json for detailed metrics")
    print("  2. Run: python visualize_results.py to create charts")
    print("  3. Include results in your capstone presentation")


if __name__ == "__main__":
    main()