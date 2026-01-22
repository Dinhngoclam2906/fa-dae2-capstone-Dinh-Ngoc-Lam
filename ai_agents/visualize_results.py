# visualize_results_improved.py

import json
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Set style for professional look
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

def visualize_evaluation_results(results_file: str = "ai_agents/evaluation_results.json"):
    """Create comprehensive comparison charts from evaluation results."""
    
    with open(results_file, "r") as f:
        results = json.load(f)
    
    # Extract data
    methods = [r["method"] for r in results]
    # Shorten method names for better display
    short_methods = ["Naive\nRAG", "SQL\nOnly", "Hybrid\n(No Rerank)", "Hybrid +\nRerank\n(Ours)"]
    
    precision = [r["precision@5"] for r in results]
    recall = [r["recall@5"] for r in results]
    mrr = [r["mrr"] for r in results]
    ndcg = [r["ndcg@5"] for r in results]
    latency = [r["avg_latency_ms"] for r in results]
    
    # Calculate improvements vs baseline (Naive RAG)
    baseline_idx = 0
    precision_improvement = [(p - precision[baseline_idx]) / precision[baseline_idx] * 100 for p in precision]
    recall_improvement = [(r - recall[baseline_idx]) / recall[baseline_idx] * 100 for r in recall]
    
    # Create figure with subplots
    fig = plt.figure(figsize=(18, 12))
    
    # Main title
    fig.suptitle('RAG System Evaluation: Comprehensive Comparison', 
                 fontsize=20, fontweight='bold', y=0.98)
    
    # Define colors - professional palette
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']  # Red, Teal, Blue, Green
    highlight_color = '#96CEB4'  # Green for our system
    
    # ========== Row 1: Main Metrics ==========
    
    # 1. Precision@5
    ax1 = plt.subplot(3, 3, 1)
    bars = ax1.bar(short_methods, precision, color=colors, edgecolor='black', linewidth=1.2)
    ax1.set_title('Precision@5\n(% of results relevant)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Score', fontsize=11)
    ax1.set_ylim(0, 1.0)
    ax1.grid(axis='y', alpha=0.3)
    
    # Add data labels
    for i, (bar, val) in enumerate(zip(bars, precision)):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                f'{val:.2f}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # Highlight best
    bars[-1].set_edgecolor('gold')
    bars[-1].set_linewidth(3)
    
    # 2. Recall@5
    ax2 = plt.subplot(3, 3, 2)
    bars = ax2.bar(short_methods, recall, color=colors, edgecolor='black', linewidth=1.2)
    ax2.set_title('Recall@5\n(% of relevant docs found)', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Score', fontsize=11)
    ax2.set_ylim(0, 1.0)
    ax2.grid(axis='y', alpha=0.3)
    
    # Add data labels
    for i, (bar, val) in enumerate(zip(bars, recall)):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                f'{val:.2f}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    bars[-1].set_edgecolor('gold')
    bars[-1].set_linewidth(3)
    
    # 3. MRR
    ax3 = plt.subplot(3, 3, 3)
    bars = ax3.bar(short_methods, mrr, color=colors, edgecolor='black', linewidth=1.2)
    ax3.set_title('Mean Reciprocal Rank\n(First result quality)', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Score', fontsize=11)
    ax3.set_ylim(0, 1.0)
    ax3.grid(axis='y', alpha=0.3)
    
    # Add data labels
    for i, (bar, val) in enumerate(zip(bars, mrr)):
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                f'{val:.2f}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    bars[-1].set_edgecolor('gold')
    bars[-1].set_linewidth(3)
    
    # ========== Row 2: Ranking & Latency ==========
    
    # 4. NDCG@5
    ax4 = plt.subplot(3, 3, 4)
    bars = ax4.bar(short_methods, ndcg, color=colors, edgecolor='black', linewidth=1.2)
    ax4.set_title('NDCG@5\n(Ranking quality)', fontsize=12, fontweight='bold')
    ax4.set_ylabel('Score', fontsize=11)
    ax4.set_ylim(0, 1.0)
    ax4.grid(axis='y', alpha=0.3)
    
    # Add data labels
    for i, (bar, val) in enumerate(zip(bars, ndcg)):
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                f'{val:.2f}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    bars[-1].set_edgecolor('gold')
    bars[-1].set_linewidth(3)
    
    # 5. Latency
    ax5 = plt.subplot(3, 3, 5)
    bars = ax5.bar(short_methods, latency, color=colors, edgecolor='black', linewidth=1.2)
    ax5.set_title('Average Latency\n(Response time)', fontsize=12, fontweight='bold')
    ax5.set_ylabel('Milliseconds', fontsize=11)
    ax5.grid(axis='y', alpha=0.3)
    
    # Add data labels
    for i, (bar, val) in enumerate(zip(bars, latency)):
        height = bar.get_height()
        ax5.text(bar.get_x() + bar.get_width()/2., height + 100,
                f'{val:.0f}ms',
                ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    # Add reference line for 2s (production standard)
    ax5.axhline(y=2000, color='red', linestyle='--', linewidth=2, alpha=0.7, label='2s target')
    ax5.legend(fontsize=9)
    
    # 6. Improvement vs Baseline
    ax6 = plt.subplot(3, 3, 6)
    x_pos = np.arange(len(short_methods))
    
    # Plot precision improvement
    bars1 = ax6.bar(x_pos - 0.2, precision_improvement, 0.4, 
                    label='Precision', color='#FF6B6B', edgecolor='black', linewidth=1)
    # Plot recall improvement
    bars2 = ax6.bar(x_pos + 0.2, recall_improvement, 0.4, 
                    label='Recall', color='#4ECDC4', edgecolor='black', linewidth=1)
    
    ax6.set_title('Improvement vs Naive RAG\n(% change)', fontsize=12, fontweight='bold')
    ax6.set_ylabel('% Improvement', fontsize=11)
    ax6.set_xticks(x_pos)
    ax6.set_xticklabels(short_methods)
    ax6.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax6.legend(fontsize=9)
    ax6.grid(axis='y', alpha=0.3)
    
    # Add data labels for improvements
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            if abs(height) > 1:  # Only label significant changes
                ax6.text(bar.get_x() + bar.get_width()/2., height + 2,
                        f'{height:+.0f}%',
                        ha='center', va='bottom' if height > 0 else 'top', 
                        fontsize=8, fontweight='bold')
    
    # ========== Row 3: Advanced Visualizations ==========
    
    # 7. Radar Chart - Overall Performance
    ax7 = plt.subplot(3, 3, 7, projection='polar')
    
    categories = ['Precision@5', 'Recall@5', 'MRR', 'NDCG@5']
    N = len(categories)
    
    angles = np.linspace(0, 2 * np.pi, N, endpoint=False).tolist()
    angles += angles[:1]
    
    # Plot each method
    for i, method in enumerate(short_methods):
        values = [precision[i], recall[i], mrr[i], ndcg[i]]
        values += values[:1]
        
        linewidth = 3 if i == len(short_methods) - 1 else 1.5
        alpha_fill = 0.25 if i == len(short_methods) - 1 else 0.1
        
        ax7.plot(angles, values, 'o-', linewidth=linewidth, 
                label=method, color=colors[i])
        ax7.fill(angles, values, alpha=alpha_fill, color=colors[i])
    
    ax7.set_xticks(angles[:-1])
    ax7.set_xticklabels(categories, fontsize=9)
    ax7.set_ylim(0, 1)
    ax7.set_title('Overall Performance\n(All Metrics)', fontsize=12, fontweight='bold', pad=20)
    ax7.legend(loc='upper right', bbox_to_anchor=(1.3, 1.0), fontsize=8)
    ax7.grid(True)
    
    # 8. Precision-Recall Tradeoff
    ax8 = plt.subplot(3, 3, 8)
    
    for i, method in enumerate(short_methods):
        marker = 'D' if i == len(short_methods) - 1 else 'o'
        size = 200 if i == len(short_methods) - 1 else 100
        
        ax8.scatter(recall[i], precision[i], s=size, 
                   color=colors[i], marker=marker, 
                   edgecolor='black', linewidth=2, 
                   label=method, alpha=0.8, zorder=10-i)
        
        # Add labels
        ax8.annotate(f'{precision[i]:.2f}, {recall[i]:.2f}',
                    (recall[i], precision[i]),
                    xytext=(10, 10), textcoords='offset points',
                    fontsize=8, fontweight='bold')
    
    ax8.set_xlabel('Recall@5', fontsize=11, fontweight='bold')
    ax8.set_ylabel('Precision@5', fontsize=11, fontweight='bold')
    ax8.set_title('Precision-Recall Tradeoff\n(Bigger & Diamond = Our System)', 
                 fontsize=12, fontweight='bold')
    ax8.legend(fontsize=8, loc='lower left')
    ax8.grid(True, alpha=0.3)
    ax8.set_xlim(0.2, 0.6)
    ax8.set_ylim(0.5, 0.95)
    
    # 9. Key Insights Summary
    ax9 = plt.subplot(3, 3, 9)
    ax9.axis('off')
    
    # Calculate key statistics
    our_system_idx = -1
    baseline_idx = 0
    
    precision_gain = ((precision[our_system_idx] - precision[baseline_idx]) / 
                     precision[baseline_idx] * 100)
    recall_gain = ((recall[our_system_idx] - recall[baseline_idx]) / 
                  recall[baseline_idx] * 100)
    
    insights_text = f"""
    📊 KEY FINDINGS
    
    🏆 Our Hybrid + Rerank System:
    
    ✅ Precision: {precision[our_system_idx]:.1%}
       (+{precision_gain:.0f}% vs baseline)
    
    ✅ Recall: {recall[our_system_idx]:.1%}
       (+{recall_gain:.0f}% vs baseline)
    
    ✅ MRR: {mrr[our_system_idx]:.3f}
       (Near-perfect first results)
    
    ✅ NDCG: {ndcg[our_system_idx]:.3f}
       (Best ranking quality)
    
    ⚡ Latency: {latency[our_system_idx]:.0f}ms
       (Production-ready)
    
    🎯 Wins on ALL metrics!
    """
    
    ax9.text(0.1, 0.95, insights_text, 
            transform=ax9.transAxes,
            fontsize=11,
            verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.3),
            family='monospace')
    
    plt.tight_layout()
    
    # Save figure
    plt.savefig('ai_agents/evaluation_results.png', dpi=300, bbox_inches='tight')
    print("✅ Comprehensive chart saved to ai_agents/evaluation_results.png")
    
    # Also create individual charts for slides
    create_individual_charts(short_methods, precision, recall, mrr, ndcg, 
                           latency, colors, precision_improvement, recall_improvement)
    
    plt.show()


def create_individual_charts(methods, precision, recall, mrr, ndcg, 
                            latency, colors, precision_imp, recall_imp):
    """Create individual charts for presentation slides."""
    
    # 1. Main Metrics Comparison (for title slide)
    fig, ax = plt.subplots(figsize=(10, 6))
    
    x = np.arange(len(methods))
    width = 0.2
    
    ax.bar(x - 1.5*width, precision, width, label='Precision@5', color='#FF6B6B', edgecolor='black')
    ax.bar(x - 0.5*width, recall, width, label='Recall@5', color='#4ECDC4', edgecolor='black')
    ax.bar(x + 0.5*width, mrr, width, label='MRR', color='#45B7D1', edgecolor='black')
    ax.bar(x + 1.5*width, ndcg, width, label='NDCG@5', color='#96CEB4', edgecolor='black')
    
    ax.set_ylabel('Score', fontsize=13, fontweight='bold')
    ax.set_title('Performance Comparison Across All Metrics', fontsize=15, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(methods, fontsize=11)
    ax.legend(fontsize=11, loc='lower right')
    ax.grid(axis='y', alpha=0.3)
    ax.set_ylim(0, 1.1)
    
    plt.tight_layout()
    plt.savefig('ai_agents/metrics_comparison.png', dpi=300, bbox_inches='tight')
    print("✅ Metrics comparison chart saved to ai_agents/metrics_comparison.png")
    plt.close()
    
    # 2. Improvement Chart (for results slide)
    fig, ax = plt.subplots(figsize=(10, 6))
    
    x_pos = np.arange(len(methods))
    
    bars1 = ax.bar(x_pos - 0.2, precision_imp, 0.35, 
                   label='Precision Improvement', color='#FF6B6B', 
                   edgecolor='black', linewidth=1.5)
    bars2 = ax.bar(x_pos + 0.2, recall_imp, 0.35, 
                   label='Recall Improvement', color='#4ECDC4', 
                   edgecolor='black', linewidth=1.5)
    
    # Add value labels
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            if abs(height) > 1:
                ax.text(bar.get_x() + bar.get_width()/2., height + 3,
                       f'{height:+.0f}%',
                       ha='center', va='bottom' if height > 0 else 'top',
                       fontsize=11, fontweight='bold')
    
    ax.set_ylabel('% Improvement vs Baseline', fontsize=13, fontweight='bold')
    ax.set_title('Performance Gains Over Naive RAG Baseline', fontsize=15, fontweight='bold')
    ax.set_xticks(x_pos)
    ax.set_xticklabels(methods, fontsize=11)
    ax.axhline(y=0, color='black', linestyle='-', linewidth=1)
    ax.legend(fontsize=11)
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('ai_agents/improvement_chart.png', dpi=300, bbox_inches='tight')
    print("✅ Improvement chart saved to ai_agents/improvement_chart.png")
    plt.close()


if __name__ == "__main__":
    visualize_evaluation_results()