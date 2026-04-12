import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os


def analysis_m2():
    indexs = ['DynamicPGM', 'LIPP', 'HybridPGMLIPP']
    workloads = {
        'mix10': {
            'file': 'fb_100M_public_uint64_ops_2M_0.000000rq_0.500000nl_0.100000i_0m_mix',
            'label': 'Mixed (10% Insert, 90% Lookup)',
        },
        'mix90': {
            'file': 'fb_100M_public_uint64_ops_2M_0.000000rq_0.500000nl_0.900000i_0m_mix',
            'label': 'Mixed (90% Insert, 10% Lookup)',
        },
    }

    results = {}

    for wk_key, wk_info in workloads.items():
        csv_path = f"results/{wk_info['file']}_results_table.csv"
        df = pd.read_csv(csv_path)
        results[wk_key] = {}

        for index in indexs:
            rows = df[df['index_name'] == index]
            if rows.empty:
                print(f"Warning: No results for {index} in {wk_key}")
                results[wk_key][index] = {'throughput': 0, 'index_size': 0}
                continue

            avg_tp = rows[['mixed_throughput_mops1', 'mixed_throughput_mops2',
                           'mixed_throughput_mops3']].mean(axis=1)
            best_idx = avg_tp.idxmax()
            best_row = rows.loc[best_idx]

            results[wk_key][index] = {
                'throughput': avg_tp.loc[best_idx],
                'index_size': best_row['index_size_bytes'],
            }

            print(f"{wk_key} | {index}: throughput={avg_tp.loc[best_idx]:.4f} Mops/s, "
                  f"size={best_row['index_size_bytes']:.0f} bytes, "
                  f"config=({best_row.get('search_method', 'N/A')}, {best_row.get('value', 'N/A')})")

    # Create 4 bar plots in a 2x2 grid
    fig, axs = plt.subplots(2, 2, figsize=(12, 10))

    bar_width = 0.5
    x = np.arange(len(indexs))
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c']

    for col, (wk_key, wk_info) in enumerate(workloads.items()):
        # Top row: throughput
        ax = axs[0, col]
        throughputs = [results[wk_key][idx]['throughput'] for idx in indexs]
        bars = ax.bar(x, throughputs, bar_width, color=colors)
        ax.set_title(f"Throughput - {wk_info['label']}")
        ax.set_ylabel('Throughput (Mops/s)')
        ax.set_xticks(x)
        ax.set_xticklabels(indexs, rotation=15)
        for bar, val in zip(bars, throughputs):
            ax.text(bar.get_x() + bar.get_width() / 2., bar.get_height(),
                    f'{val:.2f}', ha='center', va='bottom', fontsize=9)

        # Bottom row: index size
        ax = axs[1, col]
        sizes = [results[wk_key][idx]['index_size'] / (1024**3) for idx in indexs]
        bars = ax.bar(x, sizes, bar_width, color=colors)
        ax.set_title(f"Index Size - {wk_info['label']}")
        ax.set_ylabel('Index Size (GB)')
        ax.set_xticks(x)
        ax.set_xticklabels(indexs, rotation=15)
        for bar, val in zip(bars, sizes):
            ax.text(bar.get_x() + bar.get_width() / 2., bar.get_height(),
                    f'{val:.2f}', ha='center', va='bottom', fontsize=9)

    fig.suptitle('Milestone 2: Hybrid DynamicPGM+LIPP vs Baselines (Facebook Dataset)',
                 fontsize=14)
    plt.tight_layout(rect=[0, 0, 1, 0.95])

    os.makedirs('analysis_results', exist_ok=True)
    plt.savefig('analysis_results/m2_benchmark_results.png', dpi=300)
    print("\nSaved figure to analysis_results/m2_benchmark_results.png")

    # Save summary CSV
    summary_rows = []
    for wk_key, wk_info in workloads.items():
        for idx in indexs:
            summary_rows.append({
                'workload': wk_info['label'],
                'index': idx,
                'throughput_mops': results[wk_key][idx]['throughput'],
                'index_size_bytes': results[wk_key][idx]['index_size'],
                'index_size_gb': results[wk_key][idx]['index_size'] / (1024**3),
            })
    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv('analysis_results/m2_summary.csv', index=False)
    print("Saved summary to analysis_results/m2_summary.csv")
    print("\n=== Summary ===")
    print(summary_df.to_string(index=False))


if __name__ == "__main__":
    analysis_m2()
