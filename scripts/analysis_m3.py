import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os


def load_results(csv_path, indexes):
    """Load benchmark results from CSV, handling variable column counts."""
    # Read with flexible column handling
    max_cols = 12  # index_name + 3 build + 1 size + 3 throughput + 4 variants
    col_names = [
        'index_name',
        'build_time_ns1', 'build_time_ns2', 'build_time_ns3',
        'index_size_bytes',
        'mixed_throughput_mops1', 'mixed_throughput_mops2', 'mixed_throughput_mops3',
        'search_method', 'pgm_error', 'flush_pct', 'max_buffer',
    ]

    try:
        # Try reading with header first
        df = pd.read_csv(csv_path, on_bad_lines='warn')
        if 'index_name' not in df.columns:
            # No header — re-read with explicit names
            df = pd.read_csv(csv_path, header=None, names=col_names,
                             on_bad_lines='warn')
    except Exception as e:
        print(f"Warning: Could not read {csv_path}: {e}")
        return {}

    results = {}
    for index in indexes:
        rows = df[df['index_name'] == index]
        if rows.empty:
            print(f"  Warning: No results for {index} in {csv_path}")
            results[index] = {'throughput_mean': 0, 'throughput_std': 0,
                              'index_size': 0}
            continue

        # Compute mean throughput across 3 runs for each config row
        tp_cols = ['mixed_throughput_mops1', 'mixed_throughput_mops2',
                   'mixed_throughput_mops3']
        avg_tp = rows[tp_cols].mean(axis=1)

        # Pick the best-performing hyperparameter config
        best_idx = avg_tp.idxmax()
        best_row = rows.loc[best_idx]

        # Compute mean and std of the 3 runs for the best config
        tp_values = best_row[tp_cols].values.astype(float)
        results[index] = {
            'throughput_mean': np.mean(tp_values),
            'throughput_std': np.std(tp_values, ddof=1) if len(tp_values) > 1 else 0,
            'index_size': float(best_row['index_size_bytes']),
            'config': f"({best_row.get('search_method', 'N/A')}, "
                      f"pgm_err={best_row.get('pgm_error', 'N/A')}, "
                      f"flush={best_row.get('flush_pct', 'N/A')}, "
                      f"buf={best_row.get('max_buffer', 'N/A')})",
        }

        print(f"  {index}: {results[index]['throughput_mean']:.4f} "
              f"+/- {results[index]['throughput_std']:.4f} Mops/s, "
              f"size={results[index]['index_size']:.0f} bytes, "
              f"config={results[index]['config']}")

    return results


def analysis_m3():
    indexes = ['DynamicPGM', 'LIPP', 'HybridPGMLIPP']
    datasets = {
        'fb': 'fb_100M_public_uint64',
        'books': 'books_100M_public_uint64',
        'osmc': 'osmc_100M_public_uint64',
    }
    workloads = {
        'mix10': {
            'file_suffix': '_ops_2M_0.000000rq_0.500000nl_0.100000i_0m_mix',
            'label': 'Mixed (10% Insert, 90% Lookup)',
        },
        'mix90': {
            'file_suffix': '_ops_2M_0.000000rq_0.500000nl_0.900000i_0m_mix',
            'label': 'Mixed (90% Insert, 10% Lookup)',
        },
    }

    all_results = {}

    for ds_key, ds_name in datasets.items():
        for wk_key, wk_info in workloads.items():
            csv_path = f"results/{ds_name}{wk_info['file_suffix']}_results_table.csv"
            key = (ds_key, wk_key)
            print(f"\n--- {ds_key.upper()} / {wk_info['label']} ---")
            all_results[key] = load_results(csv_path, indexes)

    # Create 12 bar plots: 3 datasets x 2 workloads x 2 metrics
    fig, axs = plt.subplots(4, 3, figsize=(18, 20))

    colors = ['#1f77b4', '#ff7f0e', '#2ca02c']
    bar_width = 0.5
    x = np.arange(len(indexes))

    for col_idx, (ds_key, ds_name) in enumerate(datasets.items()):
        for wk_idx, (wk_key, wk_info) in enumerate(workloads.items()):
            key = (ds_key, wk_key)
            res = all_results.get(key, {})

            # Row 0-1: Throughput for mix10, mix90
            row = wk_idx
            ax = axs[row, col_idx]
            means = [res.get(idx, {}).get('throughput_mean', 0) for idx in indexes]
            stds = [res.get(idx, {}).get('throughput_std', 0) for idx in indexes]
            bars = ax.bar(x, means, bar_width, yerr=stds, capsize=5,
                          color=colors, edgecolor='black', linewidth=0.5)
            ax.set_title(f"{ds_key.upper()} - {wk_info['label']}", fontsize=10)
            ax.set_ylabel('Throughput (Mops/s)')
            ax.set_xticks(x)
            ax.set_xticklabels(indexes, rotation=15, fontsize=9)
            for bar, val in zip(bars, means):
                if val > 0:
                    ax.text(bar.get_x() + bar.get_width() / 2., bar.get_height(),
                            f'{val:.2f}', ha='center', va='bottom', fontsize=8)

            # Row 2-3: Index size for mix10, mix90
            row = wk_idx + 2
            ax = axs[row, col_idx]
            sizes = [res.get(idx, {}).get('index_size', 0) / (1024**3)
                     for idx in indexes]
            bars = ax.bar(x, sizes, bar_width, color=colors,
                          edgecolor='black', linewidth=0.5)
            ax.set_title(f"{ds_key.upper()} - Index Size - {wk_info['label']}",
                         fontsize=10)
            ax.set_ylabel('Index Size (GB)')
            ax.set_xticks(x)
            ax.set_xticklabels(indexes, rotation=15, fontsize=9)
            for bar, val in zip(bars, sizes):
                if val > 0:
                    ax.text(bar.get_x() + bar.get_width() / 2., bar.get_height(),
                            f'{val:.2f}', ha='center', va='bottom', fontsize=8)

    fig.suptitle('Milestone 3: Advanced Hybrid PGM-LIPP vs Baselines',
                 fontsize=14, fontweight='bold')
    plt.tight_layout(rect=[0, 0, 1, 0.97])

    os.makedirs('analysis_results', exist_ok=True)
    plt.savefig('analysis_results/m3_benchmark_results.png', dpi=300,
                bbox_inches='tight')
    print("\nSaved figure to analysis_results/m3_benchmark_results.png")

    # Save summary CSV
    summary_rows = []
    for (ds_key, wk_key), res in all_results.items():
        wk_label = workloads[wk_key]['label']
        for idx in indexes:
            r = res.get(idx, {})
            summary_rows.append({
                'dataset': ds_key,
                'workload': wk_label,
                'index': idx,
                'throughput_mops_mean': r.get('throughput_mean', 0),
                'throughput_mops_std': r.get('throughput_std', 0),
                'index_size_bytes': r.get('index_size', 0),
                'index_size_gb': r.get('index_size', 0) / (1024**3),
                'config': r.get('config', 'N/A'),
            })

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv('analysis_results/m3_summary.csv', index=False)
    print("Saved summary to analysis_results/m3_summary.csv")
    print("\n=== Summary ===")
    print(summary_df.to_string(index=False))


if __name__ == "__main__":
    analysis_m3()
