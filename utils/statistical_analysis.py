"""Statistical analysis utilities for Phase 2B."""

import os
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats


def load_all_assets_filtered(phase_1b_dir, start_date=None, end_date=None):
    """
    Load all asset Excel files from Phase 1B output directory with optional date filtering.

    Parameters:
    -----------
    phase_1b_dir : str
        Path to Phase 1B output directory
    start_date : str, optional
        Start date for filtering (e.g., "2025-07-01")
    end_date : str, optional
        End date for filtering (e.g., "2026-02-15")

    Returns:
    --------
    dict
        Dictionary mapping asset_name to asset info dict with filtered data
    """
    assets = {}

    # Find all subdirectories (asset types) in Phase 1B output
    asset_type_dirs = [d for d in Path(phase_1b_dir).iterdir() if d.is_dir()]

    for asset_type_dir in asset_type_dirs:
        asset_type = asset_type_dir.name

        # Find all Excel files in this asset type directory
        excel_files = list(asset_type_dir.glob("*.xlsx"))

        for excel_file in excel_files:
            asset_name = excel_file.stem  # Filename without extension

            try:
                df = pd.read_excel(excel_file)
                df["time"] = pd.to_datetime(df["time"])

                # Store inception date
                inception_date = df["time"].min()
                total_days_available = len(df)

                # Filter to analysis date range if specified
                if start_date or end_date:
                    if start_date:
                        df = df[df["time"] >= start_date]
                    if end_date:
                        df = df[df["time"] <= end_date]
                    df = df.copy()

                # Count overlapping data points in filtered range
                overlap = df.dropna(subset=["defi_notional_volume", "tradfi_notional_volume"]).shape[0]

                assets[asset_name] = {
                    "data": df,
                    "asset_type": asset_type,
                    "overlap": overlap,
                    "inception_date": inception_date,
                    "total_days_available": total_days_available,
                    "filtered_days": len(df),
                }

                date_range = f" (filtered to {start_date} - {end_date})" if (start_date or end_date) else ""
                print(f"{asset_name:<15} ({asset_type:<30}) Inception: {inception_date.strftime('%Y-%m-%d')}, "
                      f"Total: {total_days_available:>4} rows, Filtered: {len(df):>4} rows, {overlap:>3} overlapping{date_range}")

            except Exception as e:
                print(f"Error loading {excel_file}: {e}")

    return assets


def group_assets_by_type(assets, sort_by_overlap=True):
    """
    Group assets by type with optional sorting.

    Parameters:
    -----------
    assets : dict
        Dictionary mapping asset names to asset info
    sort_by_overlap : bool, default=True
        If True, sort by number of overlapping days (descending)

    Returns:
    --------
    dict
        Dictionary mapping asset_type to list of (asset_name, asset_info) tuples
    """
    assets_by_type = {}
    for asset_name, asset_info in assets.items():
        asset_type = asset_info["asset_type"]
        if asset_type not in assets_by_type:
            assets_by_type[asset_type] = []
        assets_by_type[asset_type].append((asset_name, asset_info))

    # Sort by number of overlapping days within each asset type
    if sort_by_overlap:
        for asset_type in assets_by_type:
            assets_by_type[asset_type].sort(
                key=lambda x: x[1]["data"][["defi_notional_volume", "tradfi_notional_volume"]].notna().all(axis=1).sum(),
                reverse=True
            )

    return assets_by_type


def create_volume_statistics_table(assets_by_type, asset_type):
    """
    Create volume statistics table for a specific asset type.

    Parameters:
    -----------
    assets_by_type : dict
        Dictionary mapping asset_type to list of (asset_name, asset_info) tuples
    asset_type : str
        Asset type to analyze

    Returns:
    --------
    pd.DataFrame or None
        Formatted DataFrame with volume statistics, or None if no data
    """
    from utils import get_labels

    if asset_type not in assets_by_type:
        return None

    labels = get_labels(asset_type)
    summary_data = []

    for asset_name, asset_info in assets_by_type[asset_type]:
        df = asset_info["data"]

        # Filter to overlapping period
        overlap_mask = df[["defi_notional_volume", "tradfi_notional_volume"]].notna().all(axis=1)

        if overlap_mask.any():
            overlap_df = df[overlap_mask]

            defi_mean = overlap_df["defi_notional_volume"].mean()
            tradfi_mean = overlap_df["tradfi_notional_volume"].mean()
            ratio = tradfi_mean / defi_mean if defi_mean > 0 else 0

            summary_data.append({
                'Asset': asset_name,
                f'{labels["onchain"]} Avg ($/day)': f'${defi_mean:,.0f}',
                f'{labels["offchain"]} Avg ($/day)': f'${tradfi_mean:,.0f}',
                f'Ratio ({labels["offchain"]}/{labels["onchain"]})': f'{ratio:.1f}x',
                'Days': overlap_mask.sum()
            })

    if summary_data:
        return pd.DataFrame(summary_data)
    return None


def get_valid_window(df, col, current_idx, window_size):
    """
    Get the most recent N valid (non-NaN, non-zero) trading days before current_idx.

    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame containing the data
    col : str
        Column name to analyze
    current_idx : int
        Current index position
    window_size : int
        Number of valid days to retrieve

    Returns:
    --------
    tuple
        (window_data, window_indices) where:
        - window_data: Series of valid values, or None if insufficient data
        - window_indices: List of indices used, or empty list if insufficient
    """
    # Filter to valid values (not NaN and not zero)
    valid_mask = (df[col].notna()) & (df[col] != 0)
    valid_indices = df.index[valid_mask].tolist()

    # Get the most recent N valid indices before current_idx
    recent_valid = [i for i in valid_indices if i < current_idx][-window_size:]

    # Require exactly window_size valid days
    if len(recent_valid) == window_size:
        return df.loc[recent_valid, col], recent_valid
    else:
        return None, []


def plot_daily_volume_ttest(asset_name, df, asset_type, window_size=3, confidence_level=0.95,
                            export_dir=None):
    """
    Create t-test analysis plot for daily volume with rolling window.

    Parameters:
    -----------
    asset_name : str
        Name of the asset
    df : pd.DataFrame
        DataFrame containing the asset data (will be filtered to start from first non-zero DeFi volume)
    asset_type : str
        Asset type for label generation
    window_size : int, default=3
        Rolling window size in trading days
    confidence_level : float, default=0.95
        Confidence level for t-test (e.g., 0.95 for 95% CI)
    export_dir : str, optional
        Directory path to export CSV files

    Returns:
    --------
    None
        Displays the plot and optionally exports CSV
    """
    from utils import get_labels

    labels = get_labels(asset_type)
    df = df.copy()

    # Filter to start at the date when defi volumes start being greater than 0
    defi_vol_start = df[df["defi_notional_volume"] > 0].index
    if len(defi_vol_start) == 0:
        print(f"Skipping {asset_name}: no defi volume data")
        return

    df = df.loc[defi_vol_start[0]:].reset_index(drop=True)

    # Skip if insufficient data
    overlap_mask = df[["defi_notional_volume", "tradfi_notional_volume"]].notna().all(axis=1)
    if overlap_mask.sum() < 5:
        print(f"Skipping {asset_name}: insufficient overlapping data")
        return

    # Calculate t-critical value
    df_val = window_size - 1  # degrees of freedom
    t_critical = stats.t.ppf((1 + confidence_level) / 2, df_val)

    # Create 2x2 subplots (t-scores on top, volumes on bottom)
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            f"{labels['onchain']} Volume T-Score", f"{labels['offchain']} Volume T-Score",
            f"{labels['onchain']} Volumes", f"{labels['offchain']} Volumes"
        ),
        vertical_spacing=0.12,
        horizontal_spacing=0.1
    )

    # Store data for CSV export
    export_data = {
        'date': df['time'],
        'defi_volume': df['defi_notional_volume'],
        'tradfi_volume': df['tradfi_notional_volume']
    }

    for i, (col, label) in enumerate([("defi_notional_volume", labels["onchain"]),
                                       ("tradfi_notional_volume", labels["offchain"])], 1):
        # Calculate t-scores and rolling stats using trading days only
        t_scores = []
        rolling_means = []
        rolling_stds = []
        window_indices_list = []

        for idx in range(len(df)):
            if idx < window_size:
                # Keep early rows as before
                t_scores.append(0)
                rolling_means.append(np.nan)
                rolling_stds.append(np.nan)
                window_indices_list.append([])
            else:
                # Get valid trading day window
                window_data, window_indices = get_valid_window(df, col, idx, window_size)
                today_value = df.loc[idx, col]

                if window_data is not None and pd.notna(today_value) and today_value != 0:
                    window_mean = window_data.mean()
                    window_std = window_data.std()

                    rolling_means.append(window_mean)
                    rolling_stds.append(window_std)
                    window_indices_list.append(window_indices)

                    # Calculate t-score with actual window size
                    if window_std > 0:
                        t_score = (today_value - window_mean) / (window_std / np.sqrt(len(window_data)))
                    else:
                        t_score = 0
                else:
                    t_score = 0
                    rolling_means.append(np.nan)
                    rolling_stds.append(np.nan)
                    window_indices_list.append([])

                t_scores.append(t_score)

        t_series = pd.Series(t_scores, index=df.index).replace([np.inf, -np.inf], 0)

        # Store in export data
        label_key = 'defi' if 'defi' in col else 'tradfi'
        export_data[f'{label_key}_t_score'] = t_series
        export_data[f'{label_key}_rolling_mean'] = rolling_means
        export_data[f'{label_key}_rolling_std'] = rolling_stds
        export_data[f'{label_key}_window_indices'] = [
            str(wi) if wi else 'insufficient_data' for wi in window_indices_list
        ]

        # Plot T-scores (row 1)
        fig.add_hrect(y0=t_critical, y1=10, fillcolor="red", opacity=0.1, line_width=0, row=1, col=i)
        fig.add_hrect(y0=-10, y1=-t_critical, fillcolor="red", opacity=0.1, line_width=0, row=1, col=i)

        fig.add_hline(y=t_critical, line_dash="dash", line_color="red", opacity=0.6, line_width=2, row=1, col=i)
        fig.add_hline(y=-t_critical, line_dash="dash", line_color="red", opacity=0.6, line_width=2, row=1, col=i)
        fig.add_hline(y=0, line_dash="solid", line_color="gray", opacity=0.3, line_width=1, row=1, col=i)

        bar_color = "darkblue" if i == 1 else "darkgreen"
        fig.add_trace(
            go.Bar(x=df["time"], y=t_series, name=f"{label} T-Score",
                  marker_color=bar_color, opacity=0.7, showlegend=False),
            row=1, col=i
        )

        # Plot Volumes (row 2)
        fig.add_trace(
            go.Scatter(x=df["time"], y=rolling_means, name=f"{window_size}-Day Mean",
                      mode="lines", line=dict(color="orange", width=2, dash="dash"),
                      showlegend=(i==1)),
            row=2, col=i
        )
        fig.add_trace(
            go.Scatter(x=df["time"], y=df[col], name="Actual Volume",
                      mode="lines", line=dict(color=bar_color, width=2),
                      showlegend=(i==1)),
            row=2, col=i
        )

    fig.update_layout(
        title=f"Daily Spot Trading Volume T-Test Analysis ({window_size}-Day Window, {int(confidence_level*100)}% CI=±{t_critical:.2f}) — {asset_name}",
        height=600,
        hovermode="x unified",
        template="plotly_white",
    )
    fig.update_yaxes(title_text="T-Score", range=[-12, 12], row=1, col=1)
    fig.update_yaxes(title_text="T-Score", range=[-12, 12], row=1, col=2)
    fig.update_yaxes(title_text="USD Volume", row=2, col=1)
    fig.update_yaxes(title_text="USD Volume", row=2, col=2)
    fig.show()

    # Export to CSV if directory specified
    if export_dir:
        os.makedirs(export_dir, exist_ok=True)
        export_df = pd.DataFrame(export_data)
        csv_path = os.path.join(export_dir, f"{asset_name}_daily_volume_ttest.csv")
        export_df.to_csv(csv_path, index=False)
        print(f"Exported: {csv_path}")


def plot_cross_correlation(asset_name, df, asset_type, lag_range=(-7, 8)):
    """
    Create cross-correlation plot between DeFi and TradFi volumes.

    Parameters:
    -----------
    asset_name : str
        Name of the asset
    df : pd.DataFrame
        DataFrame containing the asset data
    asset_type : str
        Asset type for label generation
    lag_range : tuple, default=(-7, 8)
        Range of lags to analyze (start, stop) - negative = DeFi leads, positive = TradFi leads

    Returns:
    --------
    None
        Displays the plot
    """
    from utils import get_labels

    labels = get_labels(asset_type)

    overlap = df.dropna(subset=["defi_notional_volume", "tradfi_notional_volume"]).copy()

    if len(overlap) < 10:
        print(f"{asset_name}: only {len(overlap)} overlapping days — skipping cross-correlation")
        return

    lags = range(lag_range[0], lag_range[1])
    lag_list, corr_list = [], []

    for lag in lags:
        shifted = overlap["defi_notional_volume"].shift(lag)
        valid = shifted.notna() & overlap["tradfi_notional_volume"].notna()
        if valid.sum() >= 5:
            lag_list.append(lag)
            corr_list.append(shifted[valid].corr(overlap["tradfi_notional_volume"][valid]))

    if len(corr_list) == 0:
        print(f"{asset_name}: insufficient data for cross-correlation")
        return

    peak_idx = int(np.argmax(np.abs(corr_list)))
    peak_lag, peak_corr = lag_list[peak_idx], corr_list[peak_idx]

    # Color bars based on correlation value
    colors = ['crimson' if c < 0 else 'steelblue' for c in corr_list]

    fig = go.Figure()
    fig.add_trace(go.Bar(x=lag_list, y=corr_list, marker_color=colors, name="Correlation"))
    fig.add_hline(y=0, line_dash="solid", line_color="gray", opacity=0.5)
    fig.add_annotation(
        x=peak_lag, y=peak_corr,
        text=f"Peak: lag={peak_lag}d, r={peak_corr:.3f}",
        showarrow=True, arrowhead=2, bgcolor="white"
    )
    fig.update_layout(
        title=f"Cross-Correlation: {labels['onchain']} vs {labels['offchain']} Spot Trading Volume — {asset_name}",
        xaxis_title=f"Lag (days, negative = {labels['onchain']} leads)",
        yaxis_title="Pearson Correlation",
        yaxis_range=[-1, 1],
        hovermode="x unified",
        template="plotly_white",
        showlegend=False,
    )
    fig.show()


def analyze_assets_by_type(assets_by_type, asset_type, analysis_func, **kwargs):
    """
    Apply an analysis function to all assets of a given type.

    Parameters:
    -----------
    assets_by_type : dict
        Dictionary mapping asset_type to list of (asset_name, asset_info) tuples
    asset_type : str
        Asset type to analyze
    analysis_func : callable
        Function to call for each asset. Should accept (asset_name, df, asset_type, **kwargs)
    **kwargs
        Additional keyword arguments to pass to analysis_func

    Returns:
    --------
    None
    """
    if asset_type not in assets_by_type:
        print(f"No assets found for type: {asset_type}")
        return

    for asset_name, asset_info in assets_by_type[asset_type]:
        analysis_func(asset_name, asset_info["data"], asset_type, **kwargs)


def create_price_correlation_table(assets):
    """
    Create comprehensive price correlation and tracking error table.

    Parameters:
    -----------
    assets : dict
        Dictionary mapping asset names to asset info

    Returns:
    --------
    pd.DataFrame or None
        Formatted DataFrame with price correlation metrics
    """
    summary_data = []

    for asset_name, asset_info in sorted(assets.items()):
        df = asset_info["data"]
        asset_type = asset_info["asset_type"]

        # Filter to overlapping period
        overlap_mask = df[["defi_close", "tradfi_close"]].notna().all(axis=1)

        if overlap_mask.sum() < 5:
            continue

        overlap_df = df[overlap_mask]

        # Price correlation
        price_corr = overlap_df["defi_close"].corr(overlap_df["tradfi_close"])

        # Tracking error (standard deviation of price difference)
        price_diff = overlap_df["defi_close"] - overlap_df["tradfi_close"]
        tracking_error = price_diff.std()

        # Average price difference (percentage)
        price_diff_pct = (price_diff / overlap_df["tradfi_close"] * 100)
        avg_diff_pct = price_diff_pct.mean()

        summary_data.append({
            'Asset': asset_name,
            'Asset Type': asset_type,
            'Price Correlation': f'{price_corr:.4f}',
            'Tracking Error ($)': f'${tracking_error:,.2f}',
            'Avg Price Diff (%)': f'{avg_diff_pct:+.2f}%',
            'Days Analyzed': overlap_mask.sum()
        })

    if summary_data:
        return pd.DataFrame(summary_data)
    return None


def create_asset_type_summary(assets):
    """
    Create summary statistics aggregated by asset type.

    Parameters:
    -----------
    assets : dict
        Dictionary mapping asset names to asset info

    Returns:
    --------
    pd.DataFrame or None
        Formatted DataFrame with aggregated statistics by asset type
    """
    asset_type_stats = {}

    for asset_name, asset_info in assets.items():
        df = asset_info["data"]
        asset_type = asset_info["asset_type"]

        if asset_type not in asset_type_stats:
            asset_type_stats[asset_type] = []

        # Calculate metrics for overlapping period
        overlap_mask = df[["defi_close", "tradfi_close", "defi_notional_volume", "tradfi_notional_volume"]].notna().all(axis=1)

        if overlap_mask.any():
            overlap_df = df[overlap_mask]

            asset_type_stats[asset_type].append({
                "asset": asset_name,
                "price_corr": overlap_df["defi_close"].corr(overlap_df["tradfi_close"]),
                "defi_avg_vol": overlap_df["defi_notional_volume"].mean(),
                "tradfi_avg_vol": overlap_df["tradfi_notional_volume"].mean(),
                "days": overlap_mask.sum(),
            })

    # Create summary table by asset type
    summary_data = []
    for asset_type, stats_list in sorted(asset_type_stats.items()):
        if not stats_list:
            continue

        # Average metrics
        avg_price_corr = np.mean([s["price_corr"] for s in stats_list if not np.isnan(s["price_corr"])])
        total_defi_vol = np.sum([s["defi_avg_vol"] for s in stats_list])
        total_tradfi_vol = np.sum([s["tradfi_avg_vol"] for s in stats_list])
        avg_days = np.mean([s["days"] for s in stats_list])

        summary_data.append({
            'Asset Type': asset_type,
            'Number of Assets': len(stats_list),
            'Avg Price Correlation': f'{avg_price_corr:.3f}',
            'Total DeFi Volume ($/day)': f'${total_defi_vol:,.0f}',
            'Total TradFi Volume ($/day)': f'${total_tradfi_vol:,.0f}',
            'Avg Days Analyzed': f'{avg_days:.0f}'
        })

    if summary_data:
        return pd.DataFrame(summary_data)
    return None
