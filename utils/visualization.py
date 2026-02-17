"""Visualization utilities for Phase 2A analysis."""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from utils import get_labels


def load_all_assets(phase_1b_dir):
    """
    Load all asset Excel files from Phase 1B output directory.

    Parameters:
    -----------
    phase_1b_dir : str
        Path to Phase 1B output directory

    Returns:
    --------
    tuple
        (assets, assets_by_type) where:
        - assets: dict mapping asset_name to asset info dict
        - assets_by_type: dict mapping asset_type to list of asset names
    """
    from pathlib import Path

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

                # Count overlapping data points
                overlap_price = df.dropna(subset=["defi_close", "tradfi_close"]).shape[0]
                overlap_volume = df.dropna(subset=["defi_notional_volume", "tradfi_notional_volume"]).shape[0]

                assets[asset_name] = {
                    "data": df,
                    "asset_type": asset_type,
                    "total_rows": len(df),
                    "overlap_price": overlap_price,
                    "overlap_volume": overlap_volume,
                }

                print(f"{asset_name:<15} ({asset_type:<30}) {len(df):>4} rows, {overlap_price:>3} price overlap, {overlap_volume:>3} volume overlap")

            except Exception as e:
                print(f"Error loading {excel_file}: {e}")

    # Group assets by type
    assets_by_type = {}
    for asset_name, asset_info in assets.items():
        asset_type = asset_info["asset_type"]
        if asset_type not in assets_by_type:
            assets_by_type[asset_type] = []
        assets_by_type[asset_type].append(asset_name)

    return assets, assets_by_type


def create_overview_dashboard(assets, asset_type, asset_names, date_filter=None):
    """
    Create overview dashboard for a specific asset type.

    Parameters:
    -----------
    assets : dict
        Dictionary mapping asset names to asset info
    asset_type : str
        Asset type (e.g., "Crypto Coin", "Traditional Commodity", "Traditional Equity")
    asset_names : list
        List of asset names to include in the dashboard
    date_filter : str, optional
        Date string to filter data from (e.g., "2024-07-01")

    Returns:
    --------
    pd.DataFrame
        Formatted overview dashboard with summary statistics
    """
    labels = get_labels(asset_type)
    summary_data = []

    for asset_name in asset_names:
        asset_info = assets[asset_name]
        df = asset_info["data"].copy()

        # Apply date filter if specified
        if date_filter:
            df = df[df["time"] >= date_filter]

        # Calculate statistics for overlapping period
        overlap_mask = df[["defi_close", "tradfi_close"]].notna().all(axis=1)

        if overlap_mask.any():
            overlap_df = df[overlap_mask]

            # Average daily notional volume
            onchain_avg_vol = overlap_df["defi_notional_volume"].mean()
            offchain_avg_vol = overlap_df["tradfi_notional_volume"].mean()

            # Price correlation
            price_corr = overlap_df["defi_close"].corr(overlap_df["tradfi_close"])

            # Average price difference (percentage)
            price_diff_pct = ((overlap_df["defi_close"] - overlap_df["tradfi_close"]) / overlap_df["tradfi_close"] * 100).mean()

            # Dynamic column naming based on date filter
            days_label = "Days (Jul 2024+)" if date_filter else "Total Days"
            total_days = len(df) if date_filter else asset_info["total_rows"]

            summary_data.append({
                "Asset": asset_name,
                days_label: total_days,
                "Overlap Days": overlap_mask.sum(),
                f"{labels['onchain']} Avg Vol (USD)": onchain_avg_vol,
                f"{labels['offchain']} Avg Vol (USD)": offchain_avg_vol,
                f"Vol Ratio ({labels['offchain']}/{labels['onchain']})": offchain_avg_vol / onchain_avg_vol if onchain_avg_vol > 0 else np.nan,
                "Price Correlation": price_corr,
                "Avg Price Diff %": price_diff_pct,
            })

    summary_df = pd.DataFrame(summary_data).sort_values("Asset")

    # Format numeric columns for display
    display_df = summary_df.copy()

    # Format volume columns (dynamic names)
    for col in display_df.columns:
        if "Avg Vol (USD)" in col:
            display_df[col] = display_df[col].apply(lambda x: f"${x:,.0f}" if not pd.isna(x) else "N/A")
        elif "Vol Ratio" in col:
            display_df[col] = display_df[col].apply(lambda x: f"{x:.1f}x" if not pd.isna(x) else "N/A")

    display_df["Price Correlation"] = display_df["Price Correlation"].apply(lambda x: f"{x:.3f}" if not pd.isna(x) else "N/A")
    display_df["Avg Price Diff %"] = display_df["Avg Price Diff %"].apply(lambda x: f"{x:+.2f}%" if not pd.isna(x) else "N/A")

    return display_df


def plot_volume_comparison(asset_name, df, asset_type, date_filter=None, use_dual_axis=True):
    """
    Plot volume comparison for a given asset with flexible configuration.

    Parameters:
    -----------
    asset_name : str
        Name of the asset
    df : pd.DataFrame
        DataFrame containing the asset data
    asset_type : str
        Asset type (e.g., "Crypto Coin", "Traditional Commodity", "Traditional Equity")
    date_filter : str, optional
        Date string to filter data from (e.g., "2024-07-01")
    use_dual_axis : bool, default=True
        If True, uses dual y-axes (for commodities, equities, crypto).
        If False, uses single y-axis with volume ratio subplot (original behavior).

    Returns:
    --------
    None
        Displays the plot
    """
    df = df.copy()
    labels = get_labels(asset_type)

    # Apply date filter if specified
    if date_filter:
        df = df[df["time"] >= date_filter]
        title_suffix = f" (from {date_filter})"
    else:
        title_suffix = ""

    if use_dual_axis:
        # Dual y-axis approach (for commodities, equities, and crypto)
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        # Add onchain volume line (primary y-axis)
        fig.add_trace(
            go.Scatter(
                x=df["time"],
                y=df["defi_notional_volume"],
                name=labels["onchain"],
                mode="lines",
                line=dict(color="steelblue", width=2),
                fill='tozeroy',
                fillcolor='rgba(70, 130, 180, 0.2)'
            ),
            secondary_y=False
        )

        # Add offchain volume line (secondary y-axis)
        fig.add_trace(
            go.Scatter(
                x=df["time"],
                y=df["tradfi_notional_volume"],
                name=labels["offchain"],
                mode="lines",
                line=dict(color="coral", width=2),
                fill='tozeroy',
                fillcolor='rgba(255, 127, 80, 0.2)'
            ),
            secondary_y=True
        )

        # Update axes labels
        fig.update_xaxes(title_text="Date")
        fig.update_yaxes(
            title_text=f"{labels['onchain']} Volume (USD)",
            secondary_y=False,
            title_font=dict(color="steelblue")
        )
        fig.update_yaxes(
            title_text=f"{labels['offchain']} Volume (USD)",
            secondary_y=True,
            title_font=dict(color="coral")
        )

        fig.update_layout(
            title=f"{asset_name} - Trading Volume Comparison{title_suffix}",
            height=500,
            hovermode="x unified",
            template="plotly_white",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
    else:
        # Original approach with volume ratio subplot
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.08,
            subplot_titles=(
                f"{labels['onchain']} vs {labels['offchain']} Notional Volume",
                f"Volume Ratio ({labels['offchain']}/{labels['onchain']})"
            ),
            row_heights=[0.7, 0.3]
        )

        # Volume comparison
        fig.add_trace(
            go.Bar(
                x=df["time"],
                y=df["defi_notional_volume"],
                name=f"{labels['onchain']} Volume",
                marker_color="steelblue",
                opacity=0.7
            ),
            row=1, col=1
        )
        fig.add_trace(
            go.Bar(
                x=df["time"],
                y=df["tradfi_notional_volume"],
                name=f"{labels['offchain']} Volume",
                marker_color="coral",
                opacity=0.7
            ),
            row=1, col=1
        )

        # Volume ratio
        vol_ratio = df["tradfi_notional_volume"] / df["defi_notional_volume"]
        fig.add_trace(
            go.Scatter(
                x=df["time"],
                y=vol_ratio,
                name="Volume Ratio",
                mode="lines+markers",
                line=dict(color="purple", width=2),
                showlegend=False
            ),
            row=2, col=1
        )
        fig.add_hline(
            y=1,
            line_dash="dash",
            line_color="gray",
            opacity=0.5,
            annotation_text="1:1 ratio",
            row=2, col=1
        )

        fig.update_layout(
            title=f"{asset_name} - Volume Comparison{title_suffix}",
            height=700,
            hovermode="x unified",
            template="plotly_white",
            barmode="group",
        )
        fig.update_yaxes(title_text="Volume (USD)", row=1, col=1)
        fig.update_yaxes(title_text="Ratio", type="log", row=2, col=1)
        fig.update_xaxes(title_text="Date", row=2, col=1)

    fig.show()


def plot_assets_by_type(assets, asset_type, asset_names, date_filter=None, use_dual_axis=True):
    """
    Plot volume comparisons for all assets of a given type.

    Parameters:
    -----------
    assets : dict
        Dictionary mapping asset names to asset info
    asset_type : str
        Asset type (e.g., "Crypto Coin", "Traditional Commodity", "Traditional Equity")
    asset_names : list
        List of asset names to plot
    date_filter : str, optional
        Date string to filter data from (e.g., "2024-07-01")
    use_dual_axis : bool, default=True
        If True, uses dual y-axes. If False, uses volume ratio subplot.
    """
    for asset_name in sorted(asset_names):
        df = assets[asset_name]["data"]
        plot_volume_comparison(asset_name, df, asset_type, date_filter, use_dual_axis)
