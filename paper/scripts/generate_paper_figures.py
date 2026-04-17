#!/usr/bin/env python3

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "paper" / "data"
FIG_DIR = ROOT / "paper" / "figures"
YEAR_CUTOFF = 2024


def centered_moving_average(series: pd.Series, window: int = 5) -> pd.Series:
    return series.rolling(window=window, center=True, min_periods=1).mean()


def save_figure(fig: plt.Figure, stem: str) -> None:
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG_DIR / f"{stem}.png", dpi=300, bbox_inches="tight")
    fig.savefig(FIG_DIR / f"{stem}.svg", bbox_inches="tight")
    plt.close(fig)


def build_term_trends() -> None:
    df = pd.read_csv(DATA_DIR / "annual_processed_trends.csv")
    df = df[df["pub_year"] <= YEAR_CUTOFF].copy()

    terms = [
        ("caucasian_pct", "Caucasian", "#9e2a2b", "#d8a3a3"),
        ("white_pct", "White", "#1d6f8a", "#a8d1de"),
        ("european_pct", "European-Origin", "#3b7f3b", "#b7d7a8"),
    ]

    fig, axes = plt.subplots(3, 1, figsize=(11, 9), sharex=True, layout="constrained")

    for ax, (column, label, strong, light) in zip(axes, terms):
        smoothed = centered_moving_average(df[column], window=5)
        peak_idx = smoothed.idxmax()
        peak_year = int(df.loc[peak_idx, "pub_year"])
        peak_value = float(smoothed.loc[peak_idx])
        end_value = float(smoothed.iloc[-1])

        ax.plot(df["pub_year"], df[column], color=light, linewidth=1.2, alpha=0.9)
        ax.plot(df["pub_year"], smoothed, color=strong, linewidth=2.8)
        ax.scatter([peak_year], [peak_value], color=strong, s=24, zorder=3)
        ax.text(
            peak_year + 0.4,
            peak_value,
            f"Peak {peak_year}: {peak_value:.2f}%",
            color=strong,
            fontsize=9,
            va="bottom",
        )
        ax.text(
            df["pub_year"].iloc[-1] + 0.3,
            end_value,
            f"2024: {end_value:.2f}%",
            color=strong,
            fontsize=9,
            va="center",
        )
        ax.set_title(label, loc="left", fontsize=12, fontweight="bold")
        ax.set_ylabel("% of processed\narticles")
        ax.grid(axis="y", color="#dddddd", linewidth=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    axes[-1].set_xlabel("Publication year")
    axes[-1].set_xlim(df["pub_year"].min(), YEAR_CUTOFF + 2)
    fig.suptitle(
        "Article-Level Prevalence of Focal Terms in Titles/Abstracts, 1947-2024\n"
        "thin line = annual percentage, thick line = centered 5-year moving average",
        fontsize=14,
        fontweight="bold",
    )
    save_figure(fig, "figure-1-focal-term-trends")


def build_corpus_scope() -> None:
    df = pd.read_csv(DATA_DIR / "journal_scope.csv")
    df = df.sort_values("article_count", ascending=True).copy()
    labels = [name.replace(": ", ":\n") for name in df["journal_name"]]

    cmap = plt.get_cmap("YlGnBu")
    colors = cmap(df["abstract_percentage"] / 100.0)

    fig, ax = plt.subplots(figsize=(12, 8.5), layout="constrained")
    bars = ax.barh(labels, df["article_count"], color=colors, edgecolor="#2f3e46", linewidth=0.4)

    for bar, count, pct in zip(bars, df["article_count"], df["abstract_percentage"]):
        ax.text(
            bar.get_width() + 130,
            bar.get_y() + bar.get_height() / 2,
            f"{count:,} articles | {pct:.1f}% with abstract",
            va="center",
            fontsize=8.5,
            color="#243238",
        )

    norm = plt.Normalize(vmin=0, vmax=100)
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, pad=0.02)
    cbar.set_label("CrossRef abstract coverage (%)")

    ax.set_title(
        "Corpus Scope by Journal at Export Time",
        loc="left",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel("Articles in scope")
    ax.set_ylabel("")
    ax.grid(axis="x", color="#dddddd", linewidth=0.8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    save_figure(fig, "figure-2-corpus-scope")


def main() -> None:
    plt.rcParams.update(
        {
            "font.size": 10,
            "axes.titlesize": 12,
            "axes.labelsize": 10,
            "xtick.labelsize": 9,
            "ytick.labelsize": 8.5,
        }
    )
    build_term_trends()
    build_corpus_scope()


if __name__ == "__main__":
    main()
