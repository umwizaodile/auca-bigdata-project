"""
make_charts.py
Part 4: static visualizations built from the result files saved by the earlier jobs.
Output: charts/*.png (embed them in the report).
"""
import os
import sys

import matplotlib
import matplotlib.ticker
matplotlib.use("Agg")                      # draw to files, no window needed
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

RES, OUT = "results", "charts"
BLUE, ORANGE, RED, GREY = "#2563eb", "#f59e0b", "#dc2626", "#9ca3af"
plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.titlesize": 12, "axes.titleweight": "bold"})


def load(name):
    path = os.path.join(RES, name)
    if not os.path.exists(path):
        sys.exit(f"ERROR: {path} not found. Run the earlier jobs first.")
    return pd.read_csv(path)


def save(fig, name, source):
    fig.text(0.01, 0.005, f"Source: {source}", fontsize=7.5, color="#6b7280")
    fig.tight_layout(rect=(0, 0.02, 1, 1))
    fig.savefig(os.path.join(OUT, name), dpi=200)
    plt.close(fig)
    print("saved", os.path.join(OUT, name))


def chart_revenue_by_category():
    d = load("revenue_by_category.csv").sort_values("revenue")
    colors = [GREY] * len(d)
    colors[-1], colors[0] = BLUE, RED                      # best and worst category
    fig, ax = plt.subplots(figsize=(8, 7))
    ax.barh(d["category_name"], d["revenue"] / 1e6, color=colors)
    ax.axvline(d["revenue"].mean() / 1e6, color="#374151", ls="--", lw=1)
    ax.text(d["revenue"].mean() / 1e6, len(d) - 0.4, " average", fontsize=8, color="#374151")
    top, low = d.iloc[-1], d.iloc[0]
    ax.set_title(f"Revenue by category: no category dominates\n(best {top['revenue'] / low['revenue']:.2f}x the weakest)")
    ax.set_xlabel("Revenue before discounts (millions)")
    ax.tick_params(axis="y", labelsize=8)
    save(fig, "01_revenue_by_category.png", "MongoDB aggregation on transactions (50,000 orders)")


def chart_segments():
    d = load("user_segments.csv")
    d = d[d["users"] > 0]
    users = 100 * d["users"] / d["users"].sum()
    rev = 100 * d["total_revenue"] / d["total_revenue"].sum()
    x, w = np.arange(len(d)), 0.38
    fig, ax = plt.subplots(figsize=(8, 4.8))
    b1 = ax.bar(x - w / 2, users, w, label="% of users", color=GREY)
    b2 = ax.bar(x + w / 2, rev, w, label="% of revenue", color=BLUE)
    for bars in (b1, b2):
        ax.bar_label(bars, fmt="%.1f%%", fontsize=8, padding=2)
    ax.set_xticks(x)
    ax.set_xticklabels([s.split(". ", 1)[-1] for s in d["segment"]], fontsize=9)
    ax.set_ylabel("Share (%)")
    ax.set_title("Customer segments: regular buyers are 53% of users but 66% of revenue")
    ax.legend(frameon=False)
    save(fig, "02_customer_segments.png", "MongoDB aggregation on users.summary (10,000 users)")


def chart_funnel():
    d = load("funnel_stages.csv")
    labels = ["Sessions", "Viewed a product", "Added to cart", "Reached checkout",
              "Completed checkout", "Purchase recorded"]
    n = d["sessions"].to_numpy()
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    y = np.arange(len(n))[::-1]
    ax.barh(y, n, color=BLUE, height=0.62)
    steps = d["step_conversion"].to_numpy()
    worst = int(np.nanargmin(steps[1:])) + 1               # step with the lowest conversion
    for i, (v, s) in enumerate(zip(n, steps)):
        ax.text(v + n.max() * 0.01, y[i], f"{int(v):,}", va="center", fontsize=9)
        if i > 0:
            lost = n[i - 1] - v
            color = RED if i == worst else "#374151"
            label = f"{s:.1f}% continue (-{int(lost):,})" if lost > 0 else f"{s:.0f}% continue (none lost)"
            ax.text(n.max() * 0.60, y[i] + 0.5, label, fontsize=8, color=color, va="center")
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.set_xlim(0, n.max() * 1.12)
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.set_xlabel("Sessions")
    ax.set_title("Conversion funnel: the biggest leak is between cart and checkout")
    save(fig, "03_conversion_funnel.png", "HBase user_sessions + MongoDB transactions, joined in Spark (50,000 sessions)")


def chart_spend_vs_engagement():
    d = load("sql_engagement_vs_spend.csv")
    q = [f"Q{int(i)}" for i in d["spend_quartile"]]
    fig, axes = plt.subplots(1, 2, figsize=(9, 4.2))
    axes[0].bar(q, d["avg_spent"], color=BLUE)
    axes[0].bar_label(axes[0].containers[0], fmt="%.0f", fontsize=8, padding=2)
    axes[0].set_title("Average spend per user")
    axes[1].bar(q, d["avg_sessions"], color=ORANGE)
    axes[1].bar_label(axes[1].containers[0], fmt="%.1f", fontsize=8, padding=2)
    axes[1].set_title("Average sessions per user")
    axes[1].set_ylim(0, d["avg_sessions"].max() * 1.25)
    for ax in axes:
        ax.set_xlabel("Spend quartile (Q1 = lowest spenders)")
    s_ratio = d["avg_spent"].iloc[-1] / d["avg_spent"].iloc[0]
    e_ratio = d["avg_sessions"].iloc[-1] / d["avg_sessions"].iloc[0]
    fig.suptitle(f"Spend rises {s_ratio:.1f}x across quartiles, browsing only {100 * (e_ratio - 1):.0f}%",
                 fontweight="bold")
    save(fig, "04_spend_vs_engagement.png", "Spark SQL: MongoDB-style users joined with HBase-style sessions")


def chart_cohorts():
    m = load("sql_monthly_revenue.csv").set_index("month")["days_with_data"]
    d = load("sql_cohort_revenue_per_user.csv")
    d = d[d["cohort_users"] >= 1000]                        # ignore tiny cohorts
    months = [c for c in ["2026-08", "2026-09", "2026-10"] if c in d.columns]
    per_day = d[months].div([m[c] for c in months])
    fig, ax = plt.subplots(figsize=(6.5, 4.5))
    im = ax.imshow(per_day.to_numpy(), cmap="Blues", vmin=0, vmax=per_day.to_numpy().max() * 1.4)
    ax.set_xticks(range(len(months)))
    ax.set_xticklabels(months)
    ax.set_yticks(range(len(d)))
    ax.set_yticklabels(d["cohort"])
    for i in range(per_day.shape[0]):
        for j in range(per_day.shape[1]):
            ax.text(j, i, f"{per_day.iloc[i, j]:.1f}", ha="center", va="center", fontsize=9)
    ax.set_xlabel("Purchase month")
    ax.set_ylabel("Registration cohort")
    ax.set_title("Revenue per user per day: no cohort effect")
    ax.spines[:].set_visible(False)
    save(fig, "05_cohort_heatmap.png", "Spark SQL cohort analysis (cohorts with at least 1,000 users)")


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    chart_revenue_by_category()
    chart_segments()
    chart_funnel()
    chart_spend_vs_engagement()
    chart_cohorts()
    print("Done.")