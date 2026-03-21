import matplotlib.pyplot as plt
from pathlib import Path


plt.rcParams["font.family"] = "Microsoft JhengHei"
plt.rcParams["axes.unicode_minus"] = False


def plot_trend(df, save_path=None):
    plt.figure(figsize=(10, 5))
    plt.plot(df["date"], df["avg_aqi"])
    plt.title("Daily Average AQI")
    plt.xlabel("Date")
    plt.ylabel("AQI")
    plt.xticks(rotation=45)
    plt.tight_layout()

    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=150, bbox_inches="tight")
        plt.close()
    else:
        plt.show()


def plot_county(df, save_path=None):
    top = df.head(10)

    plt.figure(figsize=(10, 5))
    plt.bar(top["county"], top["aqi"])
    plt.title("Top 10 Counties by AQI")
    plt.xlabel("County")
    plt.ylabel("Average AQI")
    plt.xticks(rotation=45)
    plt.tight_layout()

    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=150, bbox_inches="tight")
        plt.close()
    else:
        plt.show()


def plot_hours(df, save_path=None):
    plt.figure(figsize=(10, 5))
    plt.bar(df["hour"], df["high_pollution_count"])
    plt.title("High Pollution Hours (AQI > 100)")
    plt.xlabel("Hour")
    plt.ylabel("Count")
    plt.tight_layout()

    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=150, bbox_inches="tight")
        plt.close()
    else:
        plt.show()