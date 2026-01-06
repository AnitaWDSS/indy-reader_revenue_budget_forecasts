"""### Visualising Retention Curves"""

from main import retention_curves, splits
import matplotlib.pyplot as plt


def plot_km(**kwargs):
    if set(kwargs.keys()) != set(splits):
        raise ValueError(
            f"Invalid filter keys: {list(kwargs.keys())}. Keys must be: {splits}"
        )

    subset_df = retention_curves.copy()

    for col, value in kwargs.items():
        subset_df = subset_df[subset_df[col] == value]

    label = " - ".join(str(v) for v in kwargs.values())

    plt.plot(
        subset_df["month_index"],
        subset_df["retention_curve_rate"],
        linestyle="-",
        label=label,
    )


for i in range(15):
    random_row = retention_curves[splits].sample(1).iloc[0]
    random_splits = random_row.to_dict()

    print(random_splits)

    plt.figure(figsize=(8, 5))
    plt.xlabel("Month Index")
    plt.ylabel("Retention Rate")
    plt.grid(True)
    plt.tight_layout()

    plt.title("Retention Curves")

    plot_km(**random_splits)

    plt.legend()
    plt.show()
