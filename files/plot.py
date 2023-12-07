import pandas as pd
import matplotlib.pyplot as plt
import json
import os

def load_partition_data(partition):
    path = f"./files/partition-{partition}.json"
    if os.path.exists(path):
        with open(path, 'r') as f:
            data = json.load(f)
    else:
        data = {"partition": partition, "offset": 0}
    return data

def main():
    partitions = [0, 1, 2, 3]
    partition_data = {p: load_partition_data(p) for p in partitions}

    month_data = {}
    for data in partition_data.values():
        for month, years in data.items():
            if month not in ['partition', 'offset']:
                latest_year = max(years.keys())
                month_data[f"{month}-{latest_year}"] = years[latest_year]['avg']

    month_series = pd.Series(month_data)

    fig, ax = plt.subplots()
    month_series.plot.bar(ax=ax)
    ax.set_ylabel('Avg. Max Temperature')
    plt.tight_layout()
    plt.savefig("/files/month.svg")

if __name__ == "__main__":
    main()
