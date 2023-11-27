import pandas as pd
import matplotlib.pyplot as plt
import json
import os

data = {}
for p in range(4):
    filename = f'partition-{p}.json'
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            data[p] = json.load(f)

months = ['January', 'February', 'March']
avg_temperatures = {}
for month in months:
    temperatures = []
    for p in data:
        if month in data[p]:
            year = max(data[p][month].keys())
            temperatures.append(data[p][month][year]['avg'])
    avg_temperatures[month] = sum(temperatures) / len(temperatures) if temperatures else None

month_series = pd.Series(avg_temperatures)
fig, ax = plt.subplots()
month_series.plot.bar(ax=ax)
ax.set_ylabel('Avg. Max Temperature')
plt.tight_layout()
plt.savefig("/files/month.svg")
