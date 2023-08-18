import os
import pandas as pd


accum_path = '../data/results/ideal/accum/'
power_path = '../data/results/ideal/power/'
files_accum = os.listdir(accum_path)
files_power = os.listdir(power_path)

dfs_accum = []
dfs_power = []

for f in files_accum:
    data = pd.read_excel(accum_path + f, 'accumulated_distance')
    dfs_accum.append(data)

for f in files_power:
    data = pd.read_excel(power_path + f, 'power_distance')
    dfs_power.append(data)

concatenated_accum = pd.concat(dfs_accum)
concatenated_power = pd.concat(dfs_power)

print(len(concatenated_accum))
print(len(concatenated_power))

concatenated_accum.to_excel('accum.xlsx', sheet_name='accumulated_distance')
concatenated_power.to_excel('power.xlsx', sheet_name='power_distance')
