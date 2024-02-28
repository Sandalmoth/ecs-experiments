import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib import cm as cm

df = pd.read_csv('out.tsv', sep='\t')
df = df.iloc[:-1]

fig, axs = plt.subplots(ncols=len(df['n'].unique()), nrows=3)

num_Ns = len(df['n'].unique())
num_ns = len(df['nsize'].unique())
num_ls = len(df['lsize'].unique())

nmax = df['n'].max()

for k, N in enumerate(sorted(df['n'].unique())):
    a = np.zeros((num_ns, num_ls))
    g = np.zeros((num_ns, num_ls))
    d = np.zeros((num_ns, num_ls))
    for i, n in enumerate(sorted(df['nsize'].unique())):
        for j, l in enumerate(sorted(df['lsize'].unique())):
            a[i, j] = df[(df['nsize'] == n) & (df['lsize'] == l) & (df['n'] == N)]['add']
            g[i, j] = df[(df['nsize'] == n) & (df['lsize'] == l) & (df['n'] == N)]['get']
            d[i, j] = df[(df['nsize'] == n) & (df['lsize'] == l) & (df['n'] == N)]['del']

    axs[0][k].imshow(a)
    axs[1][k].imshow(g)
    axs[2][k].imshow(d)

plt.show()


fig, axs = plt.subplots(ncols=2, nrows=3)

# colors = [cm.tab20(x) for x in range(num_ns)]
colors = [cm.viridis(x) for x in np.linspace(0, 1, num_ns)]
for i, n in enumerate(sorted(df['nsize'].unique())):
    _df = df[(df['nsize'] == n) & (df['n'] == nmax)]
    axs[0][0].plot(_df['lsize'], _df['add'], label=n, color=colors[i])
    axs[1][0].plot(_df['lsize'], _df['get'], label=n, color=colors[i])
    axs[2][0].plot(_df['lsize'], _df['del'], label=n, color=colors[i])

# colors = [cm.tab20(x) for x in range(num_ls)]
colors = [cm.viridis(x) for x in np.linspace(0, 1, num_ns)]
for i, n in enumerate(sorted(df['lsize'].unique())):
    _df = df[(df['lsize'] == n) & (df['n'] == nmax)]
    axs[0][1].plot(_df['nsize'], _df['add'], label=n, color=colors[i])
    axs[1][1].plot(_df['nsize'], _df['get'], label=n, color=colors[i])
    axs[2][1].plot(_df['nsize'], _df['del'], label=n, color=colors[i])

for i in range(3):
    axs[i][1].legend(bbox_to_anchor=(1.05, 1))
    for j in range(2):
        axs[i][j].set_xscale('log')
        axs[i][j].set_yscale('log')

plt.show()

