import numpy as np
import matplotlib.pyplot as plt

def plot_counts(l, title=None):
    indexes = np.arange(len(l))
    counts = [r[1] for r in l]
    labels = [r[0] for r in l]
    if (title is not None): plt.title(title)
    plt.grid(True, axis='y')
    plt.xticks(indexes, labels, rotation='vertical')
    return plt.bar(indexes, counts)

def plot_counts_subgroups(l, sublabels, title=None):
    width = 0.35
    indexes = np.arange(len(l))
    counts1 = [r[1] for r in l]
    counts2 = [r[2] for r in l]
    labels1 = [r[0] for r in l]
    fig, ax = plt.subplots()
    plt.xticks(indexes, labels1, rotation='vertical')
    if (title is not None): plt.title(title)
    rects1 = ax.bar(indexes - width/2, counts1, width)
    rects2 = ax.bar(indexes + width/2, counts2, width)
    ax.legend((rects1[0], rects2[0]),sublabels)
    ax.grid(True, axis='y')
    return plt.show()
