""" Matplotlib utilities. """

import numpy as np
import matplotlib.pyplot as plt


def plot_counts(alist, title=None):
    """ Plot a histogram. """

    indexes = np.arange(len(alist))
    counts = [r[1] for r in alist]
    labels = [r[0] for r in alist]
    if title is not None:
        plt.title(title)
    plt.grid(True, axis='y')
    plt.xticks(indexes, labels, rotation='vertical')
    return plt.bar(indexes, counts)


def plot_counts_subgroups(alist, sublabels, title=None):
    """ Plot histogram with grouping. """

    width = 0.35
    indexes = np.arange(len(alist))
    counts1 = [r[1] for r in alist]
    counts2 = [r[2] for r in alist]
    labels1 = [r[0] for r in alist]
    _, axes = plt.subplots()
    plt.xticks(indexes, labels1, rotation='vertical')
    if title is not None:
        plt.title(title)
    rects1 = axes.bar(indexes - width/2, counts1, width)
    rects2 = axes.bar(indexes + width/2, counts2, width)
    axes.legend((rects1[0], rects2[0]), sublabels)
    axes.grid(True, axis='y')
    return plt.show()
