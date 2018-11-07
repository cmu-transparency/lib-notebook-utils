"""Matplotlib utilities."""

import itertools
import numpy as np
import matplotlib.pyplot as plt


def plot_counts(alist, title=None):
    """Plot a histogram."""

    indexes = np.arange(len(alist))
    counts = [r[1] for r in alist]
    labels = [r[0] for r in alist]
    if title is not None:
        plt.title(title)
    plt.grid(True, axis='y')
    plt.xticks(indexes, labels, rotation='vertical')
    return plt.bar(indexes, counts)


def plot_counts_subgroups(alist, sublabels, title=None):
    """Plot histogram with grouping."""

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


# http://scikit-learn.org/stable/auto_examples/model_selection/plot_confusion_matrix.html
def plot_confusion_matrix(cm, classes,
                          normalize=False,
                          title='Confusion matrix',
                          cmap=plt.cm.Blues,
                          fig=None,
                          ax=None,
                          axes={'x':'x', 'y':'y'}
                          ):
    """
    This function prints and plots the confusion matrix.
    Normalization can be applied by setting `normalize=True`.
    """

    if ax is None:
        fig, ax = plt.subplots()

    if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]

    # classes_rv = [*classes]
    # classes_rv.reverse()

    plt.imshow(cm, interpolation='nearest', cmap=cmap)
    plt.title(title)
    plt.colorbar()
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes, rotation=45)
    plt.yticks(tick_marks, classes)

    fmt = '.2f' if normalize else 'd'
    thresh = cm.max() / 2.
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        # i = len(cm) - _i - 1
        ax.text(j, i, format(cm[i, j], fmt),
                horizontalalignment="center",
                color="white" if cm[i, j] > thresh else "black")

    plt.ylabel(axes['x'])
    plt.xlabel(axes['y'])

    # plt.tight_layout()
