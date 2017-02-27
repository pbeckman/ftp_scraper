import pandas as pd
import itertools
from math import isnan
from sklearn.neighbors import KNeighborsClassifier
from sklearn.decomposition import PCA
from sklearn.model_selection import train_test_split, GridSearchCV, ShuffleSplit, StratifiedKFold
import scipy as sp
import cPickle as pkl
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
from metadata_util import is_number
from pylab import cm

np.set_printoptions(threshold=np.nan)


def get_text_rows(matrix):
    to_remove = []
    for i in range(0, len(matrix)):
        if not np.vectorize(is_number)(matrix[i]).all():
            to_remove.append(i)

    return to_remove


def fill_zeros(matrix):
    num_rows, num_cols = matrix.shape
    output_matrix = np.empty(matrix.shape)
    for i in range(0, num_rows):
        for j in range(0, num_cols):
            if matrix[i][j] is None or isnan(float(matrix[i][j])) or float(matrix[i][j]) == float('inf'):
                output_matrix[i][j] = np.float64(0)
            else:
                output_matrix[i][j] = matrix[i][j]

    return output_matrix


def clean_data(X, y):
    to_remove = get_text_rows(X)
    X = np.delete(X, to_remove, axis=0)
    y = np.delete(y, to_remove, axis=0)
    X = fill_zeros(X)
    y = fill_zeros(y)

    return X, y


def bin_null_values(y):
    y_output = np.zeros(y.shape)

    nulls = [0]
    num_rows, num_cols = y.shape
    for i in range(0, num_rows):
        for j in range(0, num_cols):
            if y[i][j] != 0:
                if y[i][j] not in nulls:
                    nulls.append(y[i][j])
                y_output[i][j] = nulls.index(y[i][j])

    return nulls, y_output


def percent_correct(actual, predicted):
    assert actual.shape == predicted.shape

    num_correct = 0
    num_rows = actual.shape[0]
    for i in range(0, num_rows):
        if (predicted[i] == actual[i]).all():
            num_correct += 1

    return float(num_correct) / num_rows


def percent_false_positive(actual, predicted):
    assert actual.shape == predicted.shape

    num_false_pos = 0
    num_rows, num_cols = actual.shape
    false_positive = False
    for i in range(0, num_rows):
        for j in range(0, num_cols):
            if actual[i][j] == 0 and predicted[i][j] != 0 \
                    or actual[i][j] != 0 and predicted[i][j] != 0 and predicted[i][j] != actual[i][j]:
                false_positive = True
        if false_positive:
            num_false_pos += 1

        false_positive = False

    return float(num_false_pos) / num_rows


def percent_false_negative(actual, predicted):
    assert actual.shape == predicted.shape

    num_false_neg = 0
    (num_rows, num_cols) = actual.shape
    false_negative = False
    for i in range(0, num_rows):
        for j in range(0, num_cols):
            if actual[i][j] != 0 and predicted[i][j] == 0:
                false_negative = True
        if false_negative:
            num_false_neg += 1

        false_negative = False

    return float(num_false_neg) / num_rows


data = pd.read_csv('col_metadata.csv')
X = data.iloc[:, 3:-1].values
y = data.iloc[:, -1:].values

X, y = clean_data(X, y)
nulls, y = bin_null_values(y)

# all_y_test = np.zeros((0, 1))
# all_y_pred = np.zeros((0, 1))

# model = KNeighborsClassifier(algorithm='auto', leaf_size=30, metric='euclidean',
#                              metric_params=None, n_jobs=1, n_neighbors=19,
#                              weights='distance')

# params = {"n_neighbors": np.arange(1, 31, 2),
#           "metric": ["euclidean", "cityblock"],
#           "weights": ['uniform', 'distance']
#           }
#
# model = GridSearchCV(KNeighborsClassifier(algorithm='auto', leaf_size=30,
#                                           metric_params=None, n_jobs=1), params)
#
# model.fit(X, y.reshape(y.shape[0], ))
#
# print model.best_params_


# for train_inds, test_inds in ShuffleSplit(n_splits=100, test_size=0.01).split(X, y):
#     # Split off the train and test set
#     X_test, y_test = X[test_inds, :], y[test_inds]
#     X_train, y_train = X[train_inds, :], y[train_inds]
#
#     # Train the model
#     model.fit(X_train, y_train)
#     y_pred = model.predict(X_test).reshape(-1, 1)  # 482, 1
#
#     # Append the results
#     all_y_test = np.concatenate((all_y_test, y_test))
#     all_y_pred = np.concatenate((all_y_pred, y_pred))
#
# print "accuracy: {}\nalpha: {}\nbeta: {}".format(percent_correct(all_y_test, all_y_pred),
#                                                  percent_false_positive(all_y_test, all_y_pred),
#                                                  percent_false_negative(all_y_test, all_y_pred)

pca = PCA(n_components=2)
X_fit_pca = pca.fit(X)
X_r = X_fit_pca.transform(X)

one = plt.scatter([X_r[i, 0] for i in range(0, 4813) if y[i] == 0],
            [X_r[i, 1] for i in range(0, 4813) if y[i] == 0],
            c='r', s=100, alpha=.5)
plt.scatter([X_r[i, 0] for i in range(0, 4813) if y[i] == 1],
            [X_r[i, 1] for i in range(0, 4813) if y[i] == 1],
            c='g', s=100, alpha=.5)
plt.scatter([X_r[i, 0] for i in range(0, 4813) if y[i] == 2],
            [X_r[i, 1] for i in range(0, 4813) if y[i] == 2],
            c='m', s=100, alpha=.5)
plt.scatter([X_r[i, 0] for i in range(0, 4813) if y[i] == 3],
            [X_r[i, 1] for i in range(0, 4813) if y[i] == 3],
            c='c', s=100, alpha=.5)
plt.suptitle('PCA visualization of null value data', fontsize=20)
# one.axes.get_xaxis().set_visible(False)
# one.axes.get_yaxis().set_visible(False)
plt.show()

# x_ranges = []
# for i in range(0, 12):
#     if i in [0, 1, 5, 10, 11]:
#         i_min, i_max = X[:, i].min() - 1, X[:, i].max() + 1
#         x_ranges.append(np.linspace(i_min, i_max, 3))
#
# x_mesh = list(itertools.product(*x_ranges))
#
# print len(x_mesh)
# print "predicting x_mesh"
# Z = model.predict(x_mesh).reshape(-1, 1)
# print Z
# print "performing PCA transformation on x_mesh result"
# Z_r = X_fit_pca.transform(Z)
#
# print Z_r



# Create color maps
# cmap_light = ListedColormap(['#FFAAAA', '#AAFFAA', '#AAAAFF'])
# cmap_bold = ListedColormap(['#FF0000', '#00FF00', '#0000FF'])

# Plot the decision boundary. For that, we will assign a color to each point in the mesh


# Put the result into a color plot
# Z = Z.reshape(xx.shape)
# plt.figure()
# plt.pcolormesh(xx, yy, Z, cmap=cmap_light)

# Plot also the training points
# plt.scatter(X[:, 0], X[:, 1], c=y, cmap=cmap_bold)
# plt.xlim(xx.min(), xx.max())
# plt.ylim(yy.min(), yy.max())
# plt.title("3-Class classification (k = %i, weights = '%s')"
#           % (5, 'uniform'))
# plt.show()
