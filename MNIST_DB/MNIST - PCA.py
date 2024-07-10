# -*- coding: utf-8 -*-
"""
Eigenvectors and PCA

@author: ManuelQuintoSabelli
"""

import numpy as np
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.datasets import fetch_openml
from sklearn.impute import SimpleImputer

#-----------------------------------------------------------------------------

# Load MNIST dataset
mnist = fetch_openml('mnist_784', version=1)
X, y = mnist.data, mnist.target

# missing values
imputer = SimpleImputer(strategy='mean')
X_imputed = imputer.fit_transform(X)

# Standardize the data // normalize it to have mean 0 and variance 1.
epsilon = 1e-10
X_mean = np.mean(X_imputed, axis=0)
X_std = np.std(X_imputed, axis=0) + epsilon
X_standardized = (X_imputed - X_mean) / X_std

#-----------------------------------------------------------------------------

explained_variances = []

# Loop through different n_components
for n_components in range(40, 121, 20):
    
    # Compute PCA
    pca = PCA(n_components=n_components)
    X_pca = pca.fit_transform(X_standardized)
    
    # Store explained variance ratio
    explained_variances.append(np.sum(pca.explained_variance_ratio_))
    
    # explained variance
    plt.figure(figsize=(12, 8))
    plt.plot(np.cumsum(pca.explained_variance_ratio_))
    plt.xlabel('Number of Components')
    plt.ylabel('Explained Variance')
    plt.title(f'Explained Variance by Number of Components - n_components={n_components}')
    plt.grid(True)
    plt.show()
    
    # first 2 principal components
    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(X_pca[:, 0], X_pca[:, 1], c=y.astype(int), alpha=0.5, cmap='viridis')
    plt.xlabel('First Principal Component')
    plt.ylabel('Second Principal Component')
    plt.title(f'MNIST Data Projected onto First Two Principal Components - n_components={n_components}')
    plt.colorbar(scatter, label='Digit Label')
    plt.grid(True)
    plt.show()

# explained variance for different n_components
plt.figure(figsize=(12, 8))
plt.plot(range(80, 121, 10), explained_variances, marker='o')
plt.xlabel('Number of Components')
plt.ylabel('Total Explained Variance')
plt.title('Total Explained Variance by Number of Components')
plt.grid(True)
plt.show()