from __future__ import division
import pandas as pd
import numpy as np

traindata=pd.read_csv("output2.csv")
array=traindata["movieid"].unique()
array1=traindata["id"].unique()
traind= (traindata.head(100000))
loaddata=pd.read_csv("movie_titles.csv",header = None, names = ['movieid', 'name'], usecols = [0,2],engine="python")
movienames=loaddata["name"]
pivottable=pd.pivot_table(traind,values="rating",index='id',columns='movieid').fillna(0)
matrix=pivottable.as_matrix()
values=pivottable.values
print (values.shape)
changevalue = values>0.5
changevalue[changevalue == True] = 1
changevalue[changevalue == False] = 0
# To be consistent with our Q matrix
changevalue = changevalue.astype(np.float64, copy=False)
#print (changevalue)
lambdaconst = 0.5
factors = 100
rows, columns = values.shape

iterations = 5
X = 5 * np.random.rand(rows, factors)
Y = 5 * np.random.rand(factors, columns)
def get_error(pivottable, X, Y, changevalue):
    return np.sum((changevalue * (pivottable - np.dot(X, Y)))**2)
err = []
for ii in range(iterations):
    X = np.linalg.solve(np.dot(Y, Y.T) + lambdaconst * np.eye(factors),
                        np.dot(Y, values.T)).T
    Y = np.linalg.solve(np.dot(X.T, X) + lambdaconst * np.eye(factors),
                        np.dot(X.T, values))
    '''if ii % 100 == 0:
        print('{}th iteration is completed'.format(ii))'''
    err.append(get_error(values, X, Y, changevalue))
converge = np.dot(X, Y)
#print('Error of rated movies: {}'.format(get_error(values, X, Y, changevalue)))

def recommend(changevalue, Q, converge, movie_titles):
    
    converge = converge-np.min(converge)
    converge =converge * (float(5) / np.max(converge))
    movie_ids = np.argmax(converge - 5 * changevalue, axis=1)
    for jj, movie_id in zip(range(rows), movie_ids):
        #if converge[jj, movie_id] < 0.1: continue
        print('User with user id {} liked {}\n'.format(jj + 1, ', '.join([movie_titles[ii] for ii, qq in enumerate(Q[jj]) if qq > 3])))
        print('User with user id {} did not like {}\n'.format(jj + 1, ', '.join([movie_titles[ii] for ii, qq in enumerate(Q[jj]) if qq < 3 and qq != 0])))
        print('\n User with user id {} and the recommended movie is {} and the predicted rating is {}'.format(
                    jj + 1, movie_titles[movie_id], converge[jj, movie_id]))
        print('\n' + 100 *  '-' + '\n')


werr = []
for ii in range(iterations):
    for u, Wu in enumerate(changevalue):
        X[u] = np.linalg.solve(np.dot(Y, np.dot(np.diag(Wu), Y.T)) + lambdaconst * np.eye(factors),
                               np.dot(Y, np.dot(np.diag(Wu), values[u].T))).T
    for i, Wi in enumerate(changevalue.T):
        Y[:,i] = np.linalg.solve(np.dot(X.T, np.dot(np.diag(Wi), X)) + lambdaconst * np.eye(factors),
                                 np.dot(X.T, np.dot(np.diag(Wi), values[:, i])))
    werr.append(get_error(values, X, Y, changevalue))
    print('{}th iteration is completed'.format(ii))
wconverge = np.dot(X,Y)

recommend(changevalue,values,wconverge,movienames)