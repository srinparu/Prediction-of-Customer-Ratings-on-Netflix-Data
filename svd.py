import pandas as pd
import numpy as np
from scipy.sparse.linalg import svds

df=pd.read_csv("output1.csv")
array2=df["id"]
df2=df.head(100000)

df1=pd.read_csv("movie_titles.csv",header = None, names = ['movieid', 'name'], usecols = [0,2],engine="python")

df_p = pd.pivot_table(df2,values='rating',index='id',columns='movieid').fillna(0)
#print(df_p.head(10))

pivotmatrix = df_p.as_matrix()
#print(pivotmatrix)
ratingsmean = np.mean(pivotmatrix, axis = 1)
#print(ratingsmean)
pivotchangematrix = pivotmatrix - ratingsmean.reshape(-1, 1)
#print(pivotchangematrix)
matrix1, matrix2, matrix3 = svds(pivotchangematrix, k = 50)

matrix2 = np.diag(matrix2)


all_user_predicted_ratings = np.dot(np.dot(matrix1, matrix2), matrix3) + ratingsmean.reshape(-1, 1)
#print(all_user_predicted_ratings)
predicted = pd.DataFrame(all_user_predicted_ratings, columns = df_p.columns)
#print(predicted)

def getrow(userid):
    for i in range(100000):
        if (userid == array2[i]):
            a=i
            break
    return a

def moviesrecmnd(predf, userid, moviesdf, ratingsdf, totalrecmnd,rowno):
    # Get and sort the user's predictions
    #print(rowno)
    user_row_number = rowno  # UserID starts at 1, not 0
    sorted_user_predictions = predf.iloc[user_row_number].sort_values(ascending=False)
    #print(sorted_user_predictions)

    # Get the user's data and merge in the movie information.
    print(ratingsdf.loc[ratingsdf['id'] == userid])
    user_data = ratingsdf[ratingsdf.id == (userid)]
    #print (user_data)
    user_full = (user_data.merge(moviesdf, how='left', left_on='movieid', right_on='movieid').
                 sort_values(['rating'], ascending=False)
                 )

    print ('User {0} has already rated {1} movies.'.format(userid, user_full.shape[0]))
    print('Recommending the highest {0} predicted ratings movies not already rated.'.format(totalrecmnd))

    # Recommend the highest predicted rating movies that the user hasn't seen yet.
    recommendations = (moviesdf[~moviesdf['movieid'].isin(user_full['movieid'])].
                           merge(pd.DataFrame(sorted_user_predictions).reset_index(), how='left',
                                 left_on='movieid',
                                 right_on='movieid').
                           rename(columns={user_row_number: 'Predictions'}).
                           sort_values('Predictions', ascending=False).
                           iloc[:totalrecmnd, :-1]
                           )

    return user_full, recommendations



a=getrow(451267)
already_rated, predictions = moviesrecmnd(predicted, 451267, df1, df2
                                            , 10,a)
print(already_rated.head(10))
print (predictions)

