# -*- coding: utf-8 -*-
"""
Created on Tue Sep  1 22:40:11 2020

@author: Marcelo
"""
import pandas as pd
import numpy as np
import datetime
import time
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import GridSearchCV
from sklearn.svm import SVR
import pickle

class ML_interpol:
    def space_interpol(full_prop_good,data_geo,dates_change, list_elements,dest_folder):
        df_fechas_elems=pd.DataFrame()
        df_best_models = pd.DataFrame()
        for fecha in dates_change:
            '''
            0- fecha inicial hacer un modelo base, con la foto instantanea
            '''
            data_propiedades = full_prop_good.loc[full_prop_good['date']==fecha, ['name_c','date','AÑO']+list_elements]    #,'PH','Nitrogeno','Fosforo','Potasio','Calcio','Magnesio','Sodio','Aluminio','Azufre','Cloro','Hierro','Manganeso','Cobre','Zinc','Boro']]#,'x','y']]
            data_propiedades = data_geo.merge(data_propiedades, how='left', left_on=['name_c'], right_on=['name_c'])
            df_elementos=pd.DataFrame()
            ################################################################################################################3
            #Machine learning model  
            '''
            1+ - traer el ultimo valor y con eso el modelo tendria pos x,y y valor anterior
            2- si se tienen trabajos y abonos, eso usarlo para predecir futuros valores
            '''
            for elemento in list_elements:
                small_data = data_propiedades.loc[:,['x','y',elemento]]
                #outlier detection
                Q1 = full_prop_good[elemento].quantile(0.25)
                Q3 = full_prop_good[elemento].quantile(0.75)
                IQR = Q3-Q1
                low_thr = Q1 - 1.5*IQR
                high_thr = Q3 + 2.1*IQR #larger gap for few data
                #replace outliers
                small_data[elemento]= np.where(small_data[elemento] > high_thr, high_thr ,
                                               np.where(small_data[elemento] < low_thr, low_thr ,small_data[elemento]))
                
                #
                X = small_data.dropna().loc[:,['x','y']].values
                y = small_data.dropna().loc[:,elemento].values
                X_val = data_propiedades[pd.isnull(data_propiedades['date'])].loc[:,['x','y']].values
                X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.20, random_state = 0)
                
                # Feature Scaling
                sc_X = StandardScaler()
                X_train = sc_X.fit_transform(X_train)  #trained scale on "train"
                X_test = sc_X.transform(X_test)        #scale test, based on "train" scaling
                X_val = sc_X.transform(X_val) 
                #if y requires to be scaled
                sc_y = StandardScaler()
                y_train = sc_y.fit_transform(y_train.reshape(-1,1)).flatten()   #reshape is required when only 1 variable
                y_test = sc_y.transform(y_test.reshape(-1,1)).flatten()
                
                #SVR   #(requires standardization of variables)
                # tunning - Applying Grid Search to find the best model and the best parameters
                regressor = SVR(kernel='rbf')
                if elemento == 'Cloro':
                    parameters = [{'C': [1,10], 'kernel': ['poly'], 'degree':[1,2],'epsilon':[0.001,0.01]}]
                else:
                    parameters = [{'C': [10, 100, 1000], 'kernel': ['rbf'], 'gamma': [0.001,0.01,0.1, 0.25, 0.5],'epsilon':[0.001,0.01,0.1,0.5,1,2]}]
                grid_search = GridSearchCV(estimator = regressor,
                                           param_grid = parameters,
                                           scoring = 'neg_mean_squared_error',#'r2',#'neg_mean_squared_error',#'r2',
                                           cv = 10,
                                           n_jobs = -1)
                grid_search = grid_search.fit(X_train, y_train)
                best_accuracy = grid_search.best_score_
                best_parameters = grid_search.best_params_
                print("Element: ", elemento, "Date: ", fecha)
                print("Best MSE: {:.4f}".format(best_accuracy))
                print("Best Parameters:", best_parameters)
                
                #table of best models by data and element
                data = [{'Fecha': fecha, 'Elemento': elemento,  'Model_parameters': best_parameters,  'Model_accuracy': best_accuracy}]
                model_df = pd.DataFrame(data)
                if df_best_models.empty:
                    df_best_models =  model_df
                else:
                    df_best_models = df_best_models.append(model_df)
                    
                # save the model to disk
                fecha_txt = datetime.datetime.strftime(fecha,'%Y-%m-%d')
                filename = dest_folder+'/'+elemento+'_'+fecha_txt+'.sav'
                pickle.dump(grid_search.best_estimator_, open(filename, 'wb'))
                
                #best model
                regressor = grid_search.best_estimator_ #SVR(best_parameters)
                #regressor.fit(X_train,y_train)
                y_pred = regressor.predict(X_test)
                
                y_val = sc_y.inverse_transform(regressor.predict(X_val))
                X_val = sc_X.inverse_transform(X_val)
                val = np.column_stack((X_val,y_val))
                val = pd.DataFrame(val,columns=small_data.columns.tolist())
                small_data = small_data.merge(val,how='left', left_on=['x','y'], right_on=['x','y'])
                small_data[elemento]= np.where(small_data[elemento+"_x"].notnull(), small_data[elemento+"_x"],small_data[elemento+"_y"])
                small_data.drop(columns=[elemento+'_x',elemento+'_y'], inplace=True)
                small_data['date'] = fecha
                if df_elementos.empty:
                    df_elementos = small_data
                else:
                    df_elementos = df_elementos.merge(small_data, how='left', left_on=['x','y','date'], right_on=['x','y','date'])
                
                
            #vertical join dataframes    
            if df_fechas_elems.empty:
                df_fechas_elems = df_elementos
            else:
                df_fechas_elems = df_fechas_elems.append(df_elementos)
        return df_fechas_elems, df_best_models
        
#Visualization
#Training results for 2D problems
# =============================================================================
# X_set, y_set = X_train, y_train
# X1, X2 = np.meshgrid(np.arange(start = X_set[:, 0].min() - 0.5, stop = X_set[:, 0].max() + 0.5, step = 0.01), #-1 and +1 to enlarge range and include all posible points
#                      np.arange(start = X_set[:, 1].min() - 0.5, stop = X_set[:, 1].max() + 0.5, step = 0.01)) #0.01 resolution of pixels to make "continuous"
# plt.contourf(X1, X2, regressor.predict(np.array([X1.ravel(), X2.ravel()]).T).reshape(X1.shape),
#              alpha = 0.15, cmap = 'viridis', vmin=-2, vmax=3)
# plt.xlim(X1.min(), X1.max())
# plt.ylim(X2.min(), X2.max())
# #for i, j in enumerate(np.unique(y_set)):
# #    plt.scatter(X_set[y_set == j, 0], X_set[y_set == j, 1],
# #                c = ListedColormap(('red', 'green'))(i), label = j)
# plt.title('SVM (Training set)') #model name
# plt.xlabel('Lat') # 1st col name
# plt.ylabel('Long') #2nd col name
# plt.legend()
# plt.scatter(x=X_train[:,0],y=X_train[:,1],c=y_train, cmap='viridis', alpha=.25, marker='o', vmin=-2, vmax=3)
# plt.scatter(x=X_test[:,0],y=X_test[:,1],c=y_pred, s=70, cmap='viridis', alpha=0.9, marker='^', vmin=-2, vmax=3)
# plt.scatter(x=X_val[:,0],y=X_val[:,1],c=regressor.predict(X_val), s=100, cmap='viridis', alpha=0.9, marker='s', vmin=-2, vmax=3)
# plt.show()
# 
# plt.scatter(x=y_test,y=y_pred)        
# =============================================================================
