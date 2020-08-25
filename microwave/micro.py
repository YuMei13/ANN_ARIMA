import pandas as pd

df=pd.read_sql_table('microwave', 'sqlite:///dissertation.db')

df_engineering= df.copy()
#df_engineering['id']=np.repeat(range(1,4033),360)
df_20weeks=df_engineering[:1209600] # 20 weeks
#df_tsfresh=df_engineering[['time','id','kWh']]
df_tsfel=df_20weeks[['kWh']]

df_hour=df_20weeks[['kWh']]*1000 # convert kwh to wh
df_hour.rename(columns={'kWh':'Wh'},inplace=True)

import tsfel
#from tsfresh import extract_relevant_features
cfg = tsfel.get_features_by_domain()
X_train = tsfel.time_series_features_extractor(cfg, df_tsfel, window_splitter=True, window_size=8640) # one day
print(list(X_train.columns))
X_train=X_train[['0_Absolute energy','0_Mean','0_Max','0_Standard deviation','0_FFT mean coefficient_0','0_Spectral kurtosis','0_Skewness','0_Zero crossing rate']]

X_hour = tsfel.time_series_features_extractor(cfg, df_hour, window_splitter=True, window_size=360) # one hour
X_hour=X_hour[['0_Absolute energy','0_Mean','0_Max','0_Standard deviation','0_FFT mean coefficient_0','0_Spectral kurtosis','0_Skewness','0_Zero crossing rate']]



from sqlalchemy import create_engine
engine = create_engine('sqlite:///dissertation.db', echo=True) #set the database name
sqlite_connection = engine.connect()
sqlite_table = "microwave_tsfel"       #set the table name
sqlite_table2 = "microwave_hourtsfel"
X_train.to_sql(sqlite_table, sqlite_connection, if_exists='replace')  # import dataframe to sqlite
X_hour.to_sql(sqlite_table2, sqlite_connection, if_exists='replace')  # import dataframe to sqlite

#print(X_train)