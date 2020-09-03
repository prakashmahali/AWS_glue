import pandas as pd
patients_df = pd.read_json('C:/Users/praka/Desktop/Currency.json')
#patients_df.head()
patients_df.columns
df=patients_df.iloc[[0],[0]]
pd.set_option('display.max_colwidth', -1)
df
dt=df.to_dict()
dt
currency=[]
date=[]
rate=[]
for i in dt['data']['items']:
    #print(i['symbol'])
    for j in i['close']:
        currency.append(i['symbol'])
        date.append(j[0])
        rate.append(j[1])
print(currency) 
print(date)
print(rate)
data={'currency':currency,'date]':date,'rate':rate}
data
df1 = pd.DataFrame(data)
df1['currency'] = df1['currency'].str[3:9]
df1
