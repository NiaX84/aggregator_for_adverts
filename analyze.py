import pandas as pd
import json


string_columns = ['title', 'description', 'sellerName', 'sellerPhones', 'sellerAddress',
       'sellerEmail', 'sellerWeb', 'currency', 'offerType', 'type',
       'images', 'address', 'url', 'dateCreated',
       'floorArea', 'condition', 'priceType',
       'postalCode', 'createdAt', 'city',
       'agencyName', 'ID']


with open('records.json', "r", encoding='utf-8') as f:
    data_record = json.loads(f.read())

df = pd.DataFrame.from_dict(data_record)

n_records = df.shape[0]
print(n_records)

print('address', df['address'][df["address"] == 'Slovensko'].count()/n_records)
print('address', df['address'][df["address"] == 'Zahraničie'].count()/n_records)
print('address', df['address'][df["address"] == 'seller_address'].count()/n_records)
print('address', df['address'].isna().sum())

print("___", "address_source", "___")
print('address_source: seller', df['address_source'][df["address_source"] == 'seller_address'].count()/n_records)
print('address_source: city', df['address_source'][df["address_source"] == 'city'].count()/n_records)
print('address_source: no_data', df['address_source'][df["address_source"] == 'default'].count()/n_records)

print("___", "address", "___")
print('address: Slovensko', df['address'][df["address"] == 'Slovensko'].count()/n_records)
print('address: Zahranicie', df['address'][df["address"] == 'Zahraničie'].count()/n_records)

print("___", 'default', "___")

for column in string_columns:
    default_values = df[column][df[column] == 'default'].count()
    if default_values > 0:
        print(column, default_values/n_records)

print("___", 'neuvedené', "___")

for column in string_columns:
    default_values = df[column][df[column] == 'neuvedené'].count()
    if default_values > 0:
        print(column, default_values/n_records)

print("___", 'no value', "___")

for column in df.columns:
    default_values = df[column].isna().sum()
    if default_values > 0:
        print(column, default_values/n_records)
