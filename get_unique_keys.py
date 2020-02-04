import json

from pandas.io.json import json_normalize

with open('records.json', 'r', encoding='utf-8') as f:
    data_record = json.loads(f.read())

df = json_normalize(data_record)

reduced_df = df[["offerType", "type", "stav"]]
print(reduced_df["offerType"].drop_duplicates().values)
print(reduced_df["type"].drop_duplicates().values)
print(reduced_df["stav"].drop_duplicates().values)
