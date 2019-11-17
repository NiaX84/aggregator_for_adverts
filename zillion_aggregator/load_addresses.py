import json

import pandas as pd


def load_address():
    with open('C:\\Users\\spoch\\repos\\aggregator_for_adverts\\addresses.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


if __name__ == '__main__':
    data = load_address()
    address_df = pd.DataFrame(data)
    address_df.to_pickle('gps.pkl')
    assert True
