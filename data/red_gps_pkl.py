import pandas as pd

if __name__ == '__main__':
    gps_df = pd.read_pickle('gps.pkl').drop(['address'], axis=1)
    new_pickle = {}
    for row in gps_df.iterrows():
        new_pickle[row[1].address_lower] = {'lat': row[1].lat, 'lon': row[1].lon, 'approx': row[1].approx}
    new_pickle_df = pd.DataFrame.from_dict(new_pickle, orient='index', columns=['lat', 'lon', 'approx'])
    new_pickle_df.to_pickle('gps_by_address_lower.pkl')
    print(new_pickle_df.index.values)
