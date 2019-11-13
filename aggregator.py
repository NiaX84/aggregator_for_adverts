import pandas as pd

import json
from urllib.request import urlopen


class Aggregator:
    column_mapping = {
        'sellerWeb': 'sellerWeb',
        'sellerName': 'sellerName',
        'id inzerátu': 'ID',
        'identifikačné číslo': 'ID',
        'createdAt': 'dateCreated',
        'vložené': 'dateCreated',
        'title': 'title',
        'description': 'description',
        'price': 'price',
        'poznámka k cene': 'priceType',
        'currency': 'currency',
        'offerType': 'offerType',
        'url': 'url',
        'type': 'type',
        'druh': 'subType',
        'images': 'images',
        'city': 'cityAdress',
        'state': 'state',
        'address': 'address',
        'zast. plocha': 'celková podlahová plocha',
        'zastavaná plocha': 'celková podlahová plocha',
        'zastavaná plocha v m2': 'celková podlahová plocha',
        'výmera zastavaného pozemku': 'celková podlahová plocha',
        'celková podlahová plocha': 'celková podlahová plocha',
        'celková podlah.plocha': 'celková podlahová plocha',
        'celková podlahová plocha bytu': 'celková podlahová plocha',
        'stav': 'stav',
        'Stav': 'stav',
        'stav nehnuteľnosti': 'stav'
    }

    grouping_keys = {'price', 'sellerName', 'sellerWeb', 'address', 'offerType', 'type', 'currency', 'latitude', 'longitude'}
    sub_group_keys = {'ID', 'dateCreated', 'title', 'description', 'priceType', 'url', 'subType', 'images',
                      'cityAddress', 'state', 'celková podlahová plocha', 'stav'}

    all_record_keys = grouping_keys.union(sub_group_keys)

    def aggregate(self, files):
        gps_coords = pd.read_pickle('gps.pkl').to_dict(orient='records')

        def extract_latitude(address):
            if not address:
                return None
            row = [record['lat'] for record in gps_coords if record['city'] == address.lower()]
            if not row:
                return None
            return row[0]

        def extract_longitude(address):
            if not address:
                return None
            row = [record['lon'] for record in gps_coords if record['city'] == address.lower()]
            if not row:
                return None
            return row[0]

        records = []
        data_files = files.split(", ")
        for file in data_files:
            records.extend(self.load_all_records(file))

        records_df = pd.DataFrame(records)
        records_df['latitude'] = records_df['address'].apply(extract_latitude)
        records_df['longitude'] = records_df['address'].apply(extract_longitude)
        result = records_df \
            .groupby(list(self.grouping_keys), as_index=False) \
            .apply(lambda x: x[list(self.sub_group_keys)].to_dict('r')) \
            .reset_index().rename(columns={0: 'details'}) \
            .to_dict(orient='records')
        return result

    def load_all_records(self, file):
        try:
            with urlopen(file) as f:
                data_record = self.collect_records(f)
                return data_record
        except ValueError:
            with open(file, "r", encoding='utf-8') as f:
                data_record = self.collect_records(f)
                return data_record

    @classmethod
    def collect_records(cls, f):
        data_record = json.loads(f.read().decode())
        for entry in data_record:
            if 'properties' not in entry:
                entry.update({'state': 'unknown'})
                entry_keys = {key for key in entry}
                entry.update({key: 'default' for key in cls.all_record_keys - entry_keys})
                entry.update(cls.calculate_position(entry))
                continue
            properties = entry['properties']
            del entry['properties']
            properties_data = [data for data in properties if data['name']]
            entry.update(Aggregator.get_property_values(properties_data))
            entry.update({'state': 'unknown'})
            entry_keys = {key for key in entry}
            entry.update({key: 'default' for key in cls.all_record_keys - entry_keys})
            entry.update(cls.calculate_position(entry))
        return data_record

    @classmethod
    def calculate_position(cls, entry):
        if entry['address'] is None or entry['address'] == 'default' or entry['address'] == "Zahraničie":
            return {'position_flag': 'unknown'}
        if ',' in entry['address']:
            return {'position_flag': 'exact'}
        else:
            return {'position_flag': 'city_center'}

    @classmethod
    def get_property_values(cls, properties_data):
        valid_property_data = (data for data in properties_data if
                               data['name'].lower().strip(':') in cls.column_mapping)
        result = {}
        for data in valid_property_data:
            try:
                result.update({cls.column_mapping[data['name'].lower().strip(':')]: data['value']})
            except KeyError:
                continue
        return result
