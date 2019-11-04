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
        'stav nehnuteľnosti': 'stav'
    }

    grouping_keys = {'price', 'sellerName', 'sellerWeb', 'address', 'offerType', 'type', 'currency'}
    sub_group_keys = {'ID', 'dateCreated', 'title', 'description', 'priceType', 'url', 'subType', 'images',
                      'cityAddress', 'state', 'celková podlahová plocha', 'stav'}

    all_record_keys = grouping_keys.union(sub_group_keys)

    unwanted_words = ['Štát:', 'Mesto:', 'Lokalita:', 'Ulica:']
    unwanted_words2 = ['Štát', 'Mesto', 'Lokalita', 'Ulica']

    def aggregate(self, files):
        # gps_coords = pd.read_pickle('gps.pkl').to_dict(orient='records')
        #
        # def extract_latitude(address):
        #     if not address:
        #         return None
        #     row = [record['lat'] for record in gps_coords if record['city'] == address.lower()]
        #     if not row:
        #         return None
        #     return row[0]
        #
        # def extract_longitude(address):
        #     if not address:
        #         return None
        #     row = [record['lon'] for record in gps_coords if record['city'] == address.lower()]
        #     if not row:
        #         return None
        #     return row[0]

        records = []
        data_files = files.split(", ")
        for file in data_files:
            records.extend(self.load_all_records(file))

        records_df = pd.DataFrame(records)
        # records_df['latitude'] = records_df['address'].apply(extract_latitude)
        # records_df['longitude'] = records_df['address'].apply(extract_longitude)
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
            if 'properties' in entry:
                properties = entry['properties']
                del entry['properties']
                properties_data = [data for data in properties if data['name']]
                entry.update(Aggregator.get_property_values(properties_data))
            entry.update(cls.get_address_specification(entry))
            entry_keys = {key for key in entry}
            entry.update({key: 'default' for key in cls.all_record_keys - entry_keys})

        return data_record

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

    @classmethod
    def get_address_specification(cls, entry):
        try:
            address = entry['address']
            if not address:
                address_tmp = [value for _, value in entry['sellerAddress'].items() if value]
                address = ', '.join(address_tmp) if address_tmp else 'Slovensko'
            if any(word in cls.unwanted_words for word in address.split()):
                address = address.replace(' - ', '-') \
                    .replace("Štát: ", ":") \
                    .replace("Mesto: ", ":") \
                    .replace("Lokalita: ", ":") \
                    .replace("Ulica: ", ":")
                address = address.split(":")
                address = ', '.join(word for word in address if word not in cls.unwanted_words2)
            if '(' in address or ")" in address:
                address = address.replace('(', ', ').replace(')', '')
            return {'address': address}
        except KeyError:
            try:
                address = entry['city']
                return {'address': address}
            except KeyError:
                return {'address': 'Slovensko'}
