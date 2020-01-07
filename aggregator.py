from functools import lru_cache
from urllib.error import URLError

import numpy as np
import pandas as pd

import json
from urllib.request import urlopen

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from Pickler import Pickler


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

    grouping_keys = {'similar_record_id'}
    sub_group_keys = {'sellerName', 'sellerWeb', 'address', 'offerType', 'type', 'currency', 'price', 'ID', 'dateCreated', 'title', 'description', 'priceType', 'url', 'subType', 'images',
                      'cityAddress', 'state', 'celková podlahová plocha', 'stav', 'lat', 'lon', 'approx', 'record_id'}

    all_record_keys = sub_group_keys

    gps_df = pd.read_pickle('gps_by_address_lower.pkl')
    gps_values = gps_df.index.values

    unwanted_words = ['Štát:', 'Mesto:', 'Lokalita:', 'Ulica:']
    unwanted_words2 = ['Štát', 'Mesto', 'Lokalita', 'Ulica']

    def aggregate(self, files):

        records = []
        data_files = files.split(", ")
        for file in data_files:
            records.extend(self.load_all_records(file))

        records_df = pd.DataFrame(records)
        records_df['description'].fillna('default', inplace=True)
        similar_records_df = self.group_records_by_description(records_df)
        records_df = records_df.merge(similar_records_df, on="record_id")
        result = records_df \
            .groupby(list(self.grouping_keys), as_index=False) \
            .apply(lambda x: x[list(self.sub_group_keys)].to_dict('r')) \
            .reset_index().rename(columns={0: 'details'}) \
            .to_dict(orient='records')
        return result

    def load_all_records(self, file):
        try:
            with urlopen(file) as f:
                data_record = json.loads(f.read().decode())
                data_record = self.collect_records(data_record)
                return data_record
        except URLError:
            with open(file, "r", encoding='utf-8') as f:
                data_record = json.loads(f.read())
                data_record = self.collect_records(data_record)
                return data_record

    @classmethod
    def collect_records(cls, data_record):
        for entry in data_record:
            address_spec = cls.get_address_specification(entry)
            entry.update(address_spec)
            if 'properties' in entry:
                properties = entry['properties']
                del entry['properties']
                properties_data = [data for data in properties if data['name']]
                entry.update(Aggregator.get_property_values(properties_data))
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
            if address == 'Zahraničie':
                address = 'Slovensko'
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
            return cls.get_position_for(address.lower())
        except KeyError:
            try:
                address = entry['city']
                return cls.get_position_for(address.lower())
            except KeyError:
                return cls.get_position_for('slovensko')

    @classmethod
    @lru_cache(maxsize=128)
    def get_position_for(cls, address):
        gps_dict = {'address': address.title()}
        if address in cls.gps_values:
            gps_dict.update(cls.gps_df.loc[address].to_dict())
        else:
            gps_dict.update(cls.gps_df.loc['slovensko'].to_dict())
        return gps_dict

    @staticmethod
    def group_records_by_description(records_df):

        def get_description_similarity():
            description_similarity[:, :] = cosine_similarity(tf_idf_matrix[step * offset: (step + 1) * offset], tf_idf_matrix)
            return description_similarity

        def init_similarity_indices():
            indices = np.nonzero(get_description_similarity() > 0.8)[0] + step*offset, np.nonzero(get_description_similarity() > 0.8)[1]
            return np.transpose(aggregated_records[indices, 0])

        def update_similarity_indices():
            indices = np.nonzero(get_description_similarity() > 0.8)[0] + step*offset, np.nonzero(get_description_similarity() > 0.8)[1]
            return np.vstack(
                (similarity_indices, np.transpose(aggregated_records[indices, 0])))

        similar_indices_all = None

        records_df['record_id'] = range(1, len(records_df) + 1)
        for _, group_df in records_df.groupby('offerType'):
            aggregated_records = group_df[['record_id', 'description']].values
            only_descriptions = aggregated_records[:, 1]
            tf_idf_vectorizer = TfidfVectorizer()
            tf_idf_matrix = tf_idf_vectorizer.fit_transform(only_descriptions)
            offset = 1 if len(only_descriptions) < 1000 else 1000
            description_similarity = np.empty((offset, len(only_descriptions)))
            n_steps = len(only_descriptions)//offset
            remainder = len(only_descriptions) % offset
            similarity_indices = None
            for step in range(n_steps):
                similarity_indices = init_similarity_indices() if step == 0 else update_similarity_indices()

            if similar_indices_all is None:
                similar_indices_all = similarity_indices
            else:
                similar_indices_all = np.vstack((similar_indices_all, similarity_indices))

        similarity_indices_pdf = pd.DataFrame(similar_indices_all, columns=['record_id', 'similar_record_id'])
        similar_records_pdf_grouped = similarity_indices_pdf.groupby('record_id', axis=0).agg(tuple)
        return similar_records_pdf_grouped
