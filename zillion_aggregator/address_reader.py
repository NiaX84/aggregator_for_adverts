import pandas as pd

import json
from urllib.request import urlopen


class AddressReader:
    unwanted_words = ['Štát:', 'Mesto:', 'Lokalita:', 'Ulica:']
    unwanted_words2 = ['Štát', 'Mesto', 'Lokalita', 'Ulica']

    def read_address(self, files):
        records = []
        data_files = files.split(", ")
        for file in data_files:
            records.extend(self.load_addresses(file))

        records_df = pd.DataFrame(records).to_dict(orient='records')
        return records_df

    def load_addresses(self, file):
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
        adresses = []
        for entry in data_record:
            try:
                address = entry['address']
                if not address:
                    address_tmp = [value for _, value in entry['sellerAddress'].items() if value]
                    address = ', '.join(address_tmp) if address_tmp else 'Slovensko'
                if any(word in cls.unwanted_words for word in address.split()):
                    address = address.replace(' - ', '-')\
                        .replace("Štát: ", ":")\
                        .replace("Mesto: ", ":")\
                        .replace("Lokalita: ", ":")\
                        .replace("Ulica: ", ":")
                    address = address.split(":")
                    address = ', '.join(word for word in address if word not in cls.unwanted_words2)
                if '(' in address or ")" in address:
                    address = address.replace('(', ', ').replace(')', '')
                adresses.append({'address': address})
            except KeyError:
                try:
                    address = entry['city']
                    adresses.append({'address': address})
                except KeyError:
                    adresses.append({'address': 'Slovensko'})

        return adresses
