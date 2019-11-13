from collections import namedtuple

import pandas as pd

import json
from urllib.request import urlopen
from mapbox import Geocoder


class AddressReader:
    unwanted_words = ['Štát:', 'Mesto:', 'Lokalita:', 'Ulica:']
    unwanted_words2 = ['Štát', 'Mesto', 'Lokalita', 'Ulica']
    token = 'pk.eyJ1IjoibmlheDg0IiwiYSI6ImNrMXAwNnYzcjBubGEzbms2dXQ3bmN3cTEifQ.uNKwpimGXu9vvzlDLFdEQg'
    collected_addresses = []

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
                approx = 1
                if not address:
                    address_tmp = [value for _, value in entry['sellerAddress'].items() if value]
                    address = ', '.join(address_tmp) if address_tmp else 'Slovensko'
                    approx = 1
                if address == 'Zahraničie':
                    address = 'Slovensko'
                    approx = 1
                if any(word in cls.unwanted_words for word in address.split()):
                    address = address.replace(' - ', '-')\
                        .replace("Štát: ", ":")\
                        .replace("Mesto: ", ":")\
                        .replace("Lokalita: ", ":")\
                        .replace("Ulica: ", ":")
                    address = address.split(":")
                    address = ', '.join(word for word in address if word not in cls.unwanted_words2)
                    approx = 0
                if '(' in address or ")" in address:
                    address = address.replace('(', ', ').replace(')', '')
                    approx = 1
            except KeyError:
                try:
                    address = entry['city']
                    approx = 1
                except KeyError:
                    address = 'Slovensko'
                    approx = 1

            try:
                if address.lower() in cls.collected_addresses:
                    continue
                geocoder = Geocoder(access_token=cls.token)
                response = geocoder.forward(address)
                first_record = response.geojson()['features'][0]
            except:
                if 'slovensko' not in cls.collected_addresses:
                    geocoder = Geocoder(access_token=cls.token)
                    response = geocoder.forward('Slovensko')
                    first_record = response.geojson()['features'][0]
                else:
                    continue
            coordinates = first_record['geometry']['coordinates']
            adresses.append({'address': address, 'lat': coordinates[0], 'lon': coordinates[1], 'approx': approx})
            cls.collected_addresses.append(address.lower())
        return adresses
