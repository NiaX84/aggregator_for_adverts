import pandas as pd

import json
from urllib.request import urlopen
import geopy
from geopy.geocoders import Nominatim

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
    stored_gps = {'ilava': (48.995445, 18.231521),
                  'nitra': (48.290831, 18.081324),
                  'bytča': (49.231211, 18.536791),
                  'komárno': (47.790892, 18.104185),
                  'dunajská streda': (48.016474, 17.624491),
                  'považská bystrica': (49.122549, 18.443565),
                  'rožňava': (48.689890, 20.560575),
                  'česká republika': (49.850728, 15.567490),
                  'medzilaborce': (49.294755, 21.927189),
                  'spišská nová ves': (48.909110, 20.535539),
                  'liptovský mikuláš': (49.074828, 19.608224),
                  'lučenec': (48.333837, 19.670935),
                  'michalovce': (48.755837, 21.910775),
                  'trenčín': (48.887797, 18.034342),
                  'šaľa': (48.152384, 17.875823),
                  'humenné': (48.929607, 21.909280),
                  'bardejov': (49.297543, 21.275370),
                  'bratislava': (48.149556, 17.116090),
                  'pezinok': (48.286553, 17.261039),
                  'tvrdošín': (49.332087, 19.548812),
                  'piešťany': (48.590070, 17.822416),
                  'snina': (48.989167, 22.164004),
                  'revúca': (48.683686, 20.115064),
                  'turčianske teplice': (48.866330, 18.866078),
                  'ružomberok': (49.074565, 19.296973),
                  'banská bystrica': (48.727735, 19.141896),
                  'krupina': (48.349515, 19.072070),
                  'levoča': (49.023135, 20.587785),
                  'námestovo': (49.407369, 19.506247),
                  'svidník': (49.307633, 21.564408),
                  'dolný kubín': (49.307633, 21.564408),
                  'košice-okolie': (48.710526, 21.003962),
                  'poprad': (49.050968, 20.291246),
                  'trebišov': (48.628728, 21.702546),
                  'detva': (48.559256, 19.414805),
                  'myjava': (48.756178, 17.567562),
                  'sobrance': (48.746561, 22.180449),
                  'púchov': (49.124481, 18.325088),
                  'levice': (48.213241, 18.606671),
                  'rimavská sobota': (48.371895, 20.029153),
                  'nové zámky': (47.984354, 18.157984),
                  'topoľčany': (48.559890, 18.173624),
                  'galanta': (48.188709, 17.731557),
                  'partizánske': (48.628111, 18.378160),
                  'bánovce nad bebravou': (48.719733, 18.268117),
                  'senica': (48.681037, 17.363301),
                  'banská štiavnica': (48.448143, 18.901932),
                  'zlaté moravce': (48.388279, 18.398865),
                  'štúrovo': (47.798745, 18.712310),
                  'čadca': (49.435963, 18.788576),
                  'brezno': (48.805408, 19.653873),
                  'gelnica': (48.850810, 20.942079),
                  'žilina': (49.216801, 18.746939),
                  'senec': (48.219384, 17.405701),
                  'žarnovica': (48.484807, 18.716811),
                  'stará ľubovňa': (49.302159, 20.695087),
                  'prešov': (49.000572, 21.241961),
                  'veľký krtíš': (48.206769, 19.345362),
                  'kežmarok': (49.133890, 20.428977),
                  'hurbanovo': (47.878377, 18.189174),
                  'trnava': (48.371188, 17.588562),
                  'stropkov': (49.202347, 21.656082),
                  'košice': (48.715759, 21.244794),
                  'prievidza': (48.773809, 18.630806),
                  'nové mesto n.váhom': (48.750849, 17.815794),
                  'skalica': (48.839999, 17.220698),
                  'kysucké nové mesto': (49.303661, 18.797764),
                  'poltár': (48.430726, 19.793189),
                  'zvolen': (48.575753, 19.131911),
                  'vranov nad topľou': (48.890727, 21.690014),
                  'žiar nad hronom': (48.592187, 18.856383),
                  'sabinov': (49.103334, 21.101740),
                  'martin': (49.062022, 18.916186),
                  'malacky': (48.436255, 17.014930), 'hlohovec': (48.425591, 17.803041)}

    geopy.geocoders.options.default_timeout = 3

    def just_read(self, files):
        records = []
        data_files = files.split(", ")
        for file in data_files:
            records.extend(self.load_all_records(file))

        records_with_lat_lon = pd.DataFrame(records)
        all_cities = set(value.lower() for value in records_with_lat_lon.loc[records_with_lat_lon['position_flag'] == 'city_center']['address'].values)
        gps_codes = []
        print(len(all_cities))
        for city in all_cities:
            if city not in self.stored_gps:
                geocoder = Nominatim()
                location = geocoder.geocode(city)
                if location:
                    gps_code = (city, location.latitude, location.longitude)
                    gps_codes.append(gps_code)
                    self.stored_gps[city] = (gps_code[1], gps_code[2])
                else:
                    gps_code = (city, 0.0, 0.0)
                    gps_codes.append(gps_code)
                    self.stored_gps[city] = (gps_code[1], gps_code[2])
            else:
                gps_code = (city, self.stored_gps[city][0], self.stored_gps[city][0])
                gps_codes.append(gps_code)

        gps_df = pd.DataFrame(gps_codes, columns=['city', 'latitude', 'longitude'])
        with open('gps.csv', 'w', encoding='utf-8') as gps_file:
            gps_df.to_csv(gps_file)

        result = records_with_lat_lon.to_dict(orient='records')
        return result

    def aggregate(self, files):
        records = []
        data_files = files.split(", ")
        for file in data_files:
            records.extend(self.load_all_records(file))

        result = pd.DataFrame(records) \
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
