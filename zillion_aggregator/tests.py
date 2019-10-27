import json

from django.test import TestCase
from django.urls import reverse


class AddressTest(TestCase):

    json_files ="https://api.apify.com/v2/datasets/fqp2x4QYowZo8YS8F/items?format=json&clean=1&attachment=1, "\
                "https://api.apify.com/v2/datasets/BQt9kfkwYGxxLxEJ9/items?format=json&clean=1&attachment=1, "\
                "https://api.apify.com/v2/datasets/u6icKFK4C3SgBwxwz/items?format=json&clean=1&attachment=1, "\
                "https://api.apify.com/v2/datasets/JdATgnS8T5hKc3RvJ/items?format=json&clean=1&attachment=1, "\
                "https://api.apify.com/v2/datasets/9AdyF5EPCatNgMQgd/items?format=json&clean=1&attachment=1, "\
                "https://api.apify.com/v2/datasets/csE5nZaerAMggzigf/items?format=json&clean=1&attachment=1, "\
                "https://api.apify.com/v2/datasets/9cEPQPLoFP697LPX2/items?format=json&clean=1&attachment=1, "\
                "https://api.apify.com/v2/datasets/FhoJ7EWyyitsTHy6Z/items?format=json&clean=1&attachment=1, "\
                "https://api.apify.com/v2/datasets/zTS5aKFj3agDdaFXq/items?format=json&clean=1&attachment=1"

    def test_read_json_files_from_web_missing_full(self):
        response = self.client.post(reverse('zillion_aggregator:read_addresses'), {'files': self.json_files})
        self.assertEqual(response.status_code, 200)
        result = json.loads(response.content, encoding='utf-8')
        with open('addresses.json', 'w', encoding='utf-8') as json_file:
            json.dump(result, json_file, ensure_ascii=False, indent=4)


# class ReadJsonFileTest(TestCase):
#
#     json_files_string = "https://api.apify.com/v2/datasets/KR4HaLLWPX7F6Qxse/items?format=json&clean=1&attachment=1, " \
#                         "https://api.apify.com/v2/datasets/2WXL6WTm2JLnja9u6/items?format=json&clean=1&attachment=1, " \
#                         "https://api.apify.com/v2/datasets/PZScBeW5zhdSxHFRD/items?format=json&clean=1&attachment=1, " \
#                         "https://api.apify.com/v2/datasets/8TPhT2r9fgwqE3BKy/items?format=json&clean=1&attachment=1, " \
#                         "https://api.apify.com/v2/datasets/jmB6mqg4FSLexkh8o/items?format=json&clean=1&attachment=1, " \
#                         "https://api.apify.com/v2/datasets/GJfSi4gKzEwxWKmLM/items?format=json&clean=1&attachment=1, " \
#                         "https://api.apify.com/v2/datasets/FZi2YWGXGgczFdCi6/items?format=json&clean=1&attachment=1, " \
#                         "https://api.apify.com/v2/datasets/RzqphczCj5XLFLedo/items?format=json&clean=1&attachment=1, " \
#                         "https://api.apify.com/v2/datasets/RrQY7Zk6TBQp7Zcnv/items?format=json&clean=1&attachment=1"
#
#     def test_read_json_files_from_web_missing_full(self):
#         response = self.client.post(reverse('zillion_aggregator:read_json'), {'files': self.json_files_string})
#         self.assertEqual(response.status_code, 200)
#         result = json.loads(response.content, encoding='utf-8')
#         with open('records.json', 'w', encoding='utf-8') as json_file:
#             json.dump(result, json_file, ensure_ascii=False, indent=4)
