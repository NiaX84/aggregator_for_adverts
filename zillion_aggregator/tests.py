import json

from django.test import TestCase
from django.urls import reverse


class ReadJsonFileTest(TestCase):

    json_files_string = "C:\\Users\\spoch\source\\repos\\aggregator_for_adverts\\test_data\\dataset_9AdyF5EPCatNgMQgd.json, "\
                        "C:\\Users\\spoch\source\\repos\\aggregator_for_adverts\\test_data\\dataset_9cEPQPLoFP697LPX2.json, "\
                        "C:\\Users\\spoch\source\\repos\\aggregator_for_adverts\\test_data\\dataset_BQt9kfkwYGxxLxEJ9.json, "\
                        "C:\\Users\\spoch\source\\repos\\aggregator_for_adverts\\test_data\\dataset_csE5nZaerAMggzigf.json, "\
                        "C:\\Users\\spoch\source\\repos\\aggregator_for_adverts\\test_data\\dataset_FhoJ7EWyyitsTHy6Z.json, "\
                        "C:\\Users\\spoch\source\\repos\\aggregator_for_adverts\\test_data\\dataset_JdATgnS8T5hKc3RvJ.json, "\
                        "C:\\Users\\spoch\source\\repos\\aggregator_for_adverts\\test_data\\dataset_u6icKFK4C3SgBwxwz.json, "\
                        "C:\\Users\\spoch\source\\repos\\aggregator_for_adverts\\test_data\\dataset_zTS5aKFj3agDdaFXq.json"

    # json_files_string = "C:\\Users\\spoch\source\\repos\\aggregator_for_adverts\\test_data\\DataSetJson\\dataset_2WXL6WTm2JLnja9u6.json, "\
    #                     "C:\\Users\\spoch\source\\repos\\aggregator_for_adverts\\test_data\\DataSetJson\\dataset_8TPhT2r9fgwqE3BKy.json, "\
    #                     "C:\\Users\\spoch\source\\repos\\aggregator_for_adverts\\test_data\\DataSetJson\\dataset_FZi2YWGXGgczFdCi6.json, "\
    #                     "C:\\Users\\spoch\source\\repos\\aggregator_for_adverts\\test_data\\DataSetJson\\dataset_GJfSi4gKzEwxWKmLM.json, "\
    #                     "C:\\Users\\spoch\source\\repos\\aggregator_for_adverts\\test_data\\DataSetJson\\dataset_jmB6mqg4FSLexkh8o.json, "\
    #                     "C:\\Users\\spoch\source\\repos\\aggregator_for_adverts\\test_data\\DataSetJson\\dataset_KR4HaLLWPX7F6Qxse.json, "\
    #                     "C:\\Users\\spoch\source\\repos\\aggregator_for_adverts\\test_data\\DataSetJson\\dataset_PZScBeW5zhdSxHFRD.json, "\
    #                     "C:\\Users\\spoch\source\\repos\\aggregator_for_adverts\\test_data\\DataSetJson\\dataset_RrQY7Zk6TBQp7Zcnv.json, "\
    #                     "C:\\Users\\spoch\source\\repos\\aggregator_for_adverts\\test_data\\DataSetJson\\dataset_RzqphczCj5XLFLedo.json"

    # json_files_string = "https://api.apify.com/v2/datasets/KR4HaLLWPX7F6Qxse/items?format=json&clean=1&attachment=1, " \
    #                     "https://api.apify.com/v2/datasets/2WXL6WTm2JLnja9u6/items?format=json&clean=1&attachment=1, " \
    #                     "https://api.apify.com/v2/datasets/PZScBeW5zhdSxHFRD/items?format=json&clean=1&attachment=1, " \
    #                     "https://api.apify.com/v2/datasets/8TPhT2r9fgwqE3BKy/items?format=json&clean=1&attachment=1, " \
    #                     "https://api.apify.com/v2/datasets/jmB6mqg4FSLexkh8o/items?format=json&clean=1&attachment=1, " \
    #                     "https://api.apify.com/v2/datasets/GJfSi4gKzEwxWKmLM/items?format=json&clean=1&attachment=1, " \
    #                     "https://api.apify.com/v2/datasets/FZi2YWGXGgczFdCi6/items?format=json&clean=1&attachment=1, " \
    #                     "https://api.apify.com/v2/datasets/RzqphczCj5XLFLedo/items?format=json&clean=1&attachment=1, " \
    #                     "https://api.apify.com/v2/datasets/RrQY7Zk6TBQp7Zcnv/items?format=json&clean=1&attachment=1"

    def test_read_json_files_from_web_missing_full(self):
        response = self.client.post(reverse('zillion_aggregator:read_json'), {'files': self.json_files_string})
        self.assertEqual(response.status_code, 200)
        result = json.loads(response.content, encoding='utf-8')
        with open('records.json', 'w', encoding='utf-8') as json_file:
            json.dump(result, json_file, ensure_ascii=False, indent=4)

    # def test_read_json_files_with_aggregation(self):
    #     json_input = "C:\\Users\\spoch\\repos\\aggregator_for_adverts\\zillion_aggregator\\aggregation_by_description.json"
    #     response = self.client.post(reverse('zillion_aggregator:read_json'), {'files': json_input})
    #     self.assertEqual(response.status_code, 200)
    #     result = json.loads(response.content, encoding='utf-8')
    #     with open('records_by_description.json', 'w', encoding='utf-8') as json_file:
    #         json.dump(result, json_file, ensure_ascii=False, indent=4)
