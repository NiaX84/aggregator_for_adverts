import pickle

from django.http import JsonResponse

pickled_module = 'zillion_aggregator/aggregator_model/aggregator.pk'

def read_json(request):
    file_paths = request.POST.get('files')
    with open(pickled_module, "rb") as f:
        loaded_model = pickle.load(f)

    data = loaded_model.aggregate(file_paths)
    return JsonResponse(data, safe=False)


def read_json_no_grouping(request):
    file_paths = request.POST.get('files')
    with open(pickled_module, "rb") as f:
        loaded_model = pickle.load(f)

    data = loaded_model.just_read(file_paths)
    return JsonResponse(data, safe=False)
