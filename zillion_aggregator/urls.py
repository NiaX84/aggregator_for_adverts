from django.urls import path

from . import views

app_name = 'zillion_aggregator'
urlpatterns = [
    path('', views.read_addresses, name='read_addresses'),
    path('', views.read_json, name='read_json'),
]
