from django.contrib import admin
from django.urls import path
import views

urlpatterns = [
    path('update_csv', views.update_csv, name = 'update_csv')
]
