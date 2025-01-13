import requests
import pandas as pd
from geopy.geocoders import GoogleV3
import os
from dotenv import load_dotenv
global api_key
load_dotenv()
api_key = os.getenv('api_key')
import json
import warnings
warnings.filterwarnings('ignore')


# fetching data from the api hosted
def fetch_data_df(url, auth_token):
    headers = {
        'Authorization' : f'Bearer {auth_token}',
        'Content-Type' : 'application/json'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        print(f'Error fetching data from api : {e}')
        return None
    
def preprocessed_data(data):
    df = pd.DataFrame(data)
    df['city'] = df['address'].apply(get_city_from_address)
    df['city'] = df['city'].apply(check_city_null)
    df['state'] = df['city'].apply(get_state_from_city)
    df['state'] = df['state'].apply(check_null_state)
    # print(df.head())
    df = df[['kiosk', 'monthWiseCounts', 'diseaseWiseMonthCount', 'city', 'state']]
    flattened_data = []
    for _, row in df.iterrows():
        for entry in row['diseaseWiseMonthCount']:
            disease_month = entry['month']
            disease_counts = entry['diseaseWiseCount']
            for disease, count in disease_counts.items():
                flattened_data.append({
                    'kiosk': row['kiosk'],
                    'city': row['city'],
                    'state' : row['state'],
                    'disease_month' : disease_month,
                    'disease_list': disease,
                    'disease_count': count
                })
    flattened_df = pd.DataFrame(flattened_data)
    flattened_df = pd.DataFrame(flattened_df)
    flattened_df.drop_duplicates(inplace=True)

    flattened_patient = []
    for _, row in df.iterrows():
        for entry in row['monthWiseCounts']:
            patient_month = entry['month']
            patient_counts = entry['count']
            flattened_patient.append({
                'kiosk': row['kiosk'],
                'city': row['city'],
                'state' : row['state'],
                'patient_month': patient_month,
                'patient_counts': patient_counts
            })
    flattened_patient = pd.DataFrame(flattened_patient)

    flattened_df = flattened_df.merge(flattened_patient, on=['kiosk','city','state'], how='right') 
    flattened_df.drop_duplicates(inplace=True)
    df = flattened_df
    print(df.isna().sum())
    df.dropna(inplace=True)
    print(df.isna().sum())

    df.to_csv('C:/Users/ADMIN/scripts/all_in_one.csv', encoding='utf-8', index=False, mode='a')



def check_null_state(state):
    if state == None:
        return 'State'
    else:
        return state

def check_city_null(city):
    if city == None:
        return 'City'
    else:
        return city

def get_city_from_address(address):
    geolocator = GoogleV3(api_key=api_key)
    location = geolocator.geocode(address)
    if location:
        address_components = location.raw.get('address_components', [])
        for component in address_components:
            if 'locality' in component['types']:
                return component['long_name']
        return location.address
    else:
        return None


def get_state_from_city(city):
    geolocator = GoogleV3(api_key=api_key)
    location = geolocator.geocode(city)
    if location:
        address_components = location.raw.get('address_components', [])
        for component in address_components:
            if 'administrative_area_level_1' in component['types']:
                return component['long_name']
        return location.address
    return None

def execute_preprocess():
    url = 'http://182.168.1.210:3100/v1/analytics/get-month-wise-analytics?startDate=01/06/2024&endDate=30/06/2024'
    auth_token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6IiIsImlkIjoiNjU5MDA4ODljMGIxMzhlYTY0MGJjZDE1IiwiaWF0IjoxNzM2NTA5NDM0fQ.rUVa4DQEPoznXph72mI4jDAnWVTS3pFmkbsrdibBbjM'
    data = fetch_data_df(url, auth_token)
    if data:
        # print(data)
        preprocessed = preprocessed_data(data)
    else:
        print('data not available')

execute_preprocess()