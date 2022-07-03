# Đây là DAG Lấy các thông tin của các bảng chiều trong CSDL DestinationCovid
import time
import requests 
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine
import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
import json
import csv, urllib.request
import numpy as np
#import psycopg2


# Task load dữ liệu thông tin các châu lục trên thế giới bao gồm dân số

def Load_Continent():
    conn_des = BaseHook.get_connection('DestinationCovid')
    engine_des = create_engine(f'postgresql://{conn_des.login}:{conn_des.password}@{conn_des.host}:{conn_des.port}/{conn_des.schema}')

    url = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/jhu/locations.csv'
    df = pd.read_csv(url)

    # Bỏ row chứa giá trị Internation
    df = df[df["location"].str.contains("International") == False] # trong cột location, bỏ những giá trị không phải là châu lục

    # Group by theo Continent và Sum dân số 
    df = df.groupby(['continent'])['population'].agg('sum')
    df = pd.DataFrame(df)
    df = df.reset_index(level=0)

    df.insert(0, 'ContinentID', range(1, 1 + len(df))) #thêm cột khóa chính, mã tự tăng
    df.rename(columns = {'continent':'ContinentName', 'population':'Population'}, inplace = True)
    df['Population'] = df['Population'].astype('int64') 
    df.insert(3, 'Last_updated', pd.Timestamp.today().strftime('%Y-%m-%d'))
    df['Last_updated'] = pd.to_datetime(df['Last_updated']).dt.date
    df.to_sql('Continent', engine_des, if_exists='replace', index=False)

# Task load các thông tin của một quốc gia

def Load_Country():
    conn_des = BaseHook.get_connection('DestinationCovid')
    engine_des = create_engine(f'postgresql://{conn_des.login}:{conn_des.password}@{conn_des.host}:{conn_des.port}/{conn_des.schema}')
    url = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/jhu/locations.csv'
    df = pd.read_csv(url)
    
    df = df[df["location"].str.contains("International") == False] # trong cột location, bỏ những giá trị không phải là châu lục
    df = df[['location','population','continent']]
    # đọc bảng Continent từ DB Destination
    df_contient = pd.read_sql("select * from \"Continent\"",engine_des)
    # Join 2 bảng và lấy khóa ngoại ContinentID
    join_temp_1 = df.set_index('continent').join(df_contient.set_index('ContinentName'))

    df_country = join_temp_1[['location','population','ContinentID']] 
    df_country.reset_index(drop=True, inplace=True)
    df_country.insert(0, 'New_ID', range(1, 1 + len(df))) #thêm cột khóa chính, mã tự tăng
    df_country.insert(3, 'Last_updated', pd.Timestamp.today().strftime('%Y-%m-%d'))
    df_country['Last_updated'] = pd.to_datetime(df_country['Last_updated']).dt.date
    df_country.rename(columns = {'New_ID':'CountryID', 'location':'CountryName','population':'Population'}, inplace = True)
    df_country['Population'] = df_country['Population'].astype('int64') 
    df_country.to_sql('Country', engine_des, if_exists='replace', index=False)  
    
def Load_Province_Region():
    # load province va Region VN vao DB DestinationCovid
    url = "https://raw.githubusercontent.com/phucjeya/TTKD-10_DATH/main/Tinh_KhuVuc_Vn.csv"
    df = pd.read_csv(url)
    conn = BaseHook.get_connection('DestinationCovid')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    locations =  df[['Số thứ tự','Tên tỉnh, thành phố','Khu vực','Dân số người']]
    df_locations = pd.DataFrame(locations)
    df_locations.columns =['ProvinceID','ProvinceName','RegionName','Population']

    # create a list of our conditions
    conditions = [
        (df_locations['RegionName'] == "Đồng bằng sông Cửu Long"),
        (df_locations['RegionName'] == "Đông Nam Bộ"),
        (df_locations['RegionName'] == "Đông Bắc Bộ"),
        (df_locations['RegionName'] == "Đồng bằng sông Hồng"),
        (df_locations['RegionName'] == "Tây Nguyên"),
        (df_locations['RegionName'] == "Duyên hải Nam Trung Bộ"),
        (df_locations['RegionName'] == "Tây Bắc Bộ"),
        (df_locations['RegionName'] == "Bắc Trung Bộ")
        ]
    # create a list of the values we want to assign for each condition
    values = [1,2,3,4,5,6,7,8]
    # create a new column and use np.select to assign values to it using our lists as arguments
    df_locations['RegionID'] = np.select(conditions, values)


    
    #display updated DataFrame
    # lấy những cột cần để vào bảng province
    temp = df_locations[['ProvinceID','ProvinceName','Population','RegionID']]
    temp.insert(4, 'Last_updated', pd.Timestamp.today().strftime('%Y-%m-%d')) #thêm cột last_update là ngày hiện tại
    df_provinces = pd.DataFrame(temp)

    #Convert Last_updated to Date
    df_provinces['Last_updated'] = pd.to_datetime(df_provinces['Last_updated']).dt.date

    #df_provinces['ProvinceName'] = df_provinces['ProvinceName'].replace(['Bà Rịa – Vũng Tàu'],'Bà Rịa - Vũng Tàu')
    df_provinces['ProvinceName'].mask(df_provinces['ProvinceName'] == 'Bà Rịa – Vũng Tàu','Bà Rịa - Vũng Tàu', inplace=True)
    df_provinces['ProvinceName'].mask(df_provinces['ProvinceName'] == 'Thành phố Hồ Chí Minh', 'TP HCM', inplace=True) # transform dữ liệu: nếu giá trị là Thành phố Hồ Chí Minh --> TP HCM
    # lấy những cột cần để vào bảng region
    temp2 = df_locations[['RegionID','RegionName']]
    temp2.insert(2, 'Last_updated', pd.Timestamp.today().strftime('%Y-%m-%d')) #thêm cột last_update là ngày hiện tại
    df_region = pd.DataFrame(temp2)

    #Convert Last_updated to Date
    df_region['Last_updated'] = pd.to_datetime(df_region['Last_updated']).dt.date
    df_region = df_region.drop_duplicates()
    #load vào DB
    df_provinces.to_sql('Province', engine, if_exists='replace', index=False)
    df_region.to_sql('Region', engine, if_exists='replace', index=False)


# [START how_to_task_group]
with DAG(dag_id="Load_SubTables",schedule_interval="0 0 1 * *", start_date=datetime(2022, 4, 23),catchup=False,  tags=["Airflow_ETL_"]) as dag: # lập lịch chạy 1 tháng 1 lần

    Load_Continent = PythonOperator(
        task_id ='Load_Continent',
        python_callable=Load_Continent,
        dag = dag,
    ) 
    Load_Country = PythonOperator(
        task_id ='Load_Country',
        python_callable=Load_Country,
        dag = dag,
    ) 
    Load_Province_Region = PythonOperator(
        task_id ='Load_Province_Region',
        python_callable=Load_Province_Region,
        dag = dag,
    ) 
    Load_Continent >> Load_Country >> Load_Province_Region

 