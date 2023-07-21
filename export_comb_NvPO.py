import pymongo
from pymongo import MongoClient
import pandas as pd
import numpy as np
import urllib.request
import json
import pandas as pd
import time
import random
import requests
import os
from dotenv import load_dotenv



def search_sale_comp( date_start = "2022-06-01", date_end = "2022-11-11", time_increment = 1 , shop = "전체"):
    # keywords = ['리노베라', '리노베라칼슘파우더', '칼슘파우더', '과일세정제']
    df_lab = jisu_rel_total(date_start, date_end)
    df_NaCa = NavCaf_Sales(date_start, date_end)

    dfX = pd.concat([df_lab, df_NaCa], axis = 1)

    if time_increment == 7:
        dfX = dfX.resample("W").sum()
    elif time_increment == 30:
        dfX = dfX.resample("M").sum()

    dfX.reset_index(inplace=True)
    dfX["yy"] = dfX["period"].dt.year
    dfX["mm"] = dfX["period"].dt.month
    dfX["dd"] = dfX["period"].dt.day
    dfX = dfX.dropna()

    if shop == "전체" :
        d_list = [ [  "검색-칼슘파우더, 과일세정제", "네이버", "자사몰"  ] ]
        for i in range(len(dfX)) :
            d_list.append([ int(dfX['yy'][i]), int(dfX['mm'][i] - 1), int(dfX['dd'][i]), int(dfX['ratio'][i]), 
            int(dfX["네이버"][i]), int(dfX["자사몰"][i])  ])
    elif shop == "자사몰" :
        d_list = [ [ "검색-칼슘파우더, 과일세정제",  "자사몰" ] ]
        for i in range(len(dfX)) :
            d_list.append([ int(dfX['yy'][i]), int(dfX['mm'][i] - 1), int(dfX['dd'][i]), int(dfX['ratio'][i]) , int(dfX["자사몰"][i]) ])
    elif shop == "네이버" :
        d_list = [ [ "검색-칼슘파우더, 과일세정제", "네이버"  ] ]
        for i in range(len(dfX)) :
            d_list.append([ int(dfX['yy'][i]), int(dfX['mm'][i] - 1), int(dfX['dd'][i]),  int(dfX['ratio'][i]), int(dfX["네이버"][i]) ])
    
    return d_list


def jisu_rel_total(date_start = '2022-10-01', date_end = '2022-10-15', timeunit = 'date'): 
    client_id = os.getenv('nv_client_id')
    client_secret = os.getenv('nv_client_secret')
    url = "https://openapi.naver.com/v1/datalab/search"
    dfs = []

    keygroup = '리노베라'
    keyword1 = '리노베라칼슘파우더'
    keyword2 = '칼슘파우더'
    keyword3 = '과일세정제'

    #real! param = '{\"startDate\":\"'+date_start+'\",\"endDate\":\"'+date_end+'\",\"timeUnit\":\"'+ timeunit+'\",\"keywordGroups\":[{\"groupName\":\"'+keygroup+'\",\"keywords\":[\"'+keygroup+'\",\"'+keyword1+'\",\"'+keyword2+'\",\"'+keyword3+'\"]}]}'
    # param = '{\"startDate\":\"'+date_start+'\",\"endDate\":\"'+date_end+'\",\"timeUnit\":\"'+ timeunit+'\",\"keywordGroups\":[{\"groupName\":\"'+key+'\",\"keywords\":[\"'+key + '\"]}]}'
    param = '{\"startDate\":\"'+date_start+'\",\"endDate\":\"'+date_end+'\",\"timeUnit\":\"'+ timeunit+'\",\"keywordGroups\":[{\"groupName\":\"'+keyword2+'\",\"keywords\":[\"'+keyword2+'\",\"'+keyword3+'\"]}]}'
    
    body = str(param)

    request = urllib.request.Request(url)
    request.add_header("X-Naver-Client-Id",client_id)
    request.add_header("X-Naver-Client-Secret",client_secret)
    request.add_header("Content-Type","application/json")
    response = urllib.request.urlopen(request, data=body.encode("utf-8"))
    rescode = response.getcode()
    if(rescode==200):
        response_body = response.read()
        r = json.loads(response_body)
        df = pd.DataFrame(r['results'][0]['data'])
        df['period'] = pd.to_datetime(df['period'])
        df.set_index('period', inplace=True)
    return df


def NavCaf_Sales(date_start, date_end) :
    client = MongoClient(os.getenv('mongoClient_reno'))
    db = client["reno"]
    b2c_18 = db["Order18"]
    b2c_19 = db["Order19"]
    b2c_20 = db["Order20"]
    b2c_21 = db["Order21"]
    b2c_22 = db["Order22"]
    b2c_23 = db["Order23"]

    d_load18 = b2c_18.find_one()
    df18 = pd.DataFrame(d_load18["data"])
    d_load19 = b2c_19.find_one()
    df19 = pd.DataFrame(d_load19["data"])
    d_load20 = b2c_20.find_one()
    df20 = pd.DataFrame(d_load20["data"])
    d_load21 = b2c_21.find_one()
    df21 = pd.DataFrame(d_load21["data"])
    d_load22 = b2c_22.find_one()
    df22 = pd.DataFrame(d_load22["data"])
    d_load23 = b2c_23.find_one()
    df23 = pd.DataFrame(d_load23["data"])

    dfs = [df18, df19, df20, df21, df22, df23]

    for dfx in dfs:
        # dfx['주문일'] = dfx['주문일'].replace(['0000-00-00', ''],'2000-01-01')
        dfx["발주일"] = pd.to_datetime(dfx["발주일"])
        dfx["주문일"] = pd.to_datetime(dfx["주문일"])
    df = pd.concat(dfs)
    df.fillna("", inplace=True)
    df.drop(df[df["배송지주소"] == ""].index, inplace=True)
    df.reset_index(inplace=True, drop=True)

    msk1 = df["주문일"] >= date_start
    msk2 = df["주문일"] <= date_end
    msk3 = (df["판매처"] == "네이버") | (df["판매처"] == "자사몰")
    df = df[msk1&msk2&msk3].copy()
    df = df.loc[~df['상품명'].str.contains("그린", case=False)].copy()
    df = df.rename(columns = {'주문일' : 'period'})

    table = pd.pivot_table(df, index='period', values='Sale', columns="판매처", aggfunc=np.sum)

    return table