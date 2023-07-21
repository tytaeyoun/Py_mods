from pymongo import MongoClient
import requests
import json
import pandas as pd
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi
from datetime import datetime
from datetime import timedelta
from datetime import date
import numpy as np
import time
import os
from dotenv import load_dotenv

load_dotenv()


def FbCafeCompare_clicks(date_start = '2022-10-01', date_end = '2022-10-15', time_increment = 1, ad_account_id = os.getenv('fb_ad_account_id1')):
    load_dotenv()
    ####
    client = MongoClient(os.getenv('mongoClient_reno'))
    cafe24 = client['cafe24'] #db
    token = cafe24['token'] #collection
    load = token.find_one()
    refresh_token = load['refresh_token']

    url = os.getenv('cafe24_oauth_url')
    payload = 'grant_type=refresh_token&refresh_token=' + refresh_token
    headers = {
        'Authorization': f"Basic {os.getenv('cafe24_header')}",
        'Content-Type': "application/x-www-form-urlencoded"
        }
    response = requests.request("POST", url, data=payload, headers=headers)
    res0 = json.loads(response.text)


    cafe24.token.update_one({
    '_id': load['_id']
    },{
    '$set': {
        'refresh_token': res0['refresh_token']
    }
    }, upsert=False)

    headers = {
        'Authorization': "Bearer " + res0['access_token'],
        'Content-Type': "application/json",
        'X-Cafe24-Api-Version': "2022-09-01"
        }
    
    d1 = callFbAccountClick(date_start, date_end, ad_account_id)
    d2 = callCafeDailyVisits(headers, date_start, date_end)
    d_f = pd.concat([d1.set_index('date_start'), d2.set_index('date')], axis = 1)
    d_f.index = pd.to_datetime(d_f.index)

    ## check dates 2023-01-03
    d_f = d_f.dropna()

    d_f['clicks'] = d_f['clicks'].astype(int)

    if time_increment == 7:
        d_f = d_f.resample("W").sum()
    elif time_increment == 30:
        d_f = d_f.resample("M").sum()

    d_f.reset_index(inplace=True)
    d_f["yy"] = d_f["index"].dt.year
    d_f["mm"] = d_f["index"].dt.month
    d_f["dd"] = d_f["index"].dt.day
    d_f.fillna(0, inplace = True)

    d_list = [[  'cafe_visits', 'fb_clicks'     ]]
    for i in range(len(d_f)):
        d_list.append(   [int(d_f["yy"][i]) , int(d_f["mm"][i]) - 1 , int(d_f["dd"][i]),   int(d_f['cafe_visits'][i]), int(d_f['clicks'][i])  ]  )   

    return d_list 


def callFbAccountClick(date_start = '2022-10-01', date_end = '2022-10-15', ad_account_id = os.getenv('fb_ad_account_id1')):
    fields = [
        'clicks'
    ]
    x = len(fields) + 2
    param1 = {
        'time_range': {'since': date_start,'until': date_end},
        'time_increment' : 1,
        'filtering': [],
        'level': 'account',
        'breakdowns': [],
    }
    data1 = AdAccount(ad_account_id).get_insights(
        fields=fields,
        params=param1,
    )

    dlist = []
    for item in data1:
        dlist.append(dict(item))

    dfX = pd.DataFrame(dlist)
    return dfX

def callCafeDailyVisits(headers, date_start = '2022-10-01', date_end = '2022-10-15'):
    url = "https://ecobiotech.cafe24api.com/api/v2/admin/financials/dailyvisits?start_date=" + date_start +"&end_date=" + date_end
    response = requests.request("GET", url, headers=headers)

    ls = json.loads(response.text)
    data = ls['dailyvisits']
    datessss = []
    vistorC = []

    for da in data:
        vistorC.append(da['visitors_count']) 
        datessss.append(da['date'])
    dfT = pd.DataFrame(list(zip(datessss, vistorC)), columns =['date', 'cafe_visits'])
    return dfT


#12 # Compare FB ads spend, conversion with Cafe24 ㅂㅁㄹㅋ and ㅁㅊ
def FbCafeCompare_sales(date_start = '2022-10-01', date_end = '2022-10-15', time_increment = 1, ad_account_id = os.getenv('fb_ad_account_id1')) :
    client = MongoClient(os.getenv('mongoClient_reno'))
    cafe24 = client['cafe24'] #db
    token = cafe24['token'] #collection
    load = token.find_one()
    refresh_token = load['refresh_token']

    url = os.getenv('cafe24_oauth_url')
    payload = 'grant_type=refresh_token&refresh_token=' + refresh_token
    headers = {
        'Authorization': f"Basic {os.getenv('cafe24_header')}",
        'Content-Type': "application/x-www-form-urlencoded"
        }
    response = requests.request("POST", url, data=payload, headers=headers)
    res0 = json.loads(response.text)


    cafe24.token.update_one({
    '_id': load['_id']
    },{
    '$set': {
        'refresh_token': res0['refresh_token']
    }
    }, upsert=False)

    headers = {
        'Authorization': "Bearer " + res0['access_token'],
        'Content-Type': "application/json",
        'X-Cafe24-Api-Version': "2022-09-01"
        }

    df1 = callFbAccountSpend(ad_account_id, date_start, date_end)
    df2 = callCafeSecretSale(headers, date_start, date_end)
    df3 = callCafeAllSale(headers, date_start, date_end)

    dfT = pd.concat([df1, df2, df3], axis = 1)
    dfT.fillna(0, inplace=True)

    dfT['spend'] = dfT['spend'].astype(int)
    dfT['fb_conversion'] = dfT['fb_conversion'].astype(int)
    dfT['sec_sale'] = dfT['sec_sale'].astype(int)
    dfT['all_sale'] = dfT['all_sale'].astype(int)
    

    if time_increment == 7:
        dfT = dfT.resample("W").sum()
    elif time_increment == 30:
        dfT = dfT.resample("M").sum()


    dfT.reset_index(inplace = True)
    dfT["yy"] = dfT["index"].dt.year
    dfT["mm"] = dfT["index"].dt.month
    dfT["dd"] = dfT["index"].dt.day
    dfT.fillna(0, inplace = True)

    d_list = [ ['spend', 'fb_conversion', 'sec_sale', 'all_sale'] ]

    for i in range(len(dfT)):
        d_list.append(   [  int(dfT["yy"][i]) , int(dfT["mm"][i]) - 1 , int(dfT["dd"][i]) , int(dfT['spend'][i]), int(dfT['fb_conversion'][i]),  int(dfT['sec_sale'][i]),   int(dfT['all_sale'][i])     ]  )

    return d_list

def callFbAccountSpend(ad_account_id = os.getenv('fb_ad_account_id1'), date_start = '2022-10-01', date_end = '2022-10-15'): #SUBFUNCTION call Account Total Spend
  
  fields = [
      'spend',
      'purchase_roas',
  ]

  x = len(fields) + 2

  param1 = {
      'time_range': {'since': date_start,'until': date_end},
      'time_increment' : 1,
      'filtering': [],
      'level': 'account',
      'breakdowns': [],
  }
  data1 = AdAccount(ad_account_id).get_insights(
      fields=fields,
      params=param1,
  )


  dlist = []
  for item in data1:
      dlist.append(dict(item))
  for i in range(len(dlist)):
      if len(dlist[i]) == x: dlist[i]["purchase_roas"] = dlist[i]["purchase_roas"][0]["value"]

  dfX = pd.DataFrame(dlist)
  dfX['spend'] = dfX['spend'].astype(float)
  dfX['purchase_roas'] = dfX['purchase_roas'].astype(float)

  dfX['fb_conversion'] = dfX['spend'] * dfX['purchase_roas']
  dfX.set_index('date_start', inplace=True)
  dfX.index = pd.to_datetime(dfX.index)
  dfX['fb_conversion'] = dfX['fb_conversion'].astype(int)

  return dfX

def callCafeSecretSale(headers, date_start = '2022-10-01', date_end = '2022-10-15'): #SUBFUNCTION call caf24 비밀링크 매출

  url_secret = "https://ecobiotech.cafe24api.com/api/v2/admin/reports/salesvolume?product_no=20&start_date=" + date_start + "&end_date=" + date_end

  res_secret = requests.request("GET", url_secret, headers=headers)
  ls_sec = json.loads(res_secret.text)
  data_sec = ls_sec['salesvolume']

  days = []
  settle = []
  for da in data_sec:
      days.append(da['collection_date'])
      settle.append(float(da['settle_count']) * float(da['product_price']))
      
  d = {'date' : days, 'sec_sale' : settle}
  df_sec = pd.DataFrame(d)

  tab_sec = pd.pivot_table(df_sec, index = 'date', values= 'sec_sale', aggfunc=np.sum)
  tab_sec.index = pd.to_datetime(tab_sec.index)
  return tab_sec

def callCafeAllSale(headers, date_start = '2022-10-01', date_end = '2022-10-15'): #SUBFUNCTION call cafe24 전체 매출
  
    ###### 카페24 전체 매출 불러오기!!!!
    ds = datetime.strptime(date_start, '%Y-%m-%d')
    de = datetime.strptime(date_end, '%Y-%m-%d')
    delta = de - ds

    dst = date_start
    if delta.days > 14 : dst = str((de - timedelta(days = 14)).date())

    url1 = "https://ecobiotech.cafe24api.com/api/v2/admin/financials/dailysales?start_date=" + dst + "&end_date=" + date_end +"&payment_method=card" #card = 카드 tcash = 계좌이체 icash = 가상계좌
    res_card = requests.request("GET", url1, headers=headers)

    time.sleep(1)

    url2 = "https://ecobiotech.cafe24api.com/api/v2/admin/financials/dailysales?start_date=" + dst + "&end_date=" + date_end +"&payment_method=tcash" #card = 카드 tcash = 계좌이체 icash = 가상계좌
    res_tcash = requests.request("GET", url2, headers=headers)

    time.sleep(1)

    url3 = "https://ecobiotech.cafe24api.com/api/v2/admin/financials/dailysales?start_date=" + dst + "&end_date=" + date_end +"&payment_method=icash" #card = 카드 tcash = 계좌이체 icash = 가상계좌
    res_icash = requests.request("GET", url3, headers=headers)

    time.sleep(1)

    url4 = "https://ecobiotech.cafe24api.com/api/v2/admin/financials/dailysales?start_date=" + dst + "&end_date=" + date_end +"&payment_method=point" #card = 카드 tcash = 계좌이체 icash = 가상계좌
    res_point = requests.request("GET", url4, headers=headers)

    time.sleep(1)

    url5 = "https://ecobiotech.cafe24api.com/api/v2/admin/financials/dailysales?start_date=" + dst + "&end_date=" + date_end +"&payment_method=cell" #card = 카드 tcash = 계좌이체 icash = 가상계좌
    res_cell = requests.request("GET", url5, headers=headers)

    lsCard = json.loads(res_card.text)
    lsTcash = json.loads(res_tcash.text)
    lsIcash = json.loads(res_icash.text)
    lsPoint = json.loads(res_point.text)
    lsCell = json.loads(res_cell.text)

    l_d = []

    try:
        l_d.append(lsCard['dailysales'])
    except:
        pass
    try:
        l_d.append(lsTcash['dailysales'])
    except:
        pass
    try:
        l_d.append(lsIcash['dailysales'])
    except:
        pass
    try:
        l_d.append(lsPoint['dailysales'])
    except:
        pass
    try:
        l_d.append(lsCell['dailysales'])
    except:
        pass

    days = []
    sale = []
    for d in l_d:
        for subs in d:
            days.append(subs['date'])
            sale.append(float(subs['payment_amount']))

    d2 = {'date' : days, 'all_sale' : sale}
    cafe_sale = pd.DataFrame(d2)
    tab_all = pd.pivot_table(cafe_sale, index = 'date', values = 'all_sale', aggfunc=np.sum)
    tab_all.index = pd.to_datetime(tab_all.index)
    return tab_all