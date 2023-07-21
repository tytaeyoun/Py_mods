## bring the data and add to the db if today > last_date_saved

import pymongo
from pymongo import MongoClient
import pandas as pd
import numpy as np
import urllib.request
import json
import time
import requests
import signaturehelper
import os
from dotenv import load_dotenv



def get_header(method, uri, api_key, secret_key, customer_id):
    timestamp = str(round(time.time() * 1000))
    signature = signaturehelper.Signature.generate(timestamp, method, uri, secret_key)
    return {'Content-Type': 'application/json; charset=UTF-8', 'X-Timestamp': timestamp, 'X-API-KEY': api_key, 'X-Customer': str(customer_id), 'X-Signature': signature}


def marketing_jisu(df1, date_start = '2022-10-01', date_end = '2022-10-15', time_increment = 1) :
    client = MongoClient(os.getenv('mongoClient_reno'))
    Ads = client["Ads"] #db
    Naver_Keyword = Ads["Naver_Keyword"] #collection
    load = Naver_Keyword.find_one()
    df = pd.DataFrame(load['data'])

    df['날짜']=pd.to_datetime(df['날짜'])


    if pd.Timestamp('today').normalize() > df['날짜'][len(df)-1]:
        BASE_URL = 'https://api.naver.com'
        API_KEY = os.getenv('nv_API_KEY')
        SECRET_KEY = os.getenv('nv_SECRET_KEY')
        CUSTOMER_ID = os.getenv('nv_CUSTOMER_ID')

        uri = '/keywordstool'
        method = 'GET'

        keywords = ['리노베라', '리노베라칼슘파우더', '칼슘파우더', '과일세정제']
        params = {'hintKeywords' : keywords, 'showDetail' : 1}
        r = requests.get(BASE_URL + uri, params = params, headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))

        naver = pd.DataFrame(r.json()['keywordList'])

        msk1 = naver["relKeyword"] == '리노베라'
        msk2 = naver["relKeyword"] == '리노베라칼슘파우더'
        msk3 = naver["relKeyword"] == '칼슘파우더'
        msk4 = naver["relKeyword"] == '과일세정제'

        nav2 = naver[msk1|msk2|msk3|msk4].copy()
        nav2['total'] = nav2['monthlyPcQcCnt'] + nav2['monthlyMobileQcCnt']
        nav2['total'] = nav2['monthlyPcQcCnt'] + nav2['monthlyMobileQcCnt']

        naver = pd.pivot_table(nav2, columns='relKeyword', values = 'total')
        naver.reset_index(inplace=True, drop = True)
        naver['합계'] = naver['리노베라'] + naver['리노베라칼슘파우더'] 
        naver['리노베라/칼슘파우더'] = naver['합계'] / naver['칼슘파우더']
        naver['날짜'] = pd.Timestamp('today').normalize()
        table = pd.concat([df, naver])
        table.reset_index(drop=True, inplace=True)

        ### save df to db
        data_updt = table.to_dict('records')
        Ads.Naver_Keyword.update_one({
            '_id': load['_id']
        },{
        '$set': {
            'data': data_updt
        }
        }, upsert=False)

        load = Naver_Keyword.find_one()
        df = pd.DataFrame(load['data'])

    msk1 = df["날짜"] >= date_start
    msk2 = df["날짜"] <= date_end
    msk3 = df["리노베라/칼슘파우더"] != 0
    df = df[msk1&msk2&msk3].copy()

    df_s = lineGraphSales_v2_naver(df1, date_start, date_end)
    df_s.rename(columns={'주문일':'날짜'}, inplace=True)

    dfT = pd.concat( [df.set_index("날짜"), df_s.set_index("날짜")], axis = 1)
    dfT = dfT.fillna(0).copy()

    if time_increment == 7 : dfT = dfT.resample("W").sum()
    elif time_increment == 30 : dfT = dfT.resample("M").sum()

    dfT = dfT.reset_index().copy()

    dfT["yy"] = dfT["날짜"].dt.year
    dfT["mm"] = dfT["날짜"].dt.month
    dfT["dd"] = dfT["날짜"].dt.day

    d_list = [ ['리노베라', '리노베라칼슘파우더', '합계-칼슘파우더 세정제', '칼슘파우더', '과일세정제', '네이버', '자사몰'] ]

    for i in range(len(dfT)):
        d_list.append([ int(dfT['yy'][i]), int(dfT['mm'][i] - 1), int(dfT['dd'][i]), int(dfT['리노베라'][i]), int(dfT['리노베라칼슘파우더'][i]), 
        int(dfT['합계'][i]), int(dfT['칼슘파우더'][i]), int(dfT['과일세정제'][i]), int(dfT['네이버'][i]), int(dfT['자사몰'][i]) ])

    return d_list 
    # return df

def jisu_rel(df1, date_start = '2022-10-01', date_end = '2022-10-15', timeunit = 'date'): 
    
    keywords = ['리노베라', '리노베라칼슘파우더', '칼슘파우더', '과일세정제']
    
    client_id = os.getenv('nv_client_id')
    client_secret = os.getenv('nv_client_secret')
    url = "https://openapi.naver.com/v1/datalab/search"
    dfs = []

    for key in keywords :
        # param = '{\"startDate\":\"'+date_start+'\",\"endDate\":\"'+date_end+'\",\"timeUnit\":\"'+ timeunit+'\",\"keywordGroups\":[{\"groupName\":\"'+keygroup+'\",\"keywords\":[\"'+keygroup+'\",\"'+keyword1+'\",\"'+keyword2+'\",\"'+keyword3+'\"]}]}'
        param = '{\"startDate\":\"'+date_start+'\",\"endDate\":\"'+date_end+'\",\"timeUnit\":\"'+ timeunit+'\",\"keywordGroups\":[{\"groupName\":\"'+key+'\",\"keywords\":[\"'+key + '\"]}]}'

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

            df = df.rename(columns={'ratio': key } )
            df.set_index('period', inplace=True)

            dfs.append(df)

    dfX = pd.concat(dfs, axis = 1)
    dfX.reset_index(inplace=True)

    df_s = lineGraphSales_v2_naver(df1, date_start, date_end)
    df_s.rename(columns={'주문일':'period'}, inplace=True)

    dfT = pd.concat( [dfX.set_index("period"), df_s.set_index("period")], axis = 1)
    dfT = dfT.fillna(0).copy()
    dfT = dfT.reset_index()

    dfT["yy"] = dfT["period"].dt.year
    dfT["mm"] = dfT["period"].dt.month
    dfT["dd"] = dfT["period"].dt.day

    d_list = [   [  "리노베라", "리노베라칼슘파우더", "칼슘파우더", "과일세정제" , "네이버", "자사몰"  ]    ]

    for i in range(len(dfT)):
        d_list.append([ int(dfT['yy'][i]), int(dfT['mm'][i] - 1), int(dfT['dd'][i]), int(dfT['리노베라'][i]), int(dfT['리노베라칼슘파우더'][i]), 
        int(dfT['칼슘파우더'][i]), int(dfT['과일세정제'][i]), int(dfT['네이버'][i]), int(dfT['자사몰'][i]) ])

    return d_list



def NaverAdsTotal(date_start = '2022-10-01', date_end = '2022-11-15', time_increment = 1, stats = ['총지출', '전환매출', '평균클릭비용'], campaign_ids = ['cmp-a001-01-000000005781522', 'cmp-a001-02-000000005780499']) :

    BASE_URL = 'https://api.naver.com'
    API_KEY = os.getenv('nv_API_KEY')
    SECRET_KEY = os.getenv('nv_SECRET_KEY')
    CUSTOMER_ID = os.getenv('nv_CUSTOMER_ID')
    uri = '/stats'
    method = 'GET'

    # impCnt        노출수 v
    # clkCnt        클릭수 v
    # ctr           클릭률 v
    # cpc           평균클릭비용
    # salesAmt      총비용 v
    # ccnt          전환수
    # crto          전환율
    # convAmt       전환매출액
    # ror           광고 수익률 (전환매출/총비용)

    campaign_ids = [
            'cmp-a001-01-000000003019639', #리노베라_파워링크
            'cmp-a001-01-000000005781522', #파워링크_칼슘파우더
            'cmp-a001-01-000000005984138', #파워링크-피톤치드스프레이
            'cmp-a001-02-000000000279234', #리노베라_쇼핑검색광고
            'cmp-a001-02-000000005780205', #리노베라_브랜드형
            'cmp-a001-02-000000005780499', #쇼핑검색_칼슘파우더
            'cmp-a001-02-000000005799125', #벌크
            'cmp-a001-02-000000005984160', #쇼핑검색-피톤치드스프레이
            'cmp-a001-03-000000004266384', #리노베라_파워컨텐츠
            'cmp-a001-04-000000004201088'  #리노베라_브랜드검색
            ]

    timerange = '{"since":"' + date_start + '","until":"' + date_end + '"}'

    df_list = []
    for cmp in campaign_ids:
            r = requests.get(BASE_URL + uri, params={'id': cmp, 'fields': '["clkCnt","impCnt","salesAmt", "ctr", "avgRnk", "ccnt", "convAmt", "crto", "cpc"]', 
                    'timeRange': timerange}, headers=get_header(method, uri, API_KEY, SECRET_KEY, CUSTOMER_ID))
            if r.json()['data'] : 
                    # df_list[0].append(cmp)
                    df_list.append(pd.DataFrame(r.json()['data']))
                    
    df_all = pd.concat(df_list).groupby(level=0).sum()
    df_all['date'] = df_list[0]['dateStart']
    
    df_all['date'] = pd.to_datetime(df_all['date'])

    if time_increment == 7:
        df_all.set_index('date', inplace=True)
        df_all = df_all.resample("W").sum()
        df_all.reset_index(inplace=True)
    elif time_increment == 30:
        df_all.set_index('date', inplace=True)
        df_all= df_all.resample("M").sum()
        df_all.reset_index(inplace=True)


    df_all['ctr'] = df_all['clkCnt']/df_all['impCnt']
    df_all['roas'] = df_all['convAmt']/df_all['salesAmt']
    df_all["yy"] = df_all["date"].dt.year
    df_all["mm"] = df_all["date"].dt.month
    df_all["dd"] = df_all["date"].dt.day

    df_all.rename( columns= {'impCnt':'노출수', 'clkCnt':'클릭수', 'ctr':'클릭률', 'cpc':'평균클릭비용', 
            'salesAmt':'총지출', 'ccnt':'전환수', 'crto':'전환율', 'convAmt':'전환매출'  }, inplace=True)
    # impCnt        노출수 v
    # clkCnt        클릭수 v
    # ctr           클릭률 v
    # cpc           평균클릭비용
    # salesAmt      총비용 v
    # ccnt          전환수
    # crto          전환율
    # convAmt       전환매출액
    # ror           광고 수익률 (전환매출/총비용)

    

    # d_list = [ ['impression', '클릭수', '클릭률', '총지출', '전환매출액', 'roas'] ]
    d_list = [ stats ]
    for i in range(len(df_all)):
        l = [int(df_all['yy'][i]), int(df_all['mm'][i]) - 1, int(df_all['dd'][i])]
        for stat in stats:
            l.append(int(df_all[stat][i]))

        d_list.append( l )


    return d_list


def lineGraphSales_v2_naver(df, date_start = '2021-10-01', date_end = '2021-10-15', time_increment = 1, mall = ['네이버', '자사몰'], item = ['리노칼파 전체'], number = 'Sale') :
    d_list = []
    indx = []
    
    for i in range(len(mall)):
        indx.append(mall[i] + ' ' + item[0])

    d_list.append(indx)
    
    if item[0] == "리노칼파 전체" :
        item = ['리노칼파 150g', '리노칼파 45g', '리노칼파 30g']
    elif item[0] == "리노모팩 전체" :
        item = ['리노모팩 100g', '리노모팩 35g']
    elif item[0] == "그린칼파 전체" :
        item = ["그린칼파 150g", "그린칼파 5포", "그린칼파 30포"]
    
    mask1 = df["주문일"] >= date_start
    mask2 = df["주문일"] <= date_end

    df = df[mask1 & mask2].copy()


    dfs = []
    for i in range(len(mall)):
        if mall[i] == "전체 몰" : msk1 = df["판매처"] != "B2B"
        else : msk1 = df["판매처"] == mall[i]
        msk2 = df["상품명"].isin(item)
        table = pd.pivot_table(df[msk1&msk2], index = '주문일', values= number, aggfunc=np.sum)
        table.rename(columns={number: mall[i]}, inplace=True )
        table.index = pd.to_datetime(table.index)
        dfs.append(table)


    df_send = pd.concat(dfs, axis = 1)
    df_send.fillna(0, inplace=True)
    df_send.reset_index(inplace=True)

    return df_send