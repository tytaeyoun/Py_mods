import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import date
from datetime import timedelta
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi
import json
import time
import requests
import urllib.request
import signaturehelper
import os
from dotenv import load_dotenv

def home(df, url, ad_account_id = os.getenv('fb_ad_account_id1')) :
    l_fb = home_fb(ad_account_id)
    l_po = home_po(df)
    l_sm = home_sm(url)
    l_srch = home_srch(df)

    d_list = [l_fb, l_po, l_sm, l_srch]

    return d_list





def get_header(method, uri, api_key, secret_key, customer_id):
    timestamp = str(round(time.time() * 1000))
    signature = signaturehelper.Signature.generate(timestamp, method, uri, secret_key)
    return {'Content-Type': 'application/json; charset=UTF-8', 'X-Timestamp': timestamp, 'X-API-KEY': api_key, 'X-Customer': str(customer_id), 'X-Signature': signature}

def jisu_rel(date_start = '2022-10-01', date_end = '2022-10-15', timeunit = 'date'): 
    
    keywords = ['리노베라', '리노베라칼슘파우더', '칼슘파우더', '과일세정제']
    keygroup = "칼슘파우더"
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
    return dfX


def home_srch(df) :
    x = df["주문일"].to_list()
    x.sort()
    latest = x[-1]

    d1_s = str((latest - timedelta(days = 360)).date())
    d1_e = str((latest - timedelta(days = 1)).date())

    d1_w = str((latest - timedelta(days = 7)).date())
    d1_3m = str((latest - timedelta(days = 90)).date())

    dx_1yr = jisu_rel(d1_s, d1_e)

    msk_n = (df["판매처"] == "네이버") 
    msk_c = (df["판매처"] == "자사몰")
    msk2 = df["주문일"] > d1_w
    msk3 = df["주문일"] > d1_s
    msk4 = df["주문일"] > d1_3m

    df_nw = df[msk_n&msk2].copy()
    df_cw = df[msk_c&msk2].copy()

    df_ny = df[msk_n&msk3].copy()
    df_cy = df[msk_c&msk3].copy()

    df_n3m = df[msk_n&msk4].copy()
    df_c3m = df[msk_c&msk4].copy()



    dx_3month = dx_1yr[ dx_1yr["period"] >  d1_3m ].copy()
    dx_1weeek = dx_1yr[ dx_1yr["period"] >  d1_w ].copy()



    sale_N_3m =  (df_n3m["Sale"].sum() / 90)  / (  df_ny["Sale"].sum() / 360  ) * 50
    sale_C_3m =  (df_c3m["Sale"].sum() / 90)  / (  df_cy["Sale"].sum() / 360  ) * 50


    sale_N_1w =  (df_nw["Sale"].sum() / 7)  / (  df_ny["Sale"].sum() / 360  ) * 50
    sale_C_1w =  (df_cw["Sale"].sum() / 7)  / (  df_cy["Sale"].sum() / 360  ) * 50

    d_list = []
    # d_list.append( [  '', '칼슘파우더 검색량(상대평가)', '과일세정제 검색량(상대평가)', '네이버 매출(상대지표)', '자사몰 매출(상대지표)' ]   )

    # d_list.append([   '1년 평균', 50, 50 , 50, 50             ])
    # d_list.append([   '지난 3개월 비교', float(dx_3month["칼슘파우더"].mean()), float(dx_3month["과일세정제"].mean()) , float(sale_N_3m), float(sale_C_3m)             ])
    # d_list.append([   '지난 일주일 비교', float(dx_1weeek["칼슘파우더"].mean()), float(dx_1weeek["과일세정제"].mean()) , float(sale_N_1w), float(sale_C_1w)                 ])
    d_list.append([   '1년 평균', 100, 100 , 100, 100             ])
    d_list.append([   '지난 3개월 비교', float(dx_3month["칼슘파우더"].mean() * 2), float(dx_3month["과일세정제"].mean() * 2) , float(sale_N_3m) *2, float(sale_C_3m) *2             ])
    d_list.append([   '지난 일주일 비교', float(dx_1weeek["칼슘파우더"].mean()* 2), float(dx_1weeek["과일세정제"].mean()* 2) , float(sale_N_1w)* 2, float(sale_C_1w)* 2              ])

    return d_list


def home_sm(url):
    xd = sms_table(url)
    d_list = []
    # d_list.append( [  '키워드', '상품수', '평균 구좌수', '평균 순위', '최고 순위', '최하 순위' ]   )
    for i in range(len(xd)):
        dff = xd[i][["rank", "구좌수"]].astype(float)
        lll = []
        lll.append(xd[i]["검색어"][0])
        lll.append(int(len(dff)))
        lll.append( float(dff["구좌수"].mean()))
        lll.append( float(dff["rank"].mean()))
        lll.append( float(dff["rank"].min()) )
        lll.append( float(dff["rank"].max()) )
        d_list.append(lll)

    return d_list



def sms_table (url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, features='lxml')

    dfs = pd.read_html(url)
    href = []
    for a_tag in soup.find_all('a', href=True):
        # print( 'href: ', a_tag['href'])
        href.append(a_tag['href'])
    df_list = []
    for df in dfs :
        new_header = df.iloc[0] #grab the first row for the header
        df = df[1:] #take the data less the header row
        df.columns = new_header #set the header row as the df header
        df_list.append(df)
    df_all = pd.concat(df_list)
    df_all.reset_index(drop = True, inplace = True)
    df_all['href'] = href

    rank = []
    for str in df_all['페이지']:
        rank.append(str[2:])
    df_all['rank'] = rank

    df_all['구좌수'] = df_all["구좌수"].str.replace('타', '')
    df_all["구좌수"] = df_all["구좌수"].astype(int)
    df_all['검색어'] = df_all['검색어'].str.replace(' ', '')


    keys = list(df_all["검색어"].unique())
    nIDs = list(df_all["Nv_Mid"].unique())

    for key in keys :
        for nID in nIDs:
            mask1 = df_all["검색어"] == key
            mask2 = df_all["Nv_Mid"] == nID
            if len(df_all[mask1&mask2]) > 1 : 
                x = df_all[mask1&mask2]["구좌수"].sum()
                indx = list(df_all[mask1&mask2].index)[0]
                df_all.loc[indx, "구좌수"] = x

    # df_all = df_all.drop_duplicates(subset ='Nv_Mid').reset_index(drop=True)

    df_list = []    
    for key in df_all['검색어'].unique():
        mask = df_all['검색어'] == key
        df_list.append(df_all[mask].drop_duplicates(subset ='Nv_Mid').reset_index(drop = True))

    d_list = []

    for df in df_list:
        lis = []
        lis.append(df['검색어'][0])
        for i in range(len(df)):
            lis.append( [ df['상품명'][i],  int(df['구좌수'][i]),   int(df['rank'][i])  , df['href'][i]] )
        d_list.append(lis)
    
    return df_list



def home_po(df) :
    x = df["주문일"].to_list()
    x.sort()
    latest = x[-1]
    d1_s = str(latest - timedelta(days = 15))
    d1_e = str(latest)

    d2_s = str(latest - timedelta(days = 380))
    d2_e = str(latest - timedelta(days = 365))

    d3_s = str(latest - timedelta(days = 745))
    d3_e = str(latest - timedelta(days = 730))
    dfR = df[["주문일", "판매처", "Sale"]].copy()

    mskA1 = dfR["주문일"] <= d1_e
    mskA2 = dfR["주문일"] > d1_s
    df1 = dfR[mskA1&mskA2].copy()

    mskB1 = dfR["주문일"] <= d2_e
    mskB2 = dfR["주문일"] > d2_s
    df2 = dfR[mskB1&mskB2].copy()

    mskC1 = dfR["주문일"] <= d3_e
    mskC2 = dfR["주문일"] > d3_s
    df3 = dfR[mskC1&mskC2].copy()

    d_list = []
    # d_list.append(['', '일일 평균 매출', '최대 판매처' ])

    table1 = pd.pivot_table(df1, index="판매처", values="Sale", aggfunc=np.sum)
    table2 = pd.pivot_table(df2, index="판매처", values="Sale", aggfunc=np.sum)
    table3 = pd.pivot_table(df3, index="판매처", values="Sale", aggfunc=np.sum)


    d_list.append([
        '2년전 동시즌',
        int(df3["Sale"].sum() / 15),
        table3["Sale"].idxmax()
    ])
    d_list.append([
        '1년전 동시즌',
        int(df2["Sale"].sum() / 15),
        table2["Sale"].idxmax()
    ])
    d_list.append([
        '지난 15일',
        int(df1["Sale"].sum() / 15),
        table1["Sale"].idxmax()
    ])

    return d_list



def home_fb(ad_account_id = os.getenv('fb_ad_account_id1')) :
    d1_s = str(date.today() - timedelta(days = 1))
    d1_e = str(date.today() - timedelta(days = 1))

    d2_s = str(date.today() - timedelta(days = 7))
    d2_e = str(date.today() - timedelta(days = 1))

    d3_s = str(date.today() - timedelta(days = 30))
    d3_e = str(date.today() - timedelta(days = 1))

    d4_s = str(date.today() - timedelta(days = 60))
    d4_e = str(date.today() - timedelta(days = 30))


    df1 = callFbAccTable(ad_account_id, d1_s, d1_e)
    df2 = callFbAccTable(ad_account_id, d2_s, d2_e)
    df3 = callFbAccTable(ad_account_id, d3_s, d3_e)
    df4 = callFbAccTable(ad_account_id, d4_s, d4_e)

    d_list_fb = []
    # d_list_fb.append( ['일일 평균', 'roas', '클릭률', '지출', '리노이익'])
    d_list_fb.append( [   '2달 전 1개월간', float(df4['roas'][0]), float(df4['ctr'][0]),  int(df4['spend'][0] / 30), int(df4['Rest'][0] / 30)    ]     )
    d_list_fb.append( [   '지난 1개월간', float(df3['roas'][0]), float(df3['ctr'][0]),  int(df3['spend'][0] / 30), int(df3['Rest'][0] / 30)    ]     )
    d_list_fb.append( [   '지난 1주일간', float(df2['roas'][0]), float(df2['ctr'][0]),  int(df2['spend'][0] / 7), int(df2['Rest'][0] / 7)    ]     )
    d_list_fb.append( [   '어제', float(df1['roas'][0]), float(df1['ctr'][0]),  int(df1['spend'][0]), int(df1['Rest'][0])    ]     )

    return d_list_fb

def callFbAccTable(ad_account_id, date_start, date_end): ## SUBFUNCTION calls the facebook ads data
    ### FACEBOOK  ###
    FacebookAdsApi.init(access_token=os.getenv('fb_accesstoken'))
    
    fields = [
        'spend',
        'purchase_roas',
        'ctr',
        'reach',
    ]
    x = len(fields) + 2
    param1 = {
        'time_range': {'since': date_start,'until': date_end},
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
    dfX[["spend", "purchase_roas", "ctr", "reach"]] = dfX[["spend", "purchase_roas", "ctr", "reach"]].astype(float)

    dfX.rename(columns = {'purchase_roas':'roas'}, inplace = True)

    msk1 = dfX["spend"] >= 0
    msk2 = dfX["roas"] >= 0
    msk3 = dfX["ctr"] >= 0

    dfX = dfX[msk1&msk2&msk3].reset_index(drop = True)
    dfX["Rest"] = dfX["spend"]*(dfX["roas"]/2.2 - 1)

    dfX['rank_rest'] = dfX['Rest'].rank(ascending=False)
    dfX['rank_ctr'] = dfX['ctr'].rank(ascending=False)
    dfX['Total'] = dfX['rank_rest'] + dfX['rank_ctr']
    
    return dfX