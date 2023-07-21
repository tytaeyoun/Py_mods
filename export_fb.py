import pandas as pd
import numpy as np
import datetime
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi
import os
from dotenv import load_dotenv

day_end = datetime.datetime.today().strftime('%Y-%m-%d')

day_s = (datetime.datetime.today() - datetime.timedelta(weeks=4)).strftime('%Y-%m-%d')


def configure () :
    load_dotenv()

# 광고 - 페이스북 현재 성적표
def fbAdTable(ad_account_id = os.getenv('fb_ad_account_id1'), date_start = str(datetime.date(2022, 9, 22)), date_end = str(datetime.date(2022, 9, 22)), findme = ''):
    load_dotenv()
    ### FACEBOOK  ###
    FacebookAdsApi.init(access_token=os.getenv('fb_accesstoken'))
    df = callFbAdTable(ad_account_id, date_start, date_end )

    df['rank_Total'] = df['Total'].rank()
    df.sort_values(by = ['rank_Total'], inplace=True)
    df.reset_index(drop = True, inplace=True)
    df['주의'] =  df['roas'] > 2.2

    if findme != '' :
        df = df.loc[ df['ad_name'].str.contains(findme, case=False)]
        df.reset_index(drop = True, inplace=True)

    data_list = []
    for i in range(len(df)):
        data_list.append([ str(df["ad_id"][i]), str(df["ad_name"][i]), str(df["quality_ranking"][i]), int(df["spend"][i]), int(df["reach"][i]),
    float(df["ctr"][i]), float(df["roas"][i]), int(df["Rest"][i]), float(df["rank_Total"][i]), int(df["주의"][i]) ])

    ### 0ad_id 1ad_name 2q_ranking 3spend 4ctr 5roas 6reach 7Rest 8rank_Total 9warning  int(df["reach"][i]),

    return [data_list, str(df["date_start"][0]), str(df["date_stop"][0])]


def fbAdanalysis(ad_account_id = os.getenv('fb_ad_account_id1')) :
    start_days_list = [str(datetime.date.today() - datetime.timedelta(days = 28)), str(datetime.date.today() - datetime.timedelta(days = 21)), 
        str(datetime.date.today() - datetime.timedelta(days = 14)), str(datetime.date.today() - datetime.timedelta(days = 7))]
    day_list = [[int(start_days_list[0][:4]), int(start_days_list[0][5:7]), int(start_days_list[0][8:])], [int(start_days_list[1][:4]), int(start_days_list[1][5:7]), int(start_days_list[1][8:])], 
    [int(start_days_list[2][:4]), int(start_days_list[2][5:7]), int(start_days_list[2][8:])], [int(start_days_list[3][:4]), int(start_days_list[3][5:7]), int(start_days_list[3][8:])]]

    end_days_list = [str(datetime.date.today() - datetime.timedelta(days = 22)), str(datetime.date.today() - datetime.timedelta(days = 15)), 
        str(datetime.date.today() - datetime.timedelta(days = 8)), str(datetime.date.today() - datetime.timedelta(days = 1))]
    
    df_test1 = callFbAdTable(ad_account_id, start_days_list[3], end_days_list[3])
    d1_1, d1_2, d1_3 = AnlTable(df_test1)

    df_test2 = callFbAdTable(ad_account_id, start_days_list[2], end_days_list[2])
    d2_1, d2_2, d2_3 = AnlTable(df_test2)

    df_test3 = callFbAdTable(ad_account_id, start_days_list[1], end_days_list[1])
    d3_1, d3_2, d3_3 = AnlTable(df_test3)

    df_test4 = callFbAdTable(ad_account_id, start_days_list[0], end_days_list[0])
    d4_1, d4_2, d4_3 = AnlTable(df_test4)

    #### AJUST the total values so big Total means good
    tbs = [d1_1, d1_2, d1_3, d2_1, d2_2, d2_3, d3_1, d3_2, d3_3, d4_1, d4_2, d4_3]
    for tb in tbs :
        tb["Total"] = tb["Total"].max() - tb["Total"] + 5

    purpose = pd.concat([d4_1.set_index(['특징']), d3_1.set_index(['특징']), d2_1.set_index(['특징']), d1_1.set_index(['특징']),], axis = 1, keys = start_days_list).reset_index()
    image = pd.concat([d4_2.set_index(['특징']), d3_2.set_index(['특징']), d2_2.set_index(['특징']), d1_2.set_index(['특징']),], axis = 1, keys = start_days_list).reset_index()
    color = pd.concat([d4_3.set_index(['특징']), d3_3.set_index(['특징']), d2_3.set_index(['특징']), d1_3.set_index(['특징']),], axis = 1, keys = start_days_list).reset_index()

    purpose.fillna(0, inplace = True)
    image.fillna(0, inplace = True)
    color.fillna(0, inplace = True)

    dic_purpose = {
            'index' : [],
            'Total' : [],
            'spend' : [],
            'roas' : [],
            'ctr' : []
        }

    dic_image = {
            'index' : [],
            'Total' : [],
            'spend' : [],
            'roas' : [],
            'ctr' : []
        }

    dic_color = {
            'index' : [],
            'Total' : [],
            'spend' : [],
            'roas' : [],
            'ctr' : []
        }

    dic_purpose["index"] = list(purpose["특징"])
    dic_image["index"] = list(image["특징"])
    dic_color["index"] = list(color["특징"])

    for day in start_days_list:
        l_t = []
        l_s = []
        l_r = []
        l_c = []
        for i in range(len(purpose)): 
            l_t.append(purpose[day]["Total"][i])
            l_s.append(purpose[day]["spend"][i])
            l_r.append(purpose[day]["roas"][i])
            l_c.append(purpose[day]["ctr"][i])

        dic_purpose["Total"].append(l_t)
        dic_purpose["spend"].append(l_s)
        dic_purpose["roas"].append(l_r)
        dic_purpose["ctr"].append(l_c)
        
    for day in start_days_list:
        l_t = []
        l_s = []
        l_r = []
        l_c = []
        for i in range(len(image)):
            l_t.append(image[day]["Total"][i])
            l_s.append(image[day]["spend"][i])
            l_r.append(image[day]["roas"][i])
            l_c.append(image[day]["ctr"][i])

        dic_image["Total"].append(l_t)
        dic_image["spend"].append(l_s)
        dic_image["roas"].append(l_r)
        dic_image["ctr"].append(l_c)


    for day in start_days_list:
        l_t = []
        l_s = []
        l_r = []
        l_c = []
        for i in range(len(color)):
            l_t.append(color[day]["Total"][i])
            l_s.append(color[day]["spend"][i])
            l_r.append(color[day]["roas"][i])
            l_c.append(color[day]["ctr"][i])

        dic_color["Total"].append(l_t)
        dic_color["spend"].append(l_s)
        dic_color["roas"].append(l_r)
        dic_color["ctr"].append(l_c)

    data_list = [day_list, dic_purpose, dic_image, dic_color]

    return data_list


def lineGraphFbCampaign (del_TS = True, date_start = day_s, date_end = day_end, ask = "spend", level = "campaign", time_increment = 1, ad_account_id = os.getenv('fb_ad_account_id1')):  
    ### FACEBOOK  ###
    FacebookAdsApi.init(access_token=os.getenv('fb_accesstoken'))
    load_dotenv()
    name = 'campaign_name'

    fields = [
        'campaign_name',
        'adset_name',
        'spend',
        'purchase_roas',
        'reach',
        'ctr'
    ]
    
    x = len(fields) + 1

    if level == 'adset':
        name = 'adset_name'
        x += 1

    param1 = {
        'time_range': {'since': date_start,'until': date_end},
        'time_increment' : time_increment,
        'filtering': [],
        'level': level,
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


    if del_TS:
        index_to_del = []
        for i in range(len(dfX)):
            if dfX[name][i][0] == "T":
                index_to_del.append(i)
        dfX.drop(index_to_del, inplace=True)

    table = pd.pivot_table(dfX, index = "date_start", columns =  name, values = ask)
    table.index = pd.to_datetime(table.index)
    table.reset_index(inplace=True)
    table["yy"] = table["date_start"].dt.year
    table["mm"] = table["date_start"].dt.month
    table["dd"] = table["date_start"].dt.day
    table.fillna(0, inplace = True)

    d_list = []
    d_list.append(list(dfX[name].unique()))

    for i in range(len(table)):
        d_list.append(   [  int(table["yy"][i]) , int(table["mm"][i] - 1) , int(table["dd"][i])                                 ]  )

    for i in range(len(d_list[0])):
        for j in range(len(table)):
            d_list[j+1].append( float( table[d_list[0][i]][j] )   )

    return d_list


#### SUB FUNCTIONSSSSS
def callFbAdTable(ad_account_id, date_start, date_end): ## SUBFUNCTION calls the facebook ads data
    load_dotenv()
    ### FACEBOOK  ###
    FacebookAdsApi.init(access_token=os.getenv('fb_accesstoken'))
    fields = [
        'ad_id',
        'spend',
        'purchase_roas',
        'ctr',
        'reach',
        'quality_ranking',
        'ad_name'
    ]
    x = len(fields) + 2
    param1 = {
        'time_range': {'since': date_start,'until': date_end},
        'filtering': [],
        'level': 'ad',
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



def AnlTable (df): ##SUBFUNCTION makes tables for analysis purpose
    typ1 = []
    typ2 = []
    typ3 = []

    a = -2
    b = -5
    c = -7
    d = -10
    e = -12

    for i in range(len(df)):
        if len(df['ad_name'][i]) > 20:
            if ('사본' not in df['ad_name'][i]):
                if (df['ad_name'][i][-2:] not in typ1):
                    typ1.append(df['ad_name'][i][a:])

                if (df['ad_name'][i][-7:-5] not in typ2):
                    typ2.append(df['ad_name'][i][c:b])

                if (df['ad_name'][i][-12:-10] not in typ3):
                    typ3.append(df['ad_name'][i][e:d])
            
            else:
                if (df['ad_name'][i][a-5:-5] not in typ1):
                    typ1.append(df['ad_name'][i][a-5:-5])

                if (df['ad_name'][i][c-5:b-5] not in typ2):
                    typ2.append(df['ad_name'][i][c-5:b-5])

                if (df['ad_name'][i][e-5:d-5] not in typ3):
                    typ3.append(df['ad_name'][i][e-5:d-5])
    typ1_val1 = findAvgs(df, typ1, 'Total')
    typ1_val2 = findAvgs(df, typ1, 'spend')
    typ1_val3 = findAvgs(df, typ1, 'roas')
    typ1_val4 = findAvgs(df, typ1, 'ctr')
    df_type1 = pd.DataFrame(list(zip(typ1, typ1_val1, typ1_val2, typ1_val3, typ1_val4)), columns = ["특징", "Total", "spend", "roas", "ctr"])

    typ2_val1 = findAvgs(df, typ2, 'Total')
    typ2_val2 = findAvgs(df, typ2, 'spend')
    typ2_val3 = findAvgs(df, typ2, 'roas')
    typ2_val4 = findAvgs(df, typ2, 'ctr')
    df_type2 = pd.DataFrame(list(zip(typ2, typ2_val1, typ2_val2, typ2_val3, typ2_val4)), columns = ["특징", "Total", "spend", "roas", "ctr"])

    typ3_val1 = findAvgs(df, typ3, 'Total')
    typ3_val2 = findAvgs(df, typ3, 'spend')
    typ3_val3 = findAvgs(df, typ3, 'roas')
    typ3_val4 = findAvgs(df, typ3, 'ctr')    
    df_type3 = pd.DataFrame(list(zip(typ3, typ3_val1, typ3_val2, typ3_val3, typ3_val4)), columns = ["특징", "Total", "spend", "roas", "ctr"])

    return df_type1, df_type2, df_type3

def findAvgs (df, list_name, value): ### SUBFUNCTION 
    list_values = []
    for i in range(len(list_name)):
        list_values.append(0)

    for i in range(len(list_name)):
        cnt = 0
        for j in range(len(df)):
            if list_name[i] in df['ad_name'][j]:
                list_values[i] += df[value][j]
                cnt += 1
        list_values[i] = list_values[i] / cnt
    return list_values