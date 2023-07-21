import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

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

    

    df_list = []    
    for key in df_all['검색어'].unique():
        mask = df_all['검색어'] == key
        df_list.append(df_all[mask].drop_duplicates(subset ='Nv_Mid').reset_index(drop = True))

    d_list = []

    for df in df_list:
        lis = []
        # href_list = []
        lis.append(df['검색어'][0])
        for i in range(len(df)):
            lis.append( [ df['상품명'][i],  int(df['구좌수'][i]),   int(df['rank'][i])  , df['href'][i]] )
        d_list.append(lis)
    
    return d_list


def sms_graph (df1, key, url):
    link_l = href_list(key, url)
    link_l = [*set(link_l)]

    df_l = []

    for link in link_l :
        df_l.append( sms_ranking(link) )
        
    table = pd.concat(df_l, axis=1)
    table.fillna(0, inplace=True)

    ###
    table = table.reset_index().copy()
    l1 = table["date"].to_list()
    l1.sort()


    df_s = lineGraphSales_v2_naver(df1, str(l1[0]), str(l1[-1]))
    df_s = df_s.rename( columns={'주문일':'date'})
   
    dfT = pd.concat( [table.set_index("date").resample("W").mean(), df_s.set_index("date").resample("W").sum()], axis = 1)
    dfT = dfT.fillna(0).copy()
    

    ###
    table = table.set_index('date').copy()

    d_list = []
    item_l = list(table.columns)
    item_l.append('네이버')
    item_l.append('자사몰')
    d_list.append( item_l )

    dfT.reset_index(inplace=True)
    dfT["yy"] = dfT["date"].dt.year
    dfT["mm"] = dfT["date"].dt.month
    dfT["dd"] = dfT["date"].dt.day

    for i in range(len(dfT)): #len(table)
        lll = []
        lll.append(int(dfT["yy"][i]) ) #, int(table["mm"][i] - 1) , int(table["dd"][i]) )
        lll.append(int(dfT["mm"][i]) - 1)
        lll.append(int(dfT["dd"][i]) )
        for item in list(table.columns):
            lll.append( int(dfT[item][i]))
            # print(item)
        lll.append(int(dfT["네이버"][i]))
        lll.append(int(dfT["자사몰"][i]))
        d_list.append(lll)
    return d_list

def sms_ranking (link):
    dfs = pd.read_html(link)
    df = dfs[1].copy()
    new_header = df.iloc[0] #grab the first row for the header
    df = df[1:] #take the data less the header row
    df.columns = new_header #set the header row as the df header
    df.reset_index(drop = True, inplace = True)

    pm = []
    hour = []
    date = []
    time = []


    for stri in df['실행시간']:
        d1 = stri.find('[')
        stri = stri[d1:]
        date.append(stri[1 : 11])

    test = pd.DataFrame(date, columns=['date'])

    test['date'] = pd.to_datetime(test['date'])

    df_final = pd.concat([df, test], axis = 1)
    df_final['순위'] = df_final['순위'].astype(int)

    df_final.rename(columns = {'순위':df_final['제목/가격/설명'][0]}, inplace = True)
    df_final = df_final[[df_final['제목/가격/설명'][0], 'date']].copy()
    df_final.drop_duplicates(subset='date', inplace=True)
    df_final.set_index('date', inplace=True)

    return df_final


def href_list(key, url) :
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

    href = []
    for i in range(len(df_all)):
        if df_all["검색어"][i] == key:
            href.append(df_all['href'][i])
    return href



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