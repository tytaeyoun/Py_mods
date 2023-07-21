import pandas as pd
import numpy as np
from datetime import date
from datetime import timedelta


def df_final (df):
    #### Signs for 재구매 건수들
    cls1 = ['리노칼파 150g', '리노칼파 30g', '리노칼파 50g', '리노칼파 7.5g', '리노칼파 1.5g',
        '리노선물 1호', '리노선물 2호','리노칼파 45g' ]
    cls2 = [ '리노모팩 100g', '리노모팩 35g']
    cls3 = ['그린칼파 150g','그린칼파 7.5g', '그린칼파 30포', '그린칼파 5포']
    cls4 = ['리노 살탈제']

    g1 = df[df["상품명"].isin(cls1)].copy() #리노칼파
    g2 = df[df["상품명"].isin(cls2)].copy() #리노모팩
    g3 = df[df["상품명"].isin(cls3)].copy() #그린칼파
    g4 = df[df["상품명"].isin(cls4)].copy() #리노살탈

    g1 = g1[["발주일", "수령자", "배송지주소"]].copy()
    g1.drop_duplicates(inplace = True) #drop dups so it neglects same day, same adress, but diff item
    g1["match"] = g1.duplicated(subset=['배송지주소'])

    g2 = g2[["발주일", "수령자", "배송지주소"]].copy()
    g2.drop_duplicates(inplace = True) #drop dups so it neglects same day, same adress, but diff item
    g2["match"] = g2.duplicated(subset=['배송지주소'])

    g3 = g3[["발주일", "수령자", "배송지주소"]].copy()
    g3.drop_duplicates(inplace = True) #drop dups so it neglects same day, same adress, but diff item
    g3["match"] = g3.duplicated(subset=['배송지주소'])

    g4 = g4[["발주일", "수령자", "배송지주소"]].copy()
    g4.drop_duplicates(inplace = True) #drop dups so it neglects same day, same adress, but diff item
    g4["match"] = g4.duplicated(subset=['배송지주소'])


    df1 = df[df["상품명"].isin(cls1)].copy() #리노칼파
    df2 = df[df["상품명"].isin(cls2)].copy() #리노모팩
    df3 = df[df["상품명"].isin(cls3)].copy() #그린칼파
    df4 = df[df["상품명"].isin(cls4)].copy() #리노살탈

    df1["match"] = df1.index.isin(g1[g1["match"]].index)
    df2["match"] = df2.index.isin(g2[g2["match"]].index)
    df3["match"] = df3.index.isin(g3[g3["match"]].index)
    df4["match"] = df4.index.isin(g4[g4["match"]].index)

    dfs_sorted = [df1, df2, df3, df4]

    for df0 in dfs_sorted:
        df0.sort_values(by = ["발주일", "배송지주소"], inplace = True)
        df0.reset_index(drop = True, inplace = True)

        df0["dup"] = df0.duplicated(subset=['발주일', '배송지주소'], keep=False)
        for i in range(len(df0) - 1):
            if df0["dup"][i] & df0["match"][i]:
                if df0["배송지주소"][i] == df0["배송지주소"][i+1]:
                    df0.loc[i+1, "match"] = df0.loc[i, "match"]

    df_final = pd.concat(dfs_sorted)
    df_final.reset_index(drop = True, inplace=True)
    return df_final


def PO_sort (df1, price) : #df1 = 발주서

    # SORT fillna, set dates
    df1["주문일"].fillna('2000-01-01', inplace = True)
    df1.loc[df1["주문일"] == "2000-01-01", "주문일"] = df1["발주일"]

    df1.fillna("", inplace=True)
    df1["발주일"] = pd.to_datetime(df1["발주일"])
    df1["주문일"] = pd.to_datetime(df1["주문일"])
    df1["판매개수"] = df1["판매개수"].astype(int)

    # SORT addup 판매개수
    df1 = pd.pivot_table(df1, values = "판매개수", index = ["발주일", "주문일", "판매처", "상품명", "수령자", "수령자핸드폰", "배송지주소", "메모"], aggfunc= np.sum)
    df1.reset_index(inplace=True)

    # SORT remove 증정품
    mask1 = df1["상품명"]=="리노칼파 150g"
    # mask2 = df1["판매개수"] == 3
    mask2 = df1["판매개수"] % 3 == 0
    indx = []
    for index, row in df1[mask1 & mask2].iterrows() :
        maskkA = df1["수령자핸드폰"] == row["수령자핸드폰"]
        maskkB = df1["발주일"] == row["발주일"]
        maskkC = df1["상품명"] == "리노 살탈제"
        indx.append(  df1[maskkA & maskkB & maskkC].index.tolist()  )
        
    iii = []
    for l in indx:
        for j in l:
            iii.append(j)

    df1.loc[iii, "판매개수"] = df1["판매개수"] - 1
    df1.loc[ ~df1.index.isin(iii), "판매개수"] = df1["판매개수"]

    df1_final = df1.drop(df1[df1["판매개수"] == 0].index).copy()
    df1_final.reset_index(inplace=True, drop=True)

    ######## UNNECESSARY AFTER DECEMBER


    msk_rc150 = df1["상품명"] == '리노칼파 150g'
    msk_rSoap = df1["상품명"] == '리노모팩 100g'
    msk_rSpra = df1["상품명"] == '리노 살탈제'
    msk_gg150 = df1["상품명"] == '그린칼파 150g'
    msk_gSpra = df1["상품명"] == '그린 소독수'
    msk_g1530 = df1["상품명"] == '그린그램1.5g*30포'


    msk_s2 = df1["판매개수"] % 2 == 0
    msk_s3 = df1["판매개수"] % 3 == 0


    df1["상품명_s"] = df1["상품명"]
    df1["판매개수_s"] = df1["판매개수"]

    df1.loc[msk_rc150 & msk_s2, "상품명_s"] = "리노칼파 150gx2"
    df1.loc[msk_rc150 & msk_s3, "상품명_s"] = "리노칼파 150gx3"
    df1.loc[msk_rc150 & msk_s2, "판매개수_s"] = df1["판매개수"] // 2
    df1.loc[msk_rc150 & msk_s3, "판매개수_s"] = df1["판매개수"] // 3

    df1.loc[msk_rSoap & msk_s2, "상품명_s"] = '리노모팩 100gx2'
    df1.loc[msk_rSoap & msk_s3, "상품명_s"] = '리노모팩 100gx3'
    df1.loc[msk_rSoap & msk_s2, "판매개수_s"] = df1["판매개수"] // 2
    df1.loc[msk_rSoap & msk_s3, "판매개수_s"] = df1["판매개수"] // 3

    df1.loc[msk_rSpra & msk_s3, "상품명_s"] = '리노 살탈제x3'
    df1.loc[msk_rSpra & msk_s3, "판매개수_s"] = df1["판매개수"] // 3

    df1.loc[msk_gg150 & msk_s2, "상품명_s"] = '그린칼파 150gx2'
    df1.loc[msk_gg150 & msk_s3, "상품명_s"] = '그린칼파 150gx3'
    df1.loc[msk_gg150 & msk_s2, "판매개수_s"] = df1["판매개수"] // 2
    df1.loc[msk_gg150 & msk_s3, "판매개수_s"] = df1["판매개수"] // 3

    df1.loc[msk_gSpra & msk_s2, "상품명_s"] = '그린 소독수x2'
    df1.loc[msk_gSpra & msk_s3, "상품명_s"] = '그린 소독수x3'
    df1.loc[msk_gSpra & msk_s2, "판매개수_s"] = df1["판매개수"] // 2
    df1.loc[msk_gSpra & msk_s3, "판매개수_s"] = df1["판매개수"] // 3

    df1.loc[msk_g1530 & msk_s2, "상품명_s"] = '그린그램1.5g*30포x2'
    df1.loc[msk_g1530 & msk_s2, "판매개수_s"] = df1["판매개수"] // 2


    Sale = []
    for i in range(len(df1)):
        try:
            Sale.append(df1["판매개수_s"][i] * int(price[df1["상품명_s"][i]][df1["판매처"][i]]))
        except:
            Sale.append(0)
    df1["Sale"] = Sale

    df1.loc[ df1["상품명_s"] == "리노 살탈제", "Sale"] = df1["판매개수_s"] * 19900
    df1.loc[ df1["상품명_s"] == "리노 살탈제x3", "Sale"] = df1["판매개수_s"] * 39800
    df1.loc[ df1["상품명_s"] == "그린칼파 7.5g", "Sale"] = df1["판매개수_s"] * 2900

    xxx = df1["Sale"]==0
    yyy = df1["상품명"] == "그린칼파 150g"
    zzz = df1["판매처"] == "네이버"

    df1.loc[ xxx&yyy&zzz, "Sale"] = df1["판매개수"] * 8900


    aaa = df1["상품명"] == "그린 소독수"
    bbb = df1["판매처"] == "온누리"

    df1.loc[ aaa&bbb, "Sale"] = df1["판매개수"] * 11900


    dfX = df1[ ["발주일", "주문일", "판매처", "상품명", "판매개수", "수령자", "수령자핸드폰", "배송지주소", "메모", "sale" ] ].copy()

    return dfX


def lineGraphSales(df, date_start = '2021-10-01', date_end = '2021-10-15', time_increment = 7, mall = ['네이버', '자사몰'], item = ['리노칼파 150g'], number = '판매개수') :
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

    if time_increment == 7 :
        df_send = df_send.resample("W").sum()
    elif time_increment == 30 :
        df_send = df_send.resample("M").sum()
    
    df_send.reset_index(inplace=True)

    df_send["yy"] = df_send["주문일"].dt.year
    df_send["mm"] = df_send["주문일"].dt.month
    df_send["dd"] = df_send["주문일"].dt.day

    for i in range(len(df_send)):
        l1 = []
        l1.append( int(df_send['yy'][i]))
        l1.append( int(df_send['mm'][i] - 1))
        l1.append( int(df_send['dd'][i]))

        for j in mall:
            l1.append( int(df_send[j][i]))
        d_list.append(l1)

    return d_list



def repur_total(df) :
    l1 = repur_compare(df)
    l2 = repurchase_calc_shop(df)

    return [l1, l2]

def repurchase_calc_shop(df): 
    timeDelta = 33
    today = str(date.today() - timedelta(days = 2))
    day_back = str(date.today() - timedelta(days = timeDelta))
    mask1 = df["발주일"] >= day_back
    df = df[mask1].copy()

    table = pd.pivot_table(df, values = "Sale", index = "판매처", aggfunc=np.sum, fill_value= 0)
    table.sort_values(by=['Sale'], ascending=False, inplace=True)
    table.reset_index(inplace=True)

    fs1 = int(len( df[ (df["판매처"] == table["판매처"][0])  & (~df["match"])  ] ))
    rs1 = int(len( df[ (df["판매처"] == table["판매처"][0])  & (df["match"])  ] ))
    fs2 =  int(len( df[ (df["판매처"] == table["판매처"][1])  & (~df["match"])  ] ))
    rs2 =  int(len( df[ (df["판매처"] == table["판매처"][1])  & (df["match"])  ] ))
    fs3 = int(len( df[ (df["판매처"] == table["판매처"][2])  & (~df["match"])  ] ))
    rs3 = int(len( df[ (df["판매처"] == table["판매처"][2])  & (df["match"])  ] ))

    return [table["판매처"][0], table["판매처"][1], table["판매처"][2], 
    fs1, rs1, fs2, rs2, fs3, rs3]


def repur_compare(df): 
    mask1 = df["match"]
    table1 = pd.pivot_table(df[mask1], values = "Sale", index = "발주일", aggfunc=np.sum, fill_value= 0)
    table1 = table1.resample("M").sum()
    table1.rename( columns= {"Sale":"Sale_Re"}, inplace=True)

    table2 = pd.pivot_table(df, values = "Sale", index = "발주일", aggfunc=np.sum, fill_value= 0)
    table2 = table2.resample("M").sum()
    table2.rename( columns= {"Sale":"Sale_To"}, inplace=True)

    table = pd.concat([table1, table2], axis = 1)
    table.reset_index(inplace=True)
    table["RepPer"] = table["Sale_Re"]/table["Sale_To"]*100

    mask1 = table["발주일"] > "2020-01-01"
    table = table[mask1].copy()
    table = table.reset_index(drop = True)

    data_list = []
    for i in range(len(table)):
        data_list.append([ int(table["발주일"].dt.year[i]), int(table["발주일"].dt.month[i])-1, int(table["발주일"].dt.day[i]), float(table["RepPer"][i]), int(table["Sale_To"][i]) ])
        # data_list.append([ int(table["발주일"].dt.year[i]), int(table["발주일"].dt.month[i]), int(table["발주일"].dt.day[i]), int(table["Sale_Re"][i]), int(table["Sale_To"][i]) ])
    return data_list