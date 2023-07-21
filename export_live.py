from pymongo import MongoClient
import pandas as pd
import os
from dotenv import load_dotenv



def live_data_v2 (item = 'powder') :
    client = MongoClient(os.getenv('mongoClient_reno'))
    live = client['live'] #db
    lp1 = live['lp1'] #collection
    lp2 = live['lp2'] #collection
    lp3 = live['lp3'] #collection
    lp4 = live['lp4'] #collection
    lp5 = live['lp5'] #collection
    lp6 = live['lp6'] #collection

    c2 = live['c2'] #collection
    c3 = live['c3'] #collection

    ls1 = live['ls1'] #collection
    ls2 = live['ls2'] #collection
    ls3 = live['ls3'] #collection
    ls4 = live['ls4'] #collection
    ls5 = live['ls5'] #collection
    ls6 = live['ls6'] #collection

    s2 = live['s2'] #collection
    s3 = live['s3'] #collection

    lsp1 = live['lsp1'] #collection
    lsp2 = live['lsp2'] #collection
    lsp3 = live['lsp3'] #collection
    lsp4 = live['lsp4'] #collection
    lsp5 = live['lsp5'] #collection
    lsp6 = live['lsp6'] #collection

    sp2 = live['sp2'] #collection
    sp3 = live['sp3'] #collection



    load_lp1 = lp1.find_one()
    df_lp1 = pd.DataFrame(load_lp1["data"])
    load_lp2 = lp2.find_one()
    df_lp2 = pd.DataFrame(load_lp2["data"])
    load_lp3 = lp3.find_one()
    df_lp3 = pd.DataFrame(load_lp3["data"])
    load_lp4 = lp4.find_one()
    df_lp4 = pd.DataFrame(load_lp4["data"])
    load_lp5 = lp5.find_one()
    df_lp5 = pd.DataFrame(load_lp5["data"])
    load_lp6 = lp6.find_one()
    df_lp6 = pd.DataFrame(load_lp6["data"])

    load_c2 = c2.find_one()
    df_c2 = pd.DataFrame(load_c2["data"])
    load_c3 = c3.find_one()
    df_c3 = pd.DataFrame(load_c3["data"])

    

    load_ls1 = ls1.find_one()
    df_ls1 = pd.DataFrame(load_ls1["data"])
    load_ls2 = ls2.find_one()
    df_ls2 = pd.DataFrame(load_ls2["data"])
    load_ls3 = ls3.find_one()
    df_ls3 = pd.DataFrame(load_ls3["data"])
    load_ls4 = ls4.find_one()
    df_ls4 = pd.DataFrame(load_ls4["data"])
    load_ls5 = ls5.find_one()
    df_ls5 = pd.DataFrame(load_ls5["data"])
    load_ls6 = ls6.find_one()
    df_ls6 = pd.DataFrame(load_ls6["data"])

    load_s2 = s2.find_one()
    df_s2 = pd.DataFrame(load_s2["data"])
    load_s3 = s3.find_one()
    df_s3 = pd.DataFrame(load_s3["data"])




    load_lsp1 = lsp1.find_one()
    df_lsp1 = pd.DataFrame(load_lsp1["data"])
    load_lsp2 = lsp2.find_one()
    df_lsp2 = pd.DataFrame(load_lsp2["data"])
    load_lsp3 = lsp3.find_one()
    df_lsp3 = pd.DataFrame(load_lsp3["data"])
    load_lsp4 = lsp4.find_one()
    df_lsp4 = pd.DataFrame(load_lsp4["data"])
    load_lsp5 = lsp5.find_one()
    df_lsp5 = pd.DataFrame(load_lsp5["data"])
    load_lsp6 = lsp6.find_one()
    df_lsp6 = pd.DataFrame(load_lsp6["data"])

    load_sp2 = sp2.find_one()
    df_sp2 = pd.DataFrame(load_sp2["data"])
    load_sp3 = sp3.find_one()
    df_sp3 = pd.DataFrame(load_sp3["data"])

    

    if item == "soap" :
        df_lp1 = df_ls1
        df_lp2 = df_ls2
        df_lp3 = df_ls3 # line graph
        df_lp4 = df_ls4 # line graph
        df_lp5 = df_ls5 # line graph
        df_lp6 = df_ls6 # line graph
        df_c2  = df_s2
        df_c3  = df_s3

    elif item == "spray" :
        df_lp1 = df_lsp1
        df_lp2 = df_lsp2
        df_lp3 = df_lsp3 # line graph
        df_lp4 = df_lsp4 # line graph
        df_lp5 = df_lsp5 # line graph
        df_lp6 = df_lsp6 # line graph
        df_c2  = df_sp2
        df_c3  = df_sp3

    df_lp1.fillna(0, inplace=True)
    df_lp2.fillna(0, inplace=True)
    df_lp3.fillna(0, inplace=True)
    df_lp4.fillna(0, inplace=True)
    df_lp5.fillna(0, inplace=True)
    df_lp6.fillna(0, inplace=True)
    df_c2.fillna(0, inplace=True)
    df_c3.fillna(0, inplace=True)
    

    df_lp1 = df_lp1.set_index("회차").T.copy()
    lXXX = ['통계']
    for i in df_lp1.columns : lXXX.append(str(i) + '회차')

    l1 = []
    l1.append(list(lXXX))
    for i in range(len(df_lp1)):
        lll = []
        lll.append( list(df_lp1.index)[i]  )
        for col in df_lp1.columns :
            lll.append( int(df_lp1[col][i]) )
        l1.append(lll)


    df_lp2 = df_lp2.set_index("회차").T.copy()
    lXXX = ['통계']
    for i in df_lp2.columns : lXXX.append(str(i) + '회차')
    l2 = []
    l2.append(list(lXXX))
    for i in range(len(df_lp2)):
        lll = []
        lll.append( list(df_lp2.index)[i]  )
        for col in df_lp2.columns :
            lll.append( int(df_lp2[col][i]) )
        l2.append(lll)


    df_c3 = df_c3.set_index("회차").T.copy()
    lXXX = ['통계']
    for i in df_c3.columns : lXXX.append(str(i) + '회차')

    l3 = []
    l3.append(list(lXXX))
    for i in range(len(df_c3)):
        lll = []
        lll.append( list(df_c3.index)[i]  )
        for col in df_c3.columns :
            lll.append( int(df_c3[col][i]) )
        l3.append(lll)


    df_c2 = df_c2.set_index("회차").T.copy()
    lXXX = ['통계']
    for i in df_c2.columns : lXXX.append(str(i) + '회차')
    l4 = []
    l4.append(list(lXXX))
    for i in range(len(df_c2)):
        lll = []
        lll.append( list(df_c2.index)[i]  )
        for col in df_c2.columns :
            lll.append( int(df_c2[col][i]) )
        l4.append(lll)


    df_lp3.fillna(0, inplace=True)
    l5 = []
    l5.append(list(df_lp3.columns))
    for i in range(len(df_lp3)) :
        lll = []
        for col in df_lp3.columns :
            lll.append( int(df_lp3[col][i]) )
        l5.append(lll)


    d_list = [l1, l2, l3, l4, l5]

    return d_list



def live_line (item = 'powder', stat = 'df3') :
    client = MongoClient(os.getenv('mongoClient_reno'))
    live = client['live'] #db

    lp3 = live['lp3'] #collection
    lp4 = live['lp4'] #collection
    lp5 = live['lp5'] #collection
    lp6 = live['lp6'] #collection

    ls3 = live['ls3'] #collection
    ls4 = live['ls4'] #collection
    ls5 = live['ls5'] #collection
    ls6 = live['ls6'] #collection

    lsp3 = live['lsp3'] #collection
    lsp4 = live['lsp4'] #collection
    lsp5 = live['lsp5'] #collection
    lsp6 = live['lsp6'] #collection


    load_lp3 = lp3.find_one()
    df_lp3 = pd.DataFrame(load_lp3["data"])
    load_lp4 = lp4.find_one()
    df_lp4 = pd.DataFrame(load_lp4["data"])
    load_lp5 = lp5.find_one()
    df_lp5 = pd.DataFrame(load_lp5["data"])
    load_lp6 = lp6.find_one()
    df_lp6 = pd.DataFrame(load_lp6["data"])

    

    load_ls3 = ls3.find_one()
    df_ls3 = pd.DataFrame(load_ls3["data"])
    load_ls4 = ls4.find_one()
    df_ls4 = pd.DataFrame(load_ls4["data"])
    load_ls5 = ls5.find_one()
    df_ls5 = pd.DataFrame(load_ls5["data"])
    load_ls6 = ls6.find_one()
    df_ls6 = pd.DataFrame(load_ls6["data"])

    load_lsp3 = lsp3.find_one()
    df_lsp3 = pd.DataFrame(load_lsp3["data"])
    load_lsp4 = lsp4.find_one()
    df_lsp4 = pd.DataFrame(load_lsp4["data"])
    load_lsp5 = lsp5.find_one()
    df_lsp5 = pd.DataFrame(load_lsp5["data"])
    load_lsp6 = lsp6.find_one()
    df_lsp6 = pd.DataFrame(load_lsp6["data"])


    if item == "soap" :
        df_lp3 = df_ls3 # line graph
        df_lp4 = df_ls4 # line graph
        df_lp5 = df_ls5 # line graph
        df_lp6 = df_ls6 # line graph
    elif item == "spray" :
        df_lp3 = df_lsp3 # line graph
        df_lp4 = df_lsp4 # line graph
        df_lp5 = df_lsp5 # line graph
        df_lp6 = df_lsp6 # line graph


    df_lp3.fillna(0, inplace=True)
    df_lp4.fillna(0, inplace=True)
    df_lp5.fillna(0, inplace=True)
    df_lp6.fillna(0, inplace=True)

    df = df_lp3
    if stat == "df4" : df = df_lp4
    elif stat == "df5" : df = df_lp5
    elif stat == "df6" : df = df_lp6


    df.fillna(0, inplace=True)
    d_list = []
    d_list.append(list(df.columns))
    for i in range(len(df)) :
        lll = []
        for col in df.columns :
            lll.append( int(df[col][i]) )
        d_list.append(lll)

    return d_list


def live_data (item = 'powder') :
    client = MongoClient(os.getenv('mongoClient_reno'))
    live = client['live'] #db
    c1 = live['c1'] #collection
    c2 = live['c2'] #collection
    c3 = live['c3'] #collection
    c4 = live['c4'] #collection

    s1 = live['s1'] #collection
    s2 = live['s2'] #collection
    s3 = live['s3'] #collection
    s4 = live['s4'] #collection

    load_c1 = c1.find_one()
    df_c1 = pd.DataFrame(load_c1["data"])
    load_c2 = c2.find_one()
    df_c2 = pd.DataFrame(load_c2["data"])
    load_c3 = c3.find_one()
    df_c3 = pd.DataFrame(load_c3["data"])
    load_c4 = c4.find_one()
    df_c4 = pd.DataFrame(load_c4["data"])

    load_s1 = s1.find_one()
    df_s1 = pd.DataFrame(load_s1["data"])
    load_s2 = s2.find_one()
    df_s2 = pd.DataFrame(load_s2["data"])
    load_s3 = s3.find_one()
    df_s3 = pd.DataFrame(load_s3["data"])
    load_s4 = s4.find_one()
    df_s4 = pd.DataFrame(load_s4["data"])

    if item == "soap" :
        df_c1 = df_s1
        df_c2 = df_s2
        df_c3 = df_s3
        df_c4 = df_s4

    df_c1 = df_c1.set_index("회차").T.copy()
    lXXX = ['통계']
    for i in df_c1.columns : lXXX.append(str(i))

    l1 = []
    l1.append(list(lXXX))
    for i in range(len(df_c1)):
        lll = []
        lll.append( list(df_c1.index)[i]  )
        for col in df_c1.columns :
            lll.append( int(df_c1[col][i]) )
        l1.append(lll)


    df_c2 = df_c2.set_index("회차").T.copy()
    lXXX = ['통계']
    for i in df_c2.columns : lXXX.append(str(i))
    l2 = []
    l2.append(list(lXXX))
    for i in range(len(df_c2)):
        lll = []
        lll.append( list(df_c2.index)[i]  )
        for col in df_c2.columns :
            lll.append( int(df_c2[col][i]) )
        l2.append(lll)


    df_c3 = df_c3.set_index("회차").T.copy()
    lXXX = ['통계']
    for i in df_c3.columns : lXXX.append(str(i))

    l3 = []
    l3.append(list(lXXX))
    for i in range(len(df_c3)):
        lll = []
        lll.append( list(df_c3.index)[i]  )
        for col in df_c3.columns :
            lll.append( int(df_c3[col][i]) )
        l3.append(lll)


    df_c4 = df_c4.set_index("회차").T.copy()
    lXXX = ['통계']
    for i in df_c4.columns : lXXX.append(str(i))
    l4 = []
    l4.append(list(lXXX))
    for i in range(len(df_c4)):
        lll = []
        lll.append( list(df_c4.index)[i]  )
        for col in df_c4.columns :
            lll.append( float(df_c4[col][i]) )
        l4.append(lll)


    d_list = [l1, l2, l3, l4]

    return d_list