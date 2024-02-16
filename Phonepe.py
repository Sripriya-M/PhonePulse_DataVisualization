import json
from os import walk
from pathlib import Path
import streamlit as st
from streamlit_option_menu import option_menu
import os
import pandas as pd
import mysql.connector
import pymysql
import numpy as np
import matplotlib.pyplot as plt
from streamlit_option_menu import option_menu
import plotly.express as px
import plotly.graph_objects as go
import PIL
from PIL import Image
import git

myconnection=pymysql.connect(host="localhost",user="root",password="Password@1234",database='Phonepepulse')
mycur=myconnection.cursor()
path='pulse/data/aggregated/transaction/country/india/state/'
# path = r'C:\Users\prett\pulse\data\aggregated\transaction\country\india\state'
Agg_state_list=os.listdir(path)
import requests
def geo_state_list():
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data = json.loads(response.content)
    geo_state = [i['properties']['ST_NM'] for i in data['features']]
    geo_state.sort(reverse=False)
    return geo_state
custom_state_list=geo_state_list()
def Aggregate_Trans():   
    path='pulse/data/aggregated/transaction/country/india/state/'
    Agg_state_list=os.listdir(path)
    Agg_trans_data = {'State':[], 'Year':[], 'Quarter':[], 'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}
    for i in Agg_state_list:
        p_i=path+i+"/"
        Agg_yr=os.listdir(p_i)
        for j in Agg_yr:
            p_j=p_i+j+"/"
            Agg_yr_list=os.listdir(p_j)
            for k in Agg_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D=json.load(Data)
                try:
                    for z in D['data']['transactionData']:
                        Name=z['name']
                        count=z['paymentInstruments'][0]['count']
                        amount=z['paymentInstruments'][0]['amount']
                        Agg_trans_data['Transaction_type'].append(Name)
                        Agg_trans_data['Transaction_count'].append(count)
                        Agg_trans_data['Transaction_amount'].append(amount)
                        Agg_trans_data['State'].append(i)
                        Agg_trans_data['Year'].append(j)
                        Agg_trans_data['Quarter'].append(int(k.strip('.json')))
                except:
                    pass
    Agg_Trans=pd.DataFrame(Agg_trans_data)
    Agg_Trans['State'] = Agg_Trans['State'].replace(dict(zip(Agg_state_list, custom_state_list)))
    return Agg_Trans
path2='pulse/data/aggregated/user/country/india/state/'
Agg_user_list=os.listdir(path2)
def Aggregate_User():   
    path2='pulse/data/aggregated/user/country/india/state/'
    Agg_user_list=os.listdir(path2)
    Agg_user_data = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'User_Count': [], 'User_Percentage': []}
    for i in Agg_user_list:
        p_i=path2+i+"/"
        Agg_yr=os.listdir(p_i)
        for j in Agg_yr:
            p_j=p_i+j+"/"
            Agg_yr_list=os.listdir(p_j)
            for k in Agg_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D2=json.load(Data)
                try:
                    for z in D2['data']['usersByDevice']:
                        BrandName=z['brand']
                        UserCount=z['count']
                        UserPercentage=z['percentage']
                        Agg_user_data['Brands'].append(BrandName)
                        Agg_user_data['User_Count'].append(UserCount)
                        Agg_user_data['User_Percentage'].append(UserPercentage)
                        Agg_user_data['State'].append(i)
                        Agg_user_data['Year'].append(j)
                        Agg_user_data['Quarter'].append(int(k.strip('.json')))
                except:
                    pass

    Agg_User=pd.DataFrame(Agg_user_data)
    Agg_User['State'] = Agg_User['State'].replace(dict(zip(Agg_state_list, custom_state_list)))
    return Agg_User
path3='pulse/data/map/transaction/hover/country/india/state/'
Map_Trans_list=os.listdir(path3)
def Map_Trans():   
    path3='pulse/data/map/transaction/hover/country/india/state/'
    Map_Trans_list=os.listdir(path3)
    Map_Trans_data = {'State': [], 'Year': [], 'Quarter': [], 'District': [],
                'Transaction_count': [], 'Transaction_amount': []}
    for i in Map_Trans_list:
        p_i=path3+i+"/"
        Agg_yr=os.listdir(p_i)
        for j in Agg_yr:
            p_j=p_i+j+"/"
            Agg_yr_list=os.listdir(p_j)
            for k in Agg_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D3=json.load(Data)
                try:
                    for z in D3['data']['hoverDataList']:
                        DistName=z['name']
                        TransCount=z['metric'][0]['count']
                        TransAmount=z['metric'][0]['amount']
                        Map_Trans_data['District'].append(DistName)
                        Map_Trans_data['Transaction_count'].append(TransCount)
                        Map_Trans_data['Transaction_amount'].append(TransAmount)
                        Map_Trans_data['State'].append(i)
                        Map_Trans_data['Year'].append(j)
                        Map_Trans_data['Quarter'].append(int(k.strip('.json')))
                except:
                    pass

    M_Trans=pd.DataFrame(Map_Trans_data)
    M_Trans['State'] = M_Trans['State'].replace(dict(zip(Agg_state_list, custom_state_list)))
    return M_Trans
path4='pulse/data/map/user/hover/country/india/state/'
Map_User_list=os.listdir(path4)
def Map_User():   
    path4='pulse/data/map/user/hover/country/india/state/'
    Map_User_list=os.listdir(path4)
    Map_User_data = {'State': [], 'Year': [], 'Quarter': [], 'District_name': [], 'Registered_user': [], 'App_opens': []}
    for i in Map_User_list:
        p_i=path4+i+"/"
        Agg_yr=os.listdir(p_i)
        for j in Agg_yr:
            p_j=p_i+j+"/"
            Agg_yr_list=os.listdir(p_j)
            for k in Agg_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D4=json.load(Data)
                try:
                    for m_key, m_value in D4['data']['hoverData'].items():
                                district = m_key.split(' district')[0]
                                reg_user = m_value['registeredUsers']
                                app_opens = m_value['appOpens']

                                Map_User_data['State'].append(i)
                                Map_User_data['Year'].append(j)
                                Map_User_data['Quarter'].append('Q'+str(k[0]))
                                Map_User_data['District_name'].append(district)
                                Map_User_data['Registered_user'].append(reg_user)
                                Map_User_data['App_opens'].append(app_opens)

                except:
                    pass

    M_User=pd.DataFrame(Map_User_data)
    M_User['State'] = M_User['State'].replace(dict(zip(Agg_state_list, custom_state_list)))
    return M_User
path5='pulse/data/top/transaction/country/india/state/'
Top_Trans_list=os.listdir(path5)
def Top_Trans():   
    path5='pulse/data/top/transaction/country/india/state/'
    Top_Trans_list=os.listdir(path5)
    Top_Trans_data = {'State': [], 'Year': [], 'Quarter': [], 'District': [],
                'Transaction_count': [], 'Transaction_amount': [], 'Pin_Codes':[]}
    for i in Top_Trans_list:
        p_i=path5+i+"/"
        Agg_yr=os.listdir(p_i)
        for j in Agg_yr:
            p_j=p_i+j+"/"
            Agg_yr_list=os.listdir(p_j)
            for k in Agg_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D5=json.load(Data)
                try:
                    for z in D5['data']['districts']:
                        for z1 in D5['data']['pincodes']:
                            district = z['entityName']
                            count = z['metric']['count']
                            amount = z['metric']['amount']
                            Pincode=z1['entityName']
                            Top_Trans_data['State'].append(i)
                            Top_Trans_data['Year'].append(j)
                            Top_Trans_data['Quarter'].append('Q'+str(k[0]))
                            Top_Trans_data['District'].append(district)
                            Top_Trans_data['Transaction_count'].append(count)
                            Top_Trans_data['Transaction_amount'].append(amount)
                            Top_Trans_data['Pin_Codes'].append(Pincode)
                except:
                    pass
        T_Trans=pd.DataFrame(Top_Trans_data)
        T_Trans['State'] = T_Trans['State'].replace(dict(zip(Agg_state_list, custom_state_list)))
        return T_Trans
path6='pulse/data/top/user/country/india/state/'
Top_user_list=os.listdir(path6)
def Top_User():   
    path6='pulse/data/top/user/country/india/state/'
    Top_user_list=os.listdir(path6)
    Top_user_data = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Registered_User': [],'Pin_Codes':[]}
    for i in Agg_user_list:
        p_i=path6+i+"/"
        Agg_yr=os.listdir(p_i)
        for j in Agg_yr:
            p_j=p_i+j+"/"
            Agg_yr_list=os.listdir(p_j)
            for k in Agg_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D6=json.load(Data)
                try:
                    for z in D6['data']['districts']:
                        for z1 in D6['data']['pincodes']:

                            DistrictName=z['name']
                            UserCount=z['registeredUsers']
                            Pincode=z1['name']
                            Top_user_data['District'].append(DistrictName)
                            Top_user_data['Registered_User'].append(UserCount)
                            Top_user_data['State'].append(i)
                            Top_user_data['Year'].append(j)
                            Top_user_data['Quarter'].append(int(k.strip('.json')))
                            Top_user_data['Pin_Codes'].append(Pincode)
                except:
                    pass

    T_User=pd.DataFrame(Top_user_data)
    T_User['State'] = T_User['State'].replace(dict(zip(Agg_state_list, custom_state_list)))
    return T_User
def Aggregate_Trans_Table():
    drop_query='''DROP TABLE IF EXISTS Agg_Trans'''
    mycur.execute(drop_query)
    # myconnection.commit()
    create_query = '''CREATE TABLE IF NOT EXISTS Agg_Trans(State text,
                                                           Year int,
                                                           Quarter int,
                                                           Transaction_type varchar(255),
                                                           Transaction_count int,
                                                           Transaction_amount bigint)'''
    mycur.execute(create_query)
    df = Aggregate_Trans()
    for index, row in df.iterrows():
        insert_query = '''INSERT INTO Agg_Trans(State,
                                                Year,
                                                Quarter,
                                                Transaction_type,
                                                Transaction_count,
                                                Transaction_amount)
                                                VALUES(%s,%s,%s,%s,%s,%s)'''
        values = (row['State'],
                  row['Year'],
                  row['Quarter'],
                  row['Transaction_type'],
                  row['Transaction_count'],
                  row['Transaction_amount'])
        try:
            mycur.execute(insert_query, values)
            myconnection.commit()
        except:
            print("Agg_Trans values are already inserted")  
def Aggregate_User_Table():
    drop_query='''DROP TABLE IF EXISTS Agg_User'''
    mycur.execute(drop_query)
    # myconnection.commit()
    create_query = '''CREATE TABLE IF NOT EXISTS Agg_User(State text,
                                                          Year int,
                                                          Quarter int,
                                                          Brands varchar(255),
                                                          User_Count int,
                                                          User_Percentage float)'''
    mycur.execute(create_query)
    df2 = Aggregate_User()
    for index, row in df2.iterrows():
        insert_query = '''INSERT INTO Agg_User( State,
                                                Year,
                                                Quarter,
                                                Brands,
                                                User_Count,
                                                User_Percentage)
                                                VALUES(%s,%s,%s,%s,%s,%s)'''
        values = (row['State'],
                  row['Year'],
                  row['Quarter'],
                  row['Brands'],
                  row['User_Count'],
                  row['User_Percentage'])
        try:
            mycur.execute(insert_query, values)
            myconnection.commit()
        except:
            print("Agg_User values are already inserted")  
def Map_Trans_Table():
    drop_query='''DROP TABLE IF EXISTS Map_Trans'''
    mycur.execute(drop_query)
    # myconnection.commit()
    create_query = '''CREATE TABLE IF NOT EXISTS Map_Trans(State text,
                                                           Year int,
                                                           Quarter int,
                                                           District varchar(255),
                                                           Transaction_count int,
                                                           Transaction_amount float)'''
    mycur.execute(create_query)
    df3 = Map_Trans()
    for index, row in df3.iterrows():
        insert_query = '''INSERT INTO Map_Trans(State,
                                                Year,
                                                Quarter,
                                                District,
                                                Transaction_count,
                                                Transaction_amount)
                                                VALUES(%s,%s,%s,%s,%s,%s)'''
        values = (row['State'],
                  row['Year'],
                  row['Quarter'],
                  row['District'],
                  row['Transaction_count'],
                  row['Transaction_amount'])
        try:
            mycur.execute(insert_query, values)
            myconnection.commit()
        except:
            print("Map_Trans values are already inserted")  
def Map_User_Table():
    drop_query='''DROP TABLE IF EXISTS Map_User'''
    mycur.execute(drop_query)
    # myconnection.commit()
    create_query = '''CREATE TABLE IF NOT EXISTS Map_User(State text,
                                                          Year int,
                                                          Quarter varchar(255),
                                                          District_name varchar(255),
                                                          Registered_user int,
                                                          App_opens int)'''
    mycur.execute(create_query)
    df3 = Map_User()
    for index, row in df3.iterrows():
        insert_query = '''INSERT INTO Map_User( State,
                                                Year,
                                                Quarter,
                                                District_name,
                                                Registered_user,
                                                App_opens)
                                                VALUES(%s,%s,%s,%s,%s,%s)'''
        values = (row['State'],
                  row['Year'],
                  row['Quarter'],
                  row['District_name'],
                  row['Registered_user'],
                  row['App_opens'])
        try:
            mycur.execute(insert_query, values)
            myconnection.commit()
        except:
            print("Map_User values are already inserted")  
def Top_Trans_Table():
    drop_query='''DROP TABLE IF EXISTS Top_Trans'''
    mycur.execute(drop_query)
    # myconnection.commit()
    create_query = '''CREATE TABLE IF NOT EXISTS Top_Trans(State text,
                                                           Year int,
                                                           Quarter varchar(255),
                                                           District varchar(255),
                                                           Pin_Codes int,
                                                           Transaction_count int,
                                                           Transaction_amount float)'''
    mycur.execute(create_query)
    df5 = Top_Trans()
    for index, row in df5.iterrows():
        insert_query = '''INSERT INTO Top_Trans(State,
                                                Year,
                                                Quarter,
                                                District,
                                                Pin_Codes,
                                                Transaction_count,
                                                Transaction_amount)
                                                VALUES(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['State'],
                  row['Year'],
                  row['Quarter'],
                  row['District'],
                  row['Pin_Codes'],
                  row['Transaction_count'],
                  row['Transaction_amount'])
        try:
            mycur.execute(insert_query, values)
            myconnection.commit()
        except:
            print("Top_Trans values are already inserted")   
def Top_User_Table():
    drop_query='''DROP TABLE IF EXISTS Top_User'''
    mycur.execute(drop_query)
    # myconnection.commit()
    create_query = '''CREATE TABLE IF NOT EXISTS Top_User(State text,
                                                          Year int,
                                                          Quarter varchar(255),
                                                          District varchar(255),
                                                          Pin_Codes int, 
                                                          Registered_User int)'''                                                     
    mycur.execute(create_query)
    df6 = Top_User()
    for index, row in df6.iterrows():
        insert_query = '''INSERT INTO Top_User( State,
                                                Year,
                                                Quarter,
                                                District,
                                                Pin_Codes,
                                                Registered_User)
                                                VALUES(%s,%s,%s,%s,%s,%s)'''
        values = (row['State'],
                  row['Year'],
                  row['Quarter'],
                  row['District'],
                  row['Pin_Codes'],
                  row['Registered_User'])
        try:
            mycur.execute(insert_query, values)
            myconnection.commit()
        except:
            print("Top_User values are already inserted")  
def state_list():
    mycur.execute(f"""select distinct State from agg_trans order by State asc;""")
    data = mycur.fetchall()
    original_state = [i[0] for i in data]
    return original_state
def year_list():
    mycur.execute("SELECT distinct year FROM phonepepulse.agg_trans order by year asc;")
    data = mycur.fetchall()
    data = [i[0] for i in data]
    return data
def quarter_list():
    mycur.execute("SELECT distinct quarter FROM phonepepulse.agg_trans order by quarter asc;")
    data = mycur.fetchall()
    data = [i[0] for i in data]
    return data
def get_transaction_type():
    mycur.execute("SELECT distinct transaction_type FROM phonepepulse.agg_trans;")
    data = mycur.fetchall()
    data = [i[0] for i in data]
    return data
def get_agg_users():
    mycur.execute("SELECT * FROM phonepepulse.agg_user;")
    data = mycur.fetchall()
    columns = [col[0] for col in mycur.description]
    d = pd.DataFrame(data, columns=columns)
    return d
def agg_trans_avg(agg_trans):
    data = []
    for i in range(0, len(agg_trans)):
        avg = agg_trans.iloc[i]["Transaction_amount"] / agg_trans.iloc[i]["Transaction_count"]
        data.append(avg)
    return data
def new_frame(v):
    i = [i for i in range(1, len(v)+1)]
    data = pd.DataFrame(v.values, columns=v.columns, index=i)
    return data
def get_map_transaction():
    mycur.execute("SELECT * FROM phonepepulse.map_trans;")
    data = mycur.fetchall()
    columns = [col[0] for col in mycur.description]
    d = pd.DataFrame(data, columns=columns)
    return d
def get_top_trans():
    mycur.execute("SELECT * FROM phonepepulse.top_trans;")
    data = mycur.fetchall()
    columns = [col[0] for col in mycur.description]
    d = pd.DataFrame(data, columns=columns)
    return d
def get_map_users():
    mycur.execute("SELECT * FROM phonepepulse.map_user;")
    data = mycur.fetchall()
    columns = [col[0] for col in mycur.description]
    d = pd.DataFrame(data, columns=columns)
    return d
def users_trans_avg(agg_trans):
    data = []
    for i in range(0, len(agg_trans)):
        avg = agg_trans.iloc[i]["App_opens"] / agg_trans.iloc[i]["Registered_User"]
        data.append(avg)
    return data
icon = Image.open("Icon.png")
st.set_page_config(page_title= "Phonepe Pulse Data Visualization",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded")
icon = Image.open("Icon.png")
st.title(":violet[Phonepe Pulse Data Visualization]")
st.header(":violet[Simple, Fast and Secure]")
with st.sidebar:
    st.header(":violet[**Welcome to PhonePe Pulse Dashboard**]")
    india=Image.open("India_Map.jpeg")
    selected = option_menu(None,
                            options=["Home","Statewise-Insights","Transactions-Insights","Users-Insights"],
                            default_index=0,
                            orientation="horizontal",
                            styles={"container": {"width": "90%"},
                                    "options": {"margin": "10px"},
                                    "icon": {"color": "black", "font-size": "24px"},
                                    "nav-link": {"font-size": "20px", "text-align": "center", "margin": "15px", "--hover-color": "#6F36AD"},
                                    "nav-link-selected": {"background-color": "#6F36AD"}})
    
    
if selected == "Home":
    im1 = Image.open("PhonePe-Coverpage.jpeg")
    im2 = Image.open("Icon.png")
    st.image(im1, width=1000)
    st.image(im2)
   
    st.subheader("PhonePe is a mobile payment platform using which you can transfer money using UPI, recharge phone numbers, pay utility bills, etc. PhonePe works on the Unified Payment Interface (UPI) system and all you need is to feed in your bank account details and create a UPI ID.")
    st.subheader(":Red[✨TECHNOLOGIES-USED]")
    st.write("****🔶Github Cloning****")
    st.write("****🔶Python****")
    st.write("****🔶Pandas****")
    st.write("****🔶MYSQL****")
    st.write("****🔶Streamlit****")
    st.write("****🔶Plotly****")    

if selected == "Statewise-Insights":
    MAP= st.selectbox("select your MAP",("Click to select","Total_transactions","Registered Users","App_opens"))
    def get_aggregated_user():
            mycur.execute( "SELECT * FROM phonepepulse.agg_trans;")
            data = mycur.fetchall()
            columns = [col[0] for col in mycur.description]
            df = pd.DataFrame(data, columns=columns)
            return df
        
    
    
    if MAP=='Total_transactions':
        total_trans=get_aggregated_user()
        total_trans=total_trans.groupby(["State"])[["Transaction_count"]].sum().reset_index()
        fig = px.choropleth(total_trans,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey='properties.ST_NM',
                            locations='State',
                            color='Transaction_count',
                            color_continuous_scale="Viridis",
                            title="Transactions state wise",
                                    height=1000, width=1200)
        fig.update_geos(fitbounds='locations', visible=False)
        st.write(fig)        
    
    if MAP =="Registered Users":
        tot_user = get_map_users()
        tot_user = tot_user.groupby(["State"])[["Registered_user", "App_opens"]].sum().reset_index()
        data =state_list()
        fig = px.choropleth(tot_user,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey='properties.ST_NM',
                            locations='State',
                            color='Registered_user',
                            color_continuous_scale="Reds",
                            title="Registered Users state wise",
                                    height=1000, width=1200)
        fig.update_geos(fitbounds='locations', visible=False)
        st.write(fig)
    if MAP=='App_opens':
        tot_user = get_map_users()
        tot_user = tot_user.groupby(["State"])["Registered_user", "App_opens"].sum().reset_index()
        fig = px.choropleth(tot_user,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey='properties.ST_NM',
                            locations='State',
                            color='App_opens',
                            color_continuous_scale="Greens",
                            title="App opens state wise",
                                    height=1000, width=1200)
        fig.update_geos(fitbounds='locations', visible=False)
        st.write(fig)        
        

if selected == "Transactions-Insights":
    with st.container():       
        st.markdown(":black[TRANSACTIONS INSIGHTS]")
        col1, col2, col3 = st.columns(3)
        # select box
        with col1:
            state = st.selectbox(label="Select the state",
                                 options=state_list(), index=0)
        with col2:
            year = st.selectbox(label="Select the year",
                                options=year_list(), index=0)
        with col3:
            quarter = st.selectbox(label="Select the Quarter", 
                                   options=quarter_list(), index=0)
   
        def get_aggregated_user():
            mycur.execute( "SELECT * FROM phonepepulse.agg_trans;")
            data = mycur.fetchall()
            columns = [col[0] for col in mycur.description]
            df = pd.DataFrame(data, columns=columns)
            return df
        
        
        df_agg_tran = get_aggregated_user()
        avg_value = agg_trans_avg( df_agg_tran)
        avg_value = pd.DataFrame(avg_value, columns=["avg_value"])
        df_av = pd.concat([df_agg_tran, avg_value], axis=1)
        v = df_av[(df_av["Year"] == year) & (df_av["Quarter"] == quarter)& (df_av["State"] == state)]
        total_transactions = v["Transaction_count"].sum()
        total_transaction_amount =v["Transaction_amount"].sum()
        total_avg=v["avg_value"].sum()
        
        col1,col2,col3=st.columns(3)
        with col1:
            st.markdown(":black[All PhonePe transactions (UPI + Cards + Wallets)]")
            st.write(total_transactions)
        with col2:
            st.markdown(":black[Total payment value]")
            st.write( total_transaction_amount)
        with col3:
            st.markdown(":black[Avg. transaction value]")
            st.write(total_avg)
                     
        plt.figure(figsize=(12, 5))
        fig = px.pie(v, values='Transaction_amount', names='Transaction_type', title='Pie Chart for Transaction Types',
            hover_data=['Transaction_count', 'avg_value'])

        fig.update_traces(textinfo='percent+label', pull=[0.1] * len(v['Transaction_type']))
        st.write(fig)
        st.markdown("")
        new_v = new_frame(v)
        st.table(new_v)

        col1, col2 = st.columns(2)
    
        with col1:
            year_df = st.selectbox(label="Select year", options=(2018, 2019, 2020, 2021, 2022, 2023), index=0)

        with col2:
            transaction_type = st.selectbox(label="Select the transaction type", options=get_transaction_type(), index=0)
        
        df_agg_total = get_aggregated_user()
        df_agg_total = df_agg_total.groupby(["State", "Year", "Transaction_type"])[["Transaction_count", "Transaction_amount"]].sum().reset_index()
        q = df_agg_total[(df_agg_total["Year"] == year_df) & (df_agg_total["Transaction_type"] == transaction_type)]

        fig = px.bar(q, x='State', y='Transaction_count',hover_data=['State', 'Transaction_count'], height=500, title="Transaction count state wise")
        st.write(fig)
                
        df_agg_total = get_aggregated_user()
        df_agg_total = df_agg_total.groupby(["State", "Year", "Transaction_type"])[["Transaction_count", "Transaction_amount"]].sum().reset_index()
        q = df_agg_total[(df_agg_total["Year"] == year_df) & (df_agg_total["Transaction_type"] == transaction_type)]

        fig = px.bar(q, x='State', y='Transaction_amount',hover_data=['State', 'Transaction_amount'], height=500, title="Transaction Amount state wise")
        st.write(fig)

        st.markdown("")
        new_v = new_frame(q)
        st.table(new_v)   
        
        st.markdown("#### Top 10 distircts")
        year_df_d = st.selectbox(label="Select year for the district wise data", options=(2018, 2019, 2020, 2021, 2022, 2023), index=0)
         
        st.markdown("#### Top 10 distircts for Transaction Count wise")
        df = get_map_transaction()
        df = df.groupby(["Year", "District"])[["Transaction_count", "Transaction_amount"]].sum().reset_index()
        k = df[df["Year"] == year_df_d]
        c = k.sort_values(by=["Transaction_count"],ascending=False).head(10)
        c = c[["Year", "District", "Transaction_count"]]
        c_df = new_frame(c)
        st.table(c_df)


        st.markdown("#### Top 10 Pin Codes")
        year_df_pc = st.selectbox(label="Select year for the postal code data", options=(2018, 2019, 2020, 2021, 2022, 2023), index=0)
        st.markdown("#### Top 10 Pin Codes for Transaction Count wise")
        df = get_top_trans()
        df = df.groupby(["Year", "Pin_Codes"])[["Transaction_count", "Transaction_amount"]].sum().reset_index()
        k = df[df["Year"] == year_df_pc]
        c = k.sort_values(by=["Transaction_count"],ascending=False).head(10)
        c = c[["Year", "Pin_Codes", "Transaction_count"]]
        c_df = new_frame(c)
        st.table(c_df) 
        
     
        st.markdown("#### Top 10 States")
        year_df_State = st.selectbox(label="Select year for the state wise data", options=(2018, 2019, 2020, 2021, 2022, 2023), index=0)

        st.markdown("#### Top 10 States for Transaction Count wise")
        df = get_aggregated_user()
        df = df.groupby(["Year", "State"])[["Transaction_count", "Transaction_amount"]].sum().reset_index()
        k1 = df[df["Year"] == year_df_State]
        c1 = k1.sort_values(by=["Transaction_count"],ascending=False).head(10)
        c1 = c1[["Year","State","Transaction_count"]]
        c1_df = new_frame(c1)
        st.table(c1_df)
         
        
       

if selected == "Users-Insights":
    
    st.markdown("#### :black[USERS INSIGHTS]")
    col1, col2, col3 = st.columns(3)
    with col1:
        user_state = st.selectbox(label="Select the state users", options=state_list(), index=0)
        
    with col2:
        user_year = st.selectbox(label="Select the year users",options=year_list(), index=0)
         
    with col3:
        user_quarter = st.selectbox(label="Select the Quarter users", options=quarter_list(), index=0)
           
    
    user_df = get_agg_users()
    user = user_df[(user_df["State"] == user_state) & (user_df["Year"] == user_year) & (user_df["Quarter"] == user_quarter)]
    User_Count=user["User_Count"].sum()
    User_Percentage=user["User_Percentage"].sum()
    
    col1,col2=st.columns(2)
    with col1:
        st.markdown("User Count")
        st.write(User_Count)
    with col2:
        st.markdown("User Percentage")
        st.write(User_Percentage)
    
    pie_fig = px.pie(user, names="Brands", values="User_Count",title="Pie Chart for Users Brands")
    st.write(pie_fig)
       
    c_df = new_frame(user)
    st.table(c_df)
   
    tot_state = st.selectbox(label="Select a state",options=state_list(), index=10)
    tot_user = get_map_users()
    tot_user = tot_user.groupby(["State", "Year",])["Registered_user", "App_opens"].sum().reset_index()
    to = tot_user[tot_user["State"] == tot_state][1:]
    
    col1, col2 = st.columns(2)

    with col1:    
        fig = px.bar(to, x='Year', y='Registered_user', width=500, color="Year",title="Year wise Registered Users")
        st.write(fig)

    with col2:

        fig = px.bar(to, x='Year', y='App_opens', width=500, color="Year", title="Year wise App opens")
        st.write(fig)

    st.markdown("")
    st.markdown("")
    to_df = new_frame(to)
    st.table(to_df)

    st.markdown("#### Top 10 districts")
    year_df_d = st.selectbox(label="Select year for the district wise data", options=(2018, 2019, 2020, 2021, 2022, 2023), index=0)
        
    st.markdown("#### Top 10 districts for Registered_user")
    df = get_map_users()
    df = df.groupby(["Year", "District_name"])[["Registered_user", "App_opens"]].sum().reset_index()
    k = df[df["Year"] == year_df_d]
    c = k.sort_values(by=["Registered_user"],ascending=False).head(10)
    c = c[["Year", "District_name", "Registered_user"]]
    c_df = new_frame(c)
    st.table(c_df)
    
    
    
    
    
   
    
    
    
    
    
    
    
