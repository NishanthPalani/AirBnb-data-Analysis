from pymongo import MongoClient
import pymongo
import pandas as pd
import numpy as np
import streamlit as st
from streamlit_option_menu import option_menu
import pymysql
#import matplotlib.pyplot as plt
import plotly.express as px 
import seaborn as sns
import matplotlib.pyplot as plt
#import geopandas as go

## Connecting to MongoDB to retrieve AirBNB data
client = pymongo.MongoClient("mongodb+srv://nishanthnici:12345@cluster0.hc429py.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0") 
mydb = client["sample_airbnb"]  ## connecting to the Databas
db = mydb.listingsAndReviews ## connecting to the collection

## Connection strings for Pymysql
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='admin@123')
myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='admin@123',database = "airbnb_data")
cur = myconnection.cursor()

## Below code will do page title(tab Name) configuration
st.set_page_config(page_title= "AirBNB data Visualization",
                   layout= "wide")

## Below code will create the header tabs in the screen 
selected = option_menu(None, ["Home","Explore More Data"], 
            icons=["house","bar-chart-line","flag-fill"],
            menu_icon= "menu-button-wide",
            default_index=0,
            orientation= "horizontal",
            styles={"nav-link": {"font-size": "20px", "text-align": "left", "margin": "-2px", "--hover-color": "#6F36AD"},
                    "nav-link-selected": {"background-color": "#6F36AD"}})

## Below code is for data preprocessing with the un cleaned data we get
def data_preprocess():
    global airbnb_df

    airbnb_data = []
    for i in db.find():
        data = dict(Name = i.get('name'),
                    Description = i['description'],
                    House_rules = i.get('house_rules'),
                    Property_type = i['property_type'],
                    Room_type = i['room_type'],
                    Bed_type = i['bed_type'],
                    Min_nights = int(i['minimum_nights']),
                    Max_nights = int(i['maximum_nights']),
                    Cancellation_policy = i['cancellation_policy'],
                    Accomodates = i['accommodates'],
                    Total_bedrooms = i.get('bedrooms'),
                    Total_beds = i.get('beds','NA'),
                    Availability_365 = i['availability']['availability_365'],
                    Price = i['price'],
                    Security_deposit = i.get('security_deposit',0),
                    Cleaning_fee = i.get('cleaning_fee',0),
                    Extra_people = i['extra_people'],
                    Guests_included= i['guests_included'],
                    No_of_reviews = i['number_of_reviews'],
                    Review_scores = i['review_scores'].get('review_scores_rating'),
                    Amenities = ', '.join(i['amenities']),
                    Host_id = i['host']['host_id'],
                    Host_name = i['host']['host_name'],
                    Street = i['address']['street'],
                    Country = i['address']['country'],
                    Country_code = i['address']['country_code'],
                    Location_type = i['address']['location']['type'],
                    Longitude = i['address']['location']['coordinates'][0],
                    Latitude = i['address']['location']['coordinates'][1]
        )
        airbnb_data.append(data)

        airbnb_df = pd.DataFrame(airbnb_data)

        ## Dropping the unwanted columns for analysis 
        airbnb_df.drop(["Country_code","Location_type"],axis=1,inplace=True) 
        ## Converting the datatypes to int/float for checking the staNDARD DEVIATION
        airbnb_df["Price"]= airbnb_df["Price"].astype(str).astype(float)
        airbnb_df["Total_bedrooms"]= airbnb_df["Total_bedrooms"].astype(str).astype(float)
        airbnb_df["Security_deposit"]= airbnb_df["Security_deposit"].astype(str).astype(float)
        airbnb_df["Extra_people"]= airbnb_df["Extra_people"].astype(str).astype(float)
        airbnb_df["Cleaning_fee"]= airbnb_df["Cleaning_fee"].astype(str).astype(float)
        airbnb_df["Review_scores"]= airbnb_df["Review_scores"].astype(str).astype(float)
        airbnb_df["Longitude"]= airbnb_df["Longitude"].astype(str).astype(float)
        airbnb_df["Latitude"]= airbnb_df["Latitude"].astype(str).astype(float)
        airbnb_df["Description"].replace(to_replace='',value='No Description Provided',inplace=True)
        airbnb_df["House_rules"].replace(to_replace='',value='No Description Provided',inplace=True)
        airbnb_df["Amenities"].replace(to_replace='',value='No Amenities Provided',inplace=True)

## below code will create table in MYSQL for loading the cleaned/processed data
def sql_table_define():

    myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='admin@123')
    cur = myconnection.cursor()
    cur.execute("create database if not exists airbnb_data")
    myconnection = pymysql.connect(host = '127.0.0.1',user='root',passwd='admin@123',database = "airbnb_data")
    cur = myconnection.cursor()
    cur.execute("drop table if exists airbnb_data")
    myconnection.commit()

    cur.execute("""create table if not exists airbnb_data(Name varchar(255), Description LongText, House_rules longtext, 
    Property_type varchar(255), Room_type varchar(255),
    Bed_type varchar(255), Min_nights int, Max_nights int, Cancellation_policy varchar(255),
    Accomodates int, Total_bedrooms varchar(255), Total_beds varchar(255), Availability_30 int, Availability_60 int,
    Availability_90 int, Availability_365 int, Price float, Security_deposit float, Cleaning_fee float, Extra_people varchar(255),
    Guests_included varchar(255), No_of_reviews int, Review_scores float, Amenities longtext,
    Host_id int, Host_name varchar(255), Host_neighbourhood varchar(255), Street varchar(255), Country varchar(255), Country_code varchar(255),
    Location_type varchar(255), Longitude float, Latitude float)""")

    sql_load()

## below code will load the data to MYSQL
def sql_load():
    sql = "insert into airbnb_data values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    for i in range(0,len(airbnb_df)):
        cur.execute(sql,tuple(airbnb_df.iloc[i]))
        myconnection.commit()


if selected == "Home":

    st.markdown("# AirBNB Data Visualization and Exploration")
    st.write(" ")
    st.markdown("### :blue[What is Airbnb? :]")
    st.markdown("### Airbnb is an American company operating as an online marketplace for short and long-term homestays and experiences. The company acts as a broker and charges a commission from each booking. The company was founded in 2008 by Brian Chesky, Nathan Blecharczyk, and Joe Gebbia. Airbnb is a shortened version of its original name, AirBedandBreakfast.com. Airbnb is the most well-known company for short-term housing rentals.")
    st.write(" ")
    st.markdown("### :blue[Overview :]")
    st.markdown("#### This streamlit app aims to give users a friendly environment which can be used to visualize the AirBNB data and gain lots of insights on Accomodations on each regions. This uses Bar charts, Pie charts and Geo map visualization views to get insights.")
    st.write(" ")
    st.markdown("### :blue[Technologies used :]")
    st.markdown("#### - MONGODB ATLAS - Cloning for input Data")
    st.markdown("#### - Python, Pandas")
    st.markdown("#### - Streamlit and Plotly")

elif selected == "Explore More Data":

    airbnb_df = pd.read_sql_query("select * from airbnb_data;",myconnection)

    st.write("### Explore More Data using interactive visuals")
    
    col1,col2= st.columns([1,1],gap="large")

    with col1:
        result = st.selectbox("Please select the country to see individual analysis",airbnb_df["Country"].unique(),index=0)
        st.write(" ")


    with col2:
        ## Below will calculate and create the cummulative total price for overall and individual regions
        airbnb_df7 = pd.read_sql_query("select sum(Price) as total_price from airbnb_data;",myconnection)
        total = int(airbnb_df7["total_price"].sum())
        st.markdown("### :violet[Cummulative Total Amount]")
        st.write("###### :orange[Overall Total Price for all Regions] =  ",f":green[{total} $]")
        airbnb_df8 = pd.read_sql_query("select sum(Price) as total_price from airbnb_data where Country = '"+result+"';",myconnection)
        total1 = int(airbnb_df8["total_price"].sum())
        st.write(f"###### :red[{result}] :orange[cummulative Total Price] =  ",f":green[{total1} $]")
        st.write(" ")

    if result == "Please select the country":
        st.error("Select a country")
    else:

        airbnb_df = pd.read_sql_query("select * from airbnb_data where Country = '"+result+"';",myconnection)

        col1,col2= st.columns([1,1],gap="large")
        with col1:
            ## Below code will display the individual contribution of country with overall data
            a = round((total1/total) * 100,2)
            st.markdown(f"##### :red[{result}] have contributed :green[{a}%] of world total price")

            ## Below code will show the bar chart Room Type Vs Price
            airbnb_df1 = airbnb_df.groupby(["Room_type"])["Price"].sum().reset_index()
            fig = px.bar(airbnb_df1,x="Room_type",y="Price",color="Room_type",text="Price",color_discrete_sequence=["yellow", "blue", "green"],
            labels={"Room_type":"Types of Accomodation","Price":"Total amount"})
            fig.update_layout(
                width=400,
                height=400
            )
            fig.update_layout(yaxis_tickprefix = '$', yaxis_tickformat = ',.')
            fig.update_traces(marker_line_color = 'black',
                    marker_line_width = 1, opacity = 1)
            fig.update_layout(title_text="Room Type Vs Price in "+result, title_x=0.2,title_font_color="orange")
            st.write(fig)

            ## Below code will show the bar chart for Top 5 host listing count
            airbnb_df2 = airbnb_df.groupby("Host_name")["host_listings_count"].sum().sort_values(ascending=False).head(5)
            fig = px.bar(airbnb_df2,x="host_listings_count",color="host_listings_count",text="host_listings_count",
            labels={"x":"Host Names","host_listings_count":"Total listings"})
            fig.update_layout(
                width=600,
                height=300
            )
            fig.update_traces(marker_line_color = 'black',
                    marker_line_width = 1, opacity = 1)
            fig.update_layout(title_text="Top 5 host listings count in "+result, title_x=0.2,title_font_color="orange")
            st.write(fig)

            st.write(" ")
            st.write(" ")
            st.write(" ")
            st.write(" ")

            #Overall top 5 Countries contribution
            airbnb_df4 = pd.read_sql_query("select * from airbnb_data;",myconnection)
            airbnb_df4 = airbnb_df4.groupby(["Country"])["Price"].sum().sort_values(ascending=False).reset_index().head(5)
            fig = px.pie(airbnb_df4,names="Country",values="Price",hover_data="Price",color_discrete_sequence=px.colors.sequential.Rainbow,width=600,height=400)
            fig.update_layout(title_text="Overall Top 5 best contributors", title_x=0.2,title_font_color="yellow")
            fig.update_layout(yaxis_tickprefix = '$', yaxis_tickformat = ',.')
            fig.update_traces(textinfo="percent + value",marker_line_color = 'black',
            marker_line_width = 2, opacity = 1,pull=[0,0,0,0,0.1])
            st.write(fig)

        with col2:
            st.write(" ")
            st.write(" ")
            ## Below code will show the pie chart for Individual Room Type contribution
            fig = px.pie(airbnb_df1,labels="Room_type",names="Room_type",values="Price",hover_data="Price",color_discrete_sequence=px.colors.sequential.Sunset,width=600,height=400)
            fig.update_layout(title_text="Individual Room Type contribution in "+result, title_x=0.1,title_font_color="orange")
            fig.update_layout(yaxis_tickprefix = '$', yaxis_tickformat = ',.')
            fig.update_traces(textinfo="percent + value",marker_line_color = 'black',
                    marker_line_width = 2, opacity = 1,pull=[0,0,0])
            st.write(fig)

            ## Below code will show the scatter plot for Property Type Vs Price distribution
            fig = px.scatter(airbnb_df,y="Price",x="Property_type",color="Room_type",color_discrete_sequence=px.colors.sequential.Rainbow_r)
            fig.update_layout(title_text="Property Type Vs Price distribution in "+result, title_x=0.3,title_font_color="orange")
            st.write(fig)


        ## Below code will show the map for Geographical representation of Price range for each location
        airbnb_df6 = pd.read_sql_query("select * from airbnb_data;",myconnection)
        #fig = px.scatter_mapbox(airbnb_df6, lat="Latitude", lon="Longitude", hover_name="Country", hover_data="Price", width=800, height=800, zoom=2, opacity=1, color_continuous_scale="red", color_discrete_sequence=px.colors.sequential.Sunset ,title="Map view with Price range for each location")
        fig = px.scatter_mapbox(airbnb_df6, lat="Latitude", lon="Longitude", hover_name="Country", hover_data="Price", width=800, height=800, zoom=2, opacity=1, color_continuous_scale="red", color_discrete_sequence=["orange", "red", "green", "blue", "purple"],title="Map view with Price range for each location")
        fig.update_layout(mapbox_style="open-street-map")
        fig.update_layout(title_text="Geographical representation of Price range for each location", title_x=0,title_font_color="orange")
        st.plotly_chart(fig, use_container_width=True)





