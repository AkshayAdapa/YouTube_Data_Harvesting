from googleapiclient.discovery import build
import pymongo
import mysql.connector as mysql
import pandas as pd
import streamlit as st
from datetime import datetime
import re
from pymongo import MongoClient


#API connection
def Api_connect():
    Api_Id = "AIzaSyBD2MdLg-JUaP3d61focZEw6AGFMrkIC68"

    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=Api_Id)
    return youtube
youtube=Api_connect()


#get channel information
def Channel_info(Channel_id):
    request = youtube.channels().list(part = "snippet,contentDetails,Statistics",
                                      id = Channel_id)
    response = request.execute()
    
    for i in response['items']:
       
        data = dict(Channel_Name=i["snippet"]["title"],
                    Channel_Id=i["id"],
                    Subscribers=i["statistics"]["subscriberCount"],
                    Views=i["statistics"]["viewCount"],
                    Total_videos=i["statistics"]["videoCount"],
                    Channel_Description=i["snippet"]["description"],
                    Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data    


#get video Id's
def Videos_ids(Channel_id):
    video_ids=[]

    response=youtube.channels().list(id = Channel_id,
                                    part="contentDetails").execute()
    Playlist_Id= response["items"][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(part='snippet',
                                                playlistId=Playlist_Id,
                                                maxResults=50,
                                                pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


#get video information
def Video_info(Video_Ids):
    video_data=[]
    for video_id in Video_Ids:
        request=youtube.videos().list(part="snippet,ContentDetails,statistics",
                                    id=video_id)
        response=request.execute()

        for item in response["items"]:
            data= dict(Channel_Name=item["snippet"]["channelTitle"],
                    Channel_Id=item["snippet"]["channelId"],
                    Video_Id=item["id"],
                    Title=item["snippet"]["title"],
                    Tags=",".join(item["snippet"].get("tags",[])),
                    Thumbnail=item["snippet"]["thumbnails"]["default"]["url"],
                    Description=item["snippet"].get("description"),
                    Published_Date=item["snippet"]["publishedAt"],
                    Duration_seconds=item["contentDetails"]["duration"],
                    Views=item["statistics"].get("viewCount"),
                    Likes=item["statistics"].get("likeCount"),
                    Comments=item["statistics"].get("commentCount"),
                    Favorite_Count=item["statistics"]["favoriteCount"],
                    Definition=item["contentDetails"]["definition"],
                    Caption_Status=item["contentDetails"]["caption"])


            video_data.append(data)
    return video_data        


#get comment information
def Comment_info(Video_Ids):
    Comment_data = []
    try:
            for video_id in Video_Ids:

                    request = youtube.commentThreads().list(part = "snippet",
                                                            videoId = video_id,
                                                            maxResults = 50)
                    response = request.execute()
                    
                    for item in response["items"]:
                            data = dict(
                                    Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                    Video_Id = item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                                    Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                    Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                    Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                            Comment_data.append(data)
    except:
            pass

    return Comment_data


#get playlist ids
def Playlist_info(Channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(part="snippet,contentDetails",
                                            channelId=Channel_id,
                                            maxResults=50,
                                            pageToken=next_page_token)
        response = request.execute()

        for item in response['items']: 
            data={  'Playlist_Id':item['id'],
                    'Title':item['snippet']['title'],
                    'Channel_Id':item['snippet']['channelId'],
                    'Channel_Name':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data


#Uploading to mongoDB
client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["YouTube"]


def Channel_Details(Channel_id):
    Ch_Details=Channel_info(Channel_id)
    Pl_Details=Playlist_info(Channel_id)
    Vi_Ids=Videos_ids(Channel_id)
    Vi_Details=Video_info(Vi_Ids)
    Com_Details=Comment_info(Vi_Ids)

    coll1=db["Channel_Details"]
    coll1.insert_one({"channel_information":Ch_Details,
                             "playlist_information":Pl_Details,
                             "video_information":Vi_Details,
                             "comment_information":Com_Details})
    
    return "upload completed sucessfully"
    

# Connect to the MySQL database

# connecting channel_table
def channel_table(): 
    mydb = mysql.connect(host="localhost",
                        user="root",
                        password="Akshay@123",
                        database="yt_data")
    cursor=mydb.cursor()

    create_query=''' CREATE TABLE if not exists Channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(80) primary key, 
                                                        Subscribers bigint, 
                                                        Views bigint,
                                                        Total_videos int,
                                                        Channel_Description text,
                                                        Playlist_Id varchar(50))'''
                                                                                            
    cursor.execute(create_query)
    mydb.commit()


    ch_list=[]
    db=client["YouTube"]
    coll1=db["Channel_Details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query='''insert ignore into channels(Channel_Name,
                                                    Channel_Id,
                                                    Subscribers,
                                                    Views,
                                                    Total_videos,
                                                    Channel_Description,
                                                    Playlist_Id)
                                                    
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except mysql.connector.Error as err:
            print("Error:",err)

            
channel_table()


# connecting play_list_table
def play_list_table():
    mydb = mysql.connect(host="localhost",
                        user="root",
                        password="Akshay@123",
                        database="yt_data")
    cursor = mydb.cursor()

    create_query = '''CREATE TABLE IF NOT EXISTS play_list (Playlist_Id VARCHAR(100),
                                                            Title VARCHAR(80), 
                                                            Channel_Id VARCHAR(50) PRIMARY KEY, 
                                                            Channel_Name VARCHAR(100),
                                                            PublishedAt TIMESTAMP,
                                                            VideoCount INT)'''
    cursor.execute(create_query)
    mydb.commit()

    pl_list = []
    db = client["YouTube"]
    coll1 = db["Channel_Details"]
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.extend(pl_data["playlist_information"][i])
    df1 = pd.DataFrame(pl_list)

    for index, row in df1.iterrows():
        if 'PublishedAt' in row:
        # Convert PublishedAt to the MySQL datetime format
            published_at = datetime.strptime(row['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

            insert_query = '''INSERT IGNORE INTO play_list (Playlist_Id,
                                                            Title,
                                                            Channel_Id,
                                                            Channel_Name,
                                                            PublishedAt,
                                                            VideoCount)
                                                          VALUES (%s, %s, %s, %s, %s, %s)'''
            
        
            
            values = (row['Playlist_Id'],
                        row['Title'],
                        row['Channel_Id'],
                        row['Channel_Name'],
                        published_at,
                        row['VideoCount'])

            try:
                cursor.execute(insert_query, values)
                mydb.commit()
            except mysql.Error as err:
                print("Error:", err)
                if err.errno==1062:  #duplicate entry error 
                    #handle duplicate entry error(ignore or update)
                    pass #placholder for handling duplicate entry error
        else:
            print("PublishedAt key not found in row:", row)

    cursor.close()
    mydb.close()

play_list_table()            


# connecting video table
def video_table():
    # Function to parse duration string and convert it to seconds
    def duration_to_seconds(duration_str):
        match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration_str)
        if match:
            hours = int(match.group(1)[:-1]) if match.group(1) else 0
            minutes = int(match.group(2)[:-1]) if match.group(2) else 0
            seconds = int(match.group(3)[:-1]) if match.group(3) else 0
            return hours * 3600 + minutes * 60 + seconds
        else:
            return None

    # Connect to the MySQL database
    mydb = mysql.connect(host="localhost",
                        user="root",
                        password="Akshay@123",
                        database="yt_data")
    cursor = mydb.cursor()

    # Create the table if it doesn't exist
    create_query = '''CREATE TABLE IF NOT EXISTS videos (Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_Id varchar(30) PRIMARY KEY,
                                                        Title varchar(150),
                                                        Tags varchar(255),
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        Published_Date datetime,
                                                        Duration_seconds bigint,
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(10),
                                                        Caption_Status varchar(50))'''
    cursor.execute(create_query)
    mydb.commit()

    # Establish connection to MongoDB
    mongo_client = MongoClient("mongodb://localhost:27017/")
    db = mongo_client["YouTube"]
    coll1 = db["Channel_Details"]

    vi_list = []
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for video_info in vi_data["video_information"]:
            vi_list.append(video_info)
    df2 = pd.DataFrame(vi_list)

    for index, row in df2.iterrows():
        try:
            # Convert duration string to seconds
            duration_seconds = duration_to_seconds(row['Duration_seconds'])
            if duration_seconds is not None:
                # Convert Published_Date to datetime format
                published_date = datetime.strptime(row['Published_Date'], '%Y-%m-%dT%H:%M:%SZ')

                insert_query = '''INSERT IGNORE INTO videos (Channel_Name, Channel_Id, Video_Id, Title, Tags, Thumbnail,
                                                            Description, Published_Date, Duration_seconds, Views, Likes,
                                                            Comments, Favorite_Count, Caption_Status, Definition)
                                                            
                                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                
                values = (row['Channel_Name'], row['Channel_Id'], row['Video_Id'],
                        row['Title'], row['Tags'], row['Thumbnail'], row['Description'],
                        published_date, duration_seconds, row['Views'], row['Likes'],
                        row['Comments'], row['Favorite_Count'], row['Caption_Status'],
                        row['Definition'])
                
                cursor.execute(insert_query, values)
                mydb.commit()
            else:
                print(f"Invalid duration string: {row['Duration_seconds']}")
        except Exception as e:
            print(f"Error inserting data: {e}")

    # Close the database connection
    cursor.close()
    mydb.close()
    mongo_client.close()

video_table()



# connecting comments table
def comments_table():
    # Connect to MySQL database
    mydb = mysql.connect(host="localhost",
                        user="root",
                        password="Akshay@123",
                        database="yt_data")
    cursor = mydb.cursor()

    create_query = '''CREATE TABLE IF NOT EXISTS comments (Comment_Id varchar(100) PRIMARY KEY,
                                                            Video_Id varchar(80),
                                                            Comment_Text text, 
                                                            Comment_Author varchar(150),
                                                            Comment_Published datetime)'''
    cursor.execute(create_query)
    mydb.commit()

    com_list = []
    db = client["YouTube"] 
    coll1 = db["Channel_Details"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for comment_info in com_data["comment_information"]:
            com_list.append(comment_info)
    df3 = pd.DataFrame(com_list)

    for index, row in df3.iterrows():
        # Convert datetime string to MySQL compatible format
        comment_published = datetime.strptime(row['Comment_Published'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

        insert_query = '''
            INSERT IGNORE INTO comments (Comment_Id,
                                        Video_Id,
                                        Comment_Text,
                                        Comment_Author,
                                        Comment_Published)

                                        VALUES (%s, %s, %s, %s, %s)
                                         '''
        values = (row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                comment_published)
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except mysql.Error as err:
            print("Error:", err)

    cursor.close()
    mydb.close()

comments_table()


def tables():
    channel_table()
    play_list_table()
    video_table()
    comments_table()

    return "Tables Created Successfully"


def show_channels_table():
    ch_list=[]
    db=client["YouTube"]
    coll1=db["Channel_Details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df


def show_play_list_table():
    pl_list = []
    db = client["YouTube"]
    coll1 = db["Channel_Details"]
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.extend(pl_data["playlist_information"])
    df1 = st.dataframe(pl_list)

    return df1


def show_videos_table():
    vi_list = []
    db = client["YouTube"]
    coll1 = db["Channel_Details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for video_info in vi_data["video_information"]:
            vi_list.append(video_info)
    df2 = st.dataframe(vi_list)

    return df2


def show_comments_table():    
    com_list = []
    db = client["YouTube"]  # Assuming client is defined elsewhere
    coll1 = db["Channel_Details"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for comment_info in com_data["comment_information"]:
            com_list.append(comment_info)
    df3 = st.dataframe(com_list)

    return df3

#Streamlit part

st.set_page_config(layout="wide")
# Image URL
image_url = "https://qph.cf2.quoracdn.net/main-qimg-a9f3c3bef883928ab2e388851f3691b7-lq"

# Display image using HTML
st.markdown('<div style="display: flex; justify-content: center;"><img src="{}" alt="image" width="400"></div>'.format(image_url), unsafe_allow_html=True)

# Centered title
st.markdown('<h1 style="text-align: center; color: #FF5733;">YOU TUBE DATA HARVESTING AND WAREHOUSING USING SQL AND STREAMLIT</h1>', unsafe_allow_html=True)

# Developer Name aligned to the right
st.markdown('<p style="text-align: right; font-size: medium;">Developed by Akshay Kumar Adapa</p>', unsafe_allow_html=True)

# Project Description
st.write("""
**Project Description:**
This project focuses on harvesting and warehousing data from YouTube, including channels, playlists, videos, and comments. 
The collected data is stored and managed using MongoDB and SQL databases.
""")

# Skill Take Away
st.write("**Skill Take Away:** API Integration, Python Scripting, Data Collection, MongoDB, Data Management using MongoDB and SQL")

# Input for Channel ID
channel_id = st.text_input("Enter the channel ID")

# Collect Data Button
if st.button("Collect Data"):
    ch_ids=[]
    db=client["YouTube"]
    coll1=db["Channel_Details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")
    else:
        insert=Channel_Details(channel_id)
        st.success(insert)
    st.success("Data collected successfully")

# Store Data Button
if st.button("Store Data"):
    st.success("Data stored successfully")

# Migrate to SQL Button
if st.button("Migrate to SQL"):
    Table=tables()
    st.success(Table)
    
# Show Table
show_table = st.radio("SELECT THE TABLE FOR VIEW", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

if show_table == "CHANNELS":
    show_channels_table()

elif show_table == "PLAYLISTS":
    show_play_list_table()

elif show_table == "VIDEOS":
    show_videos_table()

elif show_table == "COMMENTS":
    show_comments_table()

#SQL Connection

mydb = mysql.connect(   host="localhost",
                        user="root",
                        password="Akshay@123",
                        database="yt_data" )

cursor = mydb.cursor()

question=st.selectbox("Select your question",("1. Names of all the videos and their corresponding channels",
                                              "2. Channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. Comments in each video",
                                              "5. Videos with highest likes",
                                              "6. Likes of all videos",
                                              "7. Views of each channel",
                                              "8. Videos published in the year of 2022",
                                              "9. Average duration of all videos in each chennel",
                                              "10. Videos with heighest number of comments"))


if question == '1. Names of all the videos and their corresponding channels':
    query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1)
    t1 = cursor.fetchall()  # Fetch all rows from the result set
    mydb.commit()
    st.write(pd.DataFrame(t1, columns=["Video Title", "Channel Name"]))


elif question == '2. Channels with most number of videos':
    query2 = "select Channel_Name as ChannelName, Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    t2 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t2, columns=["Channel Name", "No Of Videos"]))


elif question == '3. 10 most viewed videos':
    query3 = '''select Views as views, Channel_Name as ChannelName, Title as VideoTitle from videos 
                where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    t3 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t3, columns=["views", "channel Name", "video title"]))


elif question == '4. Comments in each video':
    query4 = "select Comments as No_comments, Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    t4 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))


elif question == '5. Videos with highest likes':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    t5 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t5, columns=["video Title", "channel Name", "like count"]))


elif question == '6. Likes of all videos':
    query6 = '''select Likes as likeCount, Title as VideoTitle from videos;'''
    cursor.execute(query6)
    t6 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t6, columns=["like count", "video title"]))


elif question == '7. Views of each channel':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    t7 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t7, columns=["channel name", "total views"]))


elif question == '8. Videos published in the year of 2022':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    t8 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t8, columns=["Name", "Video Publised On", "ChannelName"]))


elif question == '9. Average duration of all videos in each chennel':
    query9 = "SELECT Channel_Name as ChannelName, AVG(Duration_seconds) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    t9 = cursor.fetchall()
    mydb.commit()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9 = []
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))


elif question == '10. Videos with heighest number of comments':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    t10 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))
