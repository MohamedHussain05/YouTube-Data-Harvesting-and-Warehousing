from googleapiclient.discovery import build
import pandas as pd
import re
import streamlit as st
import mysql.connector

#Connecting Google Api
def Api_connect():
    Api_id="AIzaSyA3W0o7_mql2GQcR0ZfRCJgUm50vF9_L9Y"
    
    api_service_name="youtube"
    api_version="v3"
    
    youtube=build(api_service_name,api_version,developerKey=Api_id)
    return youtube

youtube=Api_connect()

#Getting Channel details using API
def get_channel_info(channel_id):
    request=youtube.channels().list(
        part="snippet,contentDetails,Statistics",
        id=channel_id
    )
    response=request.execute()
    
    for i in range(0,len(response['items'])):
        data=dict(Channel_name=response['items'][i]['snippet']['title'],
                  Channel_id=response['items'][i]['id'],
                  Subscription_Count=response['items'][i]['statistics']['subscriberCount'],
                  Views=response['items'][i]['statistics']['viewCount'],
                  Total_Videos=response['items'][i]['statistics']['videoCount'],
                  Channel_Description=response['items'][i]['snippet']['description'],
                  Playlist_Id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
        return data
    
    
#Getting Video_id details using API
def get_videos_ids(channel_id):
    video_ids=[]

    reponse=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=reponse['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None
    while True:
        response1=youtube.playlistItems().list(
                part='snippet',
                playlistId=Playlist_Id,
                maxResults=50,
                pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
                video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
        if next_page_token==None:
            break
    return video_ids


#Getting Video_info details using API
def get_video_info(Video_ids):
    video_data=[]

    for video_id in Video_ids:
        response=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        ).execute()
        
        for items in response['items']:
            data=dict(
                        Channel_Name = items['snippet']['channelTitle'],
                        Channel_Id = items['snippet']['channelId'],
                        Video_Id =items['id'],
                        Title =items['snippet']['title'],
                        Tags =items['snippet'].get('tags'),
                        Thumbnail =items['snippet']['thumbnails']['default']['url'],
                        Description = items['snippet'].get('description'),
                        Published_Date =items['snippet']['publishedAt'],
                        Duration =items['contentDetails']['duration'],
                        Views =items['statistics'].get('viewCount'),
                        Likes =items['statistics'].get('likeCount'),
                        Comments = items['statistics'].get('commentCount'),
                        Favorite_Count =items['statistics']['favoriteCount'],
                        Definition =items['contentDetails']['definition'],
                        Caption_Status = items['contentDetails']['caption'] )
            video_data.append(data)
    return video_data
    

#Getting Comment_info details using API
def get_comment_info(Video_ids):
     Comment_Information = []
     try:
          for video_id in Video_ids:
               response=youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults=100).execute()
                    
               for items in response['items']:
                    data=dict(Comment_Id =items['snippet']['topLevelComment']['id'],
                              Video_Id =items['snippet']['videoId'],
                              Comment_Text =items['snippet']['topLevelComment']['snippet']['textOriginal'],
                              Comment_Author =items['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                              Comment_Published=items['snippet']['topLevelComment']['snippet']['publishedAt'])
                    Comment_Information.append(data)
     except:
          pass
     return Comment_Information


#Getting Playlists_info details using API
def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data

#connecting MongoDB and creating collections
import pymongo
client=pymongo.MongoClient("mongodb+srv://hussain:1234@cluster0.0wossp3.mongodb.net/?retryWrites=true&w=majority")
db=client["Youtube_data"]
coll1=db["channel_details"]


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_info(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)
    
    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,
                      "playlist_information":pl_details,
                      "video_information":vi_details,
                     "comment_information":com_details})
    return "upload success"


#Table creation and mirating data to Mysql
def channels_table():
    mydb=mysql.connector.connect(
        host='localhost',
        user='root',
        passwd="1234",
        auth_plugin='mysql_native_password')
    mycursor=mydb.cursor(buffered=True)
    
    #Table creation for channels in MySql
    mycursor.execute('create database if not exists youtube_data')
    mycursor.execute("use  youtube_data")
    mycursor.execute("drop table if exists channels")
    mydb.commit()
    try:
        mycursor.execute('''create table if not exists channels(Channel_Name varchar(100),
                                                                Channel_Id varchar(50) primary key,
                                                                Subscribers bigint,
                                                                Views bigint,
                                                                Total_Videos bigint,
                                                                Channel_Description text,
                                                                Playlist_Id varchar(80))''')
        mydb.commit()
    except:
        print("Table already exists")
        
    #Extracting  channels details from mongodb and making it to DataFrame 
    ch_list=[]
    coll1=db["channel_details"]
    for ch in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch['channel_information'])
    df=pd.DataFrame(ch_list)

    #Pushing data to Mysql
    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_name'],
                row['Channel_id'],
                row['Subscription_Count'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        try:
            mycursor.execute(insert_query,values)
            mydb.commit()
        except mysql.connector.errors.IntegrityError:
            print("Channel values already inserted")
            

def playlist_table():
    mydb=mysql.connector.connect(
            host='localhost',
            user='root',
            passwd="1234",
            auth_plugin='mysql_native_password')
    mycursor=mydb.cursor(buffered=True)
        
    #Table creation for channels in MySql
    mycursor.execute("create database if not exists youtube_data")
    mycursor.execute("use youtube_data")
    mycursor.execute("drop table if exists playlists")
    mydb.commit()
    try:
        mycursor.execute('''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                                    Title varchar(100),
                                                                    Channel_Id varchar(100),
                                                                    Channel_Name varchar(100),
                                                                    Published_At timestamp,
                                                                    Video_Count int)''')
        mydb.commit()

    except:
        print("Error in creating table")
        
    #Extracting  playlists details from mongodb and making it to DataFrame
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=pd.DataFrame(pl_list)

    #Pushing data to Mysql
    for index,row in df1.iterrows():
        
        row['PublishedAt'] = pd.to_datetime(row['PublishedAt']).strftime('%Y-%m-%d %H:%M:%S')
        
        insert_query='''insert into playlists(Playlist_Id,
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            Published_At,
                                            Video_Count)
                                                                        
                                            values(%s,%s,%s,%s,%s,%s)'''
    

        values=(row['PlaylistId'],
                                row['Title'],
                                row['ChannelId'],
                                row['ChannelName'],
                                row['PublishedAt'],
                                row['VideoCount'])
        try:
            mycursor.execute(insert_query,values)
            mydb.commit()
        except mysql.connector.errors.IntegrityError:
                print("Channel values already inserted")
        
def videos_table():
    mydb=mysql.connector.connect(
                host='localhost',
                user='root',
                passwd="1234",
                auth_plugin='mysql_native_password')
    mycursor=mydb.cursor(buffered=True)

    #Table creation for videos in MySql
    mycursor.execute("create database if not exists youtube_data")
    mycursor.execute("use youtube_data")
    mycursor.execute("drop table if exists videos")

    try:
        mycursor.execute('''create table if not exists videos(Channel_Name varchar(100),
                                                            Channel_Id varchar(50),
                                                            Video_Id varchar(50) primary key,
                                                            Title varchar(200),
                                                            Tags text,
                                                            Thumbnail varchar(200),
                                                            Description text,
                                                            Published_Date timestamp,
                                                            Duration int,
                                                            Views bigint,
                                                            Likes bigint,
                                                            Comments int,
                                                            Favorite_Count int,
                                                            Definition varchar(10),
                                                            Caption_Status varchar(50))''')
        mydb.commit()
    except:
        print("Error in creating videos table")  
        
    #Extracting  videos details from mongodb and making it to DataFrame 

    vi_list=[]
    for vi_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2=pd.DataFrame(vi_list)

    # Pushing data to MySQL
    for index, row in df2.iterrows():
        # Format Published Date
        row['Published_Date'] = pd.to_datetime(row['Published_Date']).strftime('%Y-%m-%d %H:%M:%S')

        # Extract minutes and seconds from Duration (handling potential missing 'M')
        duration_str = row['Duration']
        match = re.search(r"PT(?P<minutes>\d+)M(?P<seconds>\d+)S", duration_str)
        if match:
            minutes = int(match.group('minutes'))
            seconds = int(match.group('seconds'))

        # Convert to total duration in seconds
        row['Duration'] = minutes * 60 + seconds
        


        # Define insert query outside the loop (one time)
        insert_query = """insert into videos(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Title,
                                            Tags,
                                            Thumbnail,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Favorite_Count,
                                            Definition,
                                            Caption_Status)
                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        # Create values tuple from DataFrame row
        values = tuple(row[col] for col in df2.columns)

        # Handle Tags if it's a list
        if isinstance(row['Tags'], list):
            values = values[:4] + (','.join(row['Tags']),) + values[5:]

        try:
            mycursor.execute(insert_query, values)
            mydb.commit()
        except mysql.connector.errors.IntegrityError:
            print("Channel values already inserted")
        except mysql.connector.Error as err:
            print("Error inserting data:", err)

    # Close the connection
    mydb.close()

def comments_table():
    mydb=mysql.connector.connect(
                    host='localhost',
                    user='root',
                    passwd="1234",
                    auth_plugin='mysql_native_password')
    mycursor=mydb.cursor(buffered=True)

    #Table creation for videos in MySql
    mycursor.execute("create database if not exists youtube_data")
    mycursor.execute("use youtube_data")
    mycursor.execute("drop table if exists comments")

    mycursor.execute('''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                            Video_Id varchar(50),
                                                            Comment_Text text,
                                                            Comment_Author varchar(150),
                                                            Comment_Published timestamp)''')
    mydb.commit()

    #Retriving data from MongoDB
    com_list=[]
    for com_data in coll1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):   
            com_list.append(com_data['comment_information'][i])
    df3=pd.DataFrame(com_list)

    #inserting comments data into Mysql
    for index,row in df3.iterrows():
            row['Comment_Published']=pd.to_datetime(row['Comment_Published']).strftime('%Y-%m-%d %H:%M:%S')
            insert_query='''insert into comments(Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published
                                                    )
                                                    
                                                    values(%s,%s,%s,%s,%s)'''
                                                    
            values=(row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_Published'])
            
            try:
                    mycursor.execute(insert_query,values)
                    mydb.commit()
            except mysql.connector.errors.IntegrityError:
                    print("Comments values already inserted")

def tables():
    comments_table()
    videos_table()
    playlist_table()
    channels_table()
    
    return 'Tables Created Succuesfully'

tables=tables()

def show_channels_table():
    ch_list=[]
    coll1=db["channel_details"]
    for ch in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch['channel_information'])
    df=st.dataframe(ch_list)
    
    return df

def show_playlists_table():
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db['channel_details']
    for pl_data in coll1.find({},{'_id':0,'playlist_information':1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=st.dataframe(pl_list)
    return df1

def show_videos_table():   
    vi_list=[]
    for vi_data in coll1.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2=st.dataframe(vi_list)
    
    return df2

def show_comments_table():
    com_list=[]
    for com_data in coll1.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):   
            com_list.append(com_data['comment_information'][i])
    df3=st.dataframe(com_list)
    
    return df3

#Streamlit 
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("MongoDB")
    st.caption("Pyton Scripting")
    st.caption("Api Integration")
    st.caption("Data Collection")
    st.caption("Data Management Using MongoDB and SQL")

channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_id"])
        
    if channel_id in ch_ids:
        st.success("Channels details of given channle is already exists")
    
    else:
        insert=channel_details(channel_id)
        st.success(insert)
        
if st.button("Migrate to SQL"):
    Tables=tables
    st.success(Tables)
    
show_tables=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_tables=='CHANNELS':
    show_channels_table()
    
elif show_tables=='PLAYLISTS':
    show_playlists_table()
    
elif show_tables=='VIDEOS':
    show_videos_table()

elif show_tables=='COMMENTS':
    show_comments_table()
    
    
mydb=mysql.connector.connect(
                host='localhost',
                user='root',
                passwd="1234",
                auth_plugin='mysql_native_password')
mycursor=mydb.cursor(buffered=True)
mycursor.execute('use youtube_data')

questions=st.selectbox("Select Your Quesion",("1. All the videos and the channel name",
                                              "2. channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. Videos with higest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))   

if questions=="1. All the videos and the channel name":
    query1=('''select title as video, channel_name from videos''')
    mycursor.execute(query1)
    mydb.commit()
    t1=mycursor.fetchall()
    df=pd.DataFrame(t1,columns=['Video_Title','Channel_name'])
    st.write(df)
    
elif questions=="2. channels with most number of videos":
    query2=('''select Channel_Name,Total_Videos as Videos_count from channels
                order by Total_Videos desc''')
    mycursor.execute(query2)
    mydb.commit()
    t2=mycursor.fetchall()
    df2=pd.DataFrame(t2,columns=['Channel_Name','Videos_count'])
    st.write(df2)
    
elif questions=="3. 10 most viewed videos":
    query3=('''select Views,Channel_Name,Title as Video_Title
                from videos
                where Views is not null
                order by Views desc
                limit 10''')
    mycursor.execute(query3)
    mydb.commit()
    t3=mycursor.fetchall()
    df3=pd.DataFrame(t3,columns=['Views','Channel_Name','Video_Title'])
    st.write(df3)
    
elif questions=="4. comments in each videos":
    query4=('''select Comments as Comments_Count,Title as Video_Title
                FROM videos
                where Comments is not null
                order by Comments desc''')
    mycursor.execute(query4)
    mydb.commit()
    t4=mycursor.fetchall()
    df4=pd.DataFrame(t4,columns=['Comments_Count','Video_Title'])
    st.write(df4)
    
elif questions=="5. Videos with higest likes":
    query5=('''select Likes as Likes_Count, Title as Video_Title,Channel_Name
                FROM videos
                where likes is not null
                order by Likes desc''')
    mycursor.execute(query5)
    mydb.commit()
    t5=mycursor.fetchall()
    df5=pd.DataFrame(t5,columns=['Likes_Count','Video_Title','Channel_Name'])
    st.write(df5)
    
elif questions=="6. likes of all videos":
    query6=('''select likes as Like_count,title as video_title
                from videos 
                order by Likes desc''')
    mycursor.execute(query6)
    mydb.commit()
    t6=mycursor.fetchall()
    df6=pd.DataFrame(t6,columns=['Likes_Count','Video_Title'])
    st.write(df6)
    
elif questions=="7. views of each channel":
    query7=('''select Channel_Name,Views as total_views from channels''')
    mycursor.execute(query7)
    mydb.commit()
    t7=mycursor.fetchall()
    df7=pd.DataFrame(t7,columns=['Channel_Name','total_view'])
    st.write(df7)

elif questions=="8. videos published in the year of 2022":
    query8=('''select Title as Video_title,Published_Date as released_2022,Channel_Name from videos
                where extract(year from Published_Date)=2022''')
    mycursor.execute(query8)
    mydb.commit()
    t8=mycursor.fetchall()
    df8=pd.DataFrame(t8,columns=['Video_title','released_2022','Channel_Name'])
    st.write(df8)
    
elif questions=="9. average duration of all videos in each channel":
    query9=('''select Channel_Name,avg(Duration) as Average_Duration  
                from videos
                group by Channel_Name''')
    mycursor.execute(query9)
    mydb.commit()
    t9=mycursor.fetchall()
    df9=pd.DataFrame(t9,columns=['Channel_Name','Average_Duration'])
    st.write(df9)
    
elif questions=="10. videos with highest number of comments":
    query10=('''select Title as Video_Title,Comments as Comments_count,Channel_Name
                from videos
                where Comments is not null
                order by Comments desc''')
    mycursor.execute(query10)
    mydb.commit()
    t10=mycursor.fetchall()
    df10=pd.DataFrame(t10,columns=['Video_Title','Comments_count','Channel_Name'])
    st.write(df10)
    


                                                                    
                           
        
        
    

