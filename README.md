YOUTUBE DATA HARVESTING and WAREHOUSING

This project aims to collect, store, and analyze data from YouTube using the YouTube Data API.
It provides functionalities to gather information about channels, videos, playlists, and comments,
store the collected data in a MongoDB database, migrate it to a MySQL database, and offer a Streamlit-based interface for interactive data exploration and analysis.

The Required Libraries for this Project include:

google-api-python-client: This library is used to interact with Google APIs, including the YouTube Data API.

pymongo: For interacting with MongoDB database.

mysql-connector-python: For interacting with MySQL database.

pandas: For data manipulation and analysis.

streamlit: For building interactive web applications.

datetime: For working with dates and times.

re: For regular expression operations.

INTRODUCTION

YouTube is one of the largest platforms for sharing video content, making it a valuable source of data for various analytical purposes. This project leverages the YouTube Data API to retrieve information about channels, videos, playlists, and comments. It then organizes this data, stores it in a MongoDB database for initial storage and flexibility, and subsequently migrates it to a MySQL database for structured querying and analysis. Additionally, it offers a user-friendly interface powered by Streamlit, enabling users to interact with the collected data, visualize insights, and execute SQL queries seamlessly.

FEATURES

Data Collection and Storage:

Utilizes the YouTube Data API to collect comprehensive information about channels, videos, playlists, and comments.
Stores the acquired data in a MongoDB database, allowing for flexibility and scalability in the initial stages.

Data Migration to MySQL:

Facilitates the migration of collected data from MongoDB to a MySQL database for structured storage and efficient querying.
Establishes tables within the MySQL database to organize channel, video, playlist, and comment data systematically.

Streamlit Interface:

Provides an intuitive Streamlit-based interface for seamless interaction with the collected data and analytical insights.
Enables users to trigger data collection, migration to MySQL, and explore various tables and insights effortlessly.

MySQL Queries and Display:

Empowers users to execute diverse SQL queries on the MySQL database through the Streamlit interface.
Offers insights such as channel statistics, video analytics, comment trends, and more via interactive displays.

Setup Instructions

Clone the Repository:

git clone https://github.com/your_username/YouTube-Data-Harvesting-and-Warehousing.git

Install Dependencies:

pip install -r requirements.txt

Configure API Keys:

Obtain API keys for the YouTube Data API and replace YOUR_API_KEY in the script with your actual API key.

USAGE

Data Collection:

Input the channel ID and click the "collect and store data" button to retrieve data from YouTube and store it in the MongoDB database.

Migrate to MySQL:

Click the "Migrate to SQL" button to transfer the collected data from MongoDB to MySQL for structured storage.

Explore Data and Insights:

Utilize the Streamlit interface to navigate through various tables, including channels, playlists, videos, and comments.
Execute SQL queries to obtain insights such as most viewed videos, channels with the highest engagement, popular playlists, and more.
