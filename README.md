# plbroadcastbot
Telegram Bot for automatic KPI monitoring

Background
- Single concurrent user to access OSS server
- Manual data extraction/export from the server
- No persistent database (data usually compiled by an OSS engineer as a one-week period of measurement of KPI in excel format)
- No automatic management reporting

Features of these bot :
- Aggregate daily data to display TWAMP Performance KPI
- Automatic daily broadcast
- Sending graph in the form of images for hourly KPI performance

Python Library used :
- Pandas, numpy
- pymysql
- matplotlib
- python-telegram-bot
- pysftp

