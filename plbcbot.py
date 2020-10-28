from telegram.ext import (Updater, CommandHandler, MessageHandler, CallbackQueryHandler, Filters, ConversationHandler)
from telegram import (ChatAction, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove)
from functools import wraps
from getdaily import mysql_conn
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime, time
import pandas as pd
import os
import telegram.ext
from threading import Thread
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()


REQUEST_KWARGS={
	'proxy_url':'http://10.59.82.2:8080/'
}

updater = Updater(token='1266312590:AAFlgwty-YpiXM4nKuBcjYAkQTMhhAS_Wr0',use_context=True, request_kwargs = REQUEST_KWARGS)
#Create Updater object with TOKEN from @RTOreg2Bot Telegram Bot

dispatcher = updater.dispatcher
#create local dispatcher

import logging ,os, subprocess, sys
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
					 level=logging.INFO)
#Logging module

logger = logging.getLogger(__name__)

ZOOM, DAY = range(2)

whitelist=[146734512, 197936536, 369092764, 25809286, 171819491, 222773037, 229610795, 222408782, 28202311, 230620042, 222231943, 290368490, 127887269, 13255280, 207650929, 189060437, 148922693, 167432580, 263028261, 291197952, 187084155, 287228199,162069087,243336460, 227546604, 237850677, 533090154, 81571774, 392413590, 244927193, 140954919, 265929888, 164885798, 226342335, 146164857, 239665648, 69584751, 190276823, 1072102543, 263113808, 232542258, 204592493, 94929765, 305584485, 144969903, 207105247, 249936669, 112700332, 408683654, 51756070, 1317874580, 31086706, 209112319, 233839021, 8749045, 124196823]
def auth_chat_id(func):
	"""Checks for id"""

	@wraps(func)
	def whitelist_check(update, context, *args, **kwargs):
		if update.message.from_user.id in whitelist :
			return func(update,context,*args, **kwargs)
		else :
			update.message.reply_text("You are not authorized to use this function, please contact @argyaharish")
	return whitelist_check
#Decorators for checking if user is on the whitelist

def send_typing_action(func):
	"""Sends typing action while processing func command."""

	@wraps(func)
	def command_func(update, context, *args, **kwargs):
		context.bot.send_chat_action(chat_id=update.effective_message.chat_id, action=ChatAction.TYPING)
		return func(update, context, *args, **kwargs)

	return command_func
#Decorators for send typing... response for bot

def send_image_action(func):
    """Sends typing action while processing func command."""

    @wraps(func)
    def command_func(update, context, *args, **kwargs):
        context.bot.send_chat_action(chat_id=update.effective_message.chat_id, action=ChatAction.UPLOAD_PHOTO)
        return func(update, context, *args, **kwargs)

    return command_func
#Decorators for send typing... response for bot 


@send_typing_action
def start(update, context):
    usr = str(update.message.from_user.username)
    update.message.reply_text('Hi '+usr+'! This is RTO Regional 2 PL Broadcast Bot, please send command /help to view Bot Options')
#Define 4g daily achievement function

def stop_and_restart():
    """Gracefully stop the Updater and replace the current process with a new one"""
    updater.stop()
    os.execl(sys.executable, sys.executable, *sys.argv)

def restart(update, context):
    update.message.reply_text('Bot is restarting...')
    Thread(target=stop_and_restart).start()

@send_typing_action
def help(update, context):
	update.message.reply_text("*RTO REG 2 Packet Loss Broadcast*\n"
	"=========================\n"
	"1./ach4g 4G packet loss achievement D VS D-1 \n"
	"2./ach3g 3G packet loss achievement D VS D-1\n"
	"3./rtpo4g 4G packet loss achievement breakdown per RTPO by kabupaten\n"
	"4./twamp 4G PL & Latency hourly graph, syntax /twamp<space>SiteID, ex: /twamp PLG090 \n"
	"5./twamp_mod 4G PL & Latency hourly graph with the options to zoom the y-axis and custom date range \n"			
	"=========================\n"
	"For secure access to the Bots, authorization system has been implemented. Please contact @argyaharish to register your Tele ID.")
#Define help function

@auth_chat_id
@send_typing_action
def ach4g(update, context):
	today = datetime.datetime.now() #establishing today's date
	delta_curr = datetime.timedelta(days = 2)
	day_curr = today - delta_curr #current day packet loss
	day_curr_str = day_curr.strftime(("%Y"+"-"+"%m"+"-"+"%d")) #stringify current day packet loss variable
	delta_last = datetime.timedelta(days = 3)
	day_last = today - delta_last #last day packet loss
	day_last_str = day_last.strftime(("%Y"+"-"+"%m"+"-"+"%d")) #stringify last day packet loss variable
	conn = mysql_conn()
	sql_last = """SELECT 4g_ftp.date_id AS date_last,
        	COALESCE(tbl_mapping.rtp,'SUMBAGSEL') AS rtp_last,
        	COUNT(4g_ftp.site_id) AS pl_last
        	FROM 4g_ftp
         	LEFT JOIN tbl_mapping
         	ON 4g_ftp.site_id = tbl_mapping.site_id
         	WHERE 4g_ftp.status_pl_daily = 'CONS'
         	AND 4g_ftp.date_id LIKE '{}'
         	AND tbl_mapping.rtp <> 'NULL'
         	GROUP BY rtp WITH ROLLUP""".format(day_last_str)
	sql_curr = """SELECT 4g_ftp.date_id AS date_curr,
			COALESCE(tbl_mapping.rtp, 'SUMBAGSEL') AS rtp_curr,
			COUNT(4g_ftp.site_id) AS pl_curr
			FROM 4g_ftp
			LEFT JOIN tbl_mapping
			ON 4g_ftp.site_id = tbl_mapping.site_id
			WHERE 4g_ftp.status_pl_daily = 'CONS'
			AND 4g_ftp.date_id LIKE '{}'
			AND tbl_mapping.rtp <> 'NULL'
			GROUP BY rtp WITH ROLLUP""".format(day_curr_str)
	df_last = pd.read_sql(sql_last, conn)
	df_curr = pd.read_sql(sql_curr, conn)
	#df = pd.concat([df_last,df_curr], axis=1) #append dataframe for current status and last day status
	#df['delta'] = df['pl_curr']-df['pl_last'] #creating gap/delta column
	if df_last.shape[0] > df_curr.shape[0]:
		df_join = df_last.join(df_curr.set_index('rtp_curr'),on='rtp_last')
	else:
		df_join = df_curr.join(df_last.set_index('rtp_last'),on='rtp_curr')
	df_join['delta'] = df_join['pl_curr'].fillna(0)-df_join['pl_last']
	dlmtr = " | "
	txt = "Update Daily Broadcast R02 PL 4G as of: "+day_curr_str+"\n========================\nRTPO | CURR PL | LAST PL | GAP"
	for index, row in df_join.iterrows():
   		txt += "\n%s%s%s%s%s%s%s" % (row[1],dlmtr,row['pl_curr'],dlmtr,row['pl_last'],dlmtr,row['delta'])
	update.message.reply_text(txt)
#Define ach4g function

@auth_chat_id
@send_typing_action
def ach3g(update, context):
	today = datetime.datetime.now() #establishing today's date
	delta_curr = datetime.timedelta(days = 2)
	day_curr = today - delta_curr #current day packet loss
	day_curr_str = day_curr.strftime(("%Y"+"-"+"%m"+"-"+"%d")) #stringify current day packet loss variable
	delta_last = datetime.timedelta(days = 3)
	day_last = today - delta_last #last day packet loss
	day_last_str = day_last.strftime(("%Y"+"-"+"%m"+"-"+"%d")) #stringify last day packet loss variable
	conn = mysql_conn()
	sql_last = """SELECT y.date_id AS date_last, COALESCE(x.rtp,'SUMBAGSEL') AS rtp_last,
				CASE
					WHEN SUM(y.flag) > 1 THEN SUM(y.flag)
					ELSE 0
				END pl_last
				FROM tbl_mapping x
				RIGHT JOIN
				(SELECT DISTINCT date_id, ani, rnc_name,
				CASE
					WHEN status_pl_daily = 'CONS' THEN 1
					ELSE 0
				END flag
				FROM 3g_ftp
				)y
				ON x.ani=y.ani AND x.rnc = y.rnc_name
				WHERE y.date_id LIKE '{}'
				AND x.rtp <> 'NULL'
				GROUP BY rtp WITH ROLLUP""".format(day_last_str)
	sql_curr = """SELECT y.date_id AS date_curr, COALESCE(x.rtp,'SUMBAGSEL') AS rtp_curr,
				CASE
					WHEN SUM(y.flag) > 1 THEN SUM(y.flag)
					ELSE 0
				END pl_curr
				FROM tbl_mapping x
				RIGHT JOIN
				(SELECT DISTINCT date_id, ani, rnc_name,
				CASE
					WHEN status_pl_daily = 'CONS' THEN 1
					ELSE 0
				END flag
				FROM 3g_ftp
				)y
				ON x.ani=y.ani AND x.rnc = y.rnc_name
				WHERE y.date_id LIKE '{}'
				AND x.rtp <> 'NULL'
				GROUP BY rtp WITH ROLLUP""".format(day_curr_str)
	df_last = pd.read_sql(sql_last, conn)
	df_curr = pd.read_sql(sql_curr, conn)
	#df = pd.concat([df_last,df_curr], axis=1) #append dataframe for current status and last day status
	#df['delta'] = df['pl_curr']-df['pl_last'] #creating gap/delta column
	if df_last.shape[0] > df_curr.shape[0]:
		df_join = df_last.join(df_curr.set_index('rtp_curr'),on='rtp_last')
	else:
		df_join = df_curr.join(df_last.set_index('rtp_last'),on='rtp_curr')
	df_join['delta'] = df_join['pl_curr'].fillna(0)-df_join['pl_last']
	dlmtr = " | "
	txt = "Update Daily Broadcast R02 PL 3G as of: "+day_curr_str+"\n========================\nRTPO | CURR PL | LAST PL | GAP"
	for index, row in df_join.iterrows():
   		txt += "\n%s%s%s%s%s%s%s" % (row[1],dlmtr,int(row['pl_curr']),dlmtr,int(row['pl_last']),dlmtr,int(row['delta']))
	update.message.reply_text(txt)
#Define ach3g function

def rtpo4g(update, context):
    keyboard = [[InlineKeyboardButton("LAMPUNG", callback_data='RTP BANDAR LAMPUNG'),
                 InlineKeyboardButton("BENGKULU", callback_data='RTP BENGKULU'),
				 InlineKeyboardButton("BUNGO", callback_data='RTP BUNGO')],

				 [InlineKeyboardButton("JAMBI", callback_data='RTP JAMBI'),
				 InlineKeyboardButton("KOTABUMI", callback_data='RTP KOTABUMI'),
				 InlineKeyboardButton("LAHAT", callback_data='RTP LAHAT')],

				 [InlineKeyboardButton("LINGGAU", callback_data='RTP LUBUK LINGGAU'),
				 InlineKeyboardButton("METRO", callback_data='RTP METRO'),
				 InlineKeyboardButton("MUBA", callback_data='RTP MUSI BANYUASIN')],

				 [InlineKeyboardButton("OKI", callback_data='RTP OGAN KOMERING ILIR'),
				 InlineKeyboardButton("OKU", callback_data='RTP OGAN KOMERING ULU'),
				 InlineKeyboardButton("PALEMBANG", callback_data='RTP PALEMBANG')],

				 [InlineKeyboardButton("PKP INNER", callback_data='RTP PANGKAL PINANG INNER'),
				 InlineKeyboardButton("PKP OUTER", callback_data='RTP PANGKAL PINANG OUTER'),
				 InlineKeyboardButton("PRINGSEWU", callback_data='RTP PRINGSEWU')]]

    reply_markup = InlineKeyboardMarkup(keyboard)

    update.message.reply_text('Please choose your section:', reply_markup=reply_markup)

def button(update, context):
	query = update.callback_query

    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
	query.answer()
	var = query.data
	today = datetime.datetime.now() #establishing today's date
	delta_curr = datetime.timedelta(days = 2)
	day_curr = today - delta_curr #current day packet loss
	day_curr_str = day_curr.strftime(("%Y"+"-"+"%m"+"-"+"%d")) #stringify current day packet loss variable
	delta_last = datetime.timedelta(days = 3)
	day_last = today - delta_last #last day packet loss
	day_last_str = day_last.strftime(("%Y"+"-"+"%m"+"-"+"%d")) #stringify last day packet loss variable
	conn = mysql_conn()
	sql_last = """SELECT 4g_ftp.date_id AS date_last,
        	COALESCE(tbl_mapping.kabupaten,'TOTAL') AS kabupaten_last,
        	COUNT(4g_ftp.site_id) AS pl_last
        	FROM 4g_ftp
         	LEFT JOIN tbl_mapping
         	ON 4g_ftp.site_id = tbl_mapping.site_id
         	WHERE 4g_ftp.status_pl_daily = 'CONS'
         	AND 4g_ftp.date_id LIKE '{}'
         	AND tbl_mapping.rtp = '{}'
			AND tbl_mapping.kabupaten <> 'NULL'
         	GROUP BY kabupaten WITH ROLLUP""".format(day_last_str,var)
	#MySQL queries for 3 days before as a baseline or 'last'
	sql_curr = """SELECT 4g_ftp.date_id AS date_curr,
			COALESCE(tbl_mapping.kabupaten, 'TOTAL') AS kabupaten_curr,
			COUNT(4g_ftp.site_id) AS pl_curr
			FROM 4g_ftp
			LEFT JOIN tbl_mapping
			ON 4g_ftp.site_id = tbl_mapping.site_id
			WHERE 4g_ftp.status_pl_daily = 'CONS'
			AND 4g_ftp.date_id LIKE '{}'
			AND tbl_mapping.rtp ='{}'
			AND tbl_mapping.kabupaten <> 'NULL'
			GROUP BY kabupaten WITH ROLLUP""".format(day_curr_str,var)
	#MySQL queries for 2 days before as a current or 'curr'
	df_last = pd.read_sql(sql_last, conn)
	df_curr = pd.read_sql(sql_curr, conn)
	if df_last.shape[0] > df_curr.shape[0]:
		df_join = df_last.join(df_curr.set_index('kabupaten_curr'),on='kabupaten_last')
	else :
		df_join = df_curr.join(df_last.set_index('kabupaten_last'),on='kabupaten_curr')
	#the above conditional checks for the size of both the 'curr' and 'last' dataframe, whichever df that is the largest will be the main dataframe and will be left join to the smaller size df
	df_join['delta'] = df_join['pl_curr'].fillna(0)-df_join['pl_last']
	dlmtr = " | "
	txt = "BREAKDOWN KABUPATEN LEVEL 4G PL for "+var+" as of "+day_curr_str+"\n========================\nKAB | CURR PL | LAST PL | GAP"
	for index, row in df_join.iterrows():
		txt += "\n%s%s%s%s%s%s%s" % (row[1],dlmtr,row['pl_curr'],dlmtr,row['pl_last'],dlmtr,row['delta'])
	query.edit_message_text(txt)
    #query.edit_message_text(text="Selected option: {}".format(query.data))

@auth_chat_id
@send_image_action
def twamp(update, context):
    if len(context.args) != 1:
	    update.message.reply_text('Invalid syntax, please use /twamp<space>SiteID')
    else:
	    today = datetime.datetime.now()
	    todaystr = today.strftime("%Y"+"-"+"%m"+"-"+"%d")
	    delta = datetime.timedelta(days = 6)
	    deltaday = today - delta
	    deltadaystr = deltaday.strftime("%Y"+"-"+"%m"+"-"+"%d")
	    usr = str(update.message.from_user.username)
	    conn = mysql_conn()
	    sql = "SELECT * FROM tbl_hourly WHERE ne_name LIKE '{}%' AND date BETWEEN '{}' AND '{}' ORDER BY date ASC, time ASC".format(context.args[0],deltadaystr,todaystr)
	    df = pd.read_sql(sql,conn)
	    if df.shape[0] == 0:
		    update.message.reply_text('TWAMP Measurement for Site ID '+context.args[0]+' does not exists!')
	    else:
		    name = df.loc[df.index ==0,'ne_name'].values[0]
		    df['datetime'] = pd.to_datetime(df['date']) + pd.to_timedelta(df.pop('time'), unit='H')
		    maj_formatter = mdates.DateFormatter('%d %b')
		    min_formatter = mdates.DateFormatter('%H')
		    maj_locator = mdates.DayLocator(interval = 1)
		    min_locator = mdates.HourLocator(byhour=range(0,24,4))
		    plt.style.use('seaborn')
		    fig, (ax1, ax2) = plt.subplots(2,1,figsize=(11,8),sharex=False)
		    fig.suptitle('TWAMP Performance for '+name, fontweight='bold')
			#establishing subplots as 2 x 1 (2 rows 1 column)

		    ax1.plot( df['datetime'], df['avg_pl'],color='tab:blue',marker='o',linewidth=3, markersize=7)
		    ax1.axhline(y=0.1,linewidth=1, color='black',linestyle='--')
		    ax1.yaxis.grid(True)
		    ax1.xaxis.grid(True, which='major', linewidth=1.5)
		    ax1.set_ylabel('Two Way Packet Loss(%)')
		    ax1.set_ylim(bottom=0)
		    ax1.set_ylabel('Two Way Packet Loss(%)')
		    ax1.xaxis.set_major_formatter(maj_formatter)
		    ax1.xaxis.set_major_locator(maj_locator)
		    ax1.xaxis.set_minor_formatter(min_formatter)
		    ax1.xaxis.set_minor_locator(min_locator)
		    ax1.tick_params(axis='x', rotation=45)
		    ax1.tick_params(which='major', pad=15)
			#first subplot displays PL TWAMP
			
		    line2, = ax2.plot( df['datetime'], df['avg_lat'],color='tab:orange',linewidth=3)
		    line2.set_dashes([2, 2, 10, 2])
		    ax2.axhline(y=20,linewidth=1, color='black',linestyle='--')
		    ax2.yaxis.grid(True)
		    ax2.xaxis.grid(True, which='major', linewidth=1.5)
		    ax2.set_ylabel('Two Way Delay(ms)')
		    ax2.xaxis.set_major_formatter(maj_formatter)
		    ax2.xaxis.set_major_locator(maj_locator)
		    ax2.xaxis.set_minor_formatter(min_formatter)
		    ax2.xaxis.set_minor_locator(min_locator)
		    ax2.tick_params(axis='x', rotation=45)
		    ax2.tick_params(which='major', pad=15)
		    ax2.set_xlabel('Date Time', labelpad=20)
			#second sublpot displays Latency TWAMP

		    imgname = 'image_'+usr+'_'+str(today)+'.png'
		    fig.tight_layout()
		    fig.subplots_adjust(top=0.95)
		    plt.savefig(imgname)
		    context.bot.send_photo(chat_id=update.effective_chat.id, photo=open(imgname,'rb'))
		    time.sleep(10)
		    os.remove(imgname)
#Define 4g hourly graph function to the bot

@auth_chat_id
@send_typing_action
def twamp_mod(update, context):
    if len(context.args) !=1:
    	update.message.reply_text('Invalid syntax, please use /twamp_mod<space>SiteID')
    	return ConversationHandler.END
    else:
        context.user_data['siteid'] = context.args[0]
        keyboard1 = [['YES'],['NO']]
        reply_markup1 = ReplyKeyboardMarkup(keyboard1, resize_keyboard=True, one_time_keyboard=True)
        update.message.reply_text('You are about to have a conversation with me, send /cancel to dismiss me at any time\n\n'
	    'Would you like a zoomed view (maximum y-axis value of 1%) of your queries for '+context.args[0]+'?', reply_markup=reply_markup1)
        return ZOOM

def zoom(update, context):
	text = update.message.text
	context.user_data['zoom'] = text
	keyboard2 = [['1','2','3','4'],['5','6','7']]
	reply_markup2 = ReplyKeyboardMarkup(keyboard2, resize_keyborad=True, one_time_keyboard=True)
	if text == 'YES':
		update.message.reply_text('Got it! You want a zoomed-view with maximum value of 1%, please input your desired measurement period:', reply_markup=reply_markup2)
	else:
		update.message.reply_text('Got it! You want a normal-view, please input your desired measurement period:',reply_markup=reply_markup2)

	return DAY

def day(update,context):
    day = update.message.text
    context.user_data['days'] = day
    if context.user_data['zoom'] == 'YES':
	    update.message.reply_text('You have selected graph for '+context.user_data['siteid']+' with zoom-view for '+context.user_data['days']+' day(s) back', reply_markup=ReplyKeyboardRemove())
    else:
        update.message.reply_text('You have selected graph for '+context.user_data['siteid']+' with normal-view for '+context.user_data['days']+' day(s) back', reply_markup=ReplyKeyboardRemove())
    today = datetime.datetime.now()
    delta = datetime.timedelta(days=int(context.user_data['days']))
    deltaday = today - delta
    todaystr = today.strftime("%Y"+"-"+"%m"+"-"+"%d")
    deltadaystr = deltaday.strftime("%Y"+"-"+"%m"+"-"+"%d")
    conn = mysql_conn()
    sql = "SELECT * FROM tbl_hourly WHERE ne_name LIKE '{}%' AND date BETWEEN '{}' AND '{}' ORDER BY date ASC, time ASC".format(context.user_data['siteid'],deltadaystr,todaystr)
    df = pd.read_sql(sql,conn)
    if df.shape[0] == 0:
	    update.message.reply_text('TWAMP Measurement for Site ID '+context.user_data['siteid']+' does not exists!')
    else:
	    name = df.loc[df.index == 0,'ne_name'].values[0]
	    df['datetime'] = pd.to_datetime(df['date']) + pd.to_timedelta(df.pop('time'), unit='H')
	    maj_formatter = mdates.DateFormatter('%d %b')
	    min_formatter = mdates.DateFormatter('%H')
	    maj_locator = mdates.DayLocator(interval = 1)
	    min_locator = mdates.HourLocator(byhour=range(0,24,4))
	    plt.style.use('seaborn')
	    fig, (ax1, ax2) = plt.subplots(2,1,figsize=(11,8),sharex=False)
	    fig.suptitle('TWAMP Performance for '+name, fontweight='bold')
		#establishing subplots as 2 x 1 (2 rows 1 column)

	    ax1.plot( df['datetime'], df['avg_pl'],color='tab:blue',marker='o',linewidth=3, markersize=7)
	    ax1.axhline(y=0.1,linewidth=1, color='black',linestyle='--')
	    ax1.yaxis.grid(True)
	    ax1.xaxis.grid(True, which='major', linewidth=1.5)
	    ax1.set_ylabel('Two Way Packet Loss(%)')
	    if context.user_data['zoom'] == 'YES':
    		ax1.set_ylim(bottom=0, top=1)
	    else:
		    ax1.set_ylim(bottom=0)
	    ax1.xaxis.set_major_formatter(maj_formatter)
	    ax1.xaxis.set_major_locator(maj_locator)
	    ax1.xaxis.set_minor_formatter(min_formatter)
	    ax1.xaxis.set_minor_locator(min_locator)
	    ax1.tick_params(axis='x', rotation=45)
	    ax1.tick_params(which='major', pad=15)
		#first subplot displays PL TWAMP
			
	    line2, = ax2.plot( df['datetime'], df['avg_lat'],color='tab:orange',linewidth=3)
	    line2.set_dashes([2, 2, 10, 2])
	    ax2.axhline(y=20,linewidth=1, color='black',linestyle='--')
	    ax2.yaxis.grid(True)
	    ax2.xaxis.grid(True, which='major', linewidth=1.5)
	    ax2.set_ylabel('Two Way Delay(ms)')
	    ax2.xaxis.set_major_formatter(maj_formatter)
	    ax2.xaxis.set_major_locator(maj_locator)
	    ax2.xaxis.set_minor_formatter(min_formatter)
	    ax2.xaxis.set_minor_locator(min_locator)
	    ax2.tick_params(axis='x', rotation=45)
	    ax2.tick_params(which='major', pad=15)
	    ax2.set_xlabel('Date Time', labelpad=20)
		#second sublpot displays Latency TWAMP

	    imgname = 'image_'+str(today)+'.png'
	    fig.tight_layout()
	    fig.subplots_adjust(top=0.95)
	    plt.savefig(imgname)
	    context.bot.send_photo(chat_id=update.effective_chat.id, photo=open(imgname,'rb'))
	    plt.close('all')
	    time.sleep(10)
	    os.remove(imgname)
	    context.user_data.clear()

    return ConversationHandler.END

def cancel(update, context):
	user = update.message.from_user
	logger.info("User %s canceled the conversation.", user.first_name)
	update.message.reply_text('Bye!', reply_markup=ReplyKeyboardRemove())

	return ConversationHandler.END

#function for testing job queueing
def cb_30(context: telegram.ext.CallbackContext):
	context.bot.send_message(chat_id=-338888254, text="""Dear all, 
	This is an automatically generated message to inform you that, currently an automatic broadcast for daily 4G achievement will be sent at approximately 07:30 WIB (local time).
	Please contact @argyaharish for any anomaly that may present in the future. Thanks & Semangat Pagi!""")

def daily_bc4g(context: telegram.ext.CallbackContext):
	today = datetime.datetime.now() #establishing today's date
	delta_curr = datetime.timedelta(days = 2)
	day_curr = today - delta_curr #current day packet loss
	day_curr_str = day_curr.strftime(("%Y"+"-"+"%m"+"-"+"%d")) #stringify current day packet loss variable
	delta_last = datetime.timedelta(days = 3)
	day_last = today - delta_last #last day packet loss
	day_last_str = day_last.strftime(("%Y"+"-"+"%m"+"-"+"%d")) #stringify last day packet loss variable
	conn = mysql_conn()
	sql_last = """SELECT 4g_ftp.date_id AS date_last,
        	COALESCE(tbl_mapping.rtp,'SUMBAGSEL') AS rtp_last,
        	COUNT(4g_ftp.site_id) AS pl_last
        	FROM 4g_ftp
         	LEFT JOIN tbl_mapping
         	ON 4g_ftp.site_id = tbl_mapping.site_id
         	WHERE 4g_ftp.status_pl_daily = 'CONS'
         	AND 4g_ftp.date_id LIKE '{}'
         	AND tbl_mapping.rtp <> 'NULL'
         	GROUP BY rtp WITH ROLLUP""".format(day_last_str)
	sql_curr = """SELECT 4g_ftp.date_id AS date_curr,
			COALESCE(tbl_mapping.rtp, 'SUMBAGSEL') AS rtp_curr,
			COUNT(4g_ftp.site_id) AS pl_curr
			FROM 4g_ftp
			LEFT JOIN tbl_mapping
			ON 4g_ftp.site_id = tbl_mapping.site_id
			WHERE 4g_ftp.status_pl_daily = 'CONS'
			AND 4g_ftp.date_id LIKE '{}'
			AND tbl_mapping.rtp <> 'NULL'
			GROUP BY rtp WITH ROLLUP""".format(day_curr_str)
	df_last = pd.read_sql(sql_last, conn)
	df_curr = pd.read_sql(sql_curr, conn)
	#df = pd.concat([df_last,df_curr], axis=1) #append dataframe for current status and last day status
	#df['delta'] = df['pl_curr']-df['pl_last'] #creating gap/delta column
	if df_last.shape[0] > df_curr.shape[0]:
		df_join = df_last.join(df_curr.set_index('rtp_curr'),on='rtp_last')
	else:
		df_join = df_curr.join(df_last.set_index('rtp_last'),on='rtp_curr')
	df_join['delta'] = df_join['pl_curr'].fillna(0)-df_join['pl_last']
	dlmtr = " | "
	txt = "Update Daily Broadcast R02 PL 4G as of: "+day_curr_str+"\n========================\nRTPO | CURR PL | LAST PL | GAP"
	for index, row in df_join.iterrows():
   		txt += "\n%s%s%s%s%s%s%s" % (row[1],dlmtr,row['pl_curr'],dlmtr,row['pl_last'],dlmtr,row['delta'])
	context.bot.send_message(chat_id=-338888254, text=txt)

#define function get chat id from user
def my_id(update,context):
	var = str(update.message.from_user.id)
	update.message.reply_text('Your chat ID is : '+var)

def error(update, context):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, context.error)

def wl_list(update,context):
	txt = "Here are the list of approved users :\n"
	for list in whitelist:
		txt += str(list)+"\n"
	update.message.reply_text(txt)
	
def reg_id(update,context):
	if len(context.args) != 1:
		update.message.reply_text('Invalid syntax please user /reg_id<space>USERID')
	else:
		id_reg = context.args[0]
		if id_reg in whitelist:
			update.message.reply_text('User ID already exists in whitelist')
		else:
			whitelist.append(id_reg)
			update.message.reply_text('Successfully added user '+str(id_reg)+' to the whitelist')

def main():
    dispatcher.add_handler(CommandHandler('start',start))
	#Everytime the Bot receives a Telegram message that contains /start command, the Bot will reply with the text that was defined in the
	#aforementioned function

    dispatcher.add_handler(CommandHandler('ach4g',ach4g))
	#Everytime the Bot receives a Telegram message that contains the /ach4g command, the Bot will reply with the text that was defined in the
	#aforemention function

    dispatcher.add_handler(CommandHandler('ach3g',ach3g))
	#Everytime the Bot receives a Telegram message that contains the /ach3g command, the Bot will reply with the text that was defined in the
	#aforemention function

    dispatcher.add_handler(CommandHandler('help',help))
	#Everytime the Bot receives a Telegram message that contains /help command, the Bot will reply with the text that was defined in the
	#aforementioned function

    dispatcher.add_handler(CommandHandler('twamp',twamp))
	#Everytime the Bot receives a Telegram message that contains /twamp command, the Bot will reply with the previously defined function

    dispatcher.add_handler(CommandHandler('rtpo4g',rtpo4g))

    dispatcher.add_handler(CommandHandler('my_id',my_id))

    conv_handler = ConversationHandler(
		entry_points=[CommandHandler('twamp_mod',twamp_mod)],
		states ={
			ZOOM: [MessageHandler(Filters.regex('^(YES|NO)$'), zoom)],

			DAY: [MessageHandler(Filters.text & ~Filters.command, day)]

		},
		fallbacks = [CommandHandler('cancel',cancel)]
	)

    dispatcher.add_handler(conv_handler)

    dispatcher.add_handler(CallbackQueryHandler(button))
    dispatcher.add_error_handler(error)
    dispatcher.add_handler(CommandHandler('r', restart, filters=Filters.user(username='@argyaharish')))
    dispatcher.add_handler(CommandHandler('wl_list',wl_list, filters=Filters.user(username='@argyaharish')))
    dispatcher.add_handler(CommandHandler('reg_id',reg_id, filters=Filters.user(username='@argyaharish')))
    #updater.job_queue.run_once(cb_30,30)
    updater.job_queue.run_daily(daily_bc4g,datetime.time(7,17,0, tzinfo=datetime.timezone(datetime.timedelta(seconds=25200))),days=(0,1,2,3,4,5,6))

    updater.start_polling()
	#start the bot
	
    updater.idle()

if __name__ == '__main__':
	main()