import pandas as pd
from datetime import timedelta, date
from datetime import datetime as dtm
import datetime, pymongo,csv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import argparse,sys,os,pathlib
from sentence_transformers import SentenceTransformer, util
import numpy as np
import click, requests, json
import  time
from functools import wraps


class CleanData:

    def get_category(self,x):
        self.x = x
        
        em1 = self.model.encode(x,convert_to_tensor=True)
        em2 = self.model.encode(self.cat_list,convert_to_tensor=True)
        #Compute cosine-similarits
        cosine_scores = util.pytorch_cos_sim(em1, em2)
        score = np.argmax(cosine_scores[0].numpy())
        category = self.cat_list[score]
        return category
    
    def strip_unwanted_text(self,s,start,end):
        return s[s.find(start)+len(start):s.rfind(end)]
    
    def clean_jobTitle(self,x):
        self.x = x
        words = [' Recruitment', ' Job']
        dw = 'Imposible'
        for word in words:
            if word in x:
                dw = word
        nw = x.replace(dw,'')
        if len(nw)>=60:
            return ''
        else:
            return nw

    def remove_files(self,day):
        self.day = day
        current_time = time.time()

        for f in os.listdir():
            if f.endswith('csv'):
                creation_time = os.path.getctime(f)
                if (current_time - creation_time) // (24 * 3600) >= self.day:
                    os.unlink(f)
                    print('{} removed'.format(f))
                    

    def strip_spaces(self,df,column):
        self.df = df
        self.column = column
        return df[column].apply(lambda x: x.strip())


    def change_case(self,df,column,case='lower'):
        self.df = df
        self.column = column
        self.case =case
        if case == 'lower':
            return df[column].apply(lambda x: str(x).lower())
        elif case == 'upper':
            return df[column].apply(lambda x: x.upper())
        elif case == 'capitalize':
            return df[column].apply(lambda x: x.capitalize())
        else:
            raise ValueError(f"case: expected one of 'upper','lower' or 'capitalize' got {case}")
            
    def send_slack_msg(self):

        whurl = 'https://hooks.slack.com/services/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'
        slack_msg = {'username':'jobscraping-bot',\
            'text':f"<@U015NHVTWHE> and <@U01HP9KMY86> New jobs have been scraped.\n<https://docs.google.com/spreadsheets/d/{self.sid}|Click here> to find jobs. \Total number of jobs scraped is {self.scraped_data }"}
        requests.post(whurl,data=json.dumps(slack_msg))
        
    def send_status_msg(self,slack_msg):
        whurl = 'https://hooks.slack.com/services/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'
        requests.post(whurl,data=json.dumps(slack_msg))
        

class GoogleApis(CleanData):    
    path_to_credentials = 'jobscraping-project-a0d8d82c9331.json'
    model = SentenceTransformer('paraphrase-MiniLM-L12-v2')
    category = pd.read_csv('/Users/terra-016/Downloads/jobmar18 - New Category.csv')
    cat_list = list(category['Category'])

    def call_gspread_api(self):
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.path_to_credentials, scope)
        client = gspread.authorize(credentials)
        return client

    def extract_spreadsheet(self,sheet):
        self.sheet =sheet
        client = self.call_gspread_api()
        spreadsheet = client.open(self.sheet)
        return spreadsheet,client

    def create_spreadsheet(self):
        ss = []
        if date.today().weekday() == 0:
            cli = self.call_gspread_api()
            today = datetime.datetime.now().day
            month = datetime.datetime.now().strftime("%B")
            xtended = (datetime.datetime.now() + datetime.timedelta(days=7)).day
            xtended_month = (datetime.datetime.now() + datetime.timedelta(days=7)).strftime("%B")
            title_name = f'{month} {today} - {xtended_month} {xtended}'
            for name in cli.list_spreadsheet_files():
                ss.append(name['name'])
            if title_name in ss:
                pass
            else:
                print(f"{title_name} and {name['name']}")
                sheet = cli.create(f'{title_name}')
                email_list = ['akinwandekomolafe@gmail.com','dalaahmad94@gmail.com',\
                              'adenikeoadeniyi@gmail.com']
                for email in email_list:
                    sheet.share(email, role='writer',perm_type='user',notify=True,\
                        email_message='You have been granted access to this file for the week')
                worksheet = sheet.add_worksheet(title=title_name, rows="1000", cols="30")
                sheet.del_worksheet(sheet.sheet1)
            return title_name
        else:
            today = datetime.date.today()
            mday = today - datetime.timedelta(days=today.weekday())
            monday = mday.day
            month = mday.strftime("%B")
            xtended = (mday + datetime.timedelta(days=7)).day
            xtended_month = (mday + datetime.timedelta(days=7)).strftime("%B")
            title_name = f'{month} {monday} - {xtended_month} {xtended}'
            return title_name


    def get_lastweek_file(self):
        today = datetime.date.today()
        mday = (today - datetime.timedelta(days=today.weekday())).day
        last_mday = (today - datetime.timedelta(days=today.weekday()+7))
        lastwk_monday = last_mday.day
        month = (today - datetime.timedelta(days=today.weekday())).strftime("%B")
        lastwk_month = (today - datetime.timedelta(days=today.weekday()+7)).strftime("%B")
        file_name = f'{lastwk_month} {lastwk_monday} - {month} {mday}'
        return file_name



    def csv_to_googlesheet(self,new_sheet,old_sheet,sheet_num):
        self.new_sheet = new_sheet
        self.old_sheet = old_sheet
        self.sheet_num = sheet_num
        #this week spreadsheet
        spreadsheet,client = self.extract_spreadsheet(new_sheet)
        #last week spreadsheet
        lastweek_sheet = self.extract_spreadsheet(old_sheet)[0].id
        #last week filnename in path
        lastweek_filename = lastweek_sheet + '-worksheet' + str(0) + '.csv'
    #     for i, worksheet in enumerate(spreadsheet.worksheets()):
        self.docid = spreadsheet.id
        self.filename = self.docid  + f"-{self.sheet_name}" + str(0) + '.csv'
        with open(self.filename, 'w') as f:
            writer = csv.writer(f)
            writer.writerows(spreadsheet.get_worksheet(sheet_num).get_all_values())

        df = pd.read_csv(self.filename)
        df['company_name'] = self.change_case(df,'company_name')
        df['position'] = self.change_case(df,'position')
        df = df.drop_duplicates(subset=['company_name','position'])
        old_df = pd.read_csv(lastweek_filename)
        old_df['period'] = 'last-week'
        df['period'] = 'this-week'
        dataframe = pd.concat([df,old_df])
        dataframe['company_name'] = self.change_case(dataframe,'company_name')
        dataframe['position'] = self.change_case(dataframe,'position')
        dataframe['company_name'] = self.strip_spaces(dataframe,'company_name')
        dataframe['position'] = self.strip_spaces(dataframe,'position')

        drop_df = dataframe.drop_duplicates(subset=['company_name','position'],keep = False)
        self.count = drop_df[drop_df['period'] == 'this-week'].shape[0]
        self.duplicate_count = df.shape[0] - self.count
        data= drop_df[drop_df['period'] == 'this-week']
        data.drop('period',axis=1,inplace=True)
        data.to_csv('updated-jobs.csv',index=False)
        if sheet_num==2:
            self.check_and_drop('updated-jobs.csv')

        self.scraped_data = data.shape[0]
        data.to_csv('updated-jobs.csv',index=False)

        df = pd.read_csv('updated-jobs.csv')
        df.fillna('', inplace=True)
        spreadsheet.get_worksheet(self.sheet_num).clear()
        spreadsheet.get_worksheet(self.sheet_num).update([df.columns.values.tolist()] + df.values.tolist())
        self.spreadsheet = spreadsheet
        self.send_slack_msg()
        print('\n Slack message has been sent successfully')

    def append_df_to_gs(self,spread_sheet:str,lastweek_sheet,output_name,sheet_name,sheet_num):
        self.spread_sheet = spread_sheet 
        self.lastweek_sheet = lastweek_sheet
        self.output_name = output_name
        self.sheet_name = sheet_name
        
        df = pd.read_csv(self.output_name)
        df.fillna('', inplace=True)
      
        gsc = self.call_gspread_api()
        spreadsheet = gsc.open(self.spread_sheet)
        sheet = gsc.open_by_key(spreadsheet.id)
        params = {'valueInputOption': 'USER_ENTERED'}
        id =spreadsheet.get_worksheet(2).id
        self.sid  = f'{spreadsheet.id}/edit#gid={id}'
        duplicate = False
        count = len(spreadsheet.get_worksheet(2).get_all_values())
        if count<=0:
            body = {'values': [df.columns.values.tolist()] + df.values.tolist()}
        else:
            body = {'values': df.values.tolist()}
            duplicate = True
        sheet.values_append(f'{self.sheet_name}!A1:Z1', params, body)

        self.csv_to_googlesheet(self.spread_sheet,self.lastweek_sheet,sheet_num)
        slack_msg = {'username':'jobscraping-bot',\
                    'text':f"Hello, Success alert :white_check_mark:  ```Total number of jobs scraped is *{df.shape[0]}* \n {self.duplicate_count} jobs from the scraped jobs appeared last week```"}
        self.send_status_msg(slack_msg)
        
    def extractDigits(self,lst):
        return list(map(lambda el:[el], lst))
    

    def update_available_jobs(self,available_jobs,spread_sheet):
        if len(available_jobs) >= 1:
            avail_data = self.extractDigits(available_jobs)
            body = {'values': avail_data}
            params = {'valueInputOption': 'RAW'}
            spread_sheet.values_append('Available jobs!A1:N1', params, body)
        else:
            slack_msg = {'username':'jobscraping-bot',\
                    'text':f"Ooopss :disappointed: API couldnt scrape jobs.\n Reason: ```No Available Jobs right now```"}
            self.send_status_msg(slack_msg)
            return False   
        

    def remove_scraped_job(self,spread_sheet):
        spr,_ = self.extract_spreadsheet(spread_sheet)
        try:
            worksheet = spr.add_worksheet(title="Available jobs", rows="1000", cols="1")
        except:
            pass
        if len(spr.worksheets()[1].get_all_values()) < 1:
            self.update_available_jobs(self.available_jobs,spr)
        else:
            flat_list = [item for sublist in spr.worksheets()[1].get_all_values() for item in sublist]
            for i in range(len(flat_list)):
                if flat_list[i] in self.available_jobs:
                    self.available_jobs.remove(flat_list[i])
        # return self.available_jobs

class Retry(object):
    """Decorator that retries a function call a number of times, optionally
    with particular exceptions triggering a retry, whereas unlisted exceptions
    are raised.
    :param pause: Number of seconds to pause before retrying
    :param retreat: Factor by which to extend pause time each retry
    :param max_pause: Maximum time to pause before retry. Overrides pause times
                      calculated by retreat.
    :param cleanup: Function to run if all retries fail. Takes the same
                    arguments as the decorated function.
    """
    def __init__(self, times, exceptions=(IndexError), pause=1, retreat=1,
                 max_pause=None, cleanup=None):
        """Initiliase all input params"""
        self.times = times
        self.exceptions = exceptions
        self.pause = pause
        self.retreat = retreat
        self.max_pause = max_pause or (pause * retreat ** times)
        self.cleanup = cleanup
        self.error = {}

    def __call__(self, f):
        """
        A decorator function to retry a function (ie API call, web query) a
        number of times, with optional exceptions under which to retry.

        Returns results of a cleanup function if all retries fail.
        :return: decorator function.
        """
        @wraps(f)
        def wrapped_f(*args, **kwargs):
            for i in range(self.times):
                # Exponential backoff if required and limit to a max pause time
                pause = min(self.pause * self.retreat ** i, self.max_pause)
                try:
                    return f(*args, **kwargs)
                except self.exceptions as e:
                    self.error = e
                    self.excep = e.__class__
                    if self.pause is not None:
                        time.sleep(pause)
                    else:
                        pass
            if self.cleanup is not None:
                return self.cleanup(self.error,self.excep)
        return wrapped_f