from datetime import datetime as dd

print('Script has been started at {}'.format(dd.now()))
print('**********')

#import packages 
from webdriver_manager.chrome import ChromeDriverManager
import time,re, os, yaml
from time import sleep
import pandas as pd
from datetime import timedelta, date
from datetime import datetime as dtm
from selenium import webdriver
import chromedriver_binary, time  # Adds chromedriver binary to path
import datetime, pymongo,csv
import numpy as np
from dateutil.parser import parse
import click, requests, json
import warnings
warnings.filterwarnings('ignore')
from monkeylearn import MonkeyLearn
#spin up a browser

from utils import GoogleApis,CleanData, Retry
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import argparse,sys,os,pathlib
from sentence_transformers import SentenceTransformer, util

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
from requests.exceptions import RequestException
from socket import timeout


urls = ['https://www.myjobmag.com/jobs-by-industry/banking-financial-services',
        'https://www.myjobmag.com/jobs-by-industry/ict-telecommunications',
        'https://www.myjobmag.com/jobs-by-field/media',
        'https://www.myjobmag.com/jobs-by-field/sales-marketing',
'https://www.myjobmag.com/jobs-by-industry/engineering']


ml = MonkeyLearn('adffd761684115861c7100cd70e64e3f9a54a747')
model_id = 'cl_WBhyA3hc'
cat_dict = {}

if os.path.exists('./inspectPath.yaml'):
    config = yaml.safe_load(open(f"./inspectPath.yaml"))
    path = config['path']
else:
    raise ValueError('path: inspectPath.yaml file not found in path"')


def failed_call(*args, **kwargs):
    """Deal with a failed call within various web service calls.
    Will print to a log file with details of failed call.
    """
    error = f'After retrying multiple times, an exception was raised.\n{args[0]}'
    slack_msg = {'username':'jobscraping-bot',\
                'text':f" Job Extraction Pipeline Failed!!! Error alert :red_circle: \n Reason: ```{error}```"}
    CleanData().send_status_msg(slack_msg)
    raise args[1](error)
    
    
retry = Retry(times=3, pause=1, retreat=2, cleanup=failed_call,
              exceptions=(RequestException, TimeoutException))

class etl_process(GoogleApis):
    jobTitleArray=[]
    jobIdArray=[]
    companyNameArray=[]
    jobLinkArray=[]
    industryArray=[]
    jobDescriptionArray=[]
    jobLocation=[]
    dateJobPublishedArray=[]
    dateJobDeadLineArray=[]
    yearOfExpArray=[]
    specialisationArray=[]
    employmentType = []
    categoryName = []
    company_abbreviation = []
    available_jobs = []

    def get_available_jobs(self,url,start,finish):
        for i in range(start,finish):
            self.driver.get(url + f'/{i}')
            sleep(2)
            elems = self.driver.find_elements_by_xpath(path[6])#.get_attribute('href')
            for block in elems:
                woah = block.find_elements_by_tag_name("a")
                for w in woah:
                    self.available_jobs.append(w.get_attribute("href"))
        return self.available_jobs


    def confirm_path_exists(self,driver):
        path_list = path
        item = {0:'job-title',1:'description',2:'job-details',3:'company-name',4:'date',\
                5:'job-link',6:'list-of-jobs'}
        driver.get(self.available_jobs[0])
        try:
            for i in range(0,len(path_list)):
                if len(driver.find_elements_by_xpath(path_list[i])) < 1:
                    raise KeyError

        except Exception as e:
            non_existent = item.get(i)
            return (f'{non_existent} Path does not exist. You may need to inspect path')

    def start_driver(self):
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.page_load_strategy = 'eager'
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36")
        options.add_argument('--dns-prefetch-disable')
        self.driver = webdriver.Chrome(ChromeDriverManager().install(),options=options)
        self.driver.set_page_load_timeout(60)

    @retry
    def extract_data(self):
        for i in range(0,len(self.available_jobs)):
            try:
                self.driver.get(self.available_jobs[i])
            #     elems = driver.find_elements_by_xpath("//h2[@class='mag-b']")
                elems = self.driver.find_elements_by_xpath(path[0])
                for block in elems:
                    jt = block.text
                    title = self.strip_unwanted_text(jt,'',' at ')
                    self.jobTitleArray.append(title)

                descr = []
                elems = self.driver.find_elements_by_xpath(path[1])#.get_attribute('href')
                for block in elems:
                    descr.append(block.text)

                jobDetails = []
                elems = self.driver.find_elements_by_xpath(path[2])#.get_attribute('href')
                for block in elems:
                    jobDetails.append(block.text)

                try:
                    jobdescr = descr[0]+jobDetails[0]
                except:
                    continue

                company_name = []
                elems = self.driver.find_elements_by_xpath(path[3])#.get_attribute('href')
                for block in elems:
                    jt = block.text
                    companyName = self.strip_unwanted_text(jt,'at ','\n')
                    company_name.append(companyName)

                try:
                    experience = int(re.search(r'(\d+)[^\d]+[yY]ear', descr[0]).group(1))
                except:
                    experience = 'Not available'

                location = self.strip_unwanted_text(descr[0],'Location\n','\nJob')

                position = title
                try:
                    d=descr[0].lower()
                    cat = re.findall('(?<=Job Field\n)[^:]*', descr[0])[0]
                    if 'ict' in cat.lower():
                        if 'engineer' in d or 'design' in d or 'developer' in d:
                            cat = 'Information Technology'
                    elif 'human resources' in d:
                        cat = 'Human Resources'
                    elif 'Business development manager' in d:
                            cat = 'Sales and Marketing'
                    else:
                        try:
                            result = ml.classifiers.classify(model_id, [d])
                            cat = result.body[0]['classifications'][0]['tag_name']
                            cat_dict[i] = result
                        except Exception as e:
        #                     print(e)
                            cat = cat

                except:
                    try:
                        result = ml.classifiers.classify(model_id, [d])
                        cat = result.body[0]['classifications'][0]['tag_name']
                        cat_dict[i] = result
                    except:
                        cat = position
                wes = cat

                em1 = self.model.encode(wes,convert_to_tensor=True)
                em2 = self.model.encode(self.cat_list,convert_to_tensor=True)
                #Compute cosine-similarits
                cosine_scores = util.pytorch_cos_sim(em1, em2)

                score = np.argmax(cosine_scores[0].numpy())
                category = self.cat_list[score]

                dates = self.driver.find_elements_by_xpath(path[4])
                posting = [date.text for date in dates]

                posted = self.strip_unwanted_text(posting[0],'Posted: ','\nDeadline')
                end = self.strip_unwanted_text(posting[0],'\nDeadline: ','\nSave')
                dt = parse(posted)
                datePosted_dt = dt.date()
                datePosted = datePosted_dt.strftime(format='%Y-%m-%d')

                try:
                    deadline = parse(end)
                    deadline = deadline.date()
                except:
                    deadline = datePosted_dt + timedelta(days=20)

                check = self.driver.find_elements_by_xpath(path[5])

                if len(check)>=1:
                    file = check[0].get_attribute('href')

                else:
                    email_list=[]
                    for li in self.driver.find_elements_by_xpath("//div[@class='mag-b bm-b-30']"):
                        text = li.text
                        if len(text) <= 0:
                            pass
                            # print(f'index with no job link{i}')
                        jobdescr+=text
                        email = re.findall('\S+@\S+', text)
                        if len(email) > 0:
                            email = "mailto:" + ''.join(email)
                            prohibited_email = ['@gmail', '@yahoomail']
                            if any(mail in email for mail in prohibited_email):
                                email = ""
                        else:
                            email = ""
                        email_list.append(email)
                        file = email_list[0]

                self.dateJobPublishedArray.append(datePosted)
                self.dateJobDeadLineArray.append(deadline)
                self.yearOfExpArray.append(experience)
                self.jobLocation.append(location)
                self.industryArray.append('Not available')
                self.specialisationArray.append('Not specified')    
                self.categoryName.append(category)  
                self.jobDescriptionArray.append(jobdescr)
                self.companyNameArray.append(company_name[0])

                self.jobLinkArray.append(file)
                for name in self.companyNameArray:
                    abb = "".join(e[0] for e in name.split())[:3]
                    if len(abb)<3:
                        abb = name[:3]
                    self.company_abbreviation.append(abb)
                sleep(2)
            except Exception as e:
                print(e)


    def list_to_dataframe(self):
        today = datetime.datetime.today().date()
        today=today.strftime("%B, %d %Y")
        self.job_dataset=pd.DataFrame()
        for i in range(0,len(self.companyNameArray)):
            self.job_dataset = self.job_dataset.append(pd.DataFrame({'job_id':f'{self.company_abbreviation[i]}{date.today()}{i}',
                                                        'company_name':self.companyNameArray[i],
                                                    'position':self.jobTitleArray[i],
                                                        'date_created':today,
                                                        'date_published':self.dateJobPublishedArray[i],
                                                    'years_of_experience_required':self.yearOfExpArray[i],
                                                    'job_location':self.jobLocation[i],
                                                    'deadline':self.dateJobDeadLineArray[i],
                                                        'job_specialisation':self.specialisationArray[i],
                                                        'Category': self.categoryName[i],
                                                    'job_industry':self.industryArray[i],
                                                    'job_desription':self.jobDescriptionArray[i],
                                                        'Job_link':self.jobLinkArray[i]
                                                        }, index=[0]), ignore_index=True)
        return self.job_dataset


    def set_level(self,job_dataset):
        try:
            years = pd.to_numeric(job_dataset['years_of_experience_required'])
            if ( years>= 0) & (years <= 2):
                return "Entry-Level"
            elif (years > 2) & (years <= 5):
                return "Experienced Hire"
            elif (years > 5)& (years <= 8):
                return "Senior Officer"
            elif (years > 8 )& (years <= 10):
                return "Manager"
            elif (years > 10 )& (years <= 15):
                return "Senior Manager"
            elif (years > 15 ):
                return "Executive"
            else:
                return ""
        except:
            return 'Not available'


    def transform_data(self,job_dataset,output_name):
        urls_l=[]
        email_urls = []
        i = 0
        for i in range(len(job_dataset['Job_link'])):
            try:
                if job_dataset['Job_link'][i][:5] == 'https':
                    self.driver.get(job_dataset['Job_link'][i])
                    sleep(2)
                    current_url = self.driver.current_url
                    try:
                        current_url = re.search(r'(.*?)?utm_source=', current_url.lower()).group().replace('?utm_source=','')
                        urls_l.append(current_url)
                        email_urls.append('')
                    except:
                        urls_l.append(current_url)
                        email_urls.append('')
                else:
                    urls_l.append('')
                    email_urls.append(job_dataset['Job_link'][i])
            except:
                urls_l.append('')
                email_urls.append('')

        job_dataset['Job_link'] = urls_l
        job_dataset['Email_link'] = email_urls
        job_dataset['Job_link'] = job_dataset['Job_link'] + job_dataset['Email_link'] #comment out if you need email link
        job_dataset.drop('Email_link',axis=1,inplace=True)
        job_dataset['deadline'] = job_dataset['deadline'].apply(lambda x: x.strftime(format='%Y-%m-%d'))
        now = dtm.now()
        job_dataset = job_dataset[job_dataset['deadline'].apply(lambda x: parse(x) > now)]
        past_time = now + timedelta(days=-4)
        job_dataset = job_dataset[job_dataset['date_published'].apply(lambda x: parse(x) > past_time)]
        job_dataset['level'] = job_dataset.apply(self.set_level, axis = 1)
        job_dataset.to_csv(output_name,index=False)


    def load_data_toDb(self,job_dataset):
        myclient = pymongo.MongoClient("mongodb://analysis:window##1234@104.248.225.123:27017/")
        mydb = myclient["analystDB"]
        mycol = mydb["localJob"]
        mycol.insert_many(job_dataset.to_dict('records'))
        print('ob sucessfully updated:',len(job_dataset))


    def get_file_name(self):
        output_name_template=f'{date.today()}_%s.csv'
        current_piece = 1
        while current_piece<10000:
            def path_name(current_piece):
                current_out_path = os.path.join(
                    '.',
                    output_name_template % current_piece
                )
                return current_out_path
            current_out_path = path_name(current_piece)
            if os.path.isfile(current_out_path):
                current_piece+=1
            else:
                break
        return current_out_path


    


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Description of your app.')
#     parser.add_argument('spread_sheet',
#                     help='This week spread sheet')
#     parser.add_argument('lastweek_sheet',
#                     help='last week spread sheet. To check fpr duplicates')
    parser.add_argument('--output_name',
                        type=pathlib.Path,
                        default=etl_process().get_file_name(),
                       help='get filename for saving scraped job')
    
    args = parser.parse_args()
#     spread_sheet    = args.spread_sheet
#     lastweek_sheet    = args.lastweek_sheet
    output_name    = args.output_name
    try:
        
        n = 5
        jm_etl = etl_process()
        jm_etl.start_driver()
        while(n>=1):
            url = urls[n-1]
            jm_etl.driver.get(url)
            available_jobs = jm_etl.get_available_jobs(url,1,3)
            jm_etl.confirm_path_exists(jm_etl.driver)
            # print(f'{url} good to go')
            n-=1
            
        print(f'\nThe length before scraping {len(jm_etl.available_jobs)}')
        ga = GoogleApis()
        spread_sheet =  ga.create_spreadsheet()
        spr,_ = ga.extract_spreadsheet(spread_sheet)
        sheet_name = spread_sheet

        jm_etl.remove_scraped_job(spread_sheet)
        aj = jm_etl.available_jobs

        if len(aj) >= 1:
            jm_etl.extract_data()    
            print(f'\nThe length after scraping {len(aj)}')
            df = jm_etl.list_to_dataframe()
            jm_etl.transform_data(df,output_name)
#         df = pd.concat([pd.read_csv(file) for file in glob.glob(f'./{date.today()}*')], ignore_index=True)
#         df.to_csv(output_name,index=False)
            lastweek_sheet = jm_etl.get_lastweek_file()
            jm_etl.append_df_to_gs(spread_sheet,lastweek_sheet,output_name,sheet_name,0)
            jm_etl.update_available_jobs(aj,spr)
        else:
            jm_etl.update_available_jobs(aj,spr)
        jm_etl.remove_files(7)
        jm_etl.driver.quit()
    except Exception as e:
        slack_msg = {'username':'jobscraping-bot',\
                'text':f"Debugging Mode!!! Error alert :red_circle: \n Reason: ```API raised {str(e)} error.```"}
        jm_etl.send_status_msg(slack_msg)
        jm_etl.driver.quit()
        raise(str(e))
    