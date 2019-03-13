from bs4 import BeautifulSoup as Soup
import urllib, requests, re, pandas as pd
import urllib.request
pd.io.parquet.PyArrowImpl()

pd.set_option('max_colwidth',500)    # to remove column limit (Otherwise, we'll lose some info)
df = pd.DataFrame()   # create a new data frame
base_url = 'http://www.indeed.com/jobs?q=software+engineering&jt=fulltime&sort='

def get_data(base_url):
    
    baseurl = base_url
    df = pd.DataFrame()
    sort_by = 'date'          # sort by data
    start_from = '&start=' 
    
    for page in range(1,31): # page from 1 to 100 (last page we can scrape is 100)
            page = (page-1) * 10  
            url = "%s%s%s%d" % (baseurl, sort_by, start_from, page) 
            target = Soup(urllib.request.urlopen(url), "lxml") 
            targetElements = target.findAll('div', class_={"result"})


            for elem in targetElements: 

                job_title = elem.find('a', attrs={'class':'turnstileLink'}).attrs['title']
                home_url = "http://www.indeed.com"
                job_link = "%s%s" % (home_url,elem.find('a').get('href'))

                fullsoup = Soup(urllib.request.urlopen(job_link), "lxml")
                try:
                    full_description = fullsoup.find('div',class_= {"jobsearch-JobComponent-description"}).getText()
                except:
                    full_description = None

                try:
                    comp_name = elem.find('span', class_={'company'}).getText().strip()
                except:
                    comp_name = None
                try:
                    job_addr = elem.find('span', attrs={'class':'location'}).getText()
                except:
                    job_addr = None

                try:
                    job_posted = elem.find('span', attrs={'class': 'date'}).getText()
                except:
                    job_posted = None

                try:
                    job_salary = elem.find('span',attrs={'class':'salary'}).getText().strip()
                except:
                    job_salary = None

                job_summary = elem.find('span',attrs={'class':'summary'}).getText().strip()


        #         comp_link_overall = elem.find('span', attrs={'itemprop':'name'}).find('a')
        #         if comp_link_overall != None: # if company link exists, access it. Otherwise, skip.
        #             comp_link_overall = "%s%s" % (home_url, comp_link_overall.attrs['href'])
        #         else: comp_link_overall = None

                        # add a job info to our data frame
            
            df = df.append({'comp_name': comp_name, 'job_title': job_title, 
                            'job_link': job_link, 'job_posted': job_posted,
                            'job_link': job_link, 'job_location': job_addr,
                            'job_posted':job_posted,'job_summary':job_summary,
                            'job_salary':job_salary,'job_fulldescription':full_description,
                           }, ignore_index=True)
    return df

df = get_data(base_url)
df.to_parquet('indeed_companies')
