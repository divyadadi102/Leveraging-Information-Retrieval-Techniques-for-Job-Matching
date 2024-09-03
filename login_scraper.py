import logging
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters, \
    OnSiteOrRemoteFilters

# Change root logger level (default is WARN)
logging.basicConfig(level=logging.INFO)


# Fired once for each successfully processed job
import os

def on_data(data: EventData):
    print(data.title)
    job_id = data.job_id.replace('\n', ' ').replace('"', '""').replace("'", "''")
    title = data.title.replace('\n', ' ').replace('"', '""').replace("'", "''")
    company = data.company.replace('\n', ' ').replace('"', '""').replace("'", "''")
    place = data.place.replace('\n', ' ').replace('"', '""').replace("'", "''")
    date = data.date.replace('\n', ' ').replace('"', '""').replace("'", "''")
    link = data.link.replace('\n', ' ').replace('"', '""').replace("'", "''")
    description = data.description.replace('\n', ' ').replace('"', '""').replace("'", "''")

    
    
    if os.path.exists('linkedInjobs_event_new.csv'):
        with open('linkedInjobs_event_new.csv', 'a') as f:
            f.write(f'"{job_id}","{title}","{company}","{place}","{date}","{link}","{description}"\n')
    else:
        with open('linkedInjobs_event_new.csv', 'w') as f:
            f.write('Job ID,Title,Company,Location,Date,Link,Description \n')
            f.write(f'"{job_id}","{title}","{company}","{place}","{date}","{link}","{description}"\n')



# Fired once for each page (25 jobs)
def on_metrics(metrics: EventMetrics):
    print('[ON_METRICS]', str(metrics))


def on_error(error):
    print('[ON_ERROR]', error)


def on_end():
    print('[ON_END]')


scraper = LinkedinScraper(
    
    chrome_executable_path=None,  # Custom Chrome executable path (e.g. /foo/bar/bin/chromedriver)
    chrome_binary_location=None,  # Custom path to Chrome/Chromium binary (e.g. /foo/bar/chrome-mac/Chromium.app/Contents/MacOS/Chromium)
    chrome_options=None,  # Custom Chrome options here
    headless=True,  # Overrides headless mode only if chrome_options is None
    max_workers=1,  # How many threads will be spawned to run queries concurrently (one Chrome driver for each thread)
    slow_mo=0.5,  # Slow down the scraper to avoid 'Too many requests 429' errors (in seconds)
    page_load_timeout=40  # Page load timeout (in seconds)    
)

# Add event listeners
scraper.on(Events.DATA, on_data)
scraper.on(Events.ERROR, on_error)
scraper.on(Events.END, on_end)

for i in [ 
    'Financial Analyst',
    'Accountant',
    'Financial Advisor',
    'Business Analyst',
    'Finance Manager',
         ]:
    
    queries = [
      Query(
          query='{}'.format(i),
          
          
          options=QueryOptions(
                limit=1000,
              locations=['United States'],
              apply_link=True,  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page must be navigated. Default to False.
              skip_promoted_jobs=False,  # Skip promoted jobs. Default to False.
              page_offset=0,  # How many pages to skip
              filters=QueryFilters(
                  # company_jobs_url='https://www.linkedin.com/jobs/search/?f_C=1441%2C17876832%2C791962%2C2374003%2C18950635%2C16140%2C10440912&geoId=92000000',  # Filter by companies.                
                  relevance=RelevanceFilters.RECENT,
                  time=TimeFilters.MONTH,
                  type=[TypeFilters.FULL_TIME, TypeFilters.INTERNSHIP],
                  on_site_or_remote=[OnSiteOrRemoteFilters.ON_SITE, OnSiteOrRemoteFilters.REMOTE],
                  # experience=[ExperienceLevelFilters.MID_SENIOR]
              )
          )
      ),
    ]
    

    scraper.run(queries)