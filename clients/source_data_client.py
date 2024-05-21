import requests, os, json, datetime, zipfile
from bs4 import BeautifulSoup

from get_platform import verify

class SourceData:
    def __init__(self, dataset, **kwargs):
        if 'ignore_release_date' in kwargs.keys():
            self.ignore_release_date = kwargs['ignore_release_date']
        else:
            self.ignore_release_date = False
        
        self.landing_page_json = "supporting_files/landing_pages.json"
        # get landing pages from landing_pages.json
        with open(self.landing_page_json) as f:
            self.page_details = json.load(f)
        
        self.dataset = dataset
        self.ons_landing_page = "https://www.ons.gov.uk"

        if '-previous' in dataset:
            # for weekly-deaths-previous
            self.is_previous = True
        else:
            self.is_previous = False
        
        # get todays date
        todays_date = datetime.datetime.now()
        self.todays_date = datetime.datetime.strftime(todays_date, "%d %B %Y")

        self.downloaded_files = []

        # get user-agent
        email = os.getenv('FLORENCE_EMAIL')
        if not email:
            email = 'cmd@ons.gov.uk' # generic cmd email

        self.user_agent = {"User-Agent": f"cmd-run-transforms/Version1.0.0 ONS {email}"}

    def get_source_files(self):
        if self.dataset not in self.page_details.keys():
            print(f"no landing page available for {self.dataset}, will use files from current directory")
            return ""

        # downloads and writes source files
        assert self.dataset in self.page_details.keys(), f"{self.dataset} is not in {self.landing_page_json}, landing page is unknown"
        
        self.landing_pages = self.page_details[self.dataset]["pages"]
        
        for page in self.landing_pages:
            self._download(page)

        return self.downloaded_files
        
    def _download(self, page):
        results = self._check_release_date(page)
        
        # get link
        elements = results.find_all("div", class_="inline-block--md margin-bottom-sm--1")
        element = elements[0] # latest comes first

        if self.is_previous:
            element = elements[1] # previous edition uses second link
        
        link = str(element).split("href=")[-1].split(">")[0].strip('"')
        download_link = f"{self.ons_landing_page}{link}"
        
        # download the file
        source_file = download_link.split('/')[-1]
        r = requests.get(download_link, headers=self.user_agent, verify=verify)
        with open(source_file, 'wb') as output:
            output.write(r.content)
        print(f"written {source_file}")
            
        # unzip if needed
        if source_file.endswith(".zip"):
            with zipfile.ZipFile(source_file, 'r') as zip_ref:
                extracted_file = zip_ref.namelist()
                self.downloaded_files.append(extracted_file[0])
                zip_ref.extractall("")
            os.remove(source_file)
            print(f"extracted {source_file}")
        else:
            self.downloaded_files.append(source_file)

    def _get_results(self, page):
        landing_page = f"{self.ons_landing_page}{page}"
        r = requests.get(landing_page, headers=self.user_agent, verify=verify)
        if r.status_code != 200:
            raise Exception(f"{self.ons_landing_page}{page} returned a {r.status_code} error")
        
        soup = BeautifulSoup(r.content, "html.parser")
        
        results = soup.find(id="main")
        return results
    
    def _check_release_date(self, page):
        # very hacky but works
        # check release date
        results = self._get_results(page)
        elements = results.find_all("li", class_="col col--md-12 col--lg-15 meta__item")
        element = str(elements[1])
        release_date = element.split(">")[-3].split("<")[0]
        
        if self.ignore_release_date:
            # ignores release date if flag is passed
            return results

        if release_date == self.todays_date:
            return results
        else:
            results = self._get_results(f"{page}/?123") # in case it is a caching issue
            # check release date again
            elements = results.find_all("li", class_="col col--md-12 col--lg-15 meta__item")
            element = str(elements[1])
            release_date = element.split(">")[-3].split("<")[0]
            assert release_date == self.todays_date, f"Release date does not match todays date, aborting source file download"
            return results
        