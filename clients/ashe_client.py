import requests, os, glob, sys, json, zipfile
from bs4 import BeautifulSoup
import pandas as pd

from get_platform import verify

TRANSFORM_URL = "https://raw.github.com/ONS-OpenData/cmd-transforms/master"

list_of_ashe_tables = [
    "ashe-tables-3", "ashe-table-5", "ashe-tables-7-and-8", 
    "ashe-tables-9-and-10", "ashe-tables-11-and-12", "ashe-tables-20", 
    "ashe-tables-25", "ashe-tables-26", "ashe-tables-27-and-28"
]

time_series_ashe_tables = [
    "ashe-tables-3", "ashe-table-5", "ashe-tables-9-and-10", "ashe-tables-11-and-12", 
    "ashe-tables-20", "ashe-tables-25", "ashe-tables-26"
]

ashe_number_lookup = {
    "3": "ashe-tables-3",
    "5": "ashe-table-5",
    "7": "ashe-tables-7-and-8",
    "8": "ashe-tables-7-and-8",
    "9": "ashe-tables-9-and-10",
    "10": "ashe-tables-9-and-10",
    "11": "ashe-tables-11-and-12",
    "12": "ashe-tables-11-and-12",
    "20": "ashe-tables-20",
    "25": "ashe-tables-25",
    "26": "ashe-tables-26",
    "27": "ashe-tables-27-and-28",
    "28": "ashe-tables-27-and-28",
}

provisional_or_revised_lookup = {'p': 'provisional', 'r': 'revised'}

class AsheCombiner:
    def __init__(self, dataset, v4, **kwargs):
        # class for combining a single year ashe v4 with the most recent v4 from CMD
        # will only work for ashe time-series edition datasets
        self.dataset = dataset
        self.edition = "time-series" # only combining time series editions
        self.v4 = v4
        self.combiner_script = "latest_version.py"

        acceptable_datasets = [
            "ashe-tables-3", "ashe-table-5", "ashe-tables-9-and-10", "ashe-tables-11-and-12", 
            "ashe-tables-20", "ashe-tables-25", "ashe-tables-26"
            ]
        
        if self.dataset not in acceptable_datasets:
            print(f"{self.dataset} is not in the list of time-series editions, does not need to be combined")
            return
        
        if "year_of_data" in kwargs.keys():
            self.year_of_data = kwargs["year_of_data"]

        print(f"Running AsheCombiner on {self.dataset}")
        self.run_combiner()

    def run_combiner(self):
        self._write_latest_version_script()
        self._get_latest_version()
        self._del_latest_version_script()
        self._combine_data()

    def _write_latest_version_script(self):
        # getting the script
        module_url = f"{TRANSFORM_URL}/modules/latest-version/module.py"
        module_r = requests.get(module_url, verify=verify)
        if module_r.status_code != 200:
            raise Exception(f"{module_url} raised a {module_r.status_code} error")
        
        # writing the script
        module_script = module_r.text
        with open(self.combiner_script, "w") as f:
            f.write(module_script)
            f.close()
            print(f"Ashe combiner script wrote as {self.combiner_script}")
        return
    
    def _del_latest_version_script(self):
        # deletes the written transform and requirements scripts
        os.remove(self.combiner_script)
        print(f"Ashe combiner script removed - {self.combiner_script}")

    def _get_latest_version(self):
        from latest_version import get_latest_version
        self.downloaded_df = get_latest_version(self.dataset, self.edition)
        self.downloaded_df = self.downloaded_df.rename(columns={'V4_2': 'v4_2'})
        del sys.modules['latest_version']
        return 
    
    def _combine_data(self):
        df = pd.read_csv(self.v4, dtype=str)
        try:
            self.year_of_data
        except:
            year_from_df = df['Time'].unique()
            assert len(year_from_df) == 1, f"newly created v4 has {len(year_from_df)} time dimensions, should have 1"
            self.year_of_data = year_from_df[0]

        downloaded_df = self.downloaded_df[self.downloaded_df['Time'] != self.year_of_data]
        combined_df = pd.concat([downloaded_df, df])
        combined_df.to_csv(self.v4, index=False)
        del downloaded_df, df, combined_df, self.downloaded_df
        return

class AsheTransform:
    def __init__(self, dataset, **kwargs):
        self.dataset = dataset # dataset is table number
        
        # assign source files
        source_files = glob.glob(f"*")
        source_files = [file for file in source_files if not os.path.isdir(file)] # ignoring any directories
        source_files = [file for file in source_files if '.py' not in file] # ignoring py files
        source_files = [file for file in source_files if '__' not in file] # ignoring pycache
        source_files = [file for file in source_files if '.json' not in file] # ignoring json
        source_files = [file for file in source_files if '.md' not in file] # ignoring README 
        if len(source_files) == 1 and os.path.isdir(source_files[0]):
            self.source_files = glob.glob(f"{source_files[0]}/*")
        else:
            self.source_files = source_files
        
        self.transform_url = f"{TRANSFORM_URL}/ashe/{self.dataset}/main.py"
        self.requirements_url = f"{TRANSFORM_URL}/ashe/{self.dataset}/requirements.txt"
        self.transform_script = "temp_transform_script.py"
        self.requirements_dict = {} # will be empty if no requirements needed

        self.year_of_data = kwargs['year_of_data']

        #########
        # to be used if running locally
        self.path_to_local_transforms = ".."
        self.transform_location = f"{self.path_to_local_transforms}/cmd-transforms/ashe/{self.dataset}/main.py"
        self.requirements_location = f"{self.path_to_local_transforms}/cmd-transforms/ashe/{self.dataset}/requirements.txt"
        
    def _write_transform(self):
        # getting transform script
        r = requests.get(self.transform_url, verify=verify)
        if r.status_code == 404:
            raise Exception(f"{self.transform_url} raised a 404 error, does the transform exist for '{self.dataset}' on github")
        elif r.status_code != 200:
            raise Exception(f"{self.transform_url} raised a {r.status_code} error")
        
        # writing transform script
        script = r.text
        with open(self.transform_script, "w") as f:
            f.write(script)
            f.close()
            print(f"transform script wrote as {self.transform_script}")
        
        # getting any requirements
        r = requests.get(self.requirements_url, verify=verify)
        if r.status_code == 200:
            requirements = r.text
            requirements = requirements.strip().split("\n")
    
            for module in requirements:
                module_url = f"{TRANSFORM_URL}/modules/{module}/module.py"
                module_r = requests.get(module_url, verify=verify)
                if module_r.status_code != 200:
                    raise Exception(f"{module_url} raised a {module_r.status_code} error")
    
                module_script = module_r.text
                self.requirements_dict[module] = module_script
               
        # writing any requirements
        if self.requirements_dict: # if requirements not empty
            for module in self.requirements_dict:
                file_name = f"{module}.py".replace("-", "_")
                file_name = f"{file_name}"
                module_script = self.requirements_dict[module]
                with open(file_name, "w") as f:
                    f.write(module_script)
                    f.close()
                print(f"module script wrote as {file_name}")

    def _write_transform_local(self):
        # used for running local transforms
        # getting transform script
        print("Running local transform")
        with open(self.transform_location, "r") as f: 
            script = f.read()
            f.close()
        
        # writing transform script
        with open(self.transform_script, "w") as f:
            f.write(script)
            f.close()
            print(f"transform script wrote as {self.transform_script}")

        # getting any requirements
        if os.path.exists(self.requirements_location):
            with open(self.requirements_location, "r") as f:
                requirements = f.read()
                f.close()
            requirements = requirements.strip().split("\n")
        else:
            requirements = []
        
        for module in requirements:
            module_location = f"{self.path_to_local_transforms}/cmd-transforms/modules/{module}/module.py"
            with open(module_location, "r") as f:
                module_script = f.read()
                f.close()
            self.requirements_dict[module] = module_script
            
        # writing any requirements
        if self.requirements_dict: # if requirements not empty
            for module in self.requirements_dict:
                file_name = f"{module}.py".replace("-", "_")
                file_name = f"{file_name}"
                module_script = self.requirements_dict[module]
                with open(file_name, "w") as f:
                    f.write(module_script)
                    f.close()
                print(f"module script wrote as {file_name}")
         
    
    def _del_transform(self):
        # deletes the written transform and requirements scripts
        os.remove(self.transform_script)
        print(f"{self.transform_script} removed")
        for module in self.requirements_dict:
            file_name = f"{module}.py".replace("-", "_")
            file_name = f"{file_name}"
            os.remove(file_name)
            print(f"{file_name} removed")
    
    def run_transform(self):
        # runs the transform 
        
        # write the transform and any requirements
        self._write_transform()
        
        # import transform
        sys.path.append(f"")
        print(f"Running transform on: {self.dataset}")
        from temp_transform_script import transform
        # catch any errors in the transform
        try:
            self.transform_output = transform(self.source_files, year_of_data=self.year_of_data)
        except Exception as e:
            print(f"Error in transform - {self.dataset}")
            self._del_transform() # del scripts to avoid hangover
            raise Exception(e)
        # del scripts
        self._del_transform()
        del sys.modules['temp_transform_script'] # causes issue with multipart transform if not deleted

    def run_transform_local(self):
        # runs the transform 
        
        # write the transform and any requirements
        self._write_transform_local()
        
        # import transform
        sys.path.append(f"")
        print(f"Running transform on: {self.dataset}")
        from temp_transform_script import transform
        # catch any errors in the transform
        try:
            self.transform_output = transform(self.source_files, year_of_data=self.year_of_data)
        except Exception as e:
            print(f"Error in transform - {self.dataset}")
            self._del_transform() # del scripts to avoid hangover
            raise Exception(e)
        # del scripts
        self._del_transform()
        del sys.modules['temp_transform_script'] # causes issue with multipart transform if not deleted

class AsheSourceData:
    def __init__(self, table_number, year_of_data, provisional_or_revised, **kwargs):
        self.landing_page_json = "supporting_files/landing_pages.json"
        # get landing pages from landing_pages.json
        with open(self.landing_page_json) as f:
            self.page_details = json.load(f)
        
        self.table_number = table_number
        self.year_of_data = year_of_data
        self.provisional_or_revised = provisional_or_revised.lower()
        self.ons_landing_page = "https://www.ons.gov.uk"
        
        assert provisional_or_revised in ('revised', 'provisional'), f"must be 'provisional' or 'revised' not {self.provisional_or_revised}"

        self.downloaded_files = []

        # get user-agent
        email = os.getenv('FLORENCE_EMAIL')
        if not email:
            email = 'cmd@ons.gov.uk' # generic cmd email

        self.user_agent = {"User-Agent": f"cmd-run-transforms/Version1.0.0 ONS {email}"}
        
    def get_source_files(self):
        if self.table_number not in self.page_details['ashe'].keys():
            raise NotImplemented(f"no ashe landing page available for {self.table_number}")

        # downloads and writes source files
        self.landing_pages = self.page_details['ashe'][self.table_number]["pages"]

        for page in self.landing_pages:
            self._download(page)

        return self.downloaded_files
    
    def _download(self, page):
        results = self._get_results(page)
        elements = results.find_all("div", class_="inline-block--md margin-bottom-sm--1")
        
        # find correct link
        for text in elements:
            if self.year_of_data in str(text):
                if f'{self.provisional_or_revised}' in str(text):
                    element = text
                    break
        try: element
        except: raise Exception(f"could not find source data for {self.year_of_data}, {self.provisional_or_revised}")
        
        link = str(element).split("href=")[-1].split(">")[0].strip('"')
        download_link = f"{self.ons_landing_page}{link}"
        
        # download the file
        source_file = download_link.split('/')[-1]
        r = requests.get(download_link, headers=self.user_agent, verify=verify)
        with open(f"{source_file}", 'wb') as output:
            output.write(r.content)
        print(f"written {source_file}")
            
        # unzip if needed
        if source_file.endswith(".zip"):
            with zipfile.ZipFile(f"{source_file}", 'r') as zip_ref:
                extracted_file = zip_ref.namelist()
                self.downloaded_files.append(extracted_file[0])
                zip_ref.extractall("")
            os.remove(f"{source_file}")
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
    