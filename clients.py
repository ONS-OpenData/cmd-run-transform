import requests, os, glob, sys, json, datetime, time, platform, zipfile
from bs4 import BeautifulSoup


TRANSFORM_URL = "https://raw.github.com/ONS-OpenData/cmd-transforms/master"
PROXIES = {} # to be filled in with ONS proxies if using ONS machine


class Transform:
    def __init__(self, dataset, **kwargs):
        """
        kwargs - location, source_files
        """
        self.dataset = dataset
            
        # used if want to write files to a certain place
        if 'location' in kwargs.keys() and kwargs['location'] != '':
            location = kwargs['location']
            if not location.endswith('/'):
                location += '/'
        else:
            location = ''
        self.location = location
        
        if 'source_files' in kwargs.keys() and kwargs['source_files'] != '':
            source_files = kwargs['source_files']
            # source files must be a list for consistency
            if type(source_files) == str:
                source_files = [source_files]
        else:
            # if no source files provided, will gather all files in directory that
            # are not .py files
            source_files = glob.glob(f"{location}*")
            source_files = [file for file in source_files if '.py' not in file] # ignoring py files
            source_files = [file for file in source_files if '__' not in file] # ignoring pycache
            source_files = [file for file in source_files if '.json' not in file] # ignoring json 
        self.source_files = source_files

        system_type = platform.system()
        # verify=False if on windows - hack for if using network machine
        if system_type == "Windows":
            self.verify = False
            requests.packages.urllib3.disable_warnings() # disables the insure request warnings
        else:
            self.verify = True
        
        self.transform_url = f"{TRANSFORM_URL}/{self.dataset}/main.py"
        self.requirements_url = f"{TRANSFORM_URL}/{self.dataset}/requirements.txt"
        self.transform_script = f"{self.location}temp_transform_script.py"
        self.requirements_dict = {} # will be empty if no requirements needed
        
        
    def _write_transform(self):
        # getting transform script
        r = requests.get(self.transform_url, verify=self.verify, proxies=PROXIES)
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
        r = requests.get(self.requirements_url, verify=self.verify, proxies=PROXIES)
        if r.status_code == 200:
            requirements = r.text
            requirements = requirements.strip().split("\n")
    
            for module in requirements:
                module_url = f"{TRANSFORM_URL}/modules/{module}/module.py"
                module_r = requests.get(module_url, verify=self.verify, proxies=PROXIES)
                if module_r.status_code != 200:
                    raise Exception(f"{module_url} raised a {module_r.status_code} error")
    
                module_script = module_r.text
                self.requirements_dict[module] = module_script
               
        # writing any requirements
        if self.requirements_dict: # if requirements not empty
            for module in self.requirements_dict:
                file_name = f"{module}.py".replace("-", "_")
                file_name = f"{self.location}{file_name}"
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
            file_name = f"{self.location}{file_name}"
            os.remove(file_name)
            print(f"{file_name} removed")
            
            
    def run_transform(self):
        # runs the transform 
        
        # write the transform and any requirements
        self._write_transform()
        
        # import transform
        sys.path.append(f"{self.location}")
        print(f"Running transform on: {self.dataset}")
        from temp_transform_script import transform
        # catch any errors in the transform
        try:
            self.transform_output = transform(self.source_files, location=self.location)
        except Exception as e:
            print("Error in transform")
            self._del_transform() # del scripts to avoid hangover
            raise Exception(e)
        # del scripts
        self._del_transform()


class SourceData:
    def __init__(self, dataset, **kwargs):
        if 'location' in kwargs.keys() and kwargs['location'] != '':
            location = kwargs['location']
            if not location.endswith('/'):
                location += '/'
        else:
            location = ''
        self.location = location
        
        self.landing_page_json = f"{self.location}landing_pages.json"
        # get landing pages from landing_pages.json
        with open(self.landing_page_json) as f:
            self.page_details = json.load(f)
        
        self.dataset = dataset
        self.ons_landing_page = "https://www.ons.gov.uk"
        
        # get todays date
        todays_date = datetime.datetime.now()
        self.todays_date = datetime.datetime.strftime(todays_date, "%d %B %Y")
        
        system_type = platform.system()
        # verify=False if on windows - hack for if using network machine
        if system_type == "Windows":
            self.verify = False
            requests.packages.urllib3.disable_warnings() # disables the insure request warnings
        else:
            self.verify = True

        self.downloaded_files = []
        
        
    def get_source_files(self):
        if self.dataset not in self.page_details.keys():
            print(f"no landing page available for {self.dataset}, will use files from current directory")
            return ""

        # downloads and writes source files to 'location'
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
        
        link = str(element).split("href=")[-1].split(">")[0].strip('"')
        download_link = f"{self.ons_landing_page}{link}"
        
        # download the file
        source_file = download_link.split('/')[-1]
        r = requests.get(download_link, verify=self.verify, proxies=PROXIES)
        with open(f"{self.location}{source_file}", 'wb') as output:
            output.write(r.content)
        print(f"written {source_file}")
            
        # unzip if needed
        if source_file.endswith(".zip"):
            with zipfile.ZipFile(f"{self.location}{source_file}", 'r') as zip_ref:
                extracted_file = zip_ref.namelist()
                self.downloaded_files.append(extracted_file[0])
                zip_ref.extractall(f"{self.location}")
            os.remove(f"{self.location}{source_file}")
            print(f"extracted {source_file}")
        else:
            self.downloaded_files.append(source_file)

            
    def _get_results(self, page):
        landing_page = f"{self.ons_landing_page}{page}"
        r = requests.get(landing_page, verify=self.verify, proxies=PROXIES)
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
             

class TransformLocal:
    # used to run the transforms that are saved locally
    # useful when changes are made to transform
    # will only work if the transformed are saved locally
    def __init__(self, dataset, **kwargs):
        """
        kwargs - location, source_files
        """
        self.dataset = dataset
        self.location = ''
        
        if 'source_files' in kwargs.keys() and kwargs['source_files'] != '':
            source_files = kwargs['source_files']
            # source files must be a list for consistency
            if type(source_files) == str:
                source_files = [source_files]
        else:
            # if no source files provided, will gather all files in directory that
            # are not .py files
            source_files = glob.glob(f"{self.location}*")
            source_files = [file for file in source_files if '.py' not in file] # ignoring py files
            source_files = [file for file in source_files if '__' not in file] # ignoring pycache
            source_files = [file for file in source_files if '.json' not in file] # ignoring json 
        self.source_files = source_files
        
        self.transform_location = f"../cmd-transforms/{self.dataset}/main.py"
        self.requirements_location = f"../cmd-transforms/{self.dataset}/requirements.txt"
        self.transform_script = f"{self.location}temp_transform_script.py"
        self.requirements_dict = {} # will be empty if no requirements needed
        
        
    def _write_transform(self):
        # getting transform script
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
            module_location = f"../cmd-transforms/modules/{module}/module.py"
            with open(module_location, "r") as f:
                module_script = f.read()
                f.close()
            self.requirements_dict[module] = module_script
            
        # writing any requirements
        if self.requirements_dict: # if requirements not empty
            for module in self.requirements_dict:
                file_name = f"{module}.py".replace("-", "_")
                file_name = f"{self.location}{file_name}"
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
            file_name = f"{self.location}{file_name}"
            os.remove(file_name)
            print(f"{file_name} removed")
            
            
    def run_transform(self):
        # runs the transform 
        
        # write the transform and any requirements
        self._write_transform()
        
        # import transform
        sys.path.append(f"{self.location}")
        print(f"Running transform on: {self.dataset}")
        from temp_transform_script import transform
        # catch any errors in the transform
        try:
            self.transform_output = transform(self.source_files, location=self.location)
        except Exception as e:
            print("Error in transform")
            self._del_transform() # del scripts to avoid hangover
            raise Exception(e)
        # del scripts
        self._del_transform()


class Base:
    def __init__(self, **kwargs):
        # defining url's
        self.url = "https://publishing.dp-prod.aws.onsdigital.uk"
        self.dataset_url = f"{self.url}/dataset"
        self.collection_url = f"{self.dataset_url}/collection"
        self.recipe_url = f"{self.url}/recipes"
        self.upload_url = f"{self.url}/upload"
        
        # defining kwargs
        if 'credentials' in kwargs.keys() and kwargs['credentials'] != '':
            credentials = kwargs['credentials']
        else:
            # looks for credentials file in working directory
            credentials = "florence-details.json"
            if not os.path.exists(credentials):
                raise Exception("No credentials file was given or could be found in working directory")

        system_type = platform.system()
        # verify=False if on windows - hack for if using network machine
        if system_type == "Windows":
            self.verify = False
            requests.packages.urllib3.disable_warnings() # disables the insure request warnings
        else:
            self.verify = True
            
        # assigning given variables
        self.credentials = credentials
        
        # assigning futher variables
        self._get_access_token()
        self.headers = f"X-Florence-Token:{self.access_token}"
        
    
    def _get_access_token(self):
        # gets florence access token
        try: # so that token isn't generate for each __init__
            if self.access_token: 
                pass
        except:
            login_url = f"{self.dataset_url}/login"
            with open(self.credentials, 'r') as json_file:
                credentials_json = json.load(json_file)
        
            email = credentials_json['email']
            password = credentials_json['password']
            login = {"email":email, "password":password}
            
            command = f'curl -X POST -d "{login}" {login_url}'
            stream = os.popen(command)
            access_token = stream.read().strip("'").strip('"')
            self.access_token = access_token
            """
            r = requests.post(login_url, json=login, verify=self.verify)
            if r.status_code == 200:
                access_token = r.text.strip('"')
                self.access_token = access_token
            else:
                raise Exception(f"Token not created, returned a {r.status_code} error")
            """
            
    
    def _assign(self, upload_dict):
        # assigns the upload_dict as a class variable
        # but checks if it has been assigned already
        assert type(upload_dict) == dict, f"upload_dict must be a dict not {type(upload_dict)}"
        try:
            if self.upload_dict:
                pass
        except:
            self.upload_dict = upload_dict

    
    def curl_request(self, url, request_type, **kwargs):
        # request_type - GET/POST/PUT
        # kwargs will be data required for POST/PUT
        if request_type.lower() == 'get':
            response_dict = self._get_curl_request(url)
        elif request_type.lower() == 'post':
            response_dict = self._post_curl_request(url, **kwargs)
        elif request_type.lower() == 'put':
            response_dict = self._put_curl_request(url, **kwargs)
        
        return response_dict
    
    
    def _get_curl_request(self, url):
        # based on all responses being json
        command = f'curl -H "{self.headers}" {url}'
        stream = os.popen(command)
        response = stream.read()
        response_dict = json.loads(response)
        return response_dict
    

    def _post_curl_request(self, url, **kwargs):
        if 'json' in kwargs.keys():
            data = kwargs['json']
            tmp_file = 'data-for-request.json'
            with open(tmp_file, 'w') as f:
                f.write(json.dumps(data))
                f.close()
                
            command = f'curl -X POST -H "{self.headers}" {url} -d @{tmp_file}'
            stream = os.popen(command)
            response = stream.read()
            response_dict = json.loads(response)
            # del tmp file
            os.remove(tmp_file)
            return response_dict
    
    
    def _put_curl_request(self, url, **kwargs):
        if 'json' in kwargs.keys():
            data = kwargs['json']
            tmp_file = 'data-for-request.json'
            with open(tmp_file, 'w') as f:
                f.write(json.dumps(data))
                f.close()
                
            command = f'curl -X PUT -H "{self.headers}" {url} -d @{tmp_file}'
            stream = os.popen(command)
            response = stream.read()
            # del tmp file
            os.remove(tmp_file)
            return response
                

class MetadataClient:    
    def get_metadata(self, dataset_id, edition):
        try:
            if self.upload_dict[dataset_id]['metadata_dict']:
                pass
        except:
            self._get_lastest_metadata(dataset_id, edition)
            
        
    def _get_lastest_metadata(self, dataset_id, edition):
        """
        Pulls latest csvw
        """
        editions_url = f"https://api.beta.ons.gov.uk/v1/datasets/{dataset_id}/editions/{edition}/versions"
        items = requests.get(f"{editions_url}?limit=1000", verify=self.verify, proxies=PROXIES).json()['items']
        # get latest version number
        latest_version_number = items[0]['version']
        assert latest_version_number == len(items), f'Get_Latest_Version for /{dataset_id}/editions/{edition} - number of versions does not match latest version number'
        # get latest version URL
        url = f"{editions_url}/{str(latest_version_number)}"
        # get latest version data
        latest_version = requests.get(url, verify=self.verify, proxies=PROXIES).json()
        csvw_response = requests.get(latest_version['downloads']['csvw']['href'], verify=self.verify, proxies=PROXIES)
        if csvw_response.status_code != 200:
            return f"csvw download failed with a {csvw_response.status_code} error"
        self.csvw_dict = json.loads(csvw_response.text)
        self._csvw_metadata_parser(dataset_id)

    
    def _csvw_metadata_parser(self, dataset_id):
        """
        converts a csv_w created from CMD into the required metatdata format for the API's
        """
        # Not all fields from the csv_w are included here
        metadata_dict = {}
    
        # split into 3
        metadata_dict["metadata"] = {}
        metadata_dict["dimension_data"] = {}
        metadata_dict["usage_notes"] = {}
    
        # currently hacky..
        if "dct:title" in self.csvw_dict.keys():
            metadata_dict["metadata"]["title"] = self.csvw_dict["dct:title"]
            
        if "dct:description" in self.csvw_dict.keys():
            metadata_dict["metadata"]["description"] = self.csvw_dict["dct:description"]
    
        # TODO - more than one contact?
        if "dcat:contactPoint" in self.csvw_dict.keys():
            metadata_dict["metadata"]["contacts"] = [{}]
            if "vcard:fn" in self.csvw_dict["dcat:contactPoint"][0].keys():
                metadata_dict["metadata"]["contacts"][0]["name"] = self.csvw_dict["dcat:contactPoint"][0]["vcard:fn"]
            if "vcard:tel" in self.csvw_dict["dcat:contactPoint"][0].keys():
                metadata_dict["metadata"]["contacts"][0]["telephone"] = self.csvw_dict["dcat:contactPoint"][0]["vcard:tel"]
            if "vcard:email" in self.csvw_dict["dcat:contactPoint"][0].keys():
                metadata_dict["metadata"]["contacts"][0]["email"] = self.csvw_dict["dcat:contactPoint"][0]["vcard:email"]
    
        if "dct:accrualPeriodicity" in self.csvw_dict.keys():
            metadata_dict["metadata"]["release_frequency"] = self.csvw_dict["dct:accrualPeriodicity"]
    
        if "tableSchema" in self.csvw_dict.keys():
            dimension_metadata = self.csvw_dict["tableSchema"]["columns"]
            metadata_dict["dimension_data"] = self._dimension_metadata_from_csvw(dimension_metadata)
            metadata_dict["metadata"]["unit_of_measure"] = self._get_unit_of_measure(dimension_metadata)
        
        if "notes" in self.csvw_dict.keys():
            metadata_dict["usage_notes"] = self._usage_notes_from_csvw(self.csvw_dict["notes"])
            
        print("csvw_metadata_parser completed parsing")
        self.upload_dict[dataset_id]['metadata_dict'] = metadata_dict
        del self.csvw_dict # saving memory hopefully
        
    
    def _dimension_metadata_from_csvw(self, dimension_metadata):
        '''
        Converts dimension metadata from csv-w to usable format for CMD APIs
        Takes in csv_w['tableSchema']['columns'] - is a list
        Returns a dict of dicts
        '''
        assert type(dimension_metadata) == list, "dimension_metadata should be a list"
        
        # first item in list should be observations
        # quick check
        assert dimension_metadata[0]["titles"].lower().startswith("v4_"), "csv_w[tableSchema][columns][0] is not the obs column"
    
        # number of data marking columns
        number_of_data_markings = int(dimension_metadata[0]["titles"].split("_")[-1])
        
        wanted_dimension_metadata = dimension_metadata[2 + number_of_data_markings::2]
        dimension_metadata_for_cmd = {}
        
        for item in wanted_dimension_metadata:
            name = item["titles"]
            label = item["name"]
            description = item["description"]
            dimension_metadata_for_cmd[name] = {"label": label, "description": description}
            
        return dimension_metadata_for_cmd
    
    
    def _get_unit_of_measure(self, dimension_metadata):
        '''
        Pulls unit_of_measure from dimension metadata
        '''
        assert type(dimension_metadata) == list, "dimension_metadata should be a list"
        
        # first item in list should be observations
        # quick check
        assert dimension_metadata[0]["titles"].lower().startswith("v4_"), "csv_w[tableSchema][columns][0] is not the obs column"
        if "name" in dimension_metadata[0].keys():
            unit_of_measure = dimension_metadata[0]["name"]
        else:
            unit_of_measure = ""
        
        return unit_of_measure
    
    
    def _usage_notes_from_csvw(self, usage_notes):
        '''
        Pulls usage notes from csv-w to usable format for CMD APIs
        Takes in csv_w['notes'] - is a list
        Creates a list of dicts
        '''
        assert type(usage_notes) == list, "usage_notes should be a list"
        
        usage_notes_list = []
        for item in usage_notes:
            single_usage_note = {}
            single_usage_note["title"] = item["type"]
            single_usage_note["note"] = item["body"]
            usage_notes_list.append(single_usage_note)
            
        return usage_notes_list
    
    
class CollectionClient(Base):   
    def __init__(self, upload_dict, **kwargs):
        Base.__init__(self, **kwargs)
        self._assign(upload_dict)


    def create_collection(self):
        for dataset_id in self.upload_dict.keys():
            #requests.post(self.collection_url, headers=self.headers, json={"name": self.upload_dict[dataset_id]['collection_name']}, verify=self.verify)
            self.curl_request(self.collection_url, 'post', json={"name": self.upload_dict[dataset_id]['collection_name']})
            # does not return a 200, so check the collection was created, which
            # also finds the collection_id
            self._check_collection_exists(dataset_id)
            time.sleep(1)
            
    
    def add_to_collection(self):
        for dataset_id in self.upload_dict.keys():
            self._add_dataset_to_collection(dataset_id)
            self._add_dataset_version_to_collection(dataset_id)

    
    def _check_collection_exists(self, dataset_id):
        # TODO - check to make sure collection is empty
        self._get_collection_id(dataset_id)
        response_dict = self.curl_request(f"{self.collection_url}/{self.upload_dict[dataset_id]['collection_id']}", 'get')
        if 'message' in response_dict.keys():
            raise Exception(f"Collection '{self.upload_dict[dataset_id]['collection_name']}' not created")
        """
        r = requests.get(f"{self.collection_url}/{self.upload_dict[dataset_id]['collection_id']}", headers=self.headers, verify=self.verify)
        if r.status_code != 200:
            raise Exception(f"Collection '{self.upload_dict[dataset_id]['collection_name']}' not created - returned a {r.status_code} error")
        """
    
    def _get_collection_id(self, dataset_id):
        self._get_all_collections()
        for collection in self.all_collections:
            if collection["name"] == self.upload_dict[dataset_id]['collection_name']:
                collection_id = collection["id"]
                break
    
        self.upload_dict[dataset_id]['collection_id'] = collection_id

    
    def _get_all_collections(self):
        try:
            if self.all_collections:
                pass
        except:
            response_dict = self.curl_request(f"{self.collection_url}s", 'get')
            self.all_collections = response_dict
            """
            r = requests.get(f"{self.collection_url}s", headers=self.headers, verify=self.verify)
            collection_list = r.json()
            self.all_collections = collection_list
            """

    
    def _add_dataset_to_collection(self, dataset_id):
        # Adds dataset landing page to a collection
        url = f"{self.collection_url}s/{self.upload_dict[dataset_id]['collection_id']}/datasets/{dataset_id}"
        payload = {"state": "Complete"}
        
        self.curl_request(url, 'put', data=payload)
        print(f"{dataset_id} - Dataset landing page added to collection")
        
        """
        r = requests.put(
            f"{self.collection_url}s/{self.upload_dict[dataset_id]['collection_id']}/datasets/{dataset_id}", 
            headers=self.headers, 
            json={"state": "Complete"},
            verify=self.verify
            )

        if r.status_code == 200:
            print(f"{dataset_id} - Dataset landing page added to collection")
        else:
            raise Exception(f"{dataset_id} - Dataset landing page not added to collection - returned a {r.status_code} error")
        """

    def _add_dataset_version_to_collection(self, dataset_id):
        # Adds dataset version to a collection
        url = f"{self.collection_url}s/{self.upload_dict[dataset_id]['collection_id']}/datasets/{dataset_id}/editions/{self.upload_dict[dataset_id]['edition']}/versions/{self.upload_dict[dataset_id]['version_number']}"
        payload = {"state": "Complete"}
        
        self.curl_request(url, 'put', json=payload)
        print(f"{dataset_id} - Dataset version '{self.upload_dict[dataset_id]['version_number']}' added to collection")
        
        """
        r = requests.put(
            f"{self.collection_url}s/{self.upload_dict[dataset_id]['collection_id']}/datasets/{dataset_id}/editions/{self.upload_dict[dataset_id]['edition']}/versions/{self.upload_dict[dataset_id]['version_number']}",
            headers=self.headers,
            json={"state": "Complete"},
            verify=self.verify
        )

        if r.status_code == 200:
            print(f"{dataset_id} - Dataset version '{self.upload_dict[dataset_id]['version_number']}' added to collection")
        else:
            raise Exception(f"{dataset_id} - Dataset version '{self.upload_dict[dataset_id]['version_number']}' not added to collection - returned a {r.status_code} error")
        """

class RecipeClient(Base):
    def __init__(self, upload_dict, **kwargs):
        Base.__init__(self, **kwargs)
        self._assign(upload_dict)
        # assigning given variables
        self.all_recipes = None

        
    def _get_all_recipes(self):
        """
        Gets all recipes from the recipe api.
        """
        if self.all_recipes:
            return self.all_recipes

        response_dict = self.curl_request(f"{self.recipe_url}?limit=1000", 'get')
        
        # check to make sure correct dict was returned
        assert 'count' in response_dict.keys(), "recipe API was not returned"
        self.all_recipes = response_dict
        
        """
        r = requests.get(f"{self.recipe_url}?limit=1000", headers=self.headers, verify=self.verify)

        if r.status_code == 200:
            self.all_recipes = r.json()
        else:
            raise Exception(f"Recipe API returned a {r.status_code} error")
        """
    
    
    def _check_recipe_exists(self, dataset_id):
        """
        Checks to make sure a recipe exists for dataset_id
        Returns nothing if recipe exists, raise an error if not
        Uses self.get_all_recipes()
        """
        self._get_all_recipes()

        # create a list of all existing dataset ids
        dataset_id_list = []
        for item in self.all_recipes["items"]:
            # hack around incorrect recipe in database
            if item['id'] == 'b944be78-f56d-409b-9ebd-ab2b77ffe187':
                continue
            dataset_id_list.append(item["output_instances"][0]["dataset_id"])
        if dataset_id not in dataset_id_list:
            raise Exception(f"Recipe does not exist for {dataset_id}")
    
    
    def get_recipe(self):
        """
        Returns recipe for specific dataset
        Uses self.get_all_recipes()
        dataset_id is the dataset_id from the recipe
        """ 
        # iterate through dataset_ids in upload_dict
        for dataset_id in self.upload_dict.keys():
            self._check_recipe_exists(dataset_id)
            # iterate through recipe api to find correct dataset_id
            for item in self.all_recipes["items"]:
                # hack around incorrect recipe in database
                if item['id'] == 'b944be78-f56d-409b-9ebd-ab2b77ffe187':
                    continue
                if dataset_id == item["output_instances"][0]["dataset_id"]:
                    self.upload_dict[dataset_id]['dataset_recipe'] = item
                    self.upload_dict[dataset_id]['recipe_id'] = self.upload_dict[dataset_id]['dataset_recipe']["id"]


class DatasetClient(Base, MetadataClient):
    def __init__(self, upload_dict, **kwargs):
        Base.__init__(self, **kwargs)
        self._assign(upload_dict)

        
    def updating_instance(self):
        ''' 
        gets previous metadata and attaches to instance
        updates state of instance to create version number
        '''
        for dataset_id in self.upload_dict.keys():
            # get metadata from previous release
            self._get_lastest_metadata(dataset_id, self.upload_dict[dataset_id]['edition'])
            
            # updating general metadata
            self._update_metadata(dataset_id)
            
            # assigning instance a version number
            self._create_new_version_from_instance(dataset_id)
        
    
    def adding_metadata(self):
        for dataset_id in self.upload_dict.keys():
            self._update_dimensions(dataset_id)
            self._update_usage_notes(dataset_id)
            
        
    def _get_latest_job(self):
        # Returns whole content of dataset api /jobs endpoint
        
        dataset_jobs_api_url = f"{self.dataset_url}/jobs"

        response_dict = self.curl_request(dataset_jobs_api_url, 'get')
        # check to make sure correct dict was returned
        assert 'total_count' in response_dict.keys(), "jobs API not returned"
        total_count = response_dict['total_count']
        last_job_number = total_count - 1 # 0 indexing
        new_url = f"{dataset_jobs_api_url}?offset={last_job_number}"
        response_dict = self.curl_request(new_url, 'get')
        # check to make sure correct dict was returned
        assert 'total_count' in response_dict.keys(), "latest job from API not returned"
        self.lastest_job = response_dict['items'][0]
        
        """
        r = requests.get(dataset_jobs_api_url, headers=self.headers, verify=self.verify)
        if r.status_code == 200:
            whole_dict = r.json()
            total_count = whole_dict["total_count"]
            last_job_number = total_count - 1 # 0 indexing
            new_url = f"{dataset_jobs_api_url}?limit=1&offset={last_job_number}"
            new_dict = requests.get(new_url, headers=self.headers, verify=self.verify).json()
            self.lastest_job = new_dict['items'][0]
        else:
            raise Exception(
                f"/dataset/jobs API returned a {r.status_code} error"
            )
        """
    
    def _get_latest_job_info(self, dataset_id):
        """
        Returns latest job id and recipe id and instance id
        Uses Get_Dataset_Jobs_Api()
        """
        self._get_latest_job()
        self.upload_dict[dataset_id]['job_id'] = self.lastest_job["id"]
        self.upload_dict[dataset_id]['job_recipe_id'] = self.lastest_job["recipe"]  # to be used as a quick check
        self.upload_dict[dataset_id]['instance_id'] = self.lastest_job["links"]["instances"][0]["id"]
        
    
    def _get_recipe_id(self, dataset_id):
        # gets recipe_id if it has not been set
        # only used when Dataset client is used on its own
        try:
            if self.upload_dict[dataset_id]['recipe_id']:
                pass
        except:
            response_dict = self.curl_request(f"{self.recipe_url}?limit=1000", 'get')
            # check to make sure correct dict was returned
            assert 'total_count' in response_dict.keys(), "recipe API not returned"

            for item in response_dict["items"]:
                # hack around incorrect recipe in database
                if item['id'] == 'b944be78-f56d-409b-9ebd-ab2b77ffe187':
                    continue
                if dataset_id == item["output_instances"][0]["dataset_id"]:
                    self.upload_dict[dataset_id]['dataset_recipe'] = item
                    self.upload_dict[dataset_id]['recipe_id'] = self.upload_dict[dataset_id]['dataset_recipe']["id"]
                    return
            else:
                raise Exception(f"Unable to find recipe for dataset id {self.dataset_id}")

            """
            r = requests.get(f"{self.recipe_url}?limit=1000", headers=self.headers, verify=self.verify)

            if r.status_code == 200:
                all_recipes = r.json()
            else:
                raise Exception(f"Recipe API returned a {r.status_code} error")
            
            for item in all_recipes["items"]:
                # hack around incorrect recipe in database
                if item['id'] == 'b944be78-f56d-409b-9ebd-ab2b77ffe187':
                    continue
                if dataset_id == item["output_instances"][0]["dataset_id"]:
                    self.upload_dict[dataset_id]['dataset_recipe'] = item
                    self.upload_dict[dataset_id]['recipe_id'] = self.upload_dict[dataset_id]['dataset_recipe']["id"]
                    return
            else:
                raise Exception(f"Unable to find recipe for dataset id {self.dataset_id}")
            """
    
    
    def post_new_job(self):
        """
        Creates a new job in the /dataset/jobs API
        Job is created in state 'created'
        """
        for dataset_id in self.upload_dict.keys():
            try:
                if self.upload_dict[dataset_id]['s3_url']:
                    pass
            except:
                raise ValueError(
                    f"Aborting. s3_url is required for {dataset_id}, can be added manually."
                )
                
            self._get_recipe_id(dataset_id)
        
            payload = {
                "recipe": self.upload_dict[dataset_id]['recipe_id'],
                "state": "created",
                "links": {},
                "files": [
                    {
                        "alias_name": self.upload_dict[dataset_id]['dataset_recipe']['files'][0]['description'], 
                        "url": self.upload_dict[dataset_id]['s3_url']
                    }
                ],
            }
    
            response_dict = self.curl_request(f"{self.dataset_url}/jobs", 'post', json=payload)
            assert 'state' in response_dict.keys(), "new job not created"
            print("Job created successfully")
            
            """
            r = requests.post(f"{self.dataset_url}/jobs", headers=self.headers, json=payload, verify=self.verify)
            if r.status_code == 201:
                print("Job created successfully")
            else:
                raise Exception(f"Job not created, returning status code: {r.status_code}")
            """
    
            # return job ID
            self._get_latest_job_info(dataset_id)
    
            # quick check to make sure newest job id is the correct one
            if self.upload_dict[dataset_id]['job_recipe_id'] != self.upload_dict[dataset_id]['recipe_id']:
                raise Exception(
                    f"New job recipe ID ({self.upload_dict[dataset_id]['job_recipe_id']}) does not match recipe ID used to create new job ({self.upload_dict[dataset_id]['recipe_id']})"
                )
                
            self._update_state_of_job(dataset_id)
            time.sleep(2) # give api a breather
            
    
    def _get_job_info(self, dataset_id):
        dataset_jobs_id_url = f"{self.dataset_url}/jobs/{self.upload_dict[dataset_id]['job_id']}"

        response_dict = self.curl_request(dataset_jobs_id_url, 'get')
        assert 'state' in response_dict.keys(), "/dataset/jobs/{self.job_id} not found"
        self.job_info_dict = response_dict
        
        """
        r = requests.get(dataset_jobs_id_url, headers=self.headers, verify=self.verify)
        if r.status_code == 200:
            self.job_info_dict = r.json()
        else:
            raise Exception(f"/dataset/jobs/{self.job_id} returned error {r.status_code}")
        """
            
    
    def _update_state_of_job(self, dataset_id):
        """
        Updates state of job from created to submitted
        once submitted import process will begin
        """

        updating_state_of_job_url = f"{self.dataset_url}/jobs/{self.upload_dict[dataset_id]['job_id']}"

        updating_state_of_job_json = {}
        updating_state_of_job_json["state"] = "submitted"

        # make sure file is in the job before continuing
        self._get_job_info(dataset_id)

        self.curl_request(updating_state_of_job_url, 'put', json=updating_state_of_job_json)

        """
        if len(self.job_info_dict["files"]) != 0:
            r = requests.put(
                updating_state_of_job_url,
                headers=self.headers,
                json=updating_state_of_job_json,
                verify=self.verify
            )
            if r.status_code != 200:
                raise Exception(f"Unable to update job for {dataset_id} to submitted state")
        else:
            raise Exception(f"Job for {dataset_id} does not have a v4 file!")
        """
            
    
    def get_instance_id(self):
        for dataset_id in self.upload_dict.keys():
            dataset_instance_url = f"{self.dataset_url}/instances?dataset={dataset_id}"
            response_dict = self.curl_request(dataset_instance_url, 'get')
            latest_instance = response_dict['items'][0]
            # check to make sure dates match
            current_date = datetime.datetime.now()
            current_date = datetime.datetime.strftime(current_date, '%Y-%m-%d')
            instance_date = latest_instance['last_updated'].split('T')[0]
            assert current_date == instance_date, f"incorrect instance being picked up.. todays date does not match date instance - {latest_instance['id']}"

            self.upload_dict[dataset_id]['instance_id'] = latest_instance['id']
            print(self.upload_dict[dataset_id]['instance_id'])
        

    def monitor_upload(self):
        for dataset_id in self.upload_dict.keys():
            self.upload_dict[dataset_id]['upload_state'] = ""
            while self.upload_dict[dataset_id]['upload_state'] != "completed":
                time.sleep(30) # checks every 30 seconds
                self.upload_dict[dataset_id]['upload_state'] = self._get_upload_state(dataset_id)
    
    
    def _get_upload_state(self, dataset_id):
        """
        Checks state of an instance
        Returns Bool
        """
        instance_id_url = f"{self.dataset_url}/instances/{self.upload_dict[dataset_id]['instance_id']}"

        response_dict = self.curl_request(instance_id_url, 'get')
        # check to make sure correct dict was returned
        assert 'state' in response_dict.keys(), f"{instance_id_url} was not returned"

        dataset_instance_dict = response_dict
        job_state = dataset_instance_dict["state"]

        """
        r = requests.get(instance_id_url, headers=self.headers, verify=self.verify)
        if r.status_code != 200:
            raise Exception(
                f"{instance_id_url} raised a {r.status_code} error"
            )

        dataset_instance_dict = r.json()
        job_state = dataset_instance_dict["state"]
        """

        if job_state == "created":
            raise Exception(
                f"State of instance is '{job_state}', import process has not been triggered"
            )

        elif job_state == "submitted":
            total_inserted_observations = dataset_instance_dict["import_tasks"][
                "import_observations"
            ]["total_inserted_observations"]
            try:
                total_observations = dataset_instance_dict["total_observations"]
            except:
                error_message = dataset_instance_dict["events"][0]["message"]
                raise Exception(error_message)
            print("Import process is running")
            print(
                f"{total_inserted_observations} out of {total_observations} observations have been imported"
            )

        elif job_state == "completed":
            print(f"{dataset_id} - Instance upload completed!")

        return job_state
    
    
    def _update_metadata(self, dataset_id):
        """
        Updates general metadata for a dataset
        """
        metadata = self.upload_dict[dataset_id]['metadata_dict']['metadata']
        assert type(metadata) == dict, "metadata['metadata'] must be a dict"

        dataset_url = f"{self.dataset_url}/datasets/{dataset_id}"
        
        self.curl_request(dataset_url, 'put', json=metadata)
        print('Metadata updated')

        """
        r = requests.put(dataset_url, headers=self.headers, json=metadata, verify=self.verify)
        if r.status_code != 200:
            print(f"Metadata not updated, returned a {r.status_code} error")
        else:
            print('Metadata updated')
        """
            
    
    def _create_new_version_from_instance(self, dataset_id):
        '''
        Changes state of an instance to edition-confirmed so that it is assigned a version number
        Requires edition name & release date ("2021-07-08T00:00:00.000Z")
        Will currently just use current date as release date
        '''

        instance_url = f"{self.dataset_url}/instances/{self.upload_dict[dataset_id]['instance_id']}"

        current_date = datetime.datetime.now()
        release_date = datetime.datetime.strftime(current_date, '%Y-%m-%dT00:00:00.000Z')

        payload = {
                'edition':self.upload_dict[dataset_id]['edition'], 
                'state':'edition-confirmed', 
                'release_date':release_date 
                }
        
        self.curl_request(instance_url, 'put', json=payload)
        print('Instance state changed to edition-confirmed')
        self._get_version_number(dataset_id)

        """
        r = requests.put(instance_url, headers=self.headers, json={
            'edition':self.upload_dict[dataset_id]['edition'], 
            'state':'edition-confirmed', 
            'release_date':release_date
            }, verify=self.verify
        )

        if r.status_code == 200:
            print('Instance state changed to edition-confirmed')
            self._get_version_number(dataset_id)
        else:
            raise Exception(f"{dataset_id} - Instance state not changed - returned a {r.status_code} error")
        """

    def _get_version_number(self, dataset_id):
        '''
        Gets version number of instance ready to be published from /datasets/instances/{instance_id}
        Only when dataset is in a collection or edition-confirmed state
        Used to find the right version for usage notes or to add version to collection
        Returns version number as string
        '''   

        instance_url = f"{self.dataset_url}/instances/{self.upload_dict[dataset_id]['instance_id']}"

        response_dict = self.curl_request(instance_url, 'get')
        # check to make sure correct dict was returned
        assert 'state' in response_dict.keys(), f"{instance_url} not returned"
        
        instance_dict = response_dict
        self.upload_dict[dataset_id]['version_number'] = instance_dict['version']

        """
        r = requests.get(instance_url, headers=self.headers, verify=self.verify)
        if r.status_code != 200:
            raise Exception(f"/datasets/{dataset_id}/instances/{self.upload_dict[dataset_id]['instance_id']} returned a {r.status_code} error")
            
        instance_dict = r.json()
        self.upload_dict[dataset_id]['version_number'] = instance_dict['version']
        """
        
        # check to make sure is the right dataset
        assert instance_dict['links']['dataset']['id'] == dataset_id, f"{instance_dict['links']['dataset']['id']} does not match {dataset_id}"
        # check to make sure version number is a number
        assert self.upload_dict[dataset_id]['version_number'] == int(self.upload_dict[dataset_id]['version_number']), f"Version number should be a number - {self.upload_dict[dataset_id]['version_number']}"
        

    def _update_dimensions(self, dataset_id):
        '''
        Used to update dimension labels and add descriptions
        '''
        dimension_dict = self.upload_dict[dataset_id]['metadata_dict']['dimension_data']
        assert type(dimension_dict) == dict, 'dimension_data must be a dict'
        
        instance_url = f"{self.dataset_url}/instances/{self.upload_dict[dataset_id]['instance_id']}"

        for dimension in dimension_dict.keys():
            new_dimension_info = {}
            for key in dimension_dict[dimension].keys():
                new_dimension_info[key] = dimension_dict[dimension][key]
            
            # making the request for each dimension separately
            dimension_url = f"{instance_url}/dimensions/{dimension}"
            self.curl_request(dimension_url, 'put', json=new_dimension_info)
            print(f"Dimension updated - {dimension}")

        """
        for dimension in dimension_dict.keys():
            new_dimension_info = {}
            for key in dimension_dict[dimension].keys():
                new_dimension_info[key] = dimension_dict[dimension][key]
            
            # making the request for each dimension separately
            dimension_url = f"{instance_url}/dimensions/{dimension}"
            r = requests.put(dimension_url, headers=self.headers, json=new_dimension_info, verify=self.verify)
            
            if r.status_code != 200:
                print(f"Dimension info not updated for {dimension}, returned a {r.status_code} error")
            else:
                print(f"Dimension updated - {dimension}")
        """

    def _update_usage_notes(self, dataset_id):
        '''
        Adds usage notes to a version - only unpublished
        /datasets/{id}/editions/{edition}/versions/{version}
        usage_notes is a list of dict(s)
        Can do multiple at once and upload will replace any existing ones
        '''        
        if not bool(self.upload_dict[dataset_id]['metadata_dict']['usage_notes']):
            print("No usage notes to add")
            return 
    
        usage_notes = self.upload_dict[dataset_id]['metadata_dict']['usage_notes']
        
        assert type(usage_notes) == list, 'usage notes must be in a list'
        for item in usage_notes:
            for key in item.keys():
                assert key in ('note', 'title'), 'usage note can only have a note and/or a title'
            
        usage_notes_to_add = {}
        usage_notes_to_add['usage_notes'] = usage_notes
        
        version_url = f"{self.dataset_url}/datasets/{dataset_id}/editions/{self.upload_dict[dataset_id]['edition']}/versions/{self.upload_dict[dataset_id]['version_number']}"
        
        self.curl_request(version_url, 'put', json=usage_notes_to_add)
        print('Usage notes added')
        
        """
        r = requests.put(version_url, headers=self.headers, json=usage_notes_to_add, verify=self.verify)
        if r.status_code == 200:
            print('Usage notes added')
        else:
            print(f"Usage notes not added, returned a {r.status_code} error")
        """
            

class UploadClient(Base):
    def __init__(self, upload_dict, **kwargs):
        Base.__init__(self, **kwargs)
        self._assign(upload_dict)
        

    def post_v4_to_s3(self):
        for dataset_id in self.upload_dict.keys():
            v4 = self.upload_dict[dataset_id]['v4']
            s3_url = self._post_single_v4_to_s3(v4)
            self.upload_dict[dataset_id]['s3_url'] = s3_url
            time.sleep(2) # give api a breather
    
    
    def _post_single_v4_to_s3(self, v4):
        # properties that do not change for the upload
        csv_total_size = str(os.path.getsize(v4)) # size of the whole csv
        timestamp = datetime.datetime.now() # to be ued as unique resumableIdentifier
        timestamp = datetime.datetime.strftime(timestamp, "%d%m%y%H%M%S")
        file_name = v4.split("/")[-1]

        # chunk up the data
        temp_files = self._create_temp_chunks(v4) # list of temporary files
        total_number_of_chunks = len(temp_files)
        chunk_number = 1 # starting chunk number

        # uploading each chunk
        for chunk_file in temp_files:
            csv_size = str(os.path.getsize(chunk_file)) # size of the chunk

            with open(chunk_file, "rb") as f:
                files = {"file": f} # Inlcude the opened file in the request
                
                # Params that are added to the request
                params = {
                        "resumableType": "text/csv",
                        "resumableChunkNumber": chunk_number,
                        "resumableCurrentChunkSize": csv_size,
                        "resumableTotalSize": csv_total_size,
                        "resumableChunkSize": csv_size,
                        "resumableIdentifier": f"{timestamp}-{file_name.replace('.', '')}",
                        "resumableFilename": file_name,
                        "resumableRelativePath": ".",
                        "resumableTotalChunks": total_number_of_chunks
                }
                
                # making the POST request
                r = requests.post(self.upload_url, headers=self.headers, params=params, files=files, verify=self.verify)
                if r.status_code != 200:  
                    raise Exception(f"{self.upload_url} returned error {r.status_code}")
                    
                chunk_number += 1 # moving onto next chunk number

        s3_key = params["resumableIdentifier"]
        s3_url = f"https://s3-eu-west-2.amazonaws.com/ons-dp-sandbox-publishing-uploaded-datasets/{s3_key}"
    
        # delete temp files & tmp v4
        self._delete_temp_chunks(temp_files)
        print("Upload to s3 complete")
        
        return s3_url

    def _create_temp_chunks(self, v4):
        """
        Chunks up the data into text files, returns list of temp files
        """
        chunk_size = 5 * 1024 * 1024 #standard
        file_number = 1
        location = "/".join(v4.split("/")[:-1]) + "/"
        if location == "/": 
            location = ""
        temp_files = []
        with open(v4, 'rb') as f:
            chunk = f.read(chunk_size)
            while chunk:
                file_name = f"{location}temp-file-part-{str(file_number)}"
                with open(file_name, 'wb') as chunk_file:
                    chunk_file.write(chunk)
                    temp_files.append(file_name)
                file_number += 1
                chunk = f.read(chunk_size)
        return temp_files 

    def _delete_temp_chunks(self, temporary_files: list):
        """
        Deletes the temporary chunks that were uploaded
        """
        for file in temporary_files:
            os.remove(file)
            

class UploadToCmd(
        CollectionClient,
        RecipeClient,
        DatasetClient, 
        UploadClient
        ):
    
    def __init__(self, upload_dict, **kwargs):
        CollectionClient.__init__(self, upload_dict, **kwargs)
        RecipeClient.__init__(self, upload_dict)
        DatasetClient.__init__(self, upload_dict, **kwargs)
        UploadClient.__init__(self, upload_dict, **kwargs)
    
    def run_upload(self):
        # runs the full upload 
        
        # gets recipe details
        self.get_recipe()
        
        # upload v4 into s3 bucket
        self.post_v4_to_s3()
        
        # start upload into CMD
        self.post_new_job()
        
        # monitoring upload
        self.monitor_upload()
        
        # create new collection
        self.create_collection()
        
        # updating instance
        self.updating_instance()
            
        # adding data to collection
        self.add_to_collection()
        
        # adding final metadata
        self.adding_metadata()

    def run_import_after_upload(self):
        # semi automated process, v4 needs to be uploaded manually, this will pick up after that point
        # still have issue with posting v4 to s3
        
        # gets recipe details
        self.get_recipe()

        # get instance_id
        self.get_instance_id()
        
        # monitoring upload
        self.monitor_upload()
        
        # create new collection
        self.create_collection()
        
        # updating instance
        self.updating_instance()
            
        # adding data to collection
        self.add_to_collection()
        
        # adding final metadata
        self.adding_metadata()
        
        
class UploadDetails:
    def __init__(self, transform_output, **kwargs):
        if 'location' in kwargs.keys() and kwargs['location'] != '':
            location = kwargs['location']
            if not location.endswith('/'):
                location += '/'
        else:
            location = ''
        self.location = location
        
        assert type(transform_output) == dict, f"input to UploadDetails class must be a dict not a {type(transform_output)}"
        self.transform_output = transform_output
        
        # get upload details from upload_details.json
        with open(f"{self.location}upload_details.json") as f:
            self.upload_details = json.load(f)
            
    def create(self):
        # creates the upload_dict needed for the upload process
        upload_dict = {}
        for dataset_id in self.transform_output.keys():
            assert dataset_id in self.upload_details.keys(), f"{dataset_id} is not in upload_details.json, cannot continue"
            upload_dict[dataset_id] = self.upload_details[dataset_id]
            upload_dict[dataset_id]['v4'] = self.transform_output[dataset_id]
        
        return upload_dict
    

