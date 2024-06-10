import datetime, time

from clients.base_client import Base
from metadata_client import MetadataClient

class DatasetClient(Base, MetadataClient):
    """
    Uses Base and MetadataClient as parent classes
    Client is responsible for creating a new job in the dataset api, updating the state of that job
    and monitoring the state of the import in the instance api. Will then update the state of the 
    instance (which assigns the instance a version number)
    Also updates the metadata of the new version, includes dataset metadata, dimension metadata & usage 
    notes.
    """
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
            self._get_latest_metadata(dataset_id, self.upload_dict[dataset_id]['edition'])
            
            # updating general metadata
            self._update_metadata(dataset_id)
            
            # assigning instance a version number
            self._create_new_version_from_instance(dataset_id)
        
    
    def adding_metadata(self):
        for dataset_id in self.upload_dict.keys():
            try:
                self.upload_dict[dataset_id]["metadata_dict"]
            except:
                print(f"No metadata available for {dataset_id}")
                return
            self._update_dimensions(dataset_id)
            self._update_usage_notes(dataset_id)
            
        
    def _get_latest_job(self):
        # Returns whole content of dataset api /jobs endpoint
        
        dataset_jobs_api_url = f"{self.dataset_url}/jobs"
        
        response = self.http_request('get', dataset_jobs_api_url)
        if response['status_code'] == 200:
            whole_dict = response['response_dict']
            total_count = whole_dict["total_count"]
            last_job_number = total_count - 1 # 0 indexing

            new_url = f"{dataset_jobs_api_url}?limit=1&offset={last_job_number}"
            new_response = self.http_request('get', new_url)
            new_dict = new_response['response_dict']
            self.lastest_job = new_dict['items'][0]
        else:
            raise Exception(
                f"/dataset/jobs API returned a {response['status_code']} error"
            )
        
    
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
            response = self.http_request('get', f"{self.recipe_url}?limit=1000")

            if response['status_code'] == 200:
                all_recipes = response['response_dict']
            else:
                raise Exception(f"Recipe API returned a {response['status_code']} error")
            
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
            
            response = self.http_request('post', f"{self.dataset_url}/jobs", json=payload)
            if response['status_code'] == 201:
                print("Job created successfully")
            else:
                raise Exception(f"Job not created, returning status code: {response['status_code']}")
            
    
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

        response = self.http_request('get', dataset_jobs_id_url)
        if response['status_code'] == 200:
            self.job_info_dict = response['response_dict']
        else:
            raise Exception(f"/dataset/jobs/{self.job_id} returned error {response['status_code']}")
            
    
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

        if len(self.job_info_dict["files"]) != 0:
            print("Updating state of job")
            response = self.http_request('put', updating_state_of_job_url, json=updating_state_of_job_json)

            if response['status_code'] != 200:
                raise Exception(f"Unable to update job for {dataset_id} to submitted state")
        else:
            raise Exception(f"Job for {dataset_id} does not have a v4 file!")
            
    
    def get_instance_id(self, **kwargs):
        # only used in the partial upload
        if 'ignore_upload_date' not in kwargs:
            ignore_upload_date = False
        else:
            ignore_upload_date = kwargs['ignore_upload_date']

        for dataset_id in self.upload_dict.keys():
            dataset_instance_url = f"{self.dataset_url}/instances?dataset={dataset_id}"
            
            response = self.http_request('get', dataset_instance_url)
            if response['status_code'] != 200:
                raise Exception(f"instance API returned a {response['status_code']} on /instances?dataset={dataset_id}")

            response_dict = response['response_dict']
            latest_instance = response_dict['items'][0]
            # check to make sure dates match
            current_date = datetime.datetime.now()
            current_date = datetime.datetime.strftime(current_date, '%Y-%m-%d')
            instance_date = latest_instance['last_updated'].split('T')[0]
            if ignore_upload_date:
                pass
            else:
                assert current_date == instance_date, f"incorrect instance being picked up.. todays date '{current_date}' does not match instance date '{instance_date}' - {latest_instance['id']}"

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

        response = self.http_request('get', instance_id_url)
        if response['status_code'] != 200:
            raise Exception(
                f"{instance_id_url} raised a {response['status_code']} error"
            )

        dataset_instance_dict = response['response_dict']
        job_state = dataset_instance_dict["state"]
        
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
        try:
            self.upload_dict[dataset_id]['metadata_dict']
        except:
            print(f"No metadata for {dataset_id}")
            return 
        
        metadata = self.upload_dict[dataset_id]['metadata_dict']['metadata']
        assert type(metadata) == dict, "metadata['metadata'] must be a dict"

        dataset_url = f"{self.dataset_url}/datasets/{dataset_id}"
        
        response = self.http_request('put', dataset_url, json=metadata)
        if response['status_code'] != 200:
            print(f"Metadata not updated, returned a {response['status_code']} error")
        else:
            print('Metadata updated')
            
    
    def _create_new_version_from_instance(self, dataset_id):
        '''
        Changes state of an instance to edition-confirmed so that it is assigned a version number
        Requires edition name & release date ("2021-07-08T00:00:00.000Z")
        Will currently just use current date as release date
        '''

        instance_url = f"{self.dataset_url}/instances/{self.upload_dict[dataset_id]['instance_id']}"

        current_date = datetime.datetime.now()
        release_date = datetime.datetime.strftime(current_date, '%Y-%m-%dT00:00:00.000Z')
        payload={
            'edition':self.upload_dict[dataset_id]['edition'], 
            'state':'edition-confirmed', 
            'release_date':release_date
            }
        
        response = self.http_request('put', instance_url, json=payload)

        if response['status_code'] == 200:
            print('Instance state changed to edition-confirmed')
            self._get_version_number(dataset_id)
        else:
            raise Exception(f"{dataset_id} - Instance state not changed - returned a {response['status_code']} error")


    def _get_version_number(self, dataset_id):
        '''
        Gets version number of instance ready to be published from /datasets/instances/{instance_id}
        Only when dataset is in a collection or edition-confirmed state
        Used to find the right version for usage notes or to add version to collection
        Returns version number as string
        '''   

        instance_url = f"{self.dataset_url}/instances/{self.upload_dict[dataset_id]['instance_id']}"

        response = self.http_request('get', instance_url)
        if response['status_code'] != 200:
            raise Exception(f"/datasets/{dataset_id}/instances/{self.upload_dict[dataset_id]['instance_id']} returned a {response['status_code']} error")
            
        instance_dict = response['response_dict']
        self.upload_dict[dataset_id]['version_number'] = instance_dict['version']
        
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
            response = self.http_request('put', dimension_url, json=new_dimension_info)
            
            if response['status_code'] != 200:
                print(f"Dimension info not updated for {dimension}, returned a {response['status_code']} error")
            else:
                print(f"Dimension updated - {dimension}")
        

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
        
        response = self.http_request('put', version_url, json=usage_notes_to_add)
        if response['status_code'] == 200:
            print('Usage notes added')
        else:
            print(f"Usage notes not added, returned a {response['status_code']} error")           
