import time

from clients.base_client import Base

class CollectionClient(Base):   
    def __init__(self, upload_dict, **kwargs):
        Base.__init__(self, **kwargs)
        self._assign(upload_dict)


    def create_collection(self):
        for dataset_id in self.upload_dict.keys():
            payload = {"name": self.upload_dict[dataset_id]['collection_name']}
            response = self.http_request('post', self.collection_url, json=payload)
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
        response = self.http_request('get', f"{self.collection_url}/{self.upload_dict[dataset_id]['collection_id']}")
        if response['status_code'] != 200:
            raise Exception(f"Collection '{self.upload_dict[dataset_id]['collection_name']}' not created - returned a {response['status_code']} error")
        
    
    def _get_collection_id(self, dataset_id):
        self._get_all_collections()
        for collection in self.all_collections:
            if collection["name"] == self.upload_dict[dataset_id]['collection_name']:
                collection_id = collection["id"]
                break
    
        try:
            self.upload_dict[dataset_id]['collection_id'] = collection_id
        except:
            raise NotImplementedError(f"Collection not created for {dataset_id}")

    
    def _get_all_collections(self):
        response = self.http_request('get', f"{self.collection_url}s")
        collection_list = response['response_dict']
        self.all_collections = collection_list
            

    def _add_dataset_to_collection(self, dataset_id):
        # Adds dataset landing page to a collection
        
        payload = {"state": "Complete"}
        url = f"{self.collection_url}s/{self.upload_dict[dataset_id]['collection_id']}/datasets/{dataset_id}"
        response = self.http_request('put', url, json=payload)

        if response['status_code'] == 200:
            print(f"{dataset_id} - Dataset landing page added to collection")
        else:
            raise Exception(f"{dataset_id} - Dataset landing page not added to collection - returned a {response['status_code']} error")
        

    def _add_dataset_version_to_collection(self, dataset_id):
        # Adds dataset version to a collection

        payload = {"state": "Complete"}
        url = f"{self.collection_url}s/{self.upload_dict[dataset_id]['collection_id']}/datasets/{dataset_id}/editions/{self.upload_dict[dataset_id]['edition']}/versions/{self.upload_dict[dataset_id]['version_number']}"
        response = self.http_request('put', url, json=payload)

        if response['status_code'] == 200:
            print(f"{dataset_id} - Dataset version '{self.upload_dict[dataset_id]['version_number']}' added to collection")
        else:
            raise Exception(f"{dataset_id} - Dataset version '{self.upload_dict[dataset_id]['version_number']}' not added to collection - returned a {response['status_code']} error")
        