from collection_client import CollectionClient
from recipe_client import RecipeClient
from dataset_client import DatasetClient
from upload_client import UploadClient

class UploadToCmd(
        CollectionClient,
        RecipeClient,
        DatasetClient, 
        UploadClient
        ):
    """
    Uses CollectionClient, RecipeClient, DatasetClient, UploadClient as parent classes
    Client is responsible for running the full upload process, from taking a v4 all the way to 
    adding it to a collection ready to be published
    Can run a partial upload where it stops after monitoring the v4 has been uploaded into 
    CMD, this is useful when uploading a new dataset as the process is slightly different after this point
    Can also run adding a dataset to a collection, useful for very large datasets that have been left to
    upload, instead of adding dataset to collection manually this can be used
    """
    
    def __init__(self, upload_dict, **kwargs):
        for key in upload_dict:
            # used to distinguise between weekly deaths editions
            if '-previous' in key:
                new_key = '-'.join(key.split('-previous')[:-1])
                upload_dict[new_key] = upload_dict[key]
                del upload_dict[key] 

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

    def run_partial_upload(self):
        # runs a partial upload, stops after v4 is loaded into Florence and instance is complete
        
        # gets recipe details
        self.get_recipe()
        
        # upload v4 into s3 bucket
        self.post_v4_to_s3()
        
        # start upload into CMD
        self.post_new_job()
        
        # monitoring upload
        self.monitor_upload()

    def run_add_to_collection(self, **kwargs):
        if 'ignore_upload_date' not in kwargs:
            ignore_upload_date = False
        else:
            ignore_upload_date = kwargs['ignore_upload_date']
        # gets recipe details
        self.get_recipe()
        
        self.get_instance_id(ignore_upload_date=ignore_upload_date)
        
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