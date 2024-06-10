import json

class UploadDetails:
    """
    Client responsible for getting the required upload details for a given dataset
    i.e. collection name and edition
    """
    def __init__(self, transform_output, **kwargs):
        if 'edition' in kwargs.keys():
            # currently only useful for ashe datasets
            self.edition = kwargs['edition']
        
        assert type(transform_output) == dict, f"input to UploadDetails class must be a dict not a {type(transform_output)}"
        self.transform_output = transform_output
        
        # get upload details from upload_details.json
        with open("supporting_files/upload_details.json") as f:
            self.upload_details = json.load(f)
            
    def create(self):
        # creates the upload_dict needed for the upload process
        upload_dict = {}
        for dataset_id in self.transform_output.keys():
            assert dataset_id in self.upload_details.keys(), f"{dataset_id} is not in upload_details.json, cannot continue"
            upload_dict[dataset_id] = self.upload_details[dataset_id]
            upload_dict[dataset_id]['v4'] = self.transform_output[dataset_id]
            if 'ashe-table' in dataset_id:
                upload_dict[dataset_id]['edition'] = self.edition
        
        return upload_dict