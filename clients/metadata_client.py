import requests, json

from get_platform import verify

class MetadataClient:
    """
    Client gets metadata from the latest published version of the dataset, then 
    parses the metadata into a usable format for the DatasetClient
    """
    def get_metadata(self, dataset_id, edition):
        try:
            if self.upload_dict[dataset_id]['metadata_dict']:
                pass
        except:
            self._get_latest_metadata(dataset_id, edition)
            
        
    def _get_latest_metadata(self, dataset_id, edition):
        """
        Pulls latest csvw
        """
        editions_url = f"https://api.beta.ons.gov.uk/v1/datasets/{dataset_id}/editions/{edition}/versions"
        items = requests.get(f"{editions_url}?limit=1000", verify=verify).json()['items']
        # get latest version number
        latest_version_number = items[0]['version']
        assert latest_version_number == len(items), f'Get_Latest_Version for /{dataset_id}/editions/{edition} - number of versions does not match latest version number'
        # get latest version URL
        url = f"{editions_url}/{str(latest_version_number)}"
        # get latest version data
        latest_version = requests.get(url, verify=verify).json()
        try:
            csvw_response = requests.get(latest_version['downloads']['csvw']['href'], verify=verify)
            if csvw_response.status_code != 200:
                print(f"csvw download failed with a {csvw_response.status_code} error")
                return 
        except:
            print(f"csvw does not exist for {dataset_id} version {latest_version_number}")
            return 
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
            description = item["description"]
            if "name" in item:
                label = item["name"]
            else:
                label = name
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