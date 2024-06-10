import requests, os, math
import pandas as pd

from base_client import Base
from get_platform import verify

class V4Checker(Base):
    """
    Uses Base as a parent class
    CLient responsible for validating v4
    Checks v4 does not contain any sparisty
    Checks code lists found in v4 are in the recipe and then checks that the options
    within each code list are found in the code list api
    """
    def __init__(self, transform_outputs, **kwargs):
        # Base as child class to access recipe API
        Base.__init__(self, **kwargs)
        assert type(transform_outputs) == dict, f"V4Checker imput must be a dict, got '{type(transform_outputs)}'"
        self.transform_outputs = transform_outputs
        self.code_list_api_url = "https://api.beta.ons.gov.uk/v1/code-lists"

        # get user-agent
        email = os.getenv('FLORENCE_EMAIL')
        if not email:
            email = 'cmd@ons.gov.uk' # generic cmd email

        self.user_agent = {"User-Agent": f"cmd-run-transforms/Version1.0.0 ONS {email}"}
        
    def run_check(self):
        print("---")
        for dataset_id in self.transform_outputs:
            self.dataset_id = dataset_id
            v4_file = self.transform_outputs[dataset_id]
            print(f"Running V4Checker on {self.dataset_id}")
            self.df = pd.read_csv(v4_file, dtype=str)
            self._check_sparsity()
            self._check_dimensions()
            
            # deleting all specific self.<variables>
            del self.dataset_id, self.df_codelists, self.recipe_codelists
            del self.df
            print("---")
        return
    
    def _check_sparsity(self):
        # checks sparsity of only the codes (not labels)
        df_size = len(self.df)
        
        df_columns = list(self.df.columns)
        v4_marker = int(self.df.columns[0][-1])
        self.df_codelists = df_columns[v4_marker+1::2] # just code list id columns
        unsparse_length = 1
        for col in self.df_codelists:
            unsparse_length *= self.df[col].unique().size
            
        if df_size != unsparse_length:
            raise Exception(f"Sparsity found aborting... len of df - {df_size}, not equal to unsparse length - {unsparse_length}")
            
        print("Dataset sparsity complete")
        
    def _check_dimensions(self):
        # calls _get_dimensions_from_recipe()
        # checks codelist from df appear in recipe
        # checks each dimension against code list api
        self._get_dimensions_from_recipe()
        if self.recipe_codelists == []:
            raise Exception(f"Recipe not found in API for {self.dataset_id}")
            
        for codelist in self.df_codelists:
            assert codelist in self.recipe_codelists, f"code list '{codelist}' not found in recipe"
            self._check_codelist_against_api(codelist)
                
    def _get_dimensions_from_recipe(self):
        # gets recipe from recipe api
        # assigns code list id's to self.recipe_codelists
        response = self.http_request('get', f"{self.recipe_url}?limit=1000")
        if response['status_code'] == 200:
            all_recipes = response['response_dict']
        else:
            raise Exception(f"Recipe API returned a {response['status_code']} error")
        
        self.recipe_codelists = []
        for item in all_recipes["items"]:
            # hack around incorrect recipe in database
            if item['id'] == 'b944be78-f56d-409b-9ebd-ab2b77ffe187':
                continue
            if self.dataset_id == item["output_instances"][0]["dataset_id"]:
                dataset_recipe = item
                recipe_codelists_list = dataset_recipe['output_instances'][0]['code_lists']
                for codelist in recipe_codelists_list:
                    self.recipe_codelists.append(codelist['id'])
                    
    def _check_codelist_against_api(self, codelist_id):
        # checks options in a dimension appear in the code list api
        # only checks codes not labels
        codelist_url = f"{self.code_list_api_url}/{codelist_id}/editions/one-off/codes"
        codelist_dict = requests.get(codelist_url, headers=self.user_agent, verify=verify).json()
        total_count = codelist_dict['total_count'] 
        
        codes_list = []
        
        if total_count <= 1000:
            new_url = f"{codelist_url}?limit=1000"
            whole_codelist_dict = requests.get(new_url, headers=self.user_agent, verify=verify).json()
            for item in whole_codelist_dict['items']:
                codes_list.append(item['code'])
                
        else:
            number_of_iterations = math.ceil(total_count/1000)
            offset = 0
            for i in range(number_of_iterations):
                new_url = f"{codelist_url}?limit=1000&offset={offset}"
                whole_codelist_dict = requests.get(new_url, headers=self.user_agent, verify=verify).json()
                for item in whole_codelist_dict['items']:
                    codes_list.append(item['code'])
                offset += 1000
        
        for code in self.df[codelist_id].unique():
            assert code in codes_list, f"{code} does not appear in {codelist_id} code list"
        
        print(f"{codelist_id} good")