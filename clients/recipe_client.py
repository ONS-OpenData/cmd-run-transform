from clients.base_client import Base

class RecipeClient(Base):
    """
    Uses Base as a parent class
    Client responsible for interacting with the recipe api.
    Mainly to check that a recipe exists in the api and also to return a specific recipe
    """
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
        
        response = self.http_request('get', f"{self.recipe_url}?limit=1000")

        if response['status_code'] == 200:
            self.all_recipes = response['response_dict']
        else:
            raise Exception(f"Recipe API returned a {response['status_code']} error")
        
    
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
