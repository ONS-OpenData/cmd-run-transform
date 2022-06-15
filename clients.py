import requests, os, glob


TRANSFORM_URL = "https://raw.github.com/ONS-OpenData/cmd-transforms/master"



class Transform:
    def __init__(self, dataset, **kwargs):
        self.dataset = dataset
        self.transform_url = f"{TRANSFORM_URL}/{self.dataset}/main.py"
        self.requirements_url = f"{TRANSFORM_URL}/{self.dataset}/requirements.txt"
        self.transform_script = f"{dataset}_transform_script.py"
        self.requirements_dict = {} # will be empty if no requirements needed
        
        if 'source_files' in kwargs.keys():
            source_files = kwargs['source_files']
            # source files must be a list for consistency
            if type(source_files) == str:
                source_files = [source_files]
            self.source_files = source_files
        else:
            # if no source files provided, will gather all files in directory that
            # are not .py files
            source_files = glob.glob('*')
            source_files = [file for file in source_files if '.py' not in file]
        
    def _write_transform(self):
        # getting transform script
        r = requests.get(self.transform_url)
        if r.status_code != 200:
            raise Exception(f"{self.transform_url} raised a {r.status_code} error")
        
        # writing transform script
        script = r.text
        with open(self.transform_script, "w") as f:
            f.write(script)
            f.close()
            print(f"transform script wrote as {self.transform_script}")
            
            
        # getting any requirements
        r = requests.get(self.requirements_url)
        if r.status_code == 200:
            requirements = r.text
            requirements = requirements.strip().split("\n")
    
            for module in requirements:
                module_url = f"{TRANSFORM_URL}/modules/{module}/module.py"
                module_r = requests.get(module_url)
                if module_r.status_code != 200:
                    raise Exception(f"{module_url} raised a {module_r.status_code} error")
    
                module_script = module_r.text
                self.requirements_dict[module] = module_script
               
        # writing any requirements
        if self.requirements_dict: # if requirements not empty
            for module in self.requirements_dict:
                file_name = f"{module}.py".replace("-", "_")
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
            os.remove(file_name)
            print(f"{module} removed")
            
    def run_transform(self):
        # runs the transform 
        
        # write the transform and any requirements
        self._write_transform()
        
        # import transform
        from self.transform_script import transform
        # catch any errors in the transform
        try:
            self.v4_path = transform(self.source_files)
        except Exception as e:
            print("Error in transform")
            self._del_transform() # del scripts to avoid hangover
            raise Exception(e)
        # del scripts
        self._del_transform()
            
            
    # TODO - run transform
    # upload v4 via API, create more classes for upload clients
        
