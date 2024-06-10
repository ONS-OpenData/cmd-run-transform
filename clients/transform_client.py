import requests, os, glob, sys

from get_platform import verify

TRANSFORM_URL = "https://raw.github.com/ONS-OpenData/cmd-transforms/master"

list_of_transforms = [
    "construction", "cpih", "gdp-to-4dp", "house-prices", "index-private-housing-rental-prices", 
    "labour-market-cdid", "lms", "mid-year-pop-est", "online-jobs", "regional-gdp", "retail-sales",
    "shipping-data", "suicides", "trade", "traffic-camera-activity", "uk-spending-on-cards", 
    "weekly-deaths-previous", "weekly-deaths", "wellbeing-estimates", "wellbeing-quarterly"
]

class Transform:
    """
    Client used to run cmd transforms
    Picks up all files (some exceptions) within base directory - can specify source files directly
    if required
    Picks up the transform from TRANSFORM_URL, writes the file as a .py, runs the transform
    using the source files, then deletes the transform .py file
    """
    def __init__(self, dataset, **kwargs):
        self.dataset = dataset
        
        if 'source_files' in kwargs.keys() and kwargs['source_files'] != '':
            source_files = kwargs['source_files']
            # source files must be a list for consistency
            if type(source_files) == str:
                source_files = [source_files]
        else:
            # if no source files provided, will gather all files in directory that
            # are not .py files
            source_files = glob.glob("*")
            source_files = [file for file in source_files if not os.path.isdir(file)] # ignoring any directories
            source_files = [file for file in source_files if '.py' not in file] # ignoring py files
            source_files = [file for file in source_files if '__' not in file] # ignoring pycache
            source_files = [file for file in source_files if '.json' not in file] # ignoring json
            source_files = [file for file in source_files if '.md' not in file] # ignoring README 
        self.source_files = source_files
        
        self.transform_url = f"{TRANSFORM_URL}/{self.dataset}/main.py"
        self.requirements_url = f"{TRANSFORM_URL}/{self.dataset}/requirements.txt"
        self.transform_script = "temp_transform_script.py"
        self.requirements_dict = {} # will be empty if no requirements needed
        
        
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
            print(f"{file_name} removed")
            
            
    def run_transform(self):
        # runs the transform 
        
        # write the transform and any requirements
        self._write_transform()
        
        # import transform
        print(f"Running transform on: {self.dataset}")
        from temp_transform_script import transform
        # catch any errors in the transform
        try:
            self.transform_output = transform(self.source_files)
        except Exception as e:
            print(f"Error in transform - {self.dataset}")
            self._del_transform() # del scripts to avoid hangover
            raise Exception(e)
        # del scripts
        self._del_transform()
        del sys.modules['temp_transform_script'] # causes issue with multipart transform if not deleted

class TransformLocal:
    """
    Client used to run cmd transforms
    Picks up all files (some exceptions) within base directory - can specify source files directly
    if required
    Picks up the transform locally ({self.path_to_local_transforms}/cmd-transforms), writes the 
    file as a .py, runs the transform using the source files, then deletes the transform .py file
    
    used to run the transforms that are saved locally
    useful when changes are made to transform
    will only work if the transformed are saved locally
    """
    def __init__(self, dataset, **kwargs):
        self.path_to_local_transforms = ".."
        
        self.dataset = dataset
        
        if 'source_files' in kwargs.keys() and kwargs['source_files'] != '':
            source_files = kwargs['source_files']
            # source files must be a list for consistency
            if type(source_files) == str:
                source_files = [source_files]
        else:
            # if no source files provided, will gather all files in directory that
            # are not .py files
            source_files = glob.glob("*")
            source_files = [file for file in source_files if not os.path.isdir(file)] # ignoring any directories
            source_files = [file for file in source_files if '.py' not in file] # ignoring py files
            source_files = [file for file in source_files if '__' not in file] # ignoring pycache
            source_files = [file for file in source_files if '.json' not in file] # ignoring json 
            source_files = [file for file in source_files if '.md' not in file] # ignoring README
        self.source_files = source_files
        
        self.transform_location = f"{self.path_to_local_transforms}/cmd-transforms/{self.dataset}/main.py"
        self.requirements_location = f"{self.path_to_local_transforms}/cmd-transforms/{self.dataset}/requirements.txt"
        self.transform_script = "temp_transform_script.py"
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
            module_location = f"{self.path_to_local_transforms}/cmd-transforms/modules/{module}/module.py"
            with open(module_location, "r") as f:
                module_script = f.read()
                f.close()
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
            print(f"{file_name} removed")
            
            
    def run_transform(self):
        # runs the transform 
        
        # write the transform and any requirements
        self._write_transform()
        
        # import transform
        print(f"Running transform on: {self.dataset}")
        from temp_transform_script import transform
        # catch any errors in the transform
        try:
            self.transform_output = transform(self.source_files)
        except Exception as e:
            print(f"Error in transform - {self.dataset}")
            self._del_transform() # del scripts to avoid hangover
            raise Exception(e)
        # del scripts
        self._del_transform()
        del sys.modules['temp_transform_script'] # causes issue with multipart transform if not deleted
