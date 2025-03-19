import requests, os, json, datetime, getpass

from get_platform import verify, operating_system

class Base:
    """
    Client used for handling http requests to the cmd apis
    Also used for generating the access tokens to Florence and keeping them updated
    """
    def __init__(self, **kwargs):
        # defining url's
        if operating_system == 'windows':
            self.url = 'https://publishing.dp-prod.aws.onsdigital.uk'
            self.dataset_url = f"{self.url}/dataset"
            
        else:
            self.url = "http://localhost:10800/v1"
            self.dataset_url = self.url
            
        self.token_url = f"{self.url}/api/v1/tokens"
        self.collection_url = f"{self.dataset_url}/collection"
        self.recipe_url = f"{self.url}/recipes"
        self.upload_url = f"{self.url}/upload"
        
        # assigning variables
        self._get_access_token()
        
    def http_request(self, request_type, url, **kwargs):
        if 'refresh_token' in kwargs:
            # do not want to check if token needs refreshing when trying to refresh 
            # the token, would cause an infinite loop
            reponse_dict = self._put_request(url, **kwargs)
        
        else:
            # check if token needs refreshing
            self._check_access_token()
            
            if request_type.lower() == 'get':
                reponse_dict = self._get_request(url)
                
            elif request_type.lower() == 'put':
                reponse_dict = self._put_request(url, **kwargs)
                
            elif request_type.lower() == 'post':
                reponse_dict = self._post_request(url, **kwargs)
            
            else:
                raise NotImplementedError(f"No request built for {request_type.lower()}")
            
        return reponse_dict
    
    def _get_request(self, url, **kwargs):
        r = requests.get(url, headers=self.headers, verify=verify)
        status_code = r.status_code
        response_dict = r.json()
        
        return {
            'status_code': status_code, 
            'response_dict': response_dict
            }
    
    def _put_request(self, url, **kwargs):
        if 'refresh_token' in kwargs:
            # refreshing token request is different to other put requests
            headers = {"ID": self.headers['ID'], "Refresh": self.refresh_token}
            r = requests.put(url, headers=headers, verify=verify)
            status_code = r.status_code
            
            return {
                'status_code': status_code,
                'response': r
                }
        
        else: 
            # all put requests should have a json request header
            json_header = kwargs['json']
            r = requests.put(url, json=json_header, headers=self.headers, verify=verify)
            status_code = r.status_code
            
            return {
                'status_code': status_code
                }
    
    def _post_request(self, url, **kwargs):
        if 'get_access_token' in kwargs:
            login = kwargs['login']
            r = requests.post(url, json=login)
            status_code = r.status_code
            
            return {
                'status_code': status_code,
                'response': r
                }
        
        elif 'json' in kwargs:
            # most post requests only pass on json as request header
            json_header = kwargs['json']
            r = requests.post(url, json=json_header, headers=self.headers, verify=verify)
            status_code = r.status_code
        
        elif 'params' in kwargs and 'files' in kwargs:
            # uploading chunks uses these request headers
            params_dict = kwargs['params']
            files_dict = kwargs['files']
            r = requests.post(url, params=params_dict, files=files_dict, headers=self.headers, verify=verify)
            status_code = r.status_code
        
        return {
            'status_code': status_code
            }

    def _get_access_token(self):
        # gets florence access token
        try: # so that token isn't generate for each __init__
            if self.headers['X-Florence-Token']:
                pass
        except:
            print("Generating access tokens")
            # getting credential from environment variables
            email, password = self._get_credentials()
            login = {"email": email, "password": password}
            response_dict = self.http_request('post', self.token_url, login=login, get_access_token=True)
            if response_dict['status_code'] == 201:
                response_headers = response_dict['response'].headers
                response_content = response_dict['response'].content
                
                auth_token = response_headers["Authorization"]
                id_token = response_headers["ID"]
                self.refresh_token = response_headers["Refresh"]
                
                content_json = json.loads(response_content.decode("utf-8"))
                token_expiration_time = content_json["expirationTime"]
                # currently using a timer to decide when token needs refreshing
                self.token_start_time = datetime.datetime.now()
                refresh_token_expiration_time = content_json["refreshTokenExpirationTime"]
                
                self.headers = {"X-Florence-Token": auth_token, "ID": id_token}

            else:
                raise Exception(f"Token not created, returned a {response_dict['status_code']} error")
                
    def _refresh_access_token(self):
        print("Refreshing access tokens")
        # makes a put request to refresh
        response_dict = self.http_request('put', f"{self.token_url}/self", refresh_token=True)
        if response_dict['status_code'] == 201:
            response_headers = response_dict['response'].headers
            response_text = response_dict['response'].text
            
            auth_token = response_headers["Authorization"]
            id_token = response_headers["ID"]
            token_expiration_time = json.loads(response_text)['expirationTime']
            self.token_start_time = datetime.datetime.now()
            
            self.headers = {"X-Florence-Token": auth_token, "ID": id_token}
            
        else:
            raise Exception(f"Refreshing token failed, returned a {response_dict['status_code']} error")
            
    def _check_access_token(self):
        # checks if the access token needs refreshing
        try:
            self.token_start_time
            refresh_time = 10 # will refresh if token is more than 10 minutes old
            if datetime.datetime.now() - self.token_start_time > datetime.timedelta(minutes=refresh_time):
                self._refresh_access_token()
        except:
            return
            
    def _get_credentials(self):
        email = os.getenv('FLORENCE_EMAIL')
        password = os.getenv('FLORENCE_PASSWORD')
        if email and password:
            pass

        else:
            print("Florence credentials not found in environment variables")
            print("Will need to be passed")

            email = input("Florence email: ")
            password = getpass.getpass(prompt="Florence password: ")

            # will set temporary env variables on network machines
            # so that will only be asked for details once
            os.environ["FLORENCE_EMAIL"] = email
            os.environ["FLORENCE_PASSWORD"] = password

        return email, password
    
    def _assign(self, upload_dict):
        # assigns the upload_dict as a class variable
        # but checks if it has been assigned already
        assert type(upload_dict) == dict, f"upload_dict must be a dict not {type(upload_dict)}"
        try:
            if self.upload_dict:
                pass
        except:
            self.upload_dict = upload_dict