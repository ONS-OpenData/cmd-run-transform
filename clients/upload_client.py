import os, datetime, time

from clients.base_client import Base

class UploadClient(Base):
    def __init__(self, upload_dict, **kwargs):
        Base.__init__(self, **kwargs)
        self._assign(upload_dict)
        

    def post_v4_to_s3(self):
        for dataset_id in self.upload_dict.keys():
            v4 = self.upload_dict[dataset_id]['v4']
            s3_url = self._post_single_v4_to_s3(v4)
            self.upload_dict[dataset_id]['s3_url'] = s3_url
            time.sleep(2) # give api a breather
    
    
    def _post_single_v4_to_s3(self, v4):
        # properties that do not change for the upload
        csv_total_size = str(os.path.getsize(v4)) # size of the whole csv
        timestamp = datetime.datetime.now() # to be ued as unique resumableIdentifier
        timestamp = datetime.datetime.strftime(timestamp, "%d%m%y%H%M%S")
        file_name = v4.split("/")[-1]

        # chunk up the data
        temp_files = self._create_temp_chunks(v4) # list of temporary files
        total_number_of_chunks = len(temp_files)
        chunk_number = 1 # starting chunk number

        # uploading each chunk
        for chunk_file in temp_files:
            csv_size = str(os.path.getsize(chunk_file)) # size of the chunk

            with open(chunk_file, "rb") as f:
                files = {"file": f} # Inlcude the opened file in the request
                
                # Params that are added to the request
                params = {
                        "resumableType": "text/csv",
                        "resumableChunkNumber": chunk_number,
                        "resumableCurrentChunkSize": csv_size,
                        "resumableTotalSize": csv_total_size,
                        "resumableChunkSize": csv_size,
                        "resumableIdentifier": f"{timestamp}-{file_name.replace('.', '')}",
                        "resumableFilename": file_name,
                        "resumableRelativePath": ".",
                        "resumableTotalChunks": total_number_of_chunks
                }
                
                # making the POST request
                response = self.http_request('post', self.upload_url, params=params, files=files)
                if response['status_code'] != 200:  
                    raise Exception(f"{self.upload_url} returned error {response['status_code']}")
                    
                print(f"tmp file number - {chunk_number} posted")
                chunk_number += 1 # moving onto next chunk number

        s3_key = params["resumableIdentifier"]
        s3_url = f"https://s3-eu-west-2.amazonaws.com/ons-dp-prod-publishing-uploaded-datasets/{s3_key}"
    
        # delete temp files & tmp v4
        self._delete_temp_chunks(temp_files)
        print("Upload to s3 complete")
        
        return s3_url

    def _create_temp_chunks(self, v4):
        """
        Chunks up the data into text files, returns list of temp files
        """
        chunk_size = 5 * 1024 * 1024 #standard
        file_number = 1
        location = "/".join(v4.split("/")[:-1]) + "/"
        if location == "/": 
            location = ""
        temp_files = []
        with open(v4, 'rb') as f:
            chunk = f.read(chunk_size)
            while chunk:
                file_name = f"{location}temp-file-part-{str(file_number)}"
                with open(file_name, 'wb') as chunk_file:
                    chunk_file.write(chunk)
                    temp_files.append(file_name)
                file_number += 1
                chunk = f.read(chunk_size)
        return temp_files 

    def _delete_temp_chunks(self, temporary_files: list):
        """
        Deletes the temporary chunks that were uploaded
        """
        for file in temporary_files:
            os.remove(file)
