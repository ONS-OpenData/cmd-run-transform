from clients import Transform, UploadToCmd, UploadDetails
import sys

kwargs = dict(arg.split('=') for arg in sys.argv[1:])

assert 'dataset' in kwargs.keys(), "script requires a dataset to be passed"
dataset = kwargs['dataset']

if 'location' in kwargs.keys():
    location = kwargs['location']
else:
    location = ""
    
if 'source_files' in kwargs.keys():
    source_files = kwargs['source_files']
    if "," in source_files:
        source_files = source_files.split(",")
else:
    source_files = ""
    
if 'credentials' in kwargs.keys():
    credentials = kwargs['credentials']
else:
    credentials = ""
    

print(f"Running transform on: {dataset}")
if location:   
    print(f"Files will be written to: {location}")
else:
    print("Files will be written to working directory")
if source_files:    
    print(f"Using {source_files} as source file(s)")
else:
    print("Source files will be picked up from location")
if credentials:
    print(f"Credentials file: {credentials}")
else:
    print("Credentials file will be picked up from working directory")

# running the transform
transform = Transform(dataset, location=location, source_files=source_files)
transform.run_transform()

# TODO - take kwargs for whether to run whole script or just transform
"""
# creating upload_dict
upload_dict = UploadDetails(transform.transform_output, location=location).create()

# uploading data
upload = UploadToCmd(upload_dict, credentials=credentials)
upload.run_upload()
"""