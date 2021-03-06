from clients import Transform, UploadToCmd, UploadDetails, SourceData
import sys

kwargs = dict(arg.split('=') for arg in sys.argv[1:])

assert 'dataset' in kwargs.keys(), "script requires a dataset to be passed as a kwarg"
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
    # downloads source files
    source = SourceData(dataset, location=location)
    source_files = source.get_source_files()
    
if 'credentials' in kwargs.keys():
    credentials = kwargs['credentials']
else:
    credentials = ""


# running the transform
transform = Transform(dataset, location=location, source_files=source_files)
transform.run_transform()

# creating upload_dict
upload_dict = UploadDetails(transform.transform_output, location=location).create()

# uploading data
if 'upload' in kwargs.keys():
    if str(kwargs['upload']).lower() == 'true':
        upload = UploadToCmd(upload_dict, credentials=credentials)
        upload.run_upload()

