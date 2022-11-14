from clients import Transform, TransformLocal, UploadToCmd, UploadDetails, SourceData
import sys

kwargs = dict(arg.split('=') for arg in sys.argv[1:])

assert 'dataset' in kwargs.keys(), "script requires a dataset to be passed as a kwarg"
dataset = kwargs['dataset']
if "," in dataset:
    datasets = dataset.split(",")
else: # using the datasets as a list
    datasets = [dataset]

# location - used if any files not in working directory
if 'location' in kwargs.keys():
    location = kwargs['location']
else:
    location = ""

# source files - pass source file(s) path if source data is not from ons site    
if 'source_files' in kwargs.keys():
    source_files = kwargs['source_files']
    if "," in source_files:
        source_files = source_files.split(",")
else:
    source_files = None # will be downloaded if possible

# credentials - used to generate access_token
# does not need to be given if credentials file in working directory 
if 'credentials' in kwargs.keys():
    credentials = kwargs['credentials']
else:
    credentials = ""

# to run local script - used when changes are needed to a transform and want to be tested
# needed because of caching issues when pulling transform from github
if 'run' in kwargs.keys():
    if kwargs['run'] == 'locally':
        run_locally = True
    else:
        run_locally = False
else:
    run_locally = False

# upload - used to determine what will be run
# true - will run transform and full upload process
# partial - will run the partial upload -> stops after instance is complete
# false - will run transform only, is default option
if 'upload' in kwargs.keys():
    upload = kwargs['upload']
    assert upload.lower() in ('true', 'false', 'partial'), f"upload key word must be either true/false/partial not {upload}"
else:
     upload = 'false'

# running the transform
transform_output = {}
for dataset in datasets:
    if not source_files: # source files need downloading
        print(f"downloading source files for {dataset}")
        source = SourceData(dataset, location=location)
        source_files = source.get_source_files()

    if run_locally:
        transform = TransformLocal(dataset, source_files=source_files)
        transform.run_transform()

    else:
        transform = Transform(dataset, location=location, source_files=source_files)
        transform.run_transform()
        
    source_files = None # wipe previous source files
    transform_output.update(transform.transform_output)

# uploading data
if upload == 'true':
    # creating upload_dict
    upload_dict = UploadDetails(transform_output, location=location).create()

    upload = UploadToCmd(upload_dict, credentials=credentials)
    upload.run_upload()

elif upload == 'partial':
    # creating upload_dict
    upload_dict = UploadDetails(transform_output, location=location).create()

    upload = UploadToCmd(upload_dict, credentials=credentials)
    upload.run_partial_upload()

