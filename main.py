from clients import Transform, TransformLocal, UploadToCmd, UploadDetails, SourceData
import sys

kwargs = dict(arg.split('=') for arg in sys.argv[1:])

assert 'dataset' in kwargs.keys(), "script requires a dataset to be passed as a kwarg"
dataset = kwargs['dataset']
if "," in dataset:
        dataset = dataset.split(",")

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

if 'run' in kwargs.keys():
    if kwargs['run'] == 'locally':
        run_locally = True
    else:
        run_locally = False
else:
    run_locally = False

if 'upload' in kwargs.keys():
    upload = kwargs['upload']
    assert upload.lower() in ('true', 'false', 'partial'), f"upload key word must be either true/false/partial not {upload}"

# running the transform
if run_locally:
    transform = TransformLocal(dataset, source_files=source_files)
    transform.run_transform()
    
elif upload.lower() == 'partial':
    # runs a semi automated upload, requires v4 to be loaded manually, will pick up after this point
    transform_output = {dataset: ''} # v4 path is not needed for the partial upload
    upload_dict = UploadDetails(transform_output).create()
    upload = UploadToCmd(upload_dict, credentials=credentials)
    upload.run_import_after_upload()

else:
    transform = Transform(dataset, location=location, source_files=source_files)
    transform.run_transform()

    # creating upload_dict
    upload_dict = UploadDetails(transform.transform_output, location=location).create()

    # uploading data
    if upload.lower() == 'true':
        upload = UploadToCmd(upload_dict, credentials=credentials)
        upload.run_upload()
            