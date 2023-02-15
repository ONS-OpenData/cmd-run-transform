from clients import Transform, TransformLocal, UploadToCmd, UploadDetails, SourceData, ClearRepo, list_of_transforms
import argparse

description = f'''Transform and upload program - transforms available as of 15/02/23:
{list_of_transforms}
'''

parser = argparse.ArgumentParser(description=description)
parser.add_argument("-d", "--datasets", help="Datasets to be transformed", nargs="*", required=True)
parser.add_argument("-u", "--upload", help="Include if upload should be run", action="store_true")
parser.add_argument("-up", "--upload_partial", help="Include if partial upload should be run", action="store_true")
parser.add_argument("-rl", "--run_locally", help="Include if transform should be run from local script", action="store_true")
parser.add_argument("-s", "--source_files", help="Include if giving source files directly", nargs="*")
parser.add_argument("-C", "--clear_repo", help="Include to clear up repo after upload run", action="store_true")

args = parser.parse_args()

datasets = args.datasets
upload = args.upload
upload_partial = args.upload_partial
run_locally = args.run_locally # to run local script - used when changes are needed to a transform and want to be tested
source_files = args.source_files # pass source file(s) path if source data is not from ons site    
location = "" # location - used if any files not in working directory
clear_repo = args.clear_repo # clears repo of source files and v4s after upload

if upload and upload_partial:
    raise Exception("Cannot run with both '-u' & '-up' flags") 
if upload_partial:
    upload = 'partial'

# running the transform
transform_output = {}
for dataset in datasets:
    if not source_files: # source files need downloading
        print(f"downloading source files for {dataset}")
        source = SourceData(dataset, location=location)
        source_files = source.get_source_files()

    if run_locally:
        print("running transform locally")
        transform = TransformLocal(dataset, source_files=source_files)
        transform.run_transform()

    else:
        transform = Transform(dataset, location=location, source_files=source_files)
        transform.run_transform()
        
    source_files = None # wipe previous source files
    transform_output.update(transform.transform_output)

# uploading data
if upload == True:
    # creating upload_dict
    upload_dict = UploadDetails(transform_output, location=location).create()

    upload = UploadToCmd(upload_dict)
    upload.run_upload()

elif upload == 'partial':
    # creating upload_dict
    print('running partial upload')
    upload_dict = UploadDetails(transform_output, location=location).create()

    upload = UploadToCmd(upload_dict)
    upload.run_partial_upload()

if clear_repo:
    ClearRepo()

