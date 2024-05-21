import argparse, sys
from pathlib import Path

sys.path.append(f"{Path(__file__).parent.as_posix()}/clients")

from clients.ashe_client import AsheSourceData, AsheTransform, AsheCombiner, ashe_number_lookup, provisional_or_revised_lookup, time_series_ashe_tables, list_of_ashe_tables
from clients.transform_client import Transform, TransformLocal, list_of_transforms
from clients.source_data_client import SourceData
from clients.upload_details_client import UploadDetails
from clients.upload_to_cmd_client import UploadToCmd
from clients.v4_checker_client import V4Checker
from clients.clear_repo import ClearRepo

description = f'''Transform and upload program - transforms available as of 15/02/23:
{list_of_transforms}
{list_of_ashe_tables}
'''

parser = argparse.ArgumentParser(description=description)
parser.add_argument("-d", "--datasets", help="Datasets to be transformed", nargs="*", required=True)
parser.add_argument("-u", "--upload", help="Include if upload should be run", action="store_true")
parser.add_argument("-up", "--upload_partial", help="Include if partial upload should be run", action="store_true")
parser.add_argument("-rl", "--run_locally", help="Include if transform should be run from local script", action="store_true")
parser.add_argument("-s", "--source_files", help="Include if giving source files directly", nargs="*")
parser.add_argument("-C", "--clear_repo", help="Include to clear up repo after upload run", action="store_true")
parser.add_argument("-I", "--ignore_release_date", help="Include to ignore release date when downloading source files", action="store_true")

args = parser.parse_args()

datasets = args.datasets
upload = args.upload
upload_partial = args.upload_partial
run_locally = args.run_locally # to run local script - used when changes are needed to a transform and want to be tested
source_files = args.source_files # pass source file(s) path if source data is not from ons site    
clear_repo = args.clear_repo # clears repo of source files and v4s after upload
ignore_release_date = args.ignore_release_date # ignores release date of source files

if upload and upload_partial:
    raise Exception("Cannot run with both '-u' & '-up' flags") 
if upload_partial:
    upload = 'partial'

# running the transform
transform_output = {}
for dataset in datasets:
    if dataset == 'ashe':
        # separate transform process for ashe datasets 
        table_number = str(input("Ashe table number to run (Only one table number required): "))
        if table_number not in ashe_number_lookup.keys():
            raise Exception(f"Table number {table_number} not found, must be one of {ashe_number_lookup.keys()}")
        table_number = ashe_number_lookup[table_number]

        year_of_data = str(input("Year of data to be transformed: "))

        if table_number in time_series_ashe_tables:
            edition = "time-series"
        else:
            edition = year_of_data

        provisional_or_revised = input("Provisional or revised data [p/r]: ")
        provisional_or_revised = provisional_or_revised_lookup[provisional_or_revised.lower()]

        source_data = AsheSourceData(table_number, year_of_data, provisional_or_revised)
        source_data.get_source_files()
        print(source_data.downloaded_files)

        transform = AsheTransform(table_number, year_of_data=year_of_data)

        if run_locally:
            transform.run_transform_local()
        else:
            transform.run_transform()
        
        transform_output.update(transform.transform_output)

        combiner = AsheCombiner(table_number, transform_output[table_number])
    
    else:
        if not source_files: # source files need downloading
            print(f"downloading source files for {dataset}")
            source = SourceData(dataset, ignore_release_date=ignore_release_date)
            source_files = source.get_source_files()

        if run_locally:
            print("running transform locally")
            transform = TransformLocal(dataset, source_files=source_files)
            transform.run_transform()

        else:
            transform = Transform(dataset, source_files=source_files)
            transform.run_transform()
            
    source_files = None # wipe previous source files
    transform_output.update(transform.transform_output)

# uploading data
if upload == True:
    # validate v4s
    validate_object = V4Checker(transform_output)
    validate_object.run_check()

    # creating upload_dict
    if dataset == 'ashe':
        upload_dict = UploadDetails(transform_output, edition=edition).create()
    else:
        upload_dict = UploadDetails(transform_output).create()

    upload = UploadToCmd(upload_dict)
    upload.run_upload()

elif upload == 'partial':
    # validate v4s
    validate_object = V4Checker(transform_output)
    validate_object.run_check()

    # creating upload_dict
    print('running partial upload')
    if dataset == 'ashe':
        upload_dict = UploadDetails(transform_output, edition=edition).create()
    else:
        upload_dict = UploadDetails(transform_output).create()

    upload = UploadToCmd(upload_dict)
    upload.run_partial_upload()

if clear_repo:
    ClearRepo()

"""
upload_dict = {"trade": {
    "v4": "v4-trade.csv",
    "edition": "time-series",
    "collection_name": "CMD trade"
    }
}

upload = UploadToCmd(upload_dict)
# upload.run_partial_upload()
upload.run_add_to_collection(ignore_upload_date=True)
# upload.run_upload()
"""