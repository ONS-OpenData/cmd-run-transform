from clients import Transform, UploadToCmd, UploadDetails

# variables unique to the transform/upload
dataset = "index-private-housing-rental-prices" # not necessarily the dataset_id, if more than one v4 is being produced
location = "" # path to where scripts/files will be written, can leave blank to use working directory
source_files = "" # complete path to source file(s), list if more than one, can be left blank if source files are in location above
credentials = "" # path to credentials file, can leave blank if file in working directory

# running the transform
transform = Transform(dataset, location=location, source_files=source_files)
transform.run_transform()

# creating upload_dict
upload_dict = UploadDetails(transform.transform_output, location=location).create()

# uploading data
upload = UploadToCmd(upload_dict, credentials=credentials)
upload.run_upload()
