# cmd-run-transform

This repo is used to run CMD transforms and to upload the outputted data file (v4) into Florence ready to be published.

The main purpose is to transform a given dataset into v4 format which is compatiable with CMD. 

As a high level overview this is done by running the python file, for a given dataset, the python file will fetch the transform for that dataset from the [cmd transforms repo](https://github.com/ONS-OpenData/cmd-transforms), run that transform and output the v4 file into the repo directory.

The second purpose of the python script is to upload the outputted v4 into Florence. Again as a high level overview this is done by making requests to Florence's APIs to upload the v4 and then using the APIs to add the data to a collection and then add all of the relevant metadata.
However to use the upload feature of this script you must have the correct Florence access and must also be ssh'd into the correct environment, more can be found [here](https://github.com/ONSdigital/dp-cli)

## How to use

To run a transform only use the command

`python main.py -d dataset_id`

The dataset_id name must match the name of the folder for the dataset given [here](https://github.com/ONS-OpenData/cmd-transforms)

It can also run multiple transforms at once, for example:

`python main.py -d dataset_1 datset_2 dataset_3`

Uploading the file can be run at the same time using the  -u flag (order of the flags is irrelevent):

`python main.py -d dataset_id -u`

As the transform and upload is running, the script will provide feedback for the stage that it is on (number of imported observations for example) and will let you know when it is complete.

Most transforms

## Other flags
