# cmd-run-transform

This repo is used to run CMD transforms and to upload the outputted data file (v4) into Florence ready to be published.

The main purpose is to transform a given dataset into v4 format which is compatiable with CMD. 

As a high level overview this is done by running main.py through terminal, specifying one or multiple datasets, the python file will fetch the transform for that dataset from the [cmd transforms repo](https://github.com/ONS-OpenData/cmd-transforms), run that transform and output the v4 file into the repo directory.

The second purpose of this repo is to upload the outputted v4 into Florence. Again as a high level overview this is done by making http requests to Florence's APIs to upload the v4 and then using the APIs to add the data to a collection and then add all of the relevant metadata.
However to use the upload feature of this script you must have the correct Florence access and have access to the correct environement. This can be done on network or be ssh'd into the environment, more can be found [here](https://github.com/ONSdigital/dp-cli)

## How to use

To run a transform only use the command

`python main.py -d dataset_id`

The dataset_id name must match the name of the folder for the dataset given [here](https://github.com/ONS-OpenData/cmd-transforms)

It can also run multiple transforms at once, for example:

`python main.py -d dataset_1 datset_2 dataset_3`

Uploading the file can be run at the same time using the  -u flag (order of the flags is irrelevent):

`python main.py -d dataset_id -u`

As the transform and upload is running, the script will provide feedback for the stage that it is on (number of imported observations for example) and will let you know when it is complete.

Most transforms pull the source data from the ons website, a list of the transforms that do this along with the source data url can be found [here](https://github.com/ONS-OpenData/cmd-run-transform/blob/master/landing_pages.json). Any transform not on this list will need the source file(s) to be added into the repo before running.

In order to use the upload function of the app `-u` the user must have access to Florence and the login credentials must be stored as environment variables. "FLORENCE_EMAIL" as the login email and "FLORENCE_PASSWORD" as the password. If these are not saved as environemt variables or if you are running this on an on netowork machine (cannot save env variables) then the user will be prompted to input their credentials every time the app is run.

## Flags

| Flag | Description |
| --- | --- |
| `-h` | help flag, will also print a list of available transforms |
| `-d` | dataset flag (required), follow with the dataset(s) that you want run |
| `-u` | upload flag, used to run the upload |
| `-s` | source_file flag, used if specifying source files directly, not needed if files are pulled directly or if files are in repo |
| `-up` | partial upload flag, runs a partial upload - which stops after the instance upload is complete |
| `-rl` | run locally flag, runs a transform that is stored locally (rather than from github), useful when changes are needed to a transform, path to local transforms should be "../cmd-transforms/<dataset_id>/main.py" |
| `-C` | clear repo flag, clears all source files and v4s after upload is complete (useful to keep repo from getting cluttered) |
| `-I` | ignore release date flag, transform will fail if run on a different day to source file being released, use this flag to override this |

 

