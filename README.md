# Node-RED for SWA Meshup Server

## Installation and Deployment

### Build the image

Run this command to create the image:
''' docker build -t fnyffene/swa-meshup . '''

Optional: 
If you want to test the macro model simulation, also checkout and build swa-marcomodel.
The docker compose file expects swa-meshup and swa-macromodel to be in the same parent folder.

### Development Setup

1. Get the .env file from the project owner and add it to root directory
2. To start the devleopment enviornment, run: 
''' docker-compose up '''

All changes to the flow will be mapped to this directory, just commit, push and pull the changes to/from git.

### Deployment

All pushes to the master branch will be deployed to appui automatically (test environment).


## Testing

 The "Macromodel" flow allows you to create some dummy testdata, start the simulation and read the results. This is for debugging and testing of the deplyment, only. The data makes absolutley no sense.

## API

ToDo: Document the API (probably on swagger)


## Setup of production environment on APPUI


1. Create Deplyoment on open shift
'''oc new-app https://github.com/nyfelix/swa-meshup.git --strategy=docker --name=swa-meshup --allow-missing-images'''
'''oc new-app https://github.com/nyfelix/swa-macromodel.git --strategy=docker --name=swa-macromodel --allow-missing-images'''

2. Create a rounte for swa-meshup (public access)

3. Create a Volume on APPUI 
In APPUIO Console: Storage > Create Storage

4. Mount Volumes
For both deplyments (swa-msehup, swa-macromodel): Add Storage and mount to /rea-data

5. Set the enviroment variables
In APPUIO Console: Deplyoments > swa-meshup > Environmet
For the MACRO_ENDPOINT use the IP Adresse of swa-macromodel service and add port 5000

6. Add CI webhook on GitHub or GitLab
Get the Webhook on: Builds > swa-macromodel > Configuration
Add it to Repository > Settings > Webhooks