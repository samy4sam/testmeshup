# Node-RED for SWA Meshup Server

## Installation and Deployment

### Folder structure
```
/doc      : document
/meshup   : node-red component to mesh up all datasoruces the flows will be stored in this directory
/influx   : configuraiton of influx 
/chronograf : data and config of chronograf
```

For more information about the full TICK stack under docker (only partially used here) see: https://github.com/influxdata/sandbox

### Build for development

Run 
` docker-compose up `
to build all relevant containers


### Startup development environment

1. Get the .env file from the project owner and add it to root directory
2. To start the devleopment enviornment, run: 
` docker-compose up `

After succcessful start, the components can be reached on the follwing urls:
```
localhost:1880/admin     NodeRed Admin
loaclhost:5000           Rest API for the macromodel
localhost:8086           InfluxDB
localhost:8888           Chronograf frontend for InfluxDB
```

### Deployment

All pushes to the "release" branch will be deployed to appui automatically (test environment).


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