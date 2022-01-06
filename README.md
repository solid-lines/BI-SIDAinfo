# SIDAInfo-Updates
This is the node script created for updating the DHIS2 data from the SIDAinfo data dump for PSI Burundi.

## Installation of the script
- Install node (version 12 or above)
- Download the code of the script from github or another source
- Unzip the code
- Go to the ```SIDAInfo-Updates``` folder (```cd SIDAInfo-Updates```)
- Install the script dependencies (npm install)

## Preparation of the source files
1. Run SIDAInfo-DHIS2 script to generate the file new_dump_additions-patient_code_uid
2. Save the file in the root directory
3. Save generated teis folders in SIDAInfo-Updates/teis
4. Run ```retrieveDHISdata.js```to download the data from the DHIS2 server (formatted files will be stored in DHIS2_data folder)


## Folders in the script
- ```log```: For the log files generated by the script
- ```DHIS2_data```: For the formated files generated by the script from the DHIS2 data 
- ```teis```: For the TEI files generated by SIDAInfo script from the new dump
- ```actions```: For the actions files generated by SIDAInfo-Updates script

## Run the script
- Run the script using the arguments: ```node .\index.js diff --org_unit=XXXXXXXXXXXXX --export_date_current=YYYY_MM_DD```
- Arguments: ORGUNIT, EXPORT_DATE_CURRENT
- Need help? run ```node generate help```

## Output files
- ```actions.json```: objects structure to be read by ```uploadData.js```

## Comments
- OrganisationUnit is not reviewed (it's assumed that is the same since it comes from the same source file given as an argument)
