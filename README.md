# SIDAInfo-Updates
This is the node script created for updating the DHIS2 data from the SIDAinfo data dump for PSI Burundi.

## Installation of the script
- Install node (version 12 or above)
- Download the code of the script from github or another source
- Unzip the code
- Go to the ```SIDAInfo-Updates``` folder (```cd SIDAInfo-Updates```)
- Install the script dependencies (npm install)

## Folders in the script
- ```log```: For the log files generated by the script
- ```PREVIOUS_DHIS2_data```: For the formated files generated by the script from the DHIS2 data 
- ```teis```: For the TEI files generated by SIDAInfo script from the new dump
- ```actions```: For the actions files generated by SIDAInfo-Updates script

## Preparation of the source files
1. Run ```node retrieve_DHIS_data.js``` to download the data from the DHIS2 server (formatted files will be stored in DHIS2_data folder). This script generate files enfant.json, mere.json, tarv.json, teis.json and, finally, previous_all_patient_index.json in folder PREVIOUS_DHIS2_data/OUcode_YYYY_MM_DD. The file previous_all_patient_index.json & teis.json are used in the diff script.
2. Run SIDAInfo-DHIS2 script to generate the file OUcode_YYYY_MM_DD-patient_code_uid-FINAL.json
3. Copy the file ```OUcode_YYYY_MM_DD_FINAL_all_patient_index.json``` in the root directory & rename it to ```current_all_patient_index.json```
4. Copy generated teis folders in ```SIDAInfo-Updates/teis``` (like ```SIDAInfo-Updates/teis/17020203_2022_05_10```)
5. Run the script (see below)

## Run the script for generating the update actions
- Run the script using the arguments: ```node .\index.js diff --site=XXXXXXXX --export_dump_date=YYYY_MM_DD```
- Arguments: ORGUNIT, EXPORT_DATE_CURRENT
- Need help? run ```node generate help```

## Output files
- ```actions.json```: objects structure to be read by ```upload_data.js```

## Run the script for uploading the updates
- Run the script ```node .\upload_data.js```

## Genarate the executable files
- Run ```pkg .```

## Notes
- OrganisationUnit is not reviewed (it's assumed that is the same since it comes from the same source file given as an argument)
- The OUcode is the internal SIDAinfo code, NOT the dhis2 uid or code.
- npm install csv@5.3.2
