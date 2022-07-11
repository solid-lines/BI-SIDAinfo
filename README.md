# SIDAInfo
This is the node script created for managing the interoperability between SIDAinfo and DHIS2 for PSI Burundi.

## Installation of the script
- Install node (version 12 or above)
- Download the code of the script from github or another source
- Unzip the code
- Go to the ```BI-SIDAinfo``` folder (```cd BI-SIDAinfo```)
- Install the script dependencies (```npm install```)

## Genarate the executable file/s
- Run ```pkg .```

## Long documentation
Please read the documentation/SIDAInfo-DHIS2 Interoperability - Administrator's Manual (ENGLISH).docx

## Notes
- In the generation, sometimes there are empty values in the .txt files. In order to search them, run ```grep -irn ': \"\"' .``` in the log files
- The OUcode is the internal SIDAinfo site code, NOT the dhis2 uid or code.
- Particular version dependency: ```npm install csv@5.3.2```
