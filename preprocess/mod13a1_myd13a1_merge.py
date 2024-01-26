import os
import sys
import glob
import json

import xarray as xr
import rioxarray as rxr

from datetime import datetime, timedelta


def is_date_format_valid(dateStr):
    """
    Description:
        Checks if a date string matches the format '%Y%m%d'.

    Arguments:
        dateStr (str): Date string to be checked.

    Returns:
        bool: True if the date string matches the format '%Y%m%d', False otherwise.
    """
    try:
        datetime.strptime(dateStr, "%Y%m%d")
        return True
    except ValueError:
        return False


def get_dates_between(startDateStr, endDateStr):
    """
    Description:
        Returns a list of date strings between the start and end dates.

    Arguments:
        startDateStr (str): Start date string in the format 'YYYYMMDD'.
        endDateStr (str): End date string in the format 'YYYYMMDD'.

    Returns:
        list: List of date strings between the start and end dates.

    """
    startDate = datetime.strptime(startDateStr, "%Y%m%d")
    endDate = datetime.strptime(endDateStr, "%Y%m%d")
    date_list = []

    currentDate = startDate
    while currentDate <= endDate:
        date_list.append(currentDate.strftime("%Y%j"))
        currentDate += timedelta(days=1)

    return date_list


def main(args):
    """
    Main access point.
    """
    if len(args) != 3:
        sys.exit("Please provide exactly 3 arguments.")

    if not (is_date_format_valid(args[0]) and is_date_format_valid(args[1])):
        print("One of the dates does not match the format '%Y%m%d'")
        sys.exit()

    print(f"Executing `mod13a1_myd13a1_merge.py` with arguments: `{args[0]}`, `{args[1]}`")

    # Declare folders.
    dataFolderPath = args[2]
    intermediateFolder = "intermediate_satellite"
    outputFolder = "output_satellite"

    # Declare product types.
    productTypes = ["MOD13A1", "MYD13A1"]
    jsonCatalogs = [
        "catalogs/mod13a1.json",
        "catalogs/myd13a1.json"
    ]

    for productType, jsonCatalog in zip(productTypes, jsonCatalogs):
        print(f"Product type is set to `{productType}`")

        # Define the path based on the product type.
        intermediatePath = os.path.join(dataFolderPath, intermediateFolder, productType)
        outputPath = os.path.join(dataFolderPath, outputFolder, productType)

        # Retrieve the list of EVI and NDVI files in the path.
        dates = get_dates_between(args[0], args[1])

        eviFiles = []
        ndviFiles = []
        for date in dates:
            eviPattern = os.path.join(intermediatePath, f"A{date}_evi_warp.nc")
            ndviPattern = os.path.join(intermediatePath, f"A{date}_ndvi_warp.nc")

            eviFiles.extend(glob.glob(eviPattern))
            ndviFiles.extend(glob.glob(ndviPattern))

        # Sort the two lists of files.
        eviFiles.sort()
        ndviFiles.sort()

        # Create a dictionary to store file pairs based on the date.
        dct = {}
        procDict = {}

        # Populate the dictionary
        for eviFile, ndviFile, in zip(eviFiles, ndviFiles):
            date = os.path.basename(eviFile)[1:8]
            dct[date] = [(eviFile, ndviFile)]

        for date, filePairs in dct.items():
            doy = date[4:]
            year = int(date[:4])
            date = datetime(year, 1, 1) + timedelta(int(doy) - 1)
            yyyymmdd = date.strftime("%Y%m%d")

            # Define the output NetCDF file name.
            netcdfFileName = os.path.join(outputPath, f"{yyyymmdd}_vegetation.nc")
            procDict[yyyymmdd] = netcdfFileName

            for eviPath, ndviPath in filePairs:
                print("EVI:     ", eviPath)
                print("NDVI:   ", ndviPath)
                print("final-Name: ", netcdfFileName)

                # Open the LST rasters.
                eviDataset = rxr.open_rasterio(eviPath).squeeze()
                ndviDataset = rxr.open_rasterio(ndviPath).squeeze()

                # Set names for the LST day and night datasets.
                eviDataset.name = "evi"
                ndviDataset.name = "ndvi"

                # Merge the LST day and night datasets.
                mergedDataset = xr.merge([eviDataset, ndviDataset])
                # Save the merged dataset as NetCDF.
                mergedDataset.to_netcdf(netcdfFileName)

        if procDict:
            # Store the most recent products as JSON.
            print(f"Parsing JSON catalog: `{jsonCatalog}`...")
            jsonPath = os.path.join(dataFolderPath, jsonCatalog)
            if os.path.isfile(jsonPath) and os.path.getsize(jsonPath) > 0:
                with open(jsonPath, "r+") as jsonFile:
                    jsonData = json.load(jsonFile)

                    for key, value in procDict.items():
                        if key not in jsonData:
                            jsonData[key] = value
                            print(f"Added key: `{key}` to the JSON catalog.")

                        else:
                            print(f"The JSON catalog already contains an entry with key: `{key}`")

                    # Ensure that the file pointer is moved to the beginning of the file.
                    jsonFile.seek(0)
                    json.dump(jsonData, jsonFile)
                    # Remove any remaining content in the file after writing the updated JSON data.
                    jsonFile.truncate()

            else:
                with open(jsonPath, "w") as jsonFile:
                    json.dump(procDict, jsonFile, indent=4)
        else:
            print("The update of the image data as JSON was skipped")

        for fileName in os.listdir(intermediatePath):
            filePath = os.path.join(intermediatePath, fileName)
            if os.path.isfile(filePath):
                print(f"Deleting: `{filePath}`...")
                os.remove(filePath)

    print("Completion of execution.")
    print(f"{'-'*80}")


if __name__ == "__main__":
    main(sys.argv[1:])
