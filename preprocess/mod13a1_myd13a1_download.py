"""
Execution:
    conda activate lazarus
    python mod13a1_myd13a1_download.py %Y%m%d %Y%m%d
"""
import os
import sys
import json
import datetime

from pymodis import downmodis


def is_date_format_valid(dateStr):
    """
    Description:
        Checks if a date string matches the format '%Y-%m-%d'.

    Arguments:
        dateStr (str): Date string to be checked.

    Returns:
        bool: True if the date string matches the format '%Y-%m-%d', False otherwise.
    """
    try:
        datetime.datetime.strptime(dateStr, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def download_rasters(jsonCatalog, destinationFolder, startDate, endDate, delta, user, password, product, path, tiles):
    """
    Description:
        Download MODIS rasters and transfer them to a local folder.

    Arguments:
        jsonCatalog (str): Path to the JSON catalog file.
        destinationFolder (str): The destination folder-path.
        startDate (str): The start date in the `YYYY-MM-DD` format.
        endDate (str): The end date in the `YYYY-MM-DD` format.
        delta (int): The time difference in days between consecutive MODIS rasters.
        user (str): Username for authentication.
        password (str): Password for authentication.
        product (str): The code of the MODIS product.
        path (str): Something MODIS, no idea.
        tiles (str): The tiles to download.

    Returns:
        None
    """
    # Create an instance of the downModis class.
    print(f"Starting MODIS `{product}` rasters download...")
    connection = downmodis.downModis(destinationFolder=destinationFolder, tiles=tiles, today=startDate,
                                     enddate=endDate, delta=delta, user=user, password=password,
                                     product=product, path=path, jpg=False, debug=False,
                                     timeout=30, checkgdal=True)

    # Connect to the server.
    try:
        connection.connect(ncon=20)
        print(f"Connection established at: `{connection.nconnection}` attempts.")

    # If the code enters this block, the ftp server is at fault.
    except Exception as e:
        print(f"An error occurred: {e}")
        return

    print(f"Parsing JSON catalog: `{jsonCatalog}`...")
    with open(jsonCatalog) as file:
        jsonData = json.load(file)

    for day in sorted(connection.getListDays()):
        print(f"Checking date: `{day}`...")
        formattedDay = day.replace(".", "")

        if formattedDay in jsonData:
            print(f"The JSON catalog already contains an entry with date: `{formattedDay}`!")
            print("Skipping this day...")
            continue

        # Return a list of files to download.
        allFiles = connection.getFilesList(day)
        print(f"Found: `{len(allFiles)}` on the FTP server.")
        # Check if a file already exists in the local directory.
        remoteFiles = connection.checkDataExist(allFiles)
        print(f"Found: `{len(remoteFiles)}` files to download.")
        # Filter HDF files.
        hdfRemotefiles = [fl for fl in remoteFiles if fl.endswith("hdf")]

        # Download tiles for the selected day.
        if remoteFiles:
            connection.dayDownload(day, remoteFiles)
            print("Downloaded all files.")
            # Remove files in the destination directory that have a file size of 0.
            connection.removeEmptyFiles()
            print("Removed empty files.")

            # Check each HDF file with GDAL to ensure that the download was successful.
            print("Performing file checks...")
            corruptedFiles = []

            for file in hdfRemotefiles:
                filePath = os.path.join(destinationFolder, file)
                check = connection.checkFile(filePath)
                print(f"`{file}`: `{'Valid' if not check else 'Corrupted'}`.")

                if check:
                    corruptedFiles.append(file)
                    os.remove(filePath)

            print(f"Checked `{len(hdfRemotefiles)}` HDF files. `{len(corruptedFiles)}` files were corrupted and removed.")


def main(args):
    """
    Main entry point of the program.
    """

    if len(args) != 3:
        sys.exit("Please provide exactly 3 arguments.")

    if not (is_date_format_valid(args[0]) and is_date_format_valid(args[1])):
        print("One of the dates does not match the format '%Y-%m-%d'")
        sys.exit()

    print(f"Executing `mod13a1_myd13a1_download.py` with arguments: `{args[0]}`, `{args[1]}`.")

    # 1) Set connection parameters.

    # Specify the tiles to download - Greece region.
    tiles = "h19v04,h19v05,h20v05"

    # Account credentials for authentication.
    user = "stelgirt"
    password = "Kamilia90"

    # Set the interval.
    delta = 1

    # Declare folders.
    dataFolderPath = args[2]
    inputFolder = "input_satellite"
    jsonCatalogs = [
        "catalogs/mod13a1.json",
        "catalogs/myd13a1.json"
    ]

    pathTypes = ["MOLT", "MOLA"]
    productTypes = ["MOD13A1.061", "MYD13A1.061"]

    # 2) Download rasters for the specified product and path.

    for pathType, productType, jsonCatalog in zip(pathTypes, productTypes, jsonCatalogs):

        # Compute folder paths.
        destinationFolderPath = os.path.join(dataFolderPath, inputFolder, productType.split(".")[0])
        jsonCatalogPath = os.path.join(dataFolderPath, jsonCatalog)

        download_rasters(jsonCatalogPath, destinationFolderPath, args[0], args[1],
                         delta, user, password, productType, pathType, tiles)

    print("Completion of execution.")
    print(f"{'-'*80}")


if __name__ == "__main__":
    main(sys.argv[1:])

# http://www.pymodis.org/pymodis/downmodis.html#pymodis.downmodis.downModis
