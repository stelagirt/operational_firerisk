import os
import sys
import glob
import datetime
import subprocess

import pandas as pd

dataFolderPath = None
inputFolder = None
intermediateFolder = None


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
        datetime.datetime.strptime(dateStr, "%Y%m%d")
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
    startDate = datetime.datetime.strptime(startDateStr, "%Y%m%d")
    endDate = datetime.datetime.strptime(endDateStr, "%Y%m%d")
    dates = []

    currentDate = startDate
    while currentDate <= endDate:
        dates.append(currentDate.strftime("%Y%j"))
        currentDate += datetime.timedelta(days=1)

    return dates


def get_files_in_date_range(folderPath, dates):
    """
    Description:
        Returns a list of file paths that match the specified dates in the folder.

    Arguments:
        folderPath (str): Path to the folder where the files are located.
        dates (list): List of date strings to match in the file names.

    Returns:
        list: List of file paths that match the specified dates in the folder.

    """
    files = []
    for date in dates:
        pattern = f"{folderPath}/*A{date}*.hdf"
        files.extend(glob.glob(pattern))

    return files


def make_dataframe(path, startDateStr, endDateStr):
    """
    Description:
        Creates a pandas dataframe with image filenames and dates.

    Arguments:
        path (str): The path to the directory containing the image files.

    Returns:
        A pandas dataframe with columns `image` and `date`.
    """
    allDates = get_dates_between(startDateStr, endDateStr)
    absolutePaths = get_files_in_date_range(path, allDates)

    hdfFiles = [os.path.basename(file) for file in absolutePaths]
    hdfDates = [file.split(".")[1] for file in hdfFiles]

    # Create a pandas.Dataframe with HDF file names and dates.
    dfImages = pd.DataFrame(list(zip(hdfFiles, hdfDates)), columns=["image", "date"])
    # Group the pandas.Dataframe by the date and aggregate image file names into lists.
    return dfImages.groupby("date", group_keys=False)["image"].apply(list).reset_index()


def create_lst_day(frame, productType):
    """
    Description:
        Processes and creates raster files.

    Arguments:
        frame (pandas.Dataframe): A grouped dataframe.
        productType (str): The type of the product.

    Returns:
        None
    """
    productFolder = os.path.join(dataFolderPath, inputFolder, productType)

    for index, row in frame.iterrows():
        print(f"Processing date: `{row['date']}`")
        lstDayPaths = []

        print("Creating paths...")
        for name in row["image"]:
            # Construct the paths for the LST DAY raster files.
            path = f'HDF4_EOS:EOS_GRID:"{productFolder}/{name}":MODIS_Grid_8Day_1km_LST:LST_Day_1km'
            lstDayPaths.append(path)

        # Apply mosaic and clip operations to the raster files.
        mosaic_and_clip_rasters(lstDayPaths, row["date"], productType, "day")


def create_lst_night(frame, productType):
    """
    Description:
        Processes and creates raster files.

    Arguments:
        frame (pandas.Dataframe): A grouped dataframe.
        productType (str): The type of the product.

    Returns:
        None
    """
    productFolder = os.path.join(dataFolderPath, inputFolder, productType)

    for index, row in frame.iterrows():
        print(f"Processing date: `{row['date']}`")
        lstNightPaths = []

        print("Creating paths...")
        for name in row["image"]:
            # Construct the paths for the LST NIGHT raster files.
            path = f'HDF4_EOS:EOS_GRID:"{productFolder}/{name}":MODIS_Grid_8Day_1km_LST:LST_Night_1km'
            lstNightPaths.append(path)

        # Apply mosaic and clip operations to the raster files.
        mosaic_and_clip_rasters(lstNightPaths, row["date"], productType, "night")


def mosaic_and_clip_rasters(paths, date, productType, timeOfDay):
    """
    Description:
        Processes HDF files by merging and clipping them.

    Arguments:
        paths (list): The date associated with the HDF files.
        date (str): List of file paths for the HDF files.
        productType (str): The type of the product.
        timeOfDay (str): The time of day. One of ["day", "night"].

    Returns:
        None.
    """
    path = os.path.join(dataFolderPath, intermediateFolder, productType)

    # Define the intermediate and output file names based on date and mode.
    intermediateName = f"{path}/{date}_{timeOfDay}_merge.tif"
    outputTifName = f"{path}/{date}_{timeOfDay}_warp.tif"
    outputNetName = f"{path}/{date}_{timeOfDay}_warp.nc"

    # Generate the gdal_merge.py command to merge the HDF files.
    mergeCommand = ["gdal_merge.py", "-o", intermediateName, " ".join(paths)]
    mergeCommand = " ".join(mergeCommand)
    # Call the gdal_merge.py command.
    print("Running gdal_merge.py command...")
    subprocess.call(mergeCommand, shell=True)

    # Generate the gdalwarp command to warp and clip the merged file.
    warpCommand = ["gdalwarp", "-t_srs", "EPSG:4326", "-te 19.46 34.44 28.29 41.9", intermediateName, outputTifName]
    warpCommand = " ".join(warpCommand)
    # Call the gdalwarp command.
    print("Running gdalwarp command...")
    subprocess.call(warpCommand, shell=True)

    # Generate the gdal_translate command.
    translateCommand = ["gdal_translate", "-of", "NetCDF", outputTifName, outputNetName]
    translateCommand = " ".join(translateCommand)
    # Call the gdal_translate command.
    print("Running gdal_translate command...")
    subprocess.call(translateCommand, shell=True)

    # Remove the intermediate merged file.
    print(f"Deleting: `{intermediateName}`...")
    os.remove(intermediateName)


def main(args):
    """
    Main access point.
    """
    if len(args) != 3:
        sys.exit("Please provide exactly 3 arguments.")

    if not (is_date_format_valid(args[0]) and is_date_format_valid(args[1])):
        print("One of the dates does not match the format '%Y%m%d'")
        sys.exit()

    print(f"Executing `mod11a2_myd11a2_process.py` with arguments: `{args[0]}`, `{args[1]}`.")

    # Declare product types.
    productTypes = ["MOD11A2", "MYD11A2"]

    # Declare folders.
    global dataFolderPath
    global inputFolder
    global intermediateFolder
    dataFolderPath = args[2]
    inputFolder = "input_satellite"
    intermediateFolder = "intermediate_satellite"

    for productType in productTypes:
        print(f"Product type is set to `{productType}`")

        # Determine the input and intermediate paths based on the current product type.
        inputPath = os.path.join(dataFolderPath, inputFolder, productType)

        # List all files with extension "hdf" or "xml" in the input folder.
        files = [file for file in os.listdir(inputPath) if file.endswith("hdf")]

        if not files:
            print(f"Did not encounter any `{productType}` inside folder: `{inputPath}`.")
            continue

        # Create the grouped dataframe.
        print("Creating a grouped dataframe...")
        groupedFrame = make_dataframe(inputPath, args[0], args[1])

        # Create rasters based on the grouped dataframe.
        print("Processing raster files...")
        create_lst_day(groupedFrame, productType)
        create_lst_night(groupedFrame, productType)

        for fileName in os.listdir(inputPath):
            filePath = os.path.join(inputPath, fileName)
            if os.path.isfile(filePath):
                print(f"Deleting: `{filePath}`...")
                os.remove(filePath)

    print("Completion of execution.")
    print(f"{'-' * 80}")


if __name__ == "__main__":
    main(sys.argv[1:])
