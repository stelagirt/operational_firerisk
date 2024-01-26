import os
import sys
import json

import numpy as np
import pandas as pd
import xarray as xr

from datetime import datetime, timedelta

dataFolderPath = None
outputMeteoFolder = None
dailyFolder = None
stdFolder = None

mod11a2Catalog = None
myd11a2Catalog = None
mod13a1Catalog = None
myd13a1Catalog = None


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
        date_list.append(currentDate.strftime("%Y%m%d"))
        currentDate += timedelta(days=1)

    return date_list


def meteo_interpolation(date, standard, path):
    """
    Description:

    Aguments:
        date (str): The date in the format `yyyymmdd`.
        standard (xarray.Dataset): The standard dataset used for interpolation.
        path (str): The path to the meteorological data file.

    Returns:
        The interpolated meteorological dataset.
    """
    file = f"{date}_meteo.nc"

    # Open the dataset.
    dataset = xr.open_dataset(os.path.join(path, file)).squeeze()
    # Drop unused coordinates and attributes.
    dataset = dataset.drop([i for i in dataset.coords if i not in dataset.dims])
    dataset.attrs.clear()
    # Interpolate the dataset using nearest-neighbor method.
    interpolatedDataset = dataset.interp(lat=standard["y"], lon=standard["x"], method="nearest")
    # Reverse the y (latitude) dimension.
    interpolatedDataset = interpolatedDataset.reindex(y=list(reversed(interpolatedDataset.y)))

    # Interpolate missing values along the x and y dimensions.
    for var in interpolatedDataset.data_vars:
        interpolatedDataset[var] = interpolatedDataset[var].interpolate_na(dim="x", method="nearest", fill_value="extrapolate")
        interpolatedDataset[var] = interpolatedDataset[var].interpolate_na(dim="y", method="nearest", fill_value="extrapolate")

    # Drop latitude and longitude variables.
    interpolatedDataset = interpolatedDataset.drop_vars(["lat", "lon"])

    # Apply masking based on fire1 values from the standard dataset.
    maskedInterpolatedDataset = interpolatedDataset.where(standard.fire1 > 0)
    # Interpolate along the x and y dimensions using the standard dataset.
    maskedInterpolatedDataset = maskedInterpolatedDataset.interp(x=standard["x"], y=standard["y"], method="nearest")
    # Assign time coordinate based on the input date.
    maskedInterpolatedDataset = maskedInterpolatedDataset.assign_coords(time=pd.to_datetime(date).strftime("%Y-%m-%d"))

    return maskedInterpolatedDataset


def veg_interpolation(date, standard, path):
    """
    Description:
        Interpolates vegetation data for a specific date.

    Arguments:
        date (str): The date in the format `yyyymmdd`
        standard (xarray.Dataset): The standard dataset used for interpolation.
        path (str): The path to the vegetation data file.

    Returns:
        The interpolated vegetation dataset.
    """
    # Open the dataset.
    dataset = xr.open_dataset(path).squeeze()

    # Interpolate the dataset using nearest-neighbor method.
    interpolatedDataset = dataset.interp(x=standard["x"], y=standard["y"], method="nearest")
    interpolatedDataset = interpolatedDataset.squeeze()

    if "spatial_ref" in interpolatedDataset.data_vars:
        interpolatedDataset = interpolatedDataset.drop_vars("spatial_ref")

    # Assign time coordinate based on the input date.
    interpolatedDataset = interpolatedDataset.assign_coords(time=pd.to_datetime(date).strftime("%Y-%m-%d"))

    return interpolatedDataset


def lst_interpolation(date, standard, path):
    """
    Description:
        Interpolates LST data for a specific date.

    Arguments:
        date (str): The date in the format `yyyymmdd`.
        standard (xarray.Dataset): The standard dataset used for interpolation.
        path (str): The path to the LST data file.

    Returns:
        The interpolated LST dataset.
    """
    # Open the dataset.
    dataset = xr.open_dataset(path).squeeze()
    if 'crs' in dataset.data_vars:
        dataset = dataset.drop_vars('crs')
    # Interpolate the dataset using nearest-neighbor method.
    interpolatedDataset = dataset.interp(x=standard["x"], y=standard["y"], method="nearest")
    # Reverse the y (latitude) dimension.
    interpolatedDataset = interpolatedDataset.reindex(y=list(reversed(interpolatedDataset.y)))

    # Interpolate missing values along the x and y dimensions.
    for var in interpolatedDataset.data_vars:
        interpolatedDataset[var] = interpolatedDataset[var].interpolate_na(dim="x", method="nearest", fill_value="extrapolate")
        interpolatedDataset[var] = interpolatedDataset[var].interpolate_na(dim="y", method="nearest", fill_value="extrapolate")

    # Apply masking based on fire1 values from the standard dataset.
    maskedInterpolatedDataset = interpolatedDataset.where(standard.fire1 > 0)
    # Assign time coordinate based on the input date.
    maskedInterpolatedDataset = maskedInterpolatedDataset.assign_coords(time=pd.to_datetime(date).strftime("%Y-%m-%d"))
    # Interpolate along the x and y dimensions using the standard dataset.
    maskedInterpolatedDataset = maskedInterpolatedDataset.interp(x=standard["x"], y=standard["y"], method="nearest")
    # Rename 'day' and 'night' variables to 'lst_day' and 'lst_night'.
    maskedInterpolatedDataset = maskedInterpolatedDataset.rename({"day": "lst_day", "night": "lst_night"})
    # Set zero values to NaN for 'lst_day' and 'lst_night'.
    maskedInterpolatedDataset["lst_day"] = maskedInterpolatedDataset["lst_day"].where(maskedInterpolatedDataset["lst_day"] != 0, np.nan)
    maskedInterpolatedDataset["lst_night"] = maskedInterpolatedDataset["lst_night"].where(maskedInterpolatedDataset["lst_night"] != 0, np.nan)

    return maskedInterpolatedDataset


def find_nearest_file(targetDate, mode):
    """
    Description:

    Arguments:
        mode (str):

    Returns:

    """
    if mode == "mod11a2":
        jsonCatalog = os.path.join(dataFolderPath, mod11a2Catalog)

    elif mode == "myd11a2":
        jsonCatalog = os.path.join(dataFolderPath, myd11a2Catalog)

    elif mode == "mod13a1":
        jsonCatalog = os.path.join(dataFolderPath, mod13a1Catalog)

    elif mode == "myd13a1":
        jsonCatalog = os.path.join(dataFolderPath, myd13a1Catalog)

    else:
        return

    try:
        with open(jsonCatalog, "r") as file:
            jsonData = json.load(file)

        closestDate = None
        minimumDifference = timedelta.max
        targetDate = datetime.strptime(targetDate, "%Y%m%d")

        for key, value in jsonData.items():
            keyDate = datetime.strptime(key, "%Y%m%d")
            dateDifference = targetDate - keyDate

            if timedelta() <= dateDifference < minimumDifference:
                minimumDifference = dateDifference
                closestDate = keyDate

        if closestDate is not None:
            return closestDate, jsonData[closestDate.strftime("%Y%m%d")]

    except FileNotFoundError:
        print(f"File `{jsonCatalog}` not found.")

    except PermissionError:
        print(f"Permission denied for file `{jsonCatalog}`.")

    except IsADirectoryError:
        print(f"`{jsonCatalog}` is a directory, not a file.")

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON in `{jsonCatalog}`: {e}")


def make_daily_files(dates):
    """
    Description:
        Creates daily files based on a list of dates.

    Args:
        dates (list): A list of dates in the format "YYYY-MMM-DD"

    Returns:
        None
    """
    # Declare Folders.
    meteoFolder = os.path.join(dataFolderPath, outputMeteoFolder)
    standardsFolder = os.path.join(dataFolderPath, stdFolder)

    lstStandard = xr.open_dataset(os.path.join(standardsFolder, "lst_standard.nc"))
    lstStandard["fire1"] = lstStandard.lst_day.where(lstStandard.lst_day > 0, np.nan)

    for date in dates:
        try:
            print(f"Processing date: `{date}`")

            # Find nearest files.
            mod11a2Date, mod11a2Product = find_nearest_file(date, "mod11a2")
            # myd11a2Date, myd11a2Product = find_nearest_file(date, "myd11a2")

            mod13a1Date, mod13a1Product = find_nearest_file(date, "mod13a1")
            myd13a1Date, myd13a1Product = find_nearest_file(date, "myd13a1")

            # Interpolate meteorological data.
            print("Interpolating meteorological data...")
            meteoInterpolated = meteo_interpolation(date, lstStandard, meteoFolder)

            # Interpolate lst data.
            print("Interpolating mod11a2 data...")
            mod11a2Interpolated = lst_interpolation(date, lstStandard, mod11a2Product)
            # print("Interpolating myd11a2 data...")
            # myd11a2Interpolated = lst_interpolation(date, lstStandard, myd11a2Product)

            # Interpolate vegetation data.
            print("Interpolating mod13a1 data...")
            mod13a1Interpolated = veg_interpolation(date, lstStandard, mod13a1Product)
            print("Interpolating myd13a1 data...")
            myd13a1Interpolated = veg_interpolation(date, lstStandard, myd13a1Product)

            mod13a1Difference = datetime.strptime(date, "%Y%m%d") - mod13a1Date
            myd13a1Difference = datetime.strptime(date, "%Y%m%d") - myd13a1Date
            mod11a2Difference = datetime.strptime(date, "%Y%m%d") - mod11a2Date

            print(f"Latest mod11a2 date (LST): `{mod11a2Date}` with diff: `{mod11a2Difference}`")
            print(f"Latest mod13a1 date (NDVI): `{mod13a1Date}` with diff: `{mod13a1Difference}`")
            print(f"Latest myd13a1 date (NDVI): `{mod11a2Date}` with diff: `{myd13a1Difference}`")

            # Merge the interpolated data.
            print("Merging data...")

            if mod13a1Difference < myd13a1Difference:
                print("Selected `mod13a1`")
                mergedDataset = xr.merge([
                    meteoInterpolated, mod11a2Interpolated, mod13a1Interpolated
                ])

            elif myd13a1Difference < mod13a1Difference:
                print("Selected `myd13a1`")
                mergedDataset = xr.merge([
                    meteoInterpolated, mod11a2Interpolated, myd13a1Interpolated
                ])

            final = mergedDataset.copy()

            if "band" in list(final.data_vars):
                final = final.drop_vars("band")

            variable = datetime.strptime(date, "%Y%m%d")
            weekDay = variable.weekday() + 1
            month = variable.month

            newData = np.where(final["ndvi"].notnull(), weekDay, np.nan)
            data = np.where(final["ndvi"].notnull(), month, np.nan)
            final["weekday"] = xr.DataArray(newData, dims=("y", "x"))
            final["month"] = xr.DataArray(data, dims=("y", "x"))
            final.to_netcdf(os.path.join(dataFolderPath, dailyFolder, f"{date}.nc"))

        except FileNotFoundError as error:
            print(f"{error}")
            print(f"File not found error occurred for date `{date}`.")

        except Exception as error:
            print(f"{error}")


def main(args):
    """
    Main access point.
    """
    if len(args) != 2:
        sys.exit("Please provide exactly 2 arguments.")

    if not (is_date_format_valid(args[0]) and is_date_format_valid(args[1])):
        print("One of the dates does not match the format '%Y%m%d'")
        sys.exit()

    #  Declare folders.
    global dataFolderPath
    global outputMeteoFolder
    global dailyFolder
    global stdFolder

    global mod11a2Catalog
    global myd11a2Catalog
    global mod13a1Catalog
    global myd13a1Catalog

    dataFolderPath = "/home/lstam/Documents/data/"
    outputMeteoFolder = "output_meteorological"
    dailyFolder = "daily_rasters"
    stdFolder = "standards"

    mod11a2Catalog = "catalogs/mod11a2.json"
    myd11a2Catalog = "catalogs/myd11a2.json"
    mod13a1Catalog = "catalogs/mod13a1.json"
    myd13a1Catalog = "catalogs/myd13a1.json"

    meteoFolder = os.path.join(dataFolderPath, outputMeteoFolder)
    dailyFolder = os.path.join(dataFolderPath, dailyFolder)

    # Get the dates.
    files = os.listdir(meteoFolder)

    dailyFiles = os.listdir(dailyFolder)
    dailyFileDates = [dailyFile.split("_")[0] for dailyFile in dailyFiles]

    dates = get_dates_between(args[0], args[1])
    filteredDates = list(filter(lambda date: any(date in file for file in files), dates))
    filteredDates = [date for date in filteredDates if date not in dailyFileDates]

    # Create daily files based on the dates.
    make_daily_files(filteredDates)


if __name__ == "__main__":
    main(sys.argv[1:])
