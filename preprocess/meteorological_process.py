import os
import sys
import datetime
import subprocess

import numpy as np
import xarray as xr

from numpy import pi
from datetime import timedelta
import pandas as pd


def convert_grb2_to_nc(inputPath, outputPath):
    """
    Description:
        Convert GRIB2 files to NetCDF format using GDAL.

    Args:
        inputPath (str): The absolute path to the directory containing the input GRIB2 files.
        outputPath (str): The absolute path to the directory where the converted NetCDF files will be saved.

    Returns:
        -

    Throws:
        subprocess.CalledProcessError

    Execution
        inputPath = "/path/to/input/files"
        outputPath = "/path/to/output/files"

        convert_grb2_to_nc(inputPath, outputPath)

    Notes:
        The os.path.splitext() can handle different file extensions, including those with multiple dots.
        The os.system() executes commands in a subshell, which may have security implications and platform dependencies.
        The subprocess.run() to execute the command and capture the output if needed.
    """

    # Iterate over the meteo files in the input directory.
    for fileName in os.listdir(inputPath):

        # Check for files with the `.grb2` extension.
        print(f"Checking fileName: `{fileName}`")

        if fileName.endswith(".grb2"):
            outputFileName = os.path.splitext(fileName)[0] + ".nc"
            outputFilePath = os.path.join(outputPath, outputFileName)

            # Check if a corresponding file with the `.nc` extension DOES NOT exist in the output directory.
            if not os.path.exists(outputFilePath):
                print(f"Creating file: ", {outputFilePath})
                command = ["gdal_translate", "-of", "netCDF", os.path.join(inputPath, fileName), outputFilePath]

                try:
                    subprocess.run(command, check=True)
                    print("File created successfully.")
                except subprocess.CalledProcessError as error:
                    print(f"Error executing command: {error}")
            else:
                print(f"Skipping file: ", {outputFilePath}, " as it already exists.")


def concatenate_variables(dataset, path, datest, variable, bandName):
    """
    Description:
        Concatenates variables.

    Args:
        dataset (str): The path of the dataset to process.
        path (str): The path to the directory containing the dataset.
        variable (str): The variable name to select from the file (default: 'u-component of wind [m/s]').
        bandName (str): The name to assign to the concatenated variable in the output dataset (default: 'u10').

    Returns:
        dataset (xarray.Dataset): The concatenated dataset.

    Throws:
        AttributeError

    Execution
        dataset = "dataset.nc"
        path = "/pat/to/folder/"
        variable = "u-component of wind [m/s]" | "v-component of wind [m/s]"
        bandName = "u10" | "v10"

    Notes:
        Instead of a list comprehension the .sel() function is used.
        A new `inputPath` instead of the global res_path variable was added.
        Instead of manually renaming dimensions and variables the rename_dims() and rename_vars() functions are used.
"""

    # Open the dataset.
    print("Opening dataset:", os.path.join(path, dataset))
    # ds = xr.open_dataset(os.path.join(path, dataset))
    ds = xr.open_dataset(os.path.join(path, dataset), engine="netcdf4")

    # Select the variables based on the given variable name
    selectedVars = ds[[var for var in ds.data_vars if ds[var].attrs.get("GRIB_COMMENT") == variable]]
    print("Selecting variables with GRIB_COMMENT:", variable)

    # Extract the time information for each variable
    timeDict = {}

    for band in selectedVars.data_vars:

        try:
            # long_time = int(selected_vars[band].GRIB_VALID_TIME)
            longTime = int(selectedVars[band].GRIB_VALID_TIME.dt.total_seconds())
        except AttributeError:
            longTime = int(ds[band].GRIB_VALID_TIME.split("s")[0])

        dtObject = datetime.datetime.fromtimestamp(longTime)
        timeDict[band] = dtObject

    # Concatenate the selected variables along the time dimension.
    print("Concatenating selected variables along the time dimension.")
    newVariable = xr.concat([selectedVars[varName] for varName in selectedVars.data_vars], dim=list(timeDict.values()))

    # Rename the time dimension and variables.
    # new_variable = new_variable.rename({"concat_dim":"time"})
    print("Renaming time dimension and variables.")
    newVariable = newVariable.rename({"concat_dim": "time"})
    dataset = newVariable.to_dataset()
    # dataset = dataset.rename({list(dataset.data_vars)[0]:band_name})
    dataset = dataset.rename({list(dataset.data_vars)[0]: bandName})

    # Sort and slice the dataset if `time` dimension exists.
    if "time" in dataset.dims:
        print("Sorting and slicing on the time dimension for %s %s"%(variable, bandName))
        dataset = dataset.sortby("time")
        #dataset = dataset.isel(time=slice(10, 34))
        dataset = dataset.sel(time=slice(pd.to_datetime(datest, format='%Y%m%d'),
                                         pd.to_datetime(datest+"2359", format='%Y%m%d%H%M')))
        print('Time limits wind:\nFirst: %s\nLast: %s' % (dataset['time'][0].values, dataset['time'][-1].values))

    print("Dataset processing completed.")
    return dataset


def find_majority_element(elements):
    """
    Find the majority element in an array.

    Args:
        elements (numpy.ndarray): The input array.

    Returns:
        int: The majority element in the array.
    """

    # Return the element with the maximum count.
    return np.bincount(elements).argmax()


def compute_wind(ds):
    """
    Calculate wind-related variables from the input dataset.

    Args:
        ds (xarray.Dataset): The input datasets.

    Returns:
        xarray.Dataset: The dataset with additional wind-related variables.
    """

    # Calculate wind.
    print("Calculating wind.")
    ds["wind"] = np.sqrt(ds.u10 ** 2 + ds.v10 ** 2)
    # Calculate wind direction
    print("Calculating wind direction.")
    ds["dir"] = 180 + (180 / pi) * np.arctan2(ds.u10, ds.v10)

    # Categorize wind direction
    print("Categorizing wind direction.")
    ds["dir_cat"] = (ds.dir > 0).astype(int) + (ds.dir > 22.5).astype(int) + (ds.dir > 67.5).astype(int) + \
                    (ds.dir > 112.5).astype(int) + (ds.dir > 157.5).astype(int) + \
                    (ds.dir > 202.5).astype(int) + (ds.dir > 247.5).astype(int) + \
                    (ds.dir > 292.5).astype(int) - 7 * (ds.dir > 337.5).astype(int)

    # Calculate the dominant wind direction.
    print("Calculating the dominant wind direction.")
    ds["dom_dir"] = xr.DataArray(np.apply_along_axis(find_majority_element, 0, ds.dir_cat), dims=["lat", "lon"])

    print("Perform calculations.")
    # Mask the wind speed based on the dominant wind direction.
    ds["masked_velocity"] = ds.wind.where(ds.dir_cat == ds.dom_dir, drop=False)
    # Calculate the dominant wind speed.
    ds["dom_vel"] = ds["masked_velocity"].max(dim="time")
    # Calculate the maximum ?.
    ds["res_max"] = ds["wind"].max(dim="time")
    # Determine the dominant wind direction for the maximum ?.
    ds["dir_max"] = ds.dir_cat.where(ds.wind == ds.res_max, drop=False).max(dim="time")
    # Select and return the desired wind-related variables.
    ds = ds[["dom_dir", "dom_vel", "res_max", "dir_max"]]

    print("Wind calculations completed.")
    return ds


def make_temps(dataset, path, datest, variable, bandName):
    ds = xr.open_dataset(os.path.join(path, dataset), engine="netcdf4")
    ds = ds.drop_vars("crs")
    selectedVars_temp = ds[[var for var in ds.data_vars if variable in ds[var].attrs.get("GRIB_COMMENT")]]
    print("Selecting variables with GRIB_COMMENT:", variable)

    # Extract the time information for each variable
    timeDict_temp = {}

    for band in selectedVars_temp.data_vars:
        try:
            # long_time = int(selected_vars[band].GRIB_VALID_TIME)
            longTime = int(selectedVars_temp[band].GRIB_VALID_TIME.dt.total_seconds())
        except AttributeError:
            longTime = int(ds[band].GRIB_VALID_TIME.split("s")[0])

        dtObject = datetime.datetime.fromtimestamp(longTime)
        timeDict_temp[band] = dtObject

    # Concatenate the selected variables along the time dimension.
    print("Concatenating selected variables along the time dimension.")

    newVariable_temp = xr.concat([selectedVars_temp[varName] for varName in selectedVars_temp.data_vars],
                                 dim=list(timeDict_temp.values()))

    # Rename the time dimension and variables.
    # new_variable = new_variable.rename({"concat_dim":"time"})
    print("Renaming time dimension and variables.")
    newVariable_temp = newVariable_temp.rename({"concat_dim": "time"})
    dataset_temp = newVariable_temp.to_dataset()
    # dataset = dataset.rename({list(dataset.data_vars)[0]:band_name})
    dataset_temp = dataset_temp.rename({list(dataset_temp.data_vars)[0]: bandName})

    # Sort and slice the dataset if `time` dimension exists.
    if "time" in dataset_temp.dims:
        if bandName == "temp":
            list_of_vars = ["max_temp", "min_temp", "mean_temp"]
            dsetname='Temperature'
        elif bandName == "dew_temp":
            list_of_vars = ["max_dew_temp", "min_dew_temp", "mean_dew_temp"]
            dsetname = 'Dew Temperature'
        elif bandName == "tp":
            list_of_vars = ["tp"]
            print("Sorting and slicing on the time dimension for %s %s"%(variable, bandName))
            dataset_temp = dataset_temp.sortby("time")
            #dataset_temp = dataset_temp.isel(time=slice(10, 34))
            dataset_temp = dataset_temp.sel(time=slice(pd.to_datetime(datest, format='%Y%m%d'),
                                             pd.to_datetime(datest + "2359", format='%Y%m%d%H%M')))
            print('Time limits Precipitation:\nFirst: %s\nLast: %s' % (dataset_temp['time'][0].values,
                                                                       dataset_temp['time'][-1].values))
            dataset_temp[list_of_vars[0]] = dataset_temp.isel(time=23).tp
            return dataset_temp

        print("Sorting and slicing on the time dimension for %s %s"%(variable, bandName))
        dataset_temp = dataset_temp.sortby("time")
        #dataset_temp = dataset_temp.isel(time=slice(10, 34))
        dataset_temp = dataset_temp.sel(time=slice(pd.to_datetime(datest, format='%Y%m%d'),
                                         pd.to_datetime(datest + "2359", format='%Y%m%d%H%M')))
        print('Time limits %s:\nFirst: %s\nLast: %s' % (dsetname,dataset_temp['time'][0].values,
                                                        dataset_temp['time'][-1].values))
        dataset_temp[list_of_vars[0]] = dataset_temp[bandName].max(dim="time")
        dataset_temp[list_of_vars[1]] = dataset_temp[bandName].min(dim="time")
        dataset_temp[list_of_vars[2]] = dataset_temp[bandName].mean(dim="time")
        dataset_temp = dataset_temp[list_of_vars].squeeze()

    return dataset_temp


def main(args):
    """
    Main entry point of the program.
    """
    inputMeteoFolder = "input_meteorological"
    intermediateMeteoFolder = "intermediate_meteorological"
    outputMeteoFolder = "intermediate2_meteorological"
    finalMeteoFolder = "output_meteorological"

    if len(args) != 1:
        sys.exit("Please provide exactly 1 argument.")

    print(f"Executing `meteorological_process.py` with arguments: `{args[0]}`.")

    inputPath = os.path.join(args[0], inputMeteoFolder)
    intermediatePath = os.path.join(args[0], intermediateMeteoFolder)
    outputPath = os.path.join(args[0], outputMeteoFolder)

    convert_grb2_to_nc(inputPath, intermediatePath)

    # Get a list of netCDF files in the specified directory.
    files = [file for file in os.listdir(intermediatePath) if file.endswith(".nc")]

    for file in files:
        date = file.split("-")[1].split(".")[0]
        outputFilePath = os.path.join(args[0], outputMeteoFolder, date + ".nc")
        # change name point
        finalFilePath = os.path.join(args[0], finalMeteoFolder, date + "_meteo.nc")

        # Check if the output file already exists.
        if not os.path.exists(outputFilePath):
            print(f"Processing file: {file}")

            # Process wind components, merge datasets.
            print("Processing wind components.")
            # u-component
            datasetU = concatenate_variables(file, intermediatePath, date,
                                             variable="u-component of wind [m/s]", bandName="u10")
            # v-component
            datasetV = concatenate_variables(file, intermediatePath, date,
                                             variable="v-component of wind [m/s]", bandName="v10")

            merged = xr.merge([datasetU, datasetV])

            # Calculate wind-related variables.
            print("Calculating wind-related variables.")
            ds = compute_wind(merged)
            # Temp, dew_temp and Rain process
            dataset_temp = make_temps(file, intermediatePath, date,"Temperature [C]", "temp")
            dataset_dew = make_temps(file, intermediatePath, date,"Dew point temperature [C]", "dew_temp")
            dataset_tp = make_temps(file, intermediatePath, date,"Total precipitation [kg/(m^2)]", "tp")
            merged = xr.merge([ds, dataset_temp, dataset_dew, dataset_tp])
            date_datetime = datetime.datetime.strptime(date, "%Y%m%d")
            formatted_date = date_datetime.strftime("%Y-%m-%d")
            merged = merged.assign_coords({"time": formatted_date})
            # Add a new time dimension.
            merged = merged.expand_dims("time")

            # Save the processed dataset to a netCDF file.
            print(f"Saving the processed dataset to: {outputFilePath}")
            merged.to_netcdf(outputFilePath)

        if not os.path.exists(finalFilePath):
            pr_files = []
            date = datetime.datetime.strptime(date, "%Y%m%d")
            start_date = date - timedelta(days=7)
            end_date = date

            while start_date <= end_date:
                # Generate the filename for each date
                filename = start_date.strftime("%Y%m%d") + ".nc"
                # Search for files with the generated filename

                if os.path.isfile(os.path.join(outputPath, filename)):
                    pr_files.append(os.path.join(outputPath, filename))

                start_date = start_date + timedelta(days=1)

            if len(pr_files) > 1:
                # Open the files.
                dataset = xr.open_mfdataset(pr_files)
                # Calculate the sum of the variable "tp"
                tp_sum = dataset["tp"].sum(dim="time")
                ds = xr.open_dataset(outputFilePath)
                ds["tp"] = tp_sum
                ds = ds.rename({"tp": "rain_7_days"})
                #ds = ds.rename_dims({"time": "StdTime"})

                ds = ds.squeeze()
                # Print the result
                ds.to_netcdf(finalFilePath)
                print(f"Sum of 'tp' variable for {filename}: {tp_sum}")

                # Close the dataset
                dataset.close()

    paths = [inputPath, intermediatePath]#, outputPath] (do not delete intermediate2 folder used for rain)

    for path in paths:
        for fileName in os.listdir(path):
            filePath = os.path.join(path, fileName)
            if os.path.isfile(filePath):
                print(f"Deleting: `{filePath}`...")
                os.remove(filePath)

    print("Completion of execution.")
    print(f"{'-'*80}")


if __name__ == "__main__":
    main(sys.argv[1:])
