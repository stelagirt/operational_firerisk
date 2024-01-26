import os
import sys
import subprocess

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
        date_list.append(currentDate.strftime("%Y%m%d"))
        currentDate += timedelta(days=1)

    return date_list


def main(args):
    """
    Main entry point of the program.
    """
    if len(args) != 4:
        sys.exit("Please provide exactly 4 arguments.")

    if not (is_date_format_valid(args[0]) and is_date_format_valid(args[1])):
        print("One of the dates does not match the format '%Y%m%d'")
        sys.exit()

    print(f"Executing `meteorological_copy.py` with arguments: `{args[0]}`, `{args[1]}`, `{args[2]}`, `{args[3]}`.")

    username = "beyondian"
    password = "B3y0nd!"
    ipAddress = "10.201.40.16"

    inputMeteoFolder = os.path.join(args[3], "input_meteorological")
    dailyRastersFolder = os.path.join(args[3], "daily_rasters")

    strDates = get_dates_between(args[0], args[1])

    for strDate in strDates:

        print(f"Checking date: `{strDate}`")
        date = datetime.strptime(strDate, "%Y%m%d")
        dateForward = date + timedelta(days=2)
        strDateForward = dateForward.strftime("%Y%m%d")

        grbFile = f"WRF-{strDate}.grb2"
        ncFile = f"{strDateForward}.nc"
        grbFileForward = f"WRF-{strDateForward}.grb2"

        grbPath = os.path.join(inputMeteoFolder, grbFileForward)
        ncPath = os.path.join(dailyRastersFolder, ncFile)

        if not os.path.exists(ncPath):
            print(f"Did not encounter file: `{ncPath}` in local directories.")
            print("Executing: ", f"`sshpass -p \"{password}\" scp {username}@{ipAddress}:{args[2]}{grbFile} {grbPath}`...")
            subprocess.run(f"sshpass -p \"{password}\" scp {username}@{ipAddress}:{args[2]}{grbFile} {grbPath}", shell=True)

    print("Completion of execution.")
    print(f"{'-'*80}")


if __name__ == "__main__":
    main(sys.argv[1:])
