"""
Script documentation:

Base Portal Access parameters:
- puser : ArcGIS Portal User Name (String)
- ppass : ArcGIS Portal User Password (String)

Image upload parameters
- fileFullpath = Full Path of File to Upload

example:
puser= "g2202_preffered"
ppass="G2202_pr3ff3r3ed"
fileFullpath =r"/home/lstam/Documents/output_satellite/MOD13A1/20230610_veg.nc"

"""

import urllib.request
import urllib.parse
import contextlib
import requests
import datetime
import json
import os
from bs4 import BeautifulSoup
import sys
def serviceUrlByFilename(filename):
	if filename.endswith("_meteo.nc"):
		serviceUrl = "https://arcgis.geoapikonisis.gr/arcgis/rest/services/g2202_preferred/INP_METEO_HIST_ERAS_MOSAIC/ImageServer"
	elif filename.endswith("_lst.nc"):
		serviceUrl = "https://arcgis.geoapikonisis.gr/arcgis/rest/services/g2202_preferred/INP_MODIS_LST_MOSAIC/ImageServer"
	elif filename.endswith("_veg.nc"):
		serviceUrl = "https://arcgis.geoapikonisis.gr/arcgis/rest/services/g2202_preferred/INP_MODIS_VEG_MOSAIC/ImageServer"
	elif filename.endswith("_cells.tif"):
		serviceUrl = "https://arcgis.geoapikonisis.gr/arcgis/rest/services/g2202_preferred/PRD_RISK_MOSAIC/ImageServer"
	else :
		serviceUrl = "Error Filename Syntax"
	return serviceUrl

def submit_request(request):
	""" Returns the response from an HTTP request in json format."""
	with contextlib.closing(urllib.request.urlopen(request)) as response:
		content = response.read()
		content_decoded = content.decode("utf-8")
		job_info = json.loads(content_decoded)
		return job_info

def makePostRequest(url,params):
	parcedParam = urllib.parse.urlencode(params)
	data = parcedParam.encode( "ascii" )
	with urllib.request.urlopen( url, data ) as response:
		response_text = response.read()
		response_decoded = response_text.decode("utf-8")
		job_info = json.loads(response_decoded)
	return job_info

def getImageServiceInfo(imageServiceUrl, token):
	params = {
			"f": "json",
			"token": token
	}
	response = makePostRequest(imageServiceUrl,params)
	return response

def rasterTypeByFileExt(filename):
	file_ext = os.path.splitext(filename)[1]
	if file_ext == '.nc' :
		rasterType = 'NetCDF'
	elif file_ext == '.tif' :
		rasterType = 'Raster Dataset'
	else :
		rasterType = 'Not Supprted'
	return rasterType

def get_token(portal_url, username, password):
	""" Returns an authentication token for use in ArcGIS Online."""
	# Set the username and password parameters before
	#  getting the token.
	#
	params = {"username": username,
			  "password": password,
			  "referer": "https://arcgis.geoapikonisis.gr/arcgisportal",
			  "f": "json"}
	token_url = "{}/generateToken".format(portal_url)
	data = urllib.parse.urlencode(params)
	data_encoded = data.encode("utf-8")
	request = urllib.request.Request(token_url, data=data_encoded)
	token_response = submit_request(request)
	if "token" in token_response:
		print("Getting token...")
		token = token_response.get("token")
		return token
	else:
		# Request for token must be made through HTTPS.
		#
		if "error" in token_response:
			error_mess = token_response.get("error", {}).get("message")
			if "This request needs to be made over https." in error_mess:
				token_url = token_url.replace("http://", "https://")
				token = get_token(token_url, username, password)
				return token
			else:
				raise Exception("Portal error: {} ".format(error_mess))

def uploadRaster(imageServiceUrl, filefullpath , token, referer_url = "https://arcgis.geoapikonisis.gr"):
	uploadRastersUrl = "{}/uploads/upload".format(imageServiceUrl)

	with open(filefullpath, "rb") as file:
		# create the payload with file, description and parameters
		payload = {
			"file": file
		}

		params = {
			"f": "json",
			"file": payload,
			"token": token
		}
		response = requests.post(uploadRastersUrl, files=payload, params=params)
	if response.status_code == 200:
		# check if response is in JSON or HTML format
		content_type = response.headers.get("Content-Type")
		if "application/json" in content_type:
			content = response.read().decode("utf-8")
			data = json.loads(content)
			itemID = data["itemID"]
			print("Item ID: " + str(itemID))
			results = data
		elif "text/html" in content_type:
			content = response.content
			results = content
			soup = BeautifulSoup(response.content, "html.parser")
			# find the tbody tag
			tbody = soup.find("tbody")
			# find all tr tags within tbody
			rows = tbody.find_all("tr")
			# create a list to store the output JSON
			output = []
			itemsIds = []
			# loop through each row
			for row in rows:
				# find all td tags within the row
				cols = row.find_all("td")
				# create a dictionary to store the td pairs
				td_dict = {}
				# loop through each td tag and add the content to the dictionary
				for i in range(0, len(cols), 2):
					key = cols[i].get_text(strip=True)
					value = cols[i+1].get_text(strip=True)
					td_dict[key] = value
					if key == "Item ID:" :
						itemsIds.append(value)
				# add the dictionary to the output list
				output.append(td_dict)
			# output the JSON
			print(json.dumps(output))
			results = itemsIds
		else:
			print("Error: unexpected response format")
			results = "Error: unexpected response format"
	else:
		print("File upload failed.")
		print(response.content)
		results = response.content
	return results

def addRasters(imageServiceUrl, rasterType, itemsIds, token):
	print('addRasters')
	addRastersUrl = "{}/add".format(imageServiceUrl)
	for itemsId in itemsIds:
		params = {
			"f": "json",
			"itemIds": itemsId,
			"rasterType": rasterType,
			"computeStatistics": "true",
			"buildPyramids": "true",
			"buildThumbnail": "true",
			"token": token
		}
		response = requests.post(addRastersUrl, params=params)
		if response.status_code == 200:
			# check if response is in JSON or HTML format
			content_type = response.headers.get("Content-Type")
			if "application/json" in content_type:
				content = response.json()
				results = content
			elif "text/html" in content_type:
				content = response.content
				results = content
			else:
				print("Error: unexpected response format")
				results = "Error: unexpected response format"
	return results

def updSndTime(imageServiceUrl,rasterIDs,fileDates, token):
	infoService =getImageServiceInfo(imageServiceUrl, token)
	updRastersUrl = "{}/update".format(imageServiceUrl)
	maxPSresults = checkIfExist(imageServiceUrl,"", 3 , token)
	maxPS = maxPSresults["features"][0]["attributes"]["MaxPS"]
	if infoService["hasMultidimensions"] :
		attr = {
			"MinPS": 0,
			"MaxPS" : maxPS,
			"Dimensions": "StdTime",
			"StdTime": fileDates["startDate"],
			"StdTime_Max": fileDates["endDate"]
		}
	else :
		attr = {
			"MinPS": 0,
			"MaxPS" : maxPS,
			"StdTime": fileDates["startDate"],
			"StdTime_Max": fileDates["endDate"]
		}
	print(attr)
	for rasterID in rasterIDs:
		params = {
			"f": "json",
			"rasterId": rasterID,
			"attributes": attr,
			"token": token
		}
		job_info = makePostRequest(updRastersUrl,params)
		print(job_info)
	return job_info

def getDateFromFilename (filename):
	fileDates = {}
	date_str = os.path.splitext(filename)[0]  # extract the filename without extension
	date_str = date_str.split("_")[0]  # extract the first part of the filename
	startDate = datetime.datetime.strptime(date_str, "%Y%m%d")  # convert the date string to a datetime object
	endDate = startDate + datetime.timedelta(days=1) + datetime.timedelta(seconds=-2)
	startDate_milliseconds_since_epoch = startDate.strftime("%m/%d/%Y %I:%M:%S %p")
	endtDate_milliseconds_since_epoch = endDate.strftime("%m/%d/%Y %I:%M:%S %p")
	fileDates["startDate"] = startDate_milliseconds_since_epoch
	fileDates["endDate"] = endtDate_milliseconds_since_epoch
	return fileDates

def checkIfExist(imageServiceUrl,filenameIn, countOrget , token):
	queryRastersUrl = "{}/query".format(imageServiceUrl)
	filename = os.path.splitext(filenameIn)[0]  # extract the filename without extension
	if countOrget == 1:
		params = {
			"f": "json",
			"where": "Name like '{}%'".format(filename),
			"returnCountOnly":"true",
			"token": token
		}
	elif countOrget == 2:
		params = {
			"f": "json",
			"where": "Name like '{}%'".format(filename),
			"returnIdsOnly":"true",
			"token": token
		}
	else :
		params = {
			"f": "json",
			"objectIds": 1,
			"returnGeometry":"false",
			"outFields":"MaxPS",
			"token": token
		}
	response = requests.post(queryRastersUrl, params=params)
	if response.status_code == 200:
		# check if response is in JSON or HTML format
		content_type = response.headers.get("Content-Type")
		if "application/json" in content_type:
			content = response.json()
			results = content
		elif "text/html" in content_type:
			content = response.content
			results = content
		else:
			print("Error: unexpected response format")
			results = "Error: unexpected response format"
	return results

#puser="g2202_preffered"
#ppass="G2202_pr3ff3r3ed"
#ppass="G2202_pr3f3rr3ed"
#ppass="G220$_pr3f3rr3ed"

#fileFullpath =r"/home/lstam/Documents/output_meteorological/20230702_meteo.nc"
#fileFullpath="/home/lstam/Documents/daily_rasters/tif/20230630_norm_pred_cells.tif"

def publishfile(fileFullpath):
##Start
	#fileFullpath=args[0]
	global puser
	global ppass

	puser="g2202_preffered"
	ppass="G220$_pr3f3rr3ed"

	print("Publishing: %s",fileFullpath)

	if os.path.isfile(fileFullpath) :
		filename = os.path.split(fileFullpath)[1]
		#print(filename)
		imageServiceUrl = serviceUrlByFilename(filename)
		print(imageServiceUrl)
		if imageServiceUrl != "Error Filename Syntax":
			print(imageServiceUrl)
			host_url ="https://arcgis.geoapikonisis.gr/arcgisportal"
			referer_url = "https://arcgis.geoapikonisis.gr"
			portal_url = "{}/sharing/rest".format(host_url)
			rasterType = rasterTypeByFileExt(filename)
			if rasterType != 'Not Supprted':
				fileDates = getDateFromFilename (filename)
				token = get_token(portal_url,puser,ppass)
				infoService = getImageServiceInfo(imageServiceUrl, token)
				print(infoService["hasMultidimensions"])
				checkExist = checkIfExist(imageServiceUrl,filename, 1,token)
				print(checkExist)
				if checkExist["count"] == 0 :
					print('start upload file {}'.format(filename))
					upload_results = uploadRaster (imageServiceUrl,fileFullpath,token)
					print('Upload File Completed and resurt itemID is {}'.format(upload_results))
					print('Start Add File {} with itemID {} to imageService {}'.format (filename,upload_results[0],imageServiceUrl))
					add_results = addRasters (imageServiceUrl, rasterType, upload_results, token)
					checkAdded = checkIfExist(imageServiceUrl,filename, 1,token)
					print(checkAdded)
					print("Completed add file {} with itemID {} to imageService with results {}".format(filename, upload_results[0], add_results))
					getExist = checkIfExist(imageServiceUrl,filename, 2,token)
					updateDate = updSndTime(imageServiceUrl,getExist["objectIds"],fileDates, token)
				else :
					print('Given file {} already exist'.format(filename))
					getExist = checkIfExist(imageServiceUrl,filename, 2,token)
					updateDate = updSndTime(imageServiceUrl,getExist["objectIds"],fileDates, token)
				print("Process Finished Successful!!")
			else:
				print('Given file rasterType Not supported for filename {}'.format(filename))
				exit()
		else :
			print("Error! Please try again")
			exit()
	else :
		print("Error!! Input File not Exist!")
		exit()
'''
if __name__ == '__main__':
    main(sys.argv[1:])
'''

