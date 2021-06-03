### HI_Waterbird_Repro_DataJoinMerge_v3.py
### Version: 5/7/2020
### Author: Khem So, khem_so@fws.gov, (503) 231-6839
### Abstract: This Python 3 script pulls data from the HI Waterbirds Reproductive Success ArcGIS Online feature service and performs joins and merges to result in a combined CSV dataset.

import arcpy
import pandas as pd
from arcgis import GIS
import time, os, fnmatch, shutil

### ArcGIS Online stores date-time information in UTC by default. This function converts time zones and can be used to convert from UTC ("UTC") to Hawaii standard time ("US/Hawaii"; UTC -10).
from datetime import datetime
from pytz import timezone
def change_timezone_of_field(df, source_date_time_field, new_date_time_field, source_timezone, new_timezone):
    """Returns the values in *source_date_time_field* with its timezone converted to a new timezone within a new field *new_date_time_field*
    : param df: The name of the spatially enabled or pandas DataFrame containing datetime fields
    : param source_date_time_field: The name of the datetime field whose timezone is to be changed
    : param new_date_time_field: The name of the new datetime field
    : param source_timezone: The name of the source timezone
    : param new_timezone: The name of the converted timezone. For possible values, see https://gist.github.com/heyalexej/8bf688fd67d7199be4a1682b3eec7568
    """
    # Define the source timezone in the source_date_time_field
    df[source_date_time_field] = df[source_date_time_field].dt.tz_localize(source_timezone)
    # Convert the datetime in the source_date_time_field to the new timezone in a new field called new_date_time_field
    df[new_date_time_field] = df[source_date_time_field].dt.tz_convert(new_timezone)
    
### Allow authentication via login to U.S. Fish & Wildlife Service ArcGIS Online account via ArcGIS Pro
gis = GIS("pro")

### Enter path for local file saving
# uncomment next line to use ArcGIS interface, otherwise hard coding out_workspace
out_workspace = arcpy.GetParameterAsText(0)
# out_workspace = "C:/Users/kso/Desktop/"

### Paths to ArcGIS Online data
# To populate Service ItemId, go to Feature Service webpage and in bottom right corner, click on the View link.
# Current Feature Service webpage: https://fws.maps.arcgis.com/home/item.html?id=55275a4a0dc54c1c8dcab604b65a88f0
ServiceItemID = gis.content.get("55275a4a0dc54c1c8dcab604b65a88f0")

### There are separate methods for pulling spatial versus non-spatial data into Python. Spatial layers will become Spatially Enabled DataFrame objects. Non-spatial data will become regular pandas DataFrame objects.
## Define variables pointing to spatial layers
NestLocationLyr = ServiceItemID.layers[0]
BroodLocationLyr = ServiceItemID.layers[1]
CountUnitsLyr = ServiceItemID.layers[2]
## Create Spatially Enabled DataFrame objects
sedfNestLocation = pd.DataFrame.spatial.from_layer(NestLocationLyr)
sedfBroodLocation = pd.DataFrame.spatial.from_layer(BroodLocationLyr)
sedfCountUnits = pd.DataFrame.spatial.from_layer(CountUnitsLyr)
## Define variables point to non-spatial (tabular) data
NestVisitData = r"https://services.arcgis.com/QVENGdaPbd4LUkLV/arcgis/rest/services/Reproductive_Success_Survey_BETA/FeatureServer/6"
## Convert AGOL table to NumPy Array and then to pandas DataFrames
naNestVisitData = arcpy.da.TableToNumPyArray(NestVisitData,["OBJECTID","Date","ObserverName","NestCode","NumEggsObservedText","WaterLevel","Status","FailureCause","FailureComments","Bands","NestComments","GlobalID","NestLocationGlobalID","created_user","created_date","last_edited_user","last_edited_date"])
dfNestVisitData = pd.DataFrame(naNestVisitData)

## Convert datetime fields in all dataframes from UTC to HST within new field with _HST suffix
change_timezone_of_field(sedfNestLocation, "Date", "DateHST", "UTC", "US/Hawaii")
change_timezone_of_field(sedfNestLocation, "created_date", "created_DateHST", "UTC", "US/Hawaii")
change_timezone_of_field(sedfNestLocation, "last_edited_date", "last_edited_DateHST", "UTC", "US/Hawaii")
change_timezone_of_field(sedfBroodLocation, "Date", "DateHST", "UTC", "US/Hawaii")
change_timezone_of_field(sedfBroodLocation, "created_date", "created_DateHST", "UTC", "US/Hawaii")
change_timezone_of_field(sedfBroodLocation, "last_edited_date", "last_edited_DateHST", "UTC", "US/Hawaii")
change_timezone_of_field(dfNestVisitData, "Date", "DateHST", "UTC", "US/Hawaii")
change_timezone_of_field(dfNestVisitData, "created_date", "created_DateHST", "UTC", "US/Hawaii")
change_timezone_of_field(dfNestVisitData, "last_edited_date", "last_edited_DateHST", "UTC", "US/Hawaii")

## Concatenate fields to create Nest ID Label (same as in AGOL map) in NestLocation
sedfNestLocation["Year"] = sedfNestLocation["DateHST"].dt.year
sedfNestLocation["OBJECTID_Padded"] = sedfNestLocation["OBJECTID"].astype(str)
sedfNestLocation["OBJECTID_Padded"] = sedfNestLocation["OBJECTID_Padded"].str.zfill(3)
sedfNestLocation["NestBroodIDLabel"] = sedfNestLocation["Refuge"] + "_" + sedfNestLocation["Year"].astype(str) + "_" + sedfNestLocation["Species"] + "_" + sedfNestLocation["OBJECTID_Padded"]

## Join NestLocation and NestVisitData tables
dfNestVisitData = dfNestVisitData.replace("{","", regex=True)
dfNestVisitData = dfNestVisitData.replace("}","", regex=True)
dfNestLocationNestVisitData = pd.merge(sedfNestLocation,dfNestVisitData, how="left", left_on="GlobalID", right_on="NestLocationGlobalID")

## Rename columns in dfNestLocationNestVisitData
dfNestLocationNestVisitData.rename(columns = {"Date_x":"Date_NestLocation", "DateHST_x":"DateHST_NestLocation", "OBJECTID_x":"NestID", "ObserverName_x":"ObserverName_NestLocation", "WaterLevel_x":"WaterLevel_NestLocation", "Date_y":"Date_Visit", "DateHST_y":"DateHST_Visit", "ObserverName_y":"ObserverName_Visit", "WaterLevel_y":"WaterLevel_Visit"}, inplace = True)

## Reset dfNestLocationNestVisitData with columns in desired order
dfNestLocationNestVisitData = dfNestLocationNestVisitData[["Refuge", "DateHST_NestLocation", "Species", "NestID", "NestBroodIDLabel", "ObserverName_NestLocation", "VegCover10Meters", "VegCoverPond", "WaterLevel_NestLocation", "NestLocationComments", "DateHST_Visit", "ObserverName_Visit", "NestCode", "NumEggsObservedText", "WaterLevel_Visit", "Status", "FailureCause", "FailureComments", "Bands", "NestComments", "SHAPE"]]

## Define dfNestLocationNestVisitData sort order
dfNestLocationNestVisitData = dfNestLocationNestVisitData.sort_values(by=["Refuge", "NestID", "DateHST_Visit"])

## Rename columns in BroodLocation to prepare for pd.concat
sedfBroodLocation.rename(columns = {"TaxonName":"Species", "DateHST":"DateHST_Visit", "ObserverName":"ObserverName_Visit", "WaterLevel":"WaterLevel_Visit"}, inplace = True)

## Concatenate fields to create Nest ID Label (same as in AGOL map) in BroodLocation
sedfBroodLocation["Year"] = sedfBroodLocation["DateHST_Visit"].dt.year
# structure is df.loc[df['column name'] condition, 'new column name'] = 'value if condition is met'
sedfBroodLocation.loc[sedfBroodLocation["NestID"] != "Unknown", "TempID"] = sedfBroodLocation["NestID"].str.zfill(3)
sedfBroodLocation.loc[sedfBroodLocation["NestID"] == "Unknown", "TempID"] = sedfBroodLocation["BroodID"]
sedfBroodLocation["NestBroodIDLabel"] = sedfBroodLocation["Refuge"] + "_" + sedfBroodLocation["Year"].astype(str) + "_" + sedfBroodLocation["Species"] + "_" + sedfBroodLocation["TempID"]

# Replace SubadultAge domain value Fledgling with code
sedfBroodLocation[["SubadultAge"]] = sedfBroodLocation[["SubadultAge"]].replace(["Fledgling"], ["F"])

## Reset sedfBroodLocation with columns in desired order
sedfBroodLocation2 = sedfBroodLocation[["Refuge", "Species", "NestID", "NestBroodIDLabel", "BroodID", "DateHST_Visit", "ObserverName_Visit", "BroodCode", "NumSubAdults", "SubadultAge", "WaterLevel_Visit", "Status", "FailureCause", "FailureComments", "Bands", "BroodComments", "SHAPE"]]

## pd.concat to merge tables with keys for hierarchical index
dfAllNestBroodData = pd.concat([dfNestLocationNestVisitData, sedfBroodLocation2], keys=["Nest", "Brood"], sort=False)

## Add multilevel level names
dfAllNestBroodData.index.names = ["DataSource", "Index"]

## Convert Python date time into format Excel can read more easily
dfAllNestBroodData["DateHST_NestLocation"] = dfAllNestBroodData["DateHST_NestLocation"].dt.strftime('%x %X')
dfAllNestBroodData["DateHST_Visit"] = dfAllNestBroodData["DateHST_Visit"].dt.strftime('%x %X')

## Reset dfAllNestBroodData with columns in desired order
dfAllNestBroodData = dfAllNestBroodData[["Refuge", "DateHST_Visit", "ObserverName_Visit", "Species", "NestID", "BroodID", "NestBroodIDLabel", "NestCode", "BroodCode", "NumEggsObservedText", "NumSubAdults", "SubadultAge", "Status", "WaterLevel_Visit", "FailureCause", "FailureComments", "Bands", "NestComments", "BroodComments", "DateHST_NestLocation", "ObserverName_NestLocation", "VegCover10Meters", "VegCoverPond", "WaterLevel_NestLocation", "NestLocationComments", "SHAPE"]]

## Define dfAllNestBroodData sort order
dfAllNestBroodData = dfAllNestBroodData.sort_values(by=["Refuge", "NestID", "BroodID", "DateHST_Visit"])

## Create timestamp for file naming
t = time.localtime()
timestamp = time.strftime('%Y-%m-%d_%H%M', t)

## Create export paths
NestLocationOutput = os.path.join(out_workspace,(timestamp + '_NestLocation.csv'))
NestVisitDataOutput = os.path.join(out_workspace,(timestamp + '_NestVisitData.csv'))
BroodLocationOutput = os.path.join(out_workspace,(timestamp + '_BroodLocation.csv'))
NestLocationNestVisitOutput = os.path.join(out_workspace,(timestamp + '_NestLocationNestVisitData.csv'))
AllNestBroodDataOutput = os.path.join(out_workspace,(timestamp + '_AllNestBroodData.csv'))

## Export all to CSVs
sedfNestLocation.to_csv(NestLocationOutput)
dfNestVisitData.to_csv(NestVisitDataOutput)
dfNestLocationNestVisitData.to_csv(NestLocationNestVisitOutput)
sedfBroodLocation.to_csv(BroodLocationOutput)
dfAllNestBroodData.to_csv(AllNestBroodDataOutput)