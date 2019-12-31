# Retrieves the data from government website.
# Requires input provinc, # of pages to be pulled
# Creates dataframes for further input into Results.py
import os
### Adding locally installed modules to path
dPath = r'C:\Users\marko_mijovic\AppData\Roaming\Python\Python37\site-packages'
os.environ['PATH'] += ':'+dPath
import re
import requests
import bs4
import pandas as pd
import numpy as np
import sqlite3 as sq3
from datetime import date, datetime, timedelta
from IPython.display import display
today = date.today().isoformat()

class Weather():

    def __init__(self, prov, pageNum):
        '''prov = Province in Canada, pageNum = # of pages that the user wants to pull for that province'''
        self.province = prov # province for which we want the weather data
        self.pageNum = pageNum # num of pages pulled. have to maunally check on the gov. website
        self.baseUrl = 'http://climate.weather.gc.ca/historical_data/search_historic_data_stations_e.html?'
        self.provUrl = 'searchType=stnProv&timeframe=1&lstProvince={}&'.format(prov)
        self.timeUrl = 'optLimit=yearRange&StartYear={}&EndYear=2019&Year={}&Month={}&Day={}&selRowPerPage=100&'.format('1840', today[:4], today[5:7], today[8:])
        self.soupPD = []
        self.stationDF = []
    ##################
    def parseURL(self):
        ''' Uses bs4 to parse and obtain the html of the government weather data website'''
        test = True # used to filter out the 1st page then set to false afterwards
        # loop to simulate the change in URL as the page number changes from 1 to self.maxPages
        for i in range(1, self.pageNum+1):
            print('Downloading Page: ', i)
            if test:
                rowUrl = 'txtCentralLatMin=0&txtCentralLatSec=0&txtCentralLongMin=0&txtCentralLongSec=0&startRow=0'
                test = False
            else:
                rowUrl = 'txtCentralLatMin=0&txtCentralLatSec=0&txtCentralLongMin=0&txtCentralLongSec=0&startRow={}'.format(startRow)
            startRow = 1 + i*100
            # BS4 parsing
            try:
                rep = requests.get(self.baseUrl+self.provUrl+self.timeUrl+rowUrl)
            except Exception as exc:
                print('There was a problem with the download: %s' % (exc))
            soup = bs4.BeautifulSoup(rep.text, 'html.parser')
            self.soupPD.append(soup)
        self.createStationDF()
    ##################
    def createStationDF(self):
        ''' Uses the bs4 parsed html data and looks for stations and their information and stores that data in Pandas DF
            Must be called with parseURL in order to obtain the self.soupPD aka bs4 HTML'''
        stationData = []
        for line in self.soupPD:
            forms = line.findAll("form", {"id" : re.compile('stnRequest*')})
            for form in forms:
                try:
                    stID = form.find('input', {'name': 'StationID'})['value']
                    name = form.find('input', {'name': 'lstProvince'}).find_next_siblings('div')[0].text
                    timeframes = form.find('select', {'name' : 'timeframe'}).findChildren()
                    intervals = [t.text for t in timeframes]
                    years = form.find('select', {'name' : 'Year'}).findChildren()
                    minYr = years[0].text
                    maxYr = years[-1].text
                    data = [stID, name, intervals, minYr, maxYr]
                    stationData.append(data)
                except:
                    pass
        # creating pandas dataframe for the scrapped data
        self.stationDF = pd.DataFrame(stationData, columns=['ID','Name','Intervals','Start','End'])
        with pd.option_context('display.max_rows', 300): display(self.stationDF)
        self.createSQL()
    ###################
    def createSQL(self):
        """ Creates the sql database for the selected weather data"""
        conn = sq3.connect('database.db')
        cur = conn.cursor()
        sql_command = """
        DROP TABLE IF EXISTS database;
        CREATE TABLE database (
            StationID INTEGER,
            Name TEXT,
            Lat FLOAT,
            Lon FLOAT,
            Year TEXT,
            Season TEXT,
            MaxMeanTemp FLOAT,
            MinMeanTemp FLOAT,
            MeanTemp FLOAT,
            Rain_mm FLOAT,
            Snow_cm FLOAT,
            Precipitation_mm FLOAT);
            """
        cur.executescript(sql_command)
        conn.commit()
        conn.close()
        self.getWeatherData()
    ###################
    def getWeatherData(self): # prep the waether link
        ''' Obtains the actual weather data for each station in stationDF. TF 1: Hourly. TF 2: Daily. TF 3: Monthly.
            Not sure if hourly and daily work with this code. I specifically needed monthly data.'''
        tf = 3
        ID = self.stationDF.loc[:,'ID'].values # list of the station ID; needed in data gathering
        startYear = self.stationDF.loc[:,'Start'].values # list of start year for all the stations
        endYear = self.stationDF.loc[:,'End'].values # list of end year for all the stations
        years = list(zip(startYear, endYear)) # list of tuples (start year, end year)
        month = 1 # necessary for URL

        for i, item in enumerate(years):

            for year in range(int(item[0]), int(item[0])+1): #need just 1 year for monthly URL handling. Pulls all years
                """
                For monthly database (tf=3), just need 1 year and it will return values for all available years
                for the entirety of the station.
                """
                if 'Monthly' not in self.stationDF.iloc[i]['Intervals']:
                    break # if the station does not contain monthly intervals skip it
                else:
                    baseUrl = 'http://climate.weather.gc.ca/climate_data/bulk_data_e.html?'
                    queryUrl = 'format=csv&stationID={}&Year={}&Month={}&timeframe={}'.format(ID[i], year, month, tf)
                    #print('YEAR IS: ', year,'station is:', self.stationDF.loc[:,'Name'].values[i],'\n')
                    self.averageWeather(
                        self.stationDF.iloc[i]['ID'],
                        self.stationDF.iloc[i]['Name'],
                        pd.read_csv(baseUrl+queryUrl, skiprows = 0, header = 0)) #monthly avgs for all operational years
                    #with pd.option_context('display.max_columns', 29): display(weather)
    ##########
    def averageWeather(self, ID, Name, Weather):

        """ Averages summer and winter values of the station weather data and returns the averages for the year. """
        summer = [3, 4, 5, 6, 7, 8] #months for summer season
        summer_Values = [0, 0, 0, 0 ,0 ,0] # max temp, min temp, mean temp, rain (mm), snow (mm), Precipitation (mm)
        winter_Values = [0, 0, 0, 0 ,0 ,0] # max temp, min temp, mean temp, rain (mm), snow (mm), Precipitation (mm)
        s_Counter = 0
        w_Counter = 0
        for i, year in enumerate(Weather.loc[:,'Year'].values):
            month = Weather.iloc[i]['Month']
            if i+1 >= len(Weather.loc[:,'Year'].values):
                next_Year = year+1000
            else:
                next_Year = Weather.iloc[i+1]['Year']
            month_Values = self.DeleteNaN([
                Weather.iloc[i]['Mean Max Temp (°C)'],
                Weather.iloc[i]['Mean Min Temp (°C)'],
                Weather.iloc[i]['Mean Temp (°C)'],
                Weather.iloc[i]['Total Rain (mm)'],
                Weather.iloc[i]['Total Snow (cm)'],
                Weather.iloc[i]['Total Precip (mm)']
                ])
            if month in summer:
                summer_Values = [sum(x) for x in zip(summer_Values, month_Values)]
                s_Counter += 1
            else:
                winter_Values = [sum(x) for x in zip(winter_Values, month_Values)]
                w_Counter += 1
            if year != next_Year:
                print('Writing', Name, year)
                lat = Weather.iloc[i]['Latitude (y)']
                lon = Weather.iloc[i]['Longitude (x)']
                if s_Counter == 0:
                    s_Counter = 1
                elif w_Counter == 0:
                    w_Counter = 1
                self.updateWeatherData([x/s_Counter for x in summer_Values], [x/w_Counter for x in winter_Values], ID, Name, lat, lon, year)
                summer_Values = [0, 0, 0, 0, 0, 0]
                winter_Values = [0, 0, 0, 0, 0, 0]
                s_Counter = 0
                w_Counter = 0
    ###########
    def updateWeatherData(self, summer_Values, winter_Values, ID, Name, lat, lon, year):

        conn = sq3.connect('database.db')
        cur = conn.cursor()
        cur.execute("INSERT INTO database (StationID, Name, Lat, Lon, Year, Season, MaxMeanTemp, MinMeanTemp, MeanTemp, Rain_mm, Snow_cm, Precipitation_mm) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (ID, Name, lat, lon, str(year), 'summer', summer_Values[0], summer_Values[1], summer_Values[2], summer_Values[3], summer_Values[4], summer_Values[5]))
        cur.execute("INSERT INTO database (StationID, Name, Lat, Lon, Year, Season, MaxMeanTemp, MinMeanTemp, MeanTemp, Rain_mm, Snow_cm, Precipitation_mm) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (ID, Name, lat, lon, str(year), 'winter', winter_Values[0], winter_Values[1], winter_Values[2], winter_Values[3], winter_Values[4], winter_Values[5]))
        conn.commit()
        conn.close()
    ###########
    def DeleteNaN(self, lyst):
        """ Replaces NaN values with 0"""
        for i, value in enumerate(lyst):

            if value != value: # essentially if NaN
                lyst[i] = 0

        return lyst
    ####################
if __name__ == '__main__':

    w = Weather('AB', 15)
