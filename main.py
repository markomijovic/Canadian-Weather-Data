# About: Parses and takes weather data off of Can.gov website as summer/winter averages for each year.
# Summer months = [3, 4, 5, 6, 7, 8]
import os
### Changing the directory
abspath = os.path.abspath('tow.py')
dname = os.path.dirname(abspath)
os.chdir(dname)
### Adding locally installed modules to path
#dPath = r'C:\Users\marko_mijovic\AppData\Roaming\Python\Python37\site-packages' # can be ignored if used from local PC with admin access
#os.environ['PATH'] += ':'+dPath
import WeatherData
############ START CALL ###############
if __name__ == '__main__':

    ### UNCOMMENT BELOW TO GET WEATHER DATA ###
    w = WeatherData.Weather('AB', 15) #'AB' for Alberta and 15 = all the available pages
    w.parseURL()
