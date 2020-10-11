# Canadian-Weather-Data
Pulls monthly weather data for a specified Canadian province and stores yearly averages in a database.

Requires sqlite3, numpy, pandas, bs4, requests, re, datetime, IPython.display, os

To run the code, run main.py from the CMD.

By default, it will download 15 pages (all the stations) in Alberta, if you want another province just change 'AB' and manually see what is the maximum #of pages for that province and change 15 to that.
