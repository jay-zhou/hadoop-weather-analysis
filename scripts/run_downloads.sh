#!/usr/bin/python -tt
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

##---------------------------------------------
# This is a testing script only
##---------------------------------------------
import sys
import requests

# Define a main() function that prints a little greeting.
def main():

   url_xml = "http://climatedataapi.worldbank.org/climateweb/rest/v1/country/cru/pr/month/can.xml"

   r = requests.get(url_xml)

   with open("weather_can.xml", "wb") as code:
       code.write(r.content)
    
# This is the standard boilerplate that calls the main() function.
if __name__ == '__main__':
  main()
