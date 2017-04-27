import sqlite3
from pyteaser import SummarizeUrl
import datetime
import feedparser
from dateutil import parser
import re



def main():

##################### SQLITE3 DATABASE creation and ETL of data #################################################
#################################################################################################################
#################################################################################################################
#################################################################################################################

	### cityTable
	conn = sqlite3.connect('.db') #create name of local database before .db

	### Cursor for sqlite3 Database
	c = conn.cursor()

	### Example Select Statment
	# result = c.execute('''SELECT * FROM (#TABLENAME INNER JOIN cityyAbvName ON TwoLetterAbv=country) WHERE "city"='Chicago' ''').fetchall()
	# print result

	### Shows the tables I've created
	# c.execute('''CREATE TABLE #TABLENAME
	#              (locationID integer, country text, region text, city text, postalCode text, latitude real, longitude real, metroCode integer, areaCode integer)''')
	# c.execute('''CREATE TABLE TABLENAME2
	#              (countryName text, TwoLetterAbv text, ThreeLetterAbv text)''')

	### Shows how i've inserted a row of data
	# c.execute("INSERT INTO #TABLENAME VALUES (1806,'US','CA','Rohnert Park','94928',38.3433,-122.7041,'807','707')")
	# c.execute("INSERT INTO #TABLENAME VALUES (1807,'US','TX','San Antonio','78201',29.4713,-98.5353,'641','210')")
	# c.execute("INSERT INTO #TABLENAME VALUES (80502,'FR','AV','Avignon','245689',43.947927, 4.806871,'2568987','5878958')")
	# c.execute("INSERT INTO #TABLENAME VALUES (80503,'MO','Monaco','Monaco','7813585',43.737649, 7.423300,'2658795','854214')")
	# c.execute("INSERT INTO #TABLENAME VALUES (80502,'FR','FR','France','123785412',47.190109, 2.839716,'21564','585421')")
	# c.execute("INSERT INTO #TABLENAME VALUES (80502,'FR','PR','Paris','1354984',48.858091, 2.338047,'6858415','987513')")

	## Save (commit) the changes
	# conn.commit()

	## We can also close the connection if we are done with it...
	## ... Just be sure any changes have been committed or they will be lost.
	# conn.close()

	### Example Select Statment
	# result = c.execute('''SELECT * FROM (#TABLENAME INNER JOIN TABLENAME2 ON TwoLetterAbv=country) WHERE "city"='Chicago' ''').fetchall()
	# print result
	# for row in result:
	# 	print row

######################## END OF DATABASE CREATION ###############################################################
#################################################################################################################
#################################################################################################################
#################################################################################################################

	citiesUSA = ['Chicago', 'New York City', 'Palo Alto', 'Seattle', 'Phoenix', 'Cincinnati', 'London', 'Paris']

### MAKE A DICTIONARY with a list inside!!!     dangerFactors = dangerFactors
	factor1TypeFactors = open("C:\\Users\\NAMEOFUSER\\Documents\\FOLDERNAME\\factor1TypeFactors.txt", "r").read().split("\n")
	factor2TypeFactors = open("C:\\Users\\NAMEOFUSER\\Documents\\FOLDERNAME\\factor2TypeFactors.txt", "r").read().split("\n")
	factor3TypeFactors = open("C:\\Users\\NAMEOFUSER\\Documents\\FOLDERNAME\\factor3TypeFactors.txt", "r").read().split("\n")
	
	dangerFactors = {'factor1Type' : factor1TypeFactors, 'factor2Type' : factor2TypeFactors, 'factor3TypeFactors' : factor3TypeFactors}
	
	Errors = []     
	feedErrors = []     
	linkErrors = []

	#Reading factors from text file
	rssFeeds = open("C:\\Users\\NAMEOFUSER\\Documents\\FOLDERNAME\\NAMEOFTXTDOCUMENT.txt", "r").read().split("\n")
	returnList = []

	getRssFeeds(rssFeeds, dangerFactors, citiesUSA, c, returnList, Errors, feedErrors, linkErrors)
	getErrors(Errors, feedErrors, linkErrors)

#Get the RSS Feeds
def getRssFeeds(rssFeeds, dangerFactors, citiesUSA, c, returnList, Errors, feedErrors, linkErrors):
	for feed in rssFeeds:
		getRSSFeedAttributes(feed, dangerFactors, citiesUSA, c, returnList, Errors, feedErrors, linkErrors)

#Get the attributes of the RSS Feeds if we were able to get RSS Feeds
def getRSSFeedAttributes(feed, dangerFactors, citiesUSA, c, returnList, Errors, feedErrors, linkErrors):
	a = feedparser.parse(feed)
	
	for posts in a.entries:
		
		now = datetime.datetime.now()
		Date_Of_Access = str(now)

		# if 'published' in posts:
		try:
			date_published = posts['published']
			dtobject = parser.parse(date_published)
			date_published_reformated = dtobject.strftime('%Y-%m-%d %H:%M:%S')
		except Exception, e:
			date_published_reformated = None


		title = posts.title
		title = title.replace("'","''")
		if title:
			link = posts.link
			if link:
				factorList = []
				domain = []
				locationList = []
				wordFreqFactor = []
				wordFreqLocation = []
				try:
					summary =  (SummarizeUrl(link))
				except Exception, e:
					SummaryError = str(e)
					Errors.append(SummaryError)
					feedErrors.append(feed)
					linkErrors.append(link)
				if summary:
					summary = u' '.join(summary)
					summary = summary.replace("'","''").replace("\n"," ")
					summaryLower = summary.lower()
					getFactorsFromText(summaryLower, summary, factorList, domain, locationList, wordFreqFactor, wordFreqLocation, dangerFactors, citiesUSA, c, title, link, date_published_reformated, Date_Of_Access, returnList, feed)


#Get RSS feed factors from text if we get RSS feed attributes
def getFactorsFromText(summaryLower, summary, factorList, domain, locationList, wordFreqFactor, wordFreqLocation, dangerFactors, citiesUSA, c, title, link, date_published_reformated, Date_Of_Access, returnList, feed):

	for key,allfactors in dangerFactors.iteritems():
		for factor in allfactors:
			factor = factor.lower()
			if factor in summaryLower:
				factorList.append(factor)
				domain.append(key)

				wordFreqFactor.append(summaryLower.count(factor))
				

	getLocationsFromText(summary, locationList, wordFreqLocation, wordFreqFactor, citiesUSA, factorList, domain, c, title, link, date_published_reformated, Date_Of_Access, returnList, feed)

#Get a location in the text, if we found significant factors in text
def getLocationsFromText(summary, locationList, wordFreqLocation, wordFreqFactor, citiesUSA, factorList, domain, c, title, link, date_published_reformated, Date_Of_Access, returnList, feed):

	for city in citiesUSA:
		if city in summary:
			locationList.append(city)

			wordFreqLocation.append(summary.count(city))
		else:
			pass
	getFactorAndLocationTogether(summary, factorList, domain, locationList, wordFreqFactor, wordFreqLocation, c, title, link, date_published_reformated, Date_Of_Access, returnList,feed)


#Get factor and location from text together
def getFactorAndLocationTogether(summary, factorList, domain, locationList, wordFreqFactor, wordFreqLocation, c, title, link, date_published_reformated, Date_Of_Access, returnList,feed):
	if (factorList and locationList):
		allFactors = u', '.join(factorList)
		allDomains = u', '.join(domain)

		getEachCityFromList(c, title, link, summary, date_published_reformated, Date_Of_Access, allFactors, allDomains, locationList, returnList, feed, wordFreqFactor)

#Get each city from the list
def getEachCityFromList(c, title, link, summary, date_published_reformated, Date_Of_Access, allFactors, allDomains, locationList, returnList, feed, wordFreqFactor):
	for eachCity in locationList:
		
		getGeoLocationOfEachCity(eachCity, c, title, link, summary, date_published_reformated, Date_Of_Access, allFactors, allDomains, returnList, feed, wordFreqFactor)

#Get geolocation from cities in list
def getGeoLocationOfEachCity(eachCity, c, title, link, summary, date_published_reformated, Date_Of_Access, allFactors, allDomains, returnList, feed, wordFreqFactor):
	result = c.execute('SELECT "city", "countryName", "latitude", "longitude" FROM (USACities INNER JOIN cityyAbvName ON TwoLetterAbv=country) WHERE "city"=(?) LIMIT 1', (eachCity,)).fetchall()

	if result:
		for each in result:
			CITY = []
			COUNTRYNAME = []
			LAT = []
			LONG = []
			city = each[0]
			countryName = each[1]
			latitude =  each[2]
			longitude =  each[3]
			CITY.append(city)
			COUNTRYNAME.append(countryName)
			LAT.append(latitude)
			LONG.append(longitude)
			allCities = ', '.join(CITY)
			allCountries = ', '.join(COUNTRYNAME)
			allLatitude = ', '.join(str(LATS) for LATS in LAT)
			allLongitude = ', '.join(str(LONGS) for LONGS in LONG)


			print title
			print link
			print summary
			print date_published_reformated
			print Date_Of_Access
			print allCities
			print allCountries
			print allFactors
			print wordFreqFactor
			print allDomains
			print allLatitude
			print allLongitude
			print feed
			print "\n"
	
	else:
		print "This location does not have a Latitude or Longitude associated with it."

#Get error message if an error occurs
def getErrors(Errors, feedErrors, linkErrors):
	combineErrors = zip(Errors, feedErrors, linkErrors)
	for row in combineErrors:
		print u', '.join(row)


if __name__ == '__main__':
	main()

