# -*- coding: utf-8 -*-
"""
@author: Ruben Kerkhofs

This script was created for educational purposes and contains the code that was used
to create the blogpost on webscraping Airbnb data.
"""


"""
    URLs
"""
rutland = "https://www.airbnb.com/s/Rutland--VT/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&flexible_date_search_filter_type=2&adults=2&source=structured_search_input_header&checkin=2021-02-12&checkout=2021-02-15&search_type=search_query"

"""
    Setting up your environment
"""
from bs4 import BeautifulSoup 
from selenium import webdriver
# The following packages will also be used in this tutorial
import pandas as pd
import numpy as np
import time
import requests
import re
from sklearn.feature_extraction.text import CountVectorizer
from joblib import Parallel, delayed


"""
    Getting started
"""
def getPage(url):
    ''' returns a soup object that contains all the information 
    of a certain webpage'''
    result = requests.get(url)
    content = result.content
    return BeautifulSoup(content, features = "lxml")

    
def getRoomClasses(soupPage):
    ''' This function returns all the listings that can 
    be found on the page in a list.'''
    rooms = soupPage.findAll("div", {"class": "_8ssblpx"})
    result = []
    for room in rooms:
        result.append(room)
    return result

def getListingLink(listing):
    ''' This function returns the link of the listing'''
    return "http://airbnb.com" + listing.find("a")["href"]

def getListingTitle(listing):
    ''' This function returns the title of the listing'''
    return listing.find("meta")["content"]

def getTopRow(listing):
    ''' Returns the top row of listing information'''
    return listing.find("div", {"class": "_1tanv1h"}).text

def getRoomInfo(listing):
    ''' Returns the guest information'''
    return listing.find("div", {"class":"_kqh46o"}).text

def getBasicFacilities(listing):
    ''' Returns the basic facilities'''
    try:
        output = listing.findAll("div", {"class":"_kqh46o"})[1].text.replace(" ","") #Speeds up cleaning
    except:
        output = []
    return output

def getListingPrice(listing):
    ''' Returns the price'''
    return listing.find("div", {"class":"_1bbeetd"}).text

def getListingRating(listing):
    ''' Returns the rating '''
    return listing.find("span", {"class":"_krjbj"}).text

def getListingReviewNumber(listing):
    ''' Returns the number of reviews '''
    try: # Not all listings have reviews // extraction failed
        output = listing.findAll("span", {"class":"_krjbj"})[1].text
    except:
        output = -1   # Indicate that the extraction failed -> can indicate no reviews or a mistake in scraping
    return output

def extractInformation(soupPage):
    ''' Takes all the information of a single page (thus multiple listings) and
    summarizes it in a dataframe'''
    listings = getRoomClasses(soupPage)
    titles, links, toprows, roominfos, basicfacilitiess, prices, ratings, reviews = [], [], [], [], [], [], [], []
    for listing in listings:
        titles.append(getListingTitle(listing))
        links.append(getListingLink(listing))
        toprows.append(getTopRow(listing))
        roominfos.append(getRoomInfo(listing))
        basicfacilitiess.append(getBasicFacilities(listing))
        prices.append(getListingPrice(listing))
        ratings.append(getListingRating(listing))
        reviews.append(getListingReviewNumber(listing))
    dictionary = {"title": titles, "toprow": toprows, "roominfo": roominfos, "facilities" : basicfacilitiess, "price": prices, "rating": ratings, "link": links, "reviewnumber": reviews}
    return pd.DataFrame(dictionary)
       
'''
    Scraping all listings for a given city
'''
def findNextPage(soupPage):
    ''' Finds the next page with listings if it exists '''
    try:
        nextpage = "https://airbnb.com" + soupPage.find("a", class_="_za9j7e")["href"]
    except:
        nextpage = "no next page"
    return nextpage

def getPages(url):
    ''' This function returns all the links to the pages containing 
    listings for one particular city '''
    result = []
    while url != "no next page": 
        page = getPage(url)
        result = result + [page]
        url = findNextPage(page)
    return result

def extractPages(url):
    ''' This function outputs a dataframe that contains all information of a particular
    city. It thus contains information of multiple listings coming from multiple pages.'''
    pages = getPages(url)
    # Do for the first element to initialize the dataframe
    df = extractInformation(pages[0])
    # Loop over all other elements of the dataframe
    for pagenumber in range(1, len(pages)):
        df = df.append(extractInformation(pages[pagenumber]))
    return df

''' 
    Scraping all listings for a collection of cities
'''

urls = [["Rutland", rutland]
         ]


def scrapeURLs(listofURLs):
    ''' This function scrapes all listings of the cities listed in a list together
    with their URLs'''
    print(listofURLs[0][0]) # Shows which city is being scraped
    # Do it for the first element in the list to initialize dataframe
    df = extractPages(listofURLs[0][1])
    df.loc[:, "city"] = listofURLs[0][0] # Add the city as a feature
    # loop over all the other elements in the list and append to dataframe
    for i in range(1, len(listofURLs)):
        print(listofURLs[i][0]) # Shows which city is being scraped
        newrows = extractPages(listofURLs[i][1])
        newrows.loc[:, "city"] = listofURLs[i][0] # Add the city as a feature
        df = df.append(newrows)
    return df

'''
    Scraping detailed information of rooms with beautifulsoup
'''

def getDescription(detailpage):
    ''' Returns the self written description of the host '''
    return detailpage.find("div", {"class": "_eeq7h0"}).text

def getDetailedScores(detailpage):
    output = []
    scores = detailpage.findAll(class_ = '_a3qxec')
    try: # sometimes a listing does not have any reviews
        for i in range(0, 6):
            split = scores[i].text.split(".")
            output.append(float(split[0][-1] + "." + split[1]))
    except: # then we just don't want to pass any scores
        pass
    return output

def getHostInfo(detailpage):
    ''' Returns the name of the host and when they joined'''
    return detailpage.find(class_ = "_f47qa6").text


'''
    Using selenium for all other information
'''
def setupDriver(url, waiting_time = 2.5):
    ''' Initializes the driver of selenium'''
    driver = webdriver.Chrome()
    driver.get(url)
    time.sleep(waiting_time) 
    return driver


def getJSpage(url):
    ''' Extracts the html of the webpage including the JS elements,
    output should be used as the input for all functions extracting specific information
    from the detailed pages of the listings '''
    driver = setupDriver(url)
    read_more_buttons = driver.find_elements_by_class_name("_1d079j1e")
    try:
        for i in range(2, len(read_more_buttons)):
            read_more_buttons[i].click()
    except:
        pass
    html = driver.page_source
    driver.close()
    return BeautifulSoup(html, features="lxml") 


def getAmenitiesPage(detailpage):
    ''' This code fetches the html of the webpage containing the information
     about the amenities that are available in the room'''
    
    link = detailpage.find(class_ = "_1v4ygly5")["href"]
    driver = setupDriver("https://airbnb.com" + link, 5) # Amenitiespage is a link disguished as a button, this is why I need to do this
    html = driver.page_source
    driver.close()
    return BeautifulSoup(html, features="lxml")

            
first = True # These variables were coded in a smarter way when doing the actual analysis
scraped = 0  # It used the length of the intermediate_results_par dataset stored on the pc
def getAddis(url): 
    ''' This function is used to extract the html of the additional pages (detail page and amenities page)'''
    global first
    global scraped
    output = pd.DataFrame(columns=["details_page", "amenities_page", "link"])
    try:
        dp = getJSpage(url)
        output.loc[0] = [dp, getAmenitiesPage(dp), url]
    except:
        output.loc[0] = [-1, -1, url]
    if first: # Ensures that the columns have the correct titles because apparently that's difficult
        output.to_csv('intermediate_results_par.csv', mode='a', header=True, index = False)
        first = False
    else:
        output.to_csv('intermediate_results_par.csv', mode='a', header=False, index = False) 
    scraped += 1
    print("Scraped: {}".format(scraped))


# Extract Javascript enabled information    
def getReviews(detailpage):
    ''' Returns a list of the featured reviews on the page '''
    reviews = detailpage.findAll(class_ = "_50mnu4")
    output = ""
    for review in reviews:
        output += review.text + "**-**" #**-** can be used to split reviews later again
    return output


def getAmenities(amenitiespage):
    amenities = amenitiespage.findAll(class_ = "_vzrbjl")
    output = ""
    for amenity in amenities:
        output += re.findall('[A-Z][^A-Z]*', amenity.text)[0] + "**-**" # **-** will be used to split the string later for the purpose of dummification
    return output

def getResponseInfo(detailpage):
    try:
        output = detailpage.find(class_ = "_jofnfy").text
    except:
        output = ""
    return output


'''
    Clean functions basic data frame extracted using only beautifulsoup
'''

def cleanFacilities(df): # Treating the facilities as a bag of words to create dummy variables
    df.loc[:, "facilities"] = df["facilities"].astype(str).str.replace("[","").str.replace("]","")
    vectorizer = CountVectorizer(decode_error = "ignore") 
    X = vectorizer.fit_transform(df.facilities)
    bag_of_words = pd.DataFrame(X.toarray(), columns=vectorizer.get_feature_names())
    return pd.concat([df.reset_index(drop=True).drop("facilities", axis = 1), bag_of_words], axis=1)

def cleanTitle(df):
    df.loc[:, "name"] = df["title"].str.split(" null ", n = 0, expand = True)[0].str.replace("-", "")
    df.loc[:, "location"] = df["title"].str.split(" null ", n = 0, expand = True)[1].str.replace("-", "").str.strip()
    return df.drop("title", axis = 1)

def cleanTopRow(df):
    df.loc[:, 'roomtype'] = df["toprow"].str.split(" in ", n = 0, expand = True)[0] 
    df.loc[:, 'detailed_location'] = df["toprow"].str.split(" in ", n = 0, expand = True)[1] 
    return df.drop("toprow", axis = 1)

def cleanRoomInfo(df):
    df.loc[:, "guests"] = df.loc[:, "roominfo"].str.split(" · ", n = 0, expand = True)[0].str.replace(" guests", "")
    df.loc[:, "bedrooms"] = df.loc[:, "roominfo"].str.split(" . ", n = 0, expand = True)[1]
    df.loc[:, "beds"] = df.loc[:, "roominfo"].str.split(" . ", n = 0, expand = True)[2].str.replace(" bed", "").str.replace("s", "")
    df.loc[:, "bathrooms"] = df.loc[:, "roominfo"].str.split(" . ", n = 0, expand = True)[3]
    df.loc[:, "guests"] = pd.to_numeric(df.guests, errors = 'coerce')
    df.loc[:, "beds"] = pd.to_numeric(df.beds, errors = 'coerce')
    df.loc[:, "bedrooms"] = pd.to_numeric(df.bedrooms.str.split(" ", n = 0, expand = True)[0], errors = "ignore")
    df.loc[:, "bathrooms"] = pd.to_numeric(df.bathrooms.str.split(" ", n = 0, expand = True)[0], errors = "ignore")
    return df.drop("roominfo", axis = 1)

def cleanPrice(df):
    df.loc[:, "pricepernight"] = df.loc[:, "price"].str.split("Discounted", n = 0, expand = True)[0].str.replace("$", "/").str.split("/",  n = 0, expand = True)[1]
    df.loc[:, 'discountedpricepernight'] = df.loc[:, "price"].str.split("Discounted", n = 0, expand = True)[1].str.replace("$", "/").str.split("/",  n = 0, expand = True)[1]
    df.loc[:, "price"] = pd.to_numeric(df.pricepernight.str.replace(",","").str.strip())
    df.loc[:, "discountedprice"] = pd.to_numeric(df.discountedpricepernight.str.replace(" ", "").str.replace(",",""), errors = "coerce")
    return df.drop(["pricepernight", "discountedpricepernight"], axis = 1)

def cleanRating(df):
    df.loc[:, "score"] = df.loc[:, 'rating'].str.split(" ", n = 0, expand = True)[1]
    df.loc[:, "score"] = pd.to_numeric(df.score, errors = "coerce")
    return df.drop("rating", axis = 1)

def cleanReviewNumber(df):
    df.loc[:, "reviewnumber"] = df.loc[:, 'reviewnumber'].str.split(" ", n = 0, expand = True)[0]
    df.loc[:, "reviewnumber"] = pd.to_numeric(df.reviewnumber, errors = "coerce")
    return df

def clean(df):
    df = cleanTitle(df)
    df = cleanFacilities(df)
    df = cleanTopRow(df)
    df = cleanRoomInfo(df)
    df = cleanPrice(df)
    df = cleanRating(df)
    df = cleanReviewNumber(df)
    # Reorder columns
    col1 = df.pop('price')
    df = pd.concat([df.reset_index(drop=True), col1], axis=1)
    col2 = df.pop('reviewnumber')
    df = pd.concat([df.reset_index(drop=True), col2], axis=1) 
    col3 = df.pop('link')
    df = pd.concat([df.reset_index(drop=True), col3], axis=1) 
    return df


'''
    Clean functions data frame containing the html of the additional pages
'''


def cleanAmenities(df):
    df.loc[:, "amenities"] = df.amenities.replace(np.nan, '', regex=True)# fit_transform cannot handle missing values
    df.loc[:, "amenities"] = df.amenities.str.replace(" ", "_").str.replace("-", " ").str.replace("*", "") #split in two because of a python bug (https://stackoverflow.com/questions/3675144/regex-error-nothing-to-repeat)
    vectorizer = CountVectorizer(decode_error = "ignore") 
    X = vectorizer.fit_transform(df.amenities)
    bag_of_words = pd.DataFrame(X.toarray(), columns=vectorizer.get_feature_names())
    return pd.concat([df.reset_index(drop=True).drop("amenities", axis = 1), bag_of_words], axis=1)

def cleanReviews(df):
    df.loc[:, "reviews"] = df.reviews.replace(np.nan, '', regex=True)# fit_transform cannot handle missing values
    df.loc[:, "reviews"] = df.reviews.str.split("-")
    return df

def getResponseTime(string):
    if "Response time" in string:
        output = string[string.find("Response time") + 15:]
    else:
        output = "Unknown"
    return output

def getResponseRate(string):
    if "Response rate" in string:
        temp = string[string.find("Response rate") + 15:string.find("Response rate")+20] 
        output = ""
        for letter in temp:
            if letter in "0123456789":
                output += letter
    else:
        output = "Unknown"      
    return output

def getLanguages(string):
    if "Language" in string:
        if "Response" in string:
            output = string[10:string.find("Response")].strip()
        else:
            output = string[10:].strip()
    else:
        output = "Unknown"
    return output

def cleanResponseTime(df):
    df.loc[:, "response_info"] = df.response_info.replace(np.nan, '', regex=True)
    df.loc[:, "response_time"] = df.response_info.apply(lambda x: getResponseTime(x))
    return df

def cleanResponseRate(df):
    df.loc[:, "response_rate"] = df.response_info.apply(lambda x: getResponseRate(x))
    return df

def cleanLanguages(df):
    df.loc[:, "languages"] = df.response_info.apply(lambda x: getLanguages(x))
    df.loc[:, "languages"] = df.languages.str.split(",")
    return df

def cleanResponseInfo(df):
    df = cleanResponseTime(df)
    df = cleanResponseRate(df)
    df = cleanLanguages(df)
    return df.drop("response_info", axis = 1)



'''
    Scraper
'''

def scraper(urls, sample_size = None, random_state = 1234):
    df = scrapeURLs(urls)
    df = clean(df)
    if sample_size is not None:
        df = df.sample(sample_size, random_state = random_state)
    Parallel(n_jobs = -1, prefer="threads")(delayed(getAddis)(url) for url in df.link)
    df2 = pd.read_csv("intermediate_results_par.csv")
    df = df.merge(df2, on = "link")
    df.loc[:, 'reviews'] = df.details_page.apply(lambda x: getReviews(BeautifulSoup(x, features = "lxml")))
    df.loc[:, 'response_info'] = df.details_page.apply(lambda x: getResponseInfo(BeautifulSoup(x, features = "lxml")))
    df.loc[:, "amenities"] = df.amenities_page.apply(lambda x: getAmenities(BeautifulSoup(x, features = "lxml")))
    # df = cleanReviews(df)
    # df = cleanResponseInfo(df)
    # df = cleanAmenities(df)
    return df


'''
    Running te scraper
'''
df = scraper(urls, sample_size = 10)