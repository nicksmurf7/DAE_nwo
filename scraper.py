'''
Nick Murphy
8.15.22

Web scraper for the top 50 movies and associated metadata from the 
list of "Top Rated English Movies by Genre"

URL: https://www.imdb.com/feature/genre?ref_=fn_asr_ge

'''
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import json
from urllib.parse import urlparse
import os
import re

path = 'csvs/'
base_url = 'https://www.imdb.com/feature/genre?ref_=fn_asr_ge'

# Gets movie title from header of individual movie div
def getMovieTitle(header):
    try:
        return header[0].find("a").getText()
    except:
        return 'NA'

# Gets movie release year from header of individual movie div
def getReleaseYear(header):
    try:
        return header[0].find("span",  {"class": "lister-item-year text-muted unbold"}).getText()
    except:
        return 'NA'

# Gets genre from sub-heading of individual movie div
def getGenre(subheader):
    try:
        return subheader.find("span",  {"class":  "genre"}).getText()
    except:
        return 'NA'

# Gets synopsis from sub-heading of individual movie div
# UNUSED
def getSynopsis(movie):
    try:
        return movie.find_all("p", {"class":  "text-muted"})[1].getText()
    except:
        return 'NA'

# Gets movie rating from sub-heading of individual movie div
def getRating(movie):
    try:
      return movie.find('strong').getText()
    except:
      return 'NA'

# Gets director and star cast of movie
def getCastnCrew(movie):
  res = []
  try:
    lis = movie.find_all('a')
    for l in range(len(lis)):
      res.append(lis[l].getText())
    return res
  except:
    return 'NA'


def scrape_movies_by_genre(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Movie Name
    movies_list  = soup.find_all("div", {"class": "lister-item mode-advanced"})
    result = []
    for movie in movies_list:
        header = movie.find_all("h3", {"class":  "lister-item-header"})
        subheader = movie.find_all("p", {"class":  "text-muted"})[0]
        imageDiv =  movie.find("div", {"class": "lister-item-image float-left"})
        image = imageDiv.find("img", "loadlate")
        ratings = movie.find("div", {"class": "ratings-bar"})
        
        #  Movie Title
        movie_title =  getMovieTitle(header)
        
        #  Movie release year
        year = getReleaseYear(header)

        #  Movie rating
        rating = getRating(ratings)
        
        #  Genre  of movie
        genre = getGenre(subheader)
        g = genre.replace(' ', '')
        g = g.replace('\n', '')
        g = g.split(",")
        if len(g) == 3:
            genre_1 = g[0]
            genre_2 = g[1]
            genre_3 = g[2]
        elif len(g) == 2:
            genre_1 = g[0]
            genre_2 = g[1]
            genre_3 = 'NA'
        else:
            genre_1 = g[0]
            genre_2 = 'NA'
            genre_3 = 'NA'

        # Movie Synopsis
        # synopsis = getSynopsis(movie)

        #  Movie Director and Cast
        cast = getCastnCrew(movie.find_all("p")[2])
        if len(cast) > 4:
          director = cast[0]
          cast = cast[0:]
          cast_1 = cast[0]
          cast_2 = cast[1]
          cast_3 = cast[2]
          cast_4 = cast[3]
        elif len(cast) == 4:
          director = 'NA'
          cast_1 = cast[0]
          cast_2 = cast[1]
          cast_3 = cast[2]
          cast_4 = cast[3]
        elif len(cast) == 3:
          director = 'NA'
          cast_1 = cast[0]
          cast_2 = cast[1]
          cast_3 = cast[2]
          cast_4 = 'NA'
        # Store collected data to turn into a pandas df in future
        data = {"title": movie_title,
            "rating": rating,
            "year": year,
            "genre_1": genre_1,
            "genre_2": genre_2,
            "genre_3": genre_3,
            "director": director,
            "cast_1": cast_1,
            "cast_2": cast_2,
            "cast_3": cast_3,
            "cast_4": cast_4,
            }
        result.append(data)
    df = pd.DataFrame(result)
    # Delete index col
    # df = df.drop([df.columns[0]], axis=1)
    d = df.fillna('NA')
    return d

# Trampoline function for scrape_movies_by_genre
# Gets each link from list of Top Movies by Genre
# Turns scraped dataframe into csv to be read in later
def extract_links(html):
    with requests.get(html) as file:
        content = BeautifulSoup(file.text, "html.parser")
        n = 1
        for sidebar in content.find_all('div', attrs={"id": "sidebar"}):
            section = sidebar.find_all('div', {"class": "widget_content no_inline_blurb"})
            tab = section.pop(3)
            for link in tab.find_all('a', href=True):
              d = scrape_movies_by_genre('http://imdb.com' + link['href'])
              name = str(n) + '.csv'
              n += 1
              d.to_csv(path + name, index=False)
