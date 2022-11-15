"""
Scraper module updates the csv for a given stock with the latest comments or 
creates a new csv file with comments from a specified number of forum pages.
"""
import re
import sys
import os
import requests as req
import pandas as pd
from time import sleep
from random import randint
from bs4 import BeautifulSoup as BS

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Safari/605.1.15'
}
dtype = {
    0 : 'object',
    1 : 'object',
    2 : 'int64'
}
def main():

    stock = input("Input stock: ")
    path = os.path.join('data', str(stock))
    forum_url, latest_pagenum = parse_main_page(stock)

    if stock + '.csv' not in os.listdir(path):
      start_page = input('Enter start page: ')
      end_page = input('Enter end page: ')
      update_database(stock, forum_url, latest_pagenum, path, start_page=start_page, end_page=end_page)
    else:
      update_database(stock, forum_url, latest_pagenum, path)

def update_database(stock, forum_url, latest_pagenum, path, **kwargs):
    """
    Update the csv database for a given stock code by scraping i3 forums.
    Optional arguments start_page and end_page can be specified to create a new database with a specific number of pages.
    """
    filename = str(stock) + '.csv'
    filepath = os.path.join(path, filename)

    # If start and end page specified, create new file with data from the specified pages
    if 'start_page' in kwargs:
        start_page = int(kwargs['start_page'])
        end_page = int(kwargs['end_page'])

        # Generate list of pages to parse
        pages = [forum_url + '?ftp=' + str(x) for x in range(start_page, end_page+1)]

        # Create a DataFrame from page data
        new_data = []
        for page in pages:
          comments = parse_page(url=page)
          new_data.append(comments)
          sleep(randint(2,6))
        data = pd.concat(new_data, ignore_index=True)

        # Write data to a new csv filename
        data.to_csv(filepath, index=False, columns=['Date', 'Comment', 'PageNum'])
 
    # If not, update existing file
    else:

        # Read all data in csv file
        main_data = pd.read_csv(filepath, dtype=dtype)
        
        # Split data to main data and supplementary data
        s_data = main_data.iloc[-50:,:]
        main_data = main_data.iloc[:-50,:] # Main data
        current_pagenum = int(s_data.iloc[-1, 2])

        # Generate list of forum pages to parse
        pages = [forum_url + '?ftp=' + str(x) for x in range(current_pagenum, latest_pagenum+1)]

        # Parse pages and store in DataFrame.
        new_data = []
        for page in pages:
            new_data.append(parse_page(url=page))

            # Control crawl rate with 2-5s intervals between requests
            sleep(randint(2,5))

        # Remove duplicate comments.
        combined = pd.concat([s_data] + new_data)
        combined.drop_duplicates(ignore_index=True, inplace=True)
        
        # Combine with main data
        combined = pd.concat([main_data, combined])
        
        # Update data to csv file.
        combined.to_csv(filepath, columns=['Date', 'Comment', 'PageNum'], index=False)

def parse_main_page(stock):
    """
    Parse the latest page of a stocks forum and the forum url and the latest
    page number in the forum. 
    """
    # Get home page for a stock.
    main_url = 'https://klse.i3investor.com/servlets/stk/' + (str(stock)+'.jsp')
    main_page = req.get(main_url, headers=headers)
    main_soup = BS(main_page.content, 'html.parser')

    # Get link to forum page.
    latest_page_url = 'https://klse.i3investor.com' + main_soup.find(class_='comtbtop').find(href=re.compile('forum')).get('href')

    # Get the latest page number 
    soup = BS(req.get(latest_page_url, headers=headers).content, 'html.parser')
    latest_pagenum = soup.find(id='content').find(string=re.compile('Page')).parent.find('b').string

    return (latest_page_url, int(latest_pagenum))

def parse_page(**kwargs):
    """
    Takes a stock forum url or soup html object and returns parsed comments in 
    a pd.DataFrame.
    """
    def get_response(url):
        response = req.get(url, headers=headers)
        if response.status_code != 200:
            print(f'Error, response code {response.status_code}')
            print(kwargs['url'])
            sleep(10)
            response = get_response(url)
        return response

    if 'url' in kwargs:
        response = get_response(kwargs['url'])
        soup = BS(response.content, 'html.parser')
    elif 'soup' in kwargs:
        soup = kwargs['soup']
    else:
        raise TypeError("Input supplied should be a stock code or soup html.")
    
    # Parse page for the current forum page number.
    current_pagenum = int(soup.find(id='content').find(string=re.compile('Page')).parent.find('b').string )  

    comments = soup.find(id='mainforum')
    comments = comments.find_all(lambda x: x.name == 'tr' and bool(x.find('span', {'class': 'autolink'})))
    comments = pd.DataFrame([(x.find(class_='comdt').text, x.find(class_='autolink').text, current_pagenum) for x in comments], columns=['Date', 'Comment', 'PageNum'])

    # Store dates in pandas date format
    # comments['Date'] = pd.to_datetime(comments['Date'])
    return comments

if __name__ == "__main__":
    main()