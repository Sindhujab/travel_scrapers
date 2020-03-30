#!/usr/bin/env python
# coding: utf-8
import requests
from requests.exceptions import ConnectionError
import pandas as pd
from bs4 import BeautifulSoup
import csv
import random
import time
import warnings
import multiprocessing as mp
from joblib import Parallel, delayed
import re

warnings.filterwarnings(action='ignore')

proxies = pd.read_csv('proxies.csv', sep=':')
UseragentList = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:60.0) Gecko/20100101 Firefox/60.0',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36']


headers = {

}


def select_random_proxy(proxy_sf):
    row_num = random.randint(0, len(proxy_sf) - 1)
    proxy_ip = "http://{}:{}@{}:{}".format(proxy_sf.iloc[row_num]['user'],
                                           proxy_sf.iloc[row_num]['pass'],
                                           proxy_sf.iloc[row_num]['ip'],
                                           proxy_sf.iloc[row_num]['port'])
    proxy = {"http": proxy_ip, "https": proxy_ip}
    return proxy

def select_random_UserAgent(UseragentList):
    row_num = random.randint(0, len(UseragentList) - 1)
    return UseragentList[row_num]

#extracting reviews
def booking_extract_reviews(product):
    MainData = []
    product_url = product.get('source_hotel_url')
    dest_id = product.get('city_id')
    hotel_id = product.get('source_hotel_id')
    #product_name = product.get('source_hotel_name')
    page_name = product_url.split('/')[-1].split('.')[0]
    print page_name
    review_count = product.get('review_count')
    for of in range(((int(review_count) / 10) + 2)):
    #for of in range(2):
        #print of

        offset = of * 10
        # if of>0:
        #     of1=of-1
        #     offset1=of1*10
        #     referer_url='https://www.booking.com/reviewlist.en-gb.html?label=gen173bo-1DCAsobEIyZm9ybXVsZTEtaHlkZXJhYmFkLWhpdGVjLWNpdHktb3BlbmluZy1vY3RvYmVyLTIwMTVICVgDaGyIAQGYAQm4AQfIAQzYAQPoAQH4AQKIAgGYAgKoAgO4AqT_9_IFwAIB;cc1=in&pagename={}&r_lang=&review_topic_category_id=&type=total&score=&dist=1&offset={}&rows=10&rurl=&sort=f_recent_desc&text=&translate=&_=1583218748574'.format(page_name, offset1)
        # else:
        #     referer_url=product_url
        #print(of)
        #review_url = 'https://www.booking.com/reviewlist.en-gb.html?aid=304142&amp;label=gen173rf-1FCAQoggI4lQRICVgDaGyIAQGYAQm4AQfIAQzYAQHoAQH4AQOIAgGiAg5sb2NhbGhvc3Q6ODk0OagCA7gCjoa77QXAAgE&sid=fde01d7751f6fd1b014102989918355d&cc1=gb&dist=1&pagename={}&type=total&offset={}&rows=10&_=157173578424'.format(page_name, offset)
        review_url='https://www.booking.com/reviewlist.en-gb.html?label=gen173bo-1DCAsobEIyZm9ybXVsZTEtaHlkZXJhYmFkLWhpdGVjLWNpdHktb3BlbmluZy1vY3RvYmVyLTIwMTVICVgDaGyIAQGYAQm4AQfIAQzYAQPoAQH4AQKIAgGYAgKoAgO4AqT_9_IFwAIB;cc1=in&pagename={}&r_lang=&review_topic_category_id=&type=total&score=&dist=1&offset={}&rows=10&rurl=&sort=f_recent_desc&text=&translate=&_=1583218748574'.format(page_name, offset)
        #review_url='https://www.booking.com/reviewlist.en-gb.html?label=gen173bo-1DCAsobEIyZm9ybXVsZTEtaHlkZXJhYmFkLWhpdGVjLWNpdHktb3BlbmluZy1vY3RvYmVyLTIwMTVICVgDaGyIAQGYAQm4AQfIAQzYAQPoAQH4AQKIAgGYAgKoAgO4AqT_9_IFwAIB;cc1=in&pagename=ibis-budget-edinburgh-park&r_lang=&review_topic_category_id=&type=total&score=&dist=1&offset=2&rows=10&rurl=&sort=&text=&translate=&_=1583218748574'
        print review_url
        time.sleep(random.randint(0, 3))
        try:
            headers['User-agent'] = select_random_UserAgent(UseragentList)
            #headers['referer']=referer_url
            #res = requests.get(url, headers=headers, proxies=select_random_proxy(proxies))
            r = requests.get(review_url,headers=headers, proxies=select_random_proxy(proxies))
            print r
        # print(r.headers.get('Content-Encoding'))
        # if 'br' in r.headers.get('Content-Encoding', '') and r.text:
        #     content = brotli.decompress(r.content)
        #else:
            content = r.text
            #content = r.content.decode(encoding='UTF-8')
            soup2 = BeautifulSoup(content, 'html.parser')
            #print "soup2",soup2
            reviews = soup2.find_all('li', attrs={"class": "review_list_new_item_block"})
            print "reviews are",(reviews)

            for review in reviews:

                reviewer_name = review.find('span', attrs={'class': "bui-avatar-block__title"})
                if reviewer_name:
                    reviewer_name =reviewer_name.text.strip()
                else:
                    reviewer_name=None
                print("rn",reviewer_name)
                rating = review.find('div', attrs={'class': "bui-review-score__badge"})
                review_url=review_url
                if rating:
                    rating=rating.text.strip()
                else:
                    rating=None
                print("r",rating)
                room_info=soup2.find('div', attrs={'class': "room_info_heading"})
                if room_info:
                    room_info=room_info.text.replace("Stayed in: ", "")
                else:
                    room_info="no information"
                review_title = review.find('h3', attrs={'class': "c-review-block__title"})
                if (review_title):
                    review_title = review_title.text.strip()
                else:
                    review_title = None
                print("review_title",review_title)
                review_id=review["data-review-url"]
                date_list = review.find_all('span', attrs={'class': "c-review-block__date"})
                print("dl",date_list)

                review_date = None
                month_of_stay=None
                m = {
                'January': "01",
                'February': "02",
                'March': "03",
                'April': "04",
                'May': "05",
                'June': "06",
                'July': "07",
                'August': "08",
                'September': "09",
                'October': "10",
                'November': "11",
                'December': "12"
                }
                year = " "
                date_stay = " "
                for date in date_list:
                    if ("Reviewed" in date.text.strip()):
                        review_date = date.text.strip()
                        day = review_date.split("Reviewed: ")[1].split(" ")[0]
                        month = review_date.split("Reviewed: ")[1].split(" ")[1]
                        year = review_date.split("Reviewed: ")[1].split(" ")[2]
                        try:
                            mon_num=m[month]
                            review_date = day + "-" + mon_num + "-" + year
                        except:
                            review_date=None
                    else:
                        date_stay = date.text.strip()
                if date_stay!=" ":
                    month_of_stay = date_stay.split(" ")[0] + ", " + year
                else:
                    month_of_stay=" "



                review_list=review.find_all('span',attrs={'class':"c-review__body"})
                print review_list

                reviewtext = ''
                for rev in review_list:
                    print rev.text.strip().replace("\r","")
                    reviewtext = reviewtext +' ' + rev.text.strip()
                    print reviewtext
                    print "................................"
                print "r",reviewtext
                OnePGReview = {"reviewer_name": reviewer_name,
                           "rating": rating,
                            "review_url":review_url,
                           "review_title": review_title,
                           "review_date": review_date,
                           "review_text": reviewtext,
                           "hotel_id": hotel_id,
                            "review_id":review_id,
                           "city_id": dest_id,
                           "month_of_stay":month_of_stay,
                            "room_info":room_info}
                #

                print "onepgreview",(OnePGReview)

                MainData.append(OnePGReview)
        except Exception as e:
            print "exception",e
            continue

    return MainData

def multi_process(product):
    tt = 0
    while tt < 7:
        try:
            #print(product.get('source_hotel_name'))
            FinalMainData.append(booking_extract_reviews(product))
            #print(len(FinalMainData))
            tt = 7

        except ConnectionError:

            print "Connection Error!"
            tt = tt + 1
        except Exception as ex:
            print('Error', ex)
            break
    return FinalMainData
#dest_ids = [2097701,2114520]
#dest_ids=[2097701,2114520,2092174,2097701]
#dest_ids=[2088213,2102835,2094211,2092511,2108205]
dest_ids=[20002903,20005244,20088325,20033173]
#,2090174,2097893,2095020,2107500]
#dest_ids=[2097701]
url = "https://www.booking.com/searchresults.en-gb.html?city={}"
print url
for dest_id in dest_ids:
    prop_details = []
    r = requests.get(url.format(dest_id), proxies=select_random_proxy(proxies))
    print r
    content = r.content.decode(encoding='UTF-8')

    soup = BeautifulSoup(content, 'html.parser')
    Page_url = "https://www.booking.com/searchresults.en-gb.html?city={}&rows=15&offset={}"
    try:
        pages = soup.find_all('a', attrs={"class": "sr_pagination_link"})[-1].text
        print "pages",pages
        print("this destination works")
        pages = int(re.findall("[0-9]+", pages)[-1])
    except:
        dest_ids.remove(dest_id)
        print dest_ids
        continue

    #print(pages)
    #pages=1
    for page in range(pages):

        page_no = page * 15
        #page_no = page * 1
        time.sleep(random.randint(0, 3))
        r = requests.get(Page_url.format(dest_id, page_no))
        print Page_url.format(dest_id, page_no)

        content = r.content.decode(encoding='UTF-8')
        soup = BeautifulSoup(content, 'html.parser')

        products = soup.find_all('div', attrs={"class": "sr_property_block"})
        # print("p",products)

        for product in products:
            try:
                hotel_id = product["data-hotelid"]
                link = product.find('a', attrs={"class": "hotel_name_link"})
                product_url = 'https://www.booking.com' + link['href'].strip().split(';')[0]
                print product_url
                product_name = product.find('span', attrs={"class": "sr-hotel__name"}).text.strip()
                product_code = product_url.split('/')[-1].split('.')[0]
                review_url = product_url.split(';')[0] + '#tab-reviews'
                review_count = 0
                rr = requests.get(review_url)
                content_r = rr.content.decode(encoding='UTF-8')
                r_soup = BeautifulSoup(content_r, 'html.parser')
                rating=r_soup.find('span', attrs={'class': "review-score-badge"})
                if rating:
                    rating=rating.text.strip()
                else:
                    rating=None

                for span in r_soup.find_all('span'):
                    if not span.attrs.values():
                        if 'Guest reviews' in span.text:
                            review_count = span.text.split(" ")[2].replace("(", "").replace(")", "").replace(",", "")
                prod_details = {'source_hotel_id': hotel_id,
                            'source_hotel_url': product_url,
                            'source_hotel_name': product_name,
                            'rating':rating,
                                "review_count":review_count,
                            'review_url': review_url,
                            'city_id': dest_id,
                            'source_id':39}
                prop_details.append(prod_details)
            except Exception as e:
                print e
                continue
    pd.DataFrame(prop_details).to_csv('booking_hotelList' + str(dest_id) +'.csv', encoding='utf8')
for dest_id in dest_ids:
    FinalMainData = []
    prop_details = pd.read_csv('booking_hotelList' + str(dest_id) +'.csv', encoding='utf8').to_dict("records")
#prop_details=[{u'rating': 8.5, u'source_hotel_name': u'Holiday Inn Express Hyderabad Banjara Hills', u'review_count': 20, u'city_id': 2097701, u'source_hotel_url': u'https://www.booking.com/hotel/in/formule1-hyderabad-banjara-hills.en-gb.html', u'source_id': 39, 'Unnamed: 0': 0, u'review_url': u'https://www.booking.com/hotel/in/formule1-hyderabad-banjara-hills.en-gb.html#tab-reviews', u'source_hotel_id': 2480984}, {u'rating': 8.6, u'source_hotel_name': u'Taj Banjara', u'review_count': 20, u'city_id': 2097701, u'source_hotel_url': u'https://www.booking.com/hotel/in/taj-banjara.en-gb.html', u'source_id': 39, 'Unnamed: 0': 1, u'review_url': u'https://www.booking.com/hotel/in/taj-banjara.en-gb.html#tab-reviews', u'source_hotel_id': 74726}, {u'rating': 7.1, u'source_hotel_name': u'Treebo Trip Arastu Inn', u'review_count': 20, u'city_id': 2097701, u'source_hotel_url': u'https://www.booking.com/hotel/in/treebo-arastu-inn.en-gb.html', u'source_id': 39, 'Unnamed: 0': 2, u'review_url': u'https://www.booking.com/hotel/in/treebo-arastu-inn.en-gb.html#tab-reviews', u'source_hotel_id': 2204252}]
    CompleteData = pd.DataFrame()
    num_cores = mp.cpu_count()
    total_data = []
    total_data.extend(
    Parallel(n_jobs=num_cores)(
        delayed(multi_process)(product) for product in prop_details))
    print total_data

    for i in range(len(total_data)):

        CompleteData = CompleteData.append(pd.DataFrame(total_data[i][0]))
    CompleteData.to_csv('booking_Hotel_Review'+ str(dest_id) +'.csv',quoting=csv.QUOTE_ALL, encoding='utf8',index=False)








