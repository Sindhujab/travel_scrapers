import pandas as pd
import requests
import random
import time
import warnings
from datetime import datetime,timedelta
warnings.filterwarnings(action='ignore')
import json
import multiprocessing as mp
from joblib import Parallel, delayed
def select_random_proxy(proxy_sf):
    row_num = random.randint(0, len(proxy_sf) - 1)
    proxy_ip = "http://{}:{}@{}:{}".format(proxy_sf.iloc[row_num]['user'],
                                           proxy_sf.iloc[row_num]['pass'],
                                           proxy_sf.iloc[row_num]['ip'],
                                           proxy_sf.iloc[row_num]['port'])
    proxy = {"http": proxy_ip, "https": proxy_ip}
    return proxy

proxies = pd.read_csv('proxies.csv', sep=':')
# UseragentList = [
#     'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
#     'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:60.0) Gecko/20100101 Firefox/60.0',
#     'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
#     'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0',
#     'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36']
# def select_random_UserAgent(UseragentList):
#     row_num = random.randint(0, len(UseragentList) - 1)
#     return UseragentList[row_num]

def extract_MMT_Data(info):
    global countrycode, citycode
    data = {
        "advancedFiltering": True,
        "appVersion": "72.0.3626.109",
        "applicationId": 410,
        "bookingDevice": "DESKTOP",
        "cancellationPolicyRulesReq": "no",
        "cdfContextId": "B2C",
        "channel": "B2Cweb",
        "currency": "INR",
        "deviceId": "Chrome",
        "deviceType": "Desktop",
        "domain": "B2C",
        "idContext": "B2C",
        "limit": 5000,
        "visitorId": "5f854464-5ab7-4c5b-b3fc-38996396d222",
        "responseFilterFlags": {"bestCoupon": True, "cityTaxExclusive": True, "flyfishSummaryRequired": True,
                                "freeCancellationAvail": True, "mmtPrime": False, "persuasionSeg": "P1000",
                                "soldOutInfoReq": True, "staticData": True, "walletRequired": True},
        "guestRecommendationEnabled": {"maxRecommendations": "1", "text": "true"},
        "roomStayCandidates": [{"guestCounts": [{"ageQualifyingCode": "10", "count": "2"}]}],
        "requestType": "B2CAgent",
        "token": "468c80d7-c216-4a4e-9dd4-43d98e30ecdd",
        "pageContext": "LISTING",
        "reqContext": "LISTING"
    }
    data2 = {
        "bookingDevice": "DESKTOP",
        "filter": {"ota": "MMT"},
        "limit": 10000,
        "sortCriteria": {"sortBy": "DATE", "order": "DESC"},
        "start": 0
    }
    headers = {
        'Content-Type': 'application/json',
        'Host': 'mapi.makemytrip.com',
        'cache-control': 'no-cache',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'origin': 'https://www.makemytrip.com',
        'os': 'desktop',
        'scheme': 'https'
    }
    url = 'https://mapi.makemytrip.com/clientbackend/entity/api/searchHotels'
    checkin = datetime.today() + timedelta(days=15)
    checkout = datetime.today() + timedelta(days=16)
    checkin = checkin.strftime('%Y-%m-%d')
    checkout = checkout.strftime('%Y-%m-%d')
    # checkin="2080-04-04"
    # checkout="2080-04-05"
    for key, value in info.items():
        countrycode=key
        citycode=value
    data['cityCode'] = citycode
    data['countryCode'] = countrycode
    data['checkin']=checkin
    data['checkout']=checkout
    m = {
        'Jan': "01",
        'Feb': "02",
        'Mar': "03",
        'Apr': "04",
        'May': "05",
        'Jun': "06",
        'Jul': "07",
        'Aug': "08",
        'Sep': "09",
        'Oct': "10",
        'Nov': "11",
        'Dec': "12"
    }
    month = {
        'Jan': "January",
        'Feb': "February",
        'Mar': "March",
        'Apr': "April",
        'May': "May",
        'Jun': "June",
        'Jul': "July",
        'Aug': "August",
        'Sep': "September",
        'Oct': "October",
        'Nov': "November",
        'Dec': "December"
    }
    def review_url_func(id,key,currency,lat,lon):
        #checkin_date = checkin.split("-")[2] + checkin.split("-")[1] + checkin.split("-")[0]
        #print checkin_date
        #checkout_date = checkout.split("-")[2] + checkout.split("-")[1] + checkout.split("-")[0]
        #print checkout_date
        #print "currency",currency
        url3 = "https://www.makemytrip.com/hotels/hotel-details/?hotelId={}&mtkeys={}&_uCurrency={}&checkin=04042080&checkout=05032080&city={}&country={}&lat={}&lng={}reference=hotel&roomStayQualifier=1e0e".format(
            id,key,currency, citycode, countrycode, lat,lon)
        #print "url3",url3

        return url3
    #print content
    try:
        r = requests.post(url, data=json.dumps(data), headers=headers,proxies=select_random_proxy(proxies))
        print r
        content = r.content.decode(encoding='UTF-8')
        hotelList = json.loads(content).get('hotelList')
        #print "hotelList is",hotelList
        df = pd.DataFrame(hotelList)
        df = df[['geoLocation', 'id', 'starRating', 'name', 'currencyCode', 'mtkey']]
        try:
            df['review_url'] = df.apply(lambda x: review_url_func(x['id'], x['mtkey'], x['currencyCode']['value'],x["geoLocation"]['latitude'], x["geoLocation"]['longitude']),axis=1)
            df['source_hotel_url']=df['review_url']
        except Exception as e :
            print e
            df['review_url'] = ""
            df['source_hotel_url'] = ""

        #str(id) + '||a'
        df['city_id']=citycode
        df['source_id']=40
        df=df[[ 'city_id', 'starRating','review_url','id', 'name','source_hotel_url','source_id']]
        df.rename(columns={'id': 'source_hotel_id', 'name': 'source_hotel_name', 'starRating': 'rating'}, inplace=True)
        #df.to_csv('MMTriphotelList_{}.csv'.format(citycode), encoding='utf8')
        print "file is written"
        #print hotelList
        MainReviewData = pd.DataFrame()
        count = []
        for i in range(len(hotelList)):
            hotel_id = hotelList[i].get('id')
            try:
                url2 = "https://mapi.makemytrip.com/clientbackend/entity/api/hotel/{}/flyfishReviews?srcClient=DESKTOP".format(
                    hotel_id)
                r = requests.post(url2, data=json.dumps(data2), headers=headers,proxies=select_random_proxy(proxies))
                content = r.content.decode(encoding='UTF-8')
                Review = json.loads(content).get('payload', {'response': {'MMT': None}}).get('response').get('MMT')
                if (Review == None):
                    continue
                else:
                    data = []
                    print len(Review)
                    r_count={}
                    r_count['hotel_id']=hotel_id
                    r_count['review_count']=len(Review)
                    count.append(r_count)
                    for i in range(0, len(Review)):
                        #print i
                        # print Review[i]
                        review_data = {}
                        try:
                            review_data['rating'] = Review[i]['rating']
                        except:
                            review_data['rating'] = ""
                        try:
                            review_data['review_id'] = Review[i]['id']
                        except:
                            review_data['review_id'] = ""
                        try:
                            review_data['reviewer_name'] = Review[i]['travellerName']
                        except:
                            review_data['reviewer_name'] = ""
                        try:
                            review_data['review_text'] = Review[i]['reviewText']
                        except:
                            review_data['review_text'] = ""
                        try:
                            review_data['review_title'] = Review[i]['title']
                        except:
                            review_data['review_title'] = ""
                        try:
                            review_data['room_info'] = Review[i]['roomType']
                        except:
                            review_data['room_info'] = ""
                        try:
                            mon = Review[i]['publishDate'].split(" ")[0]
                            d = Review[i]['publishDate'].split(" ")[1].split(",")[0]
                            y = Review[i]['publishDate'].split(" ")[2]
                            review_date = d + "/" + m[mon] + "/" + y
                            review_data['review_date']=review_date
                            review_data['month_of_stay']=month[mon] +' ,' + y
                        except:
                            review_data['review_date'] = ""
                        review_data['city_id']=citycode
                        review_data['hotel_id']=str(hotel_id) + ' '
                        review_data['review_url']=""
                        data.append(review_data)

                    MainReviewData = MainReviewData.append(data)

            except ValueError as Ex:
                print Ex
                print "continuing"
                continue
        df1_count=pd.DataFrame(count)
        merged_left = pd.merge(left=df, right=df1_count, how='left', left_on='source_hotel_id',
                               right_on='hotel_id')
        merged_left.to_csv('MMTriphotelList_{}.csv'.format(citycode), encoding='utf8')

        MainReviewData.to_csv(str(citycode) + '_MMT_Hotel_Review.csv', encoding='utf8')
        #
        # return "Done"

    except Exception as ex:
        print ex
        #print content
        time.sleep(20)

#info=[{'ID':'JKT'},{'ID':'BDO'},{'ID':'SUB'}]
#info=[{'ID':'JKT'}]
#info=[{'IN':'CTBOM'},{'IN':'CTHYDERA'},{'IN':'CTVTZ'},{'IN':'CTBLR'},{'IN':'CTITA'},{'IN':'CTPAT'},{'IN':'CTRPR'},{'IN':'CTGOI'},{'IN':'CTIXC'},{'IN':'CTSLV'},{'IN':'CTIXR'},{'IN':'CTTRV'},{'IN':'CTBHO'},{'IN':'CTIMP'}]
#info=[{'IN':'CTBOM'}]
info=[{'IN':'CTSHL'},{'IN':'CTAJL'},{'IN':'CTKOH'},{'IN':'CTBBI'},{'IN':'CTJAI'},{'IN':'CTXGA'},{'IN':'CTMAA'},{'IN':'CTIXA'},{'IN':'CTLKO'},{'IN':'CTDED'},{'IN':'CTCCU'},{'IN':'CTAMRA'},{'IN':'CTIXZ'},{'IN':'CTXDM'},{'IN':'CTDEL'},{'IN':'RGPUDD'},{'IN':'CTSXR'},{'IN':'CTIXJ'}]
num_cores = mp.cpu_count()
Parallel(n_jobs=num_cores)(
        delayed(extract_MMT_Data)(i) for i in info)







