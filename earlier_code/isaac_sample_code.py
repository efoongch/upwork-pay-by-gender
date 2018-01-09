"""slistener.py: twitter stream listeners for various output formats"""
import json, time, sys
import psycopg2, psycopg2.extras
from tweepy import StreamListener, API
__author__ = 'Isaac Johnson'
__credits__ = 'http://badhessian.org/2012/10/collecting-real-time-twitter-data-with-the-streaming-api/'
__email__ = 'joh12041@cs.umn.edu'
class SListenerJSONToPSQL(StreamListener):
    def __init__(self, api = None):
        self.api = api or API()
        self.counter = 0
        self.geolocated = 0
        self.log = open('log_twitter_streaming_7_6_15.txt', 'a')
        self.log.write("Starting stream for bb = [-180, -90, 180, 90]\
          at " + str(time.localtime()) + "\n")
        #self.conn = psycopg2.connect("dbname=twidber user=twidber password=twidber host=localhost")
        self.conn = psycopg2.connect("dbname=twitterstream_zh_us")
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.commiton = 20000
        psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
    def on_data(self, data):
        if  'in_reply_to_status' in data:
            self.on_status(data)
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False
        elif 'warning' in data:
            warning = json.loads(data)['warnings']
            print warning['message']
            return False
    def on_status(self, status):
        tweet = json.loads(status)
        try:
            created_at = time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')
            id = tweet['id']
            uid = tweet['user']['id']
            if tweet['place']:
                bb = tweet['place']['bounding_box']
            else:
                bb = None
            if tweet['coordinates']:
                lon = tweet['coordinates']['coordinates'][0]
                lat = tweet['coordinates']['coordinates'][1]
                geo = True
            else:
                lon = None
                lat = None
                geo = False
            if geo:
                self.geolocated += 1
                self.cur.execute("INSERT INTO tweet_as_json_2015_05_27 (id, created_at, uid, bb, geo, lat, lon, tweet) VALUES (%s,%s,%s,%s,%s,%s,%s, %s);",
                             [id, time.strftime("%Y-%m-%d %H:%M:%S",created_at), uid, bb, geo, lat, lon,
                              psycopg2.extras.Json(tweet)])
        except:
            self.cur.execute("INSERT INTO tweet_as_json_2015_05_27 (tweet) VALUES (%s);", [psycopg2.extras.Json(tweet)])
            self.log.write("Failed to parse tweet: " + str(tweet) + "\n")
            self.log.flush()
            print "Failed to parse tweet: " + str(tweet) + "\n"
        self.counter += 1
        if self.counter >= self.commiton:
            self.conn.commit()
            self.log.write("Committed " + str(self.geolocated) + " of " + str(self.counter) + ": " + time.strftime('%Y%m%d-%H%M%S') + "\n")
            self.log.flush()
            print "Committed " + str(self.geolocated) + " of " + str(self.counter) + ": " + time.strftime('%Y%m%d-%H%M%S') + "\n"
            self.counter = 0
            self.geolocated = 0
        return
    def on_delete(self, status_id, user_id):
        self.log.write("Delete: " + str(status_id) + "\n")
        print "Delete: " + str(status_id) + "\n"
        return
    def on_limit(self, track):
        self.log.write("Limit: " + str(track) + "\n")
        print "Limit: " + str(track) + "\n"
        return
    def on_error(self, status_code):
        self.log.write('Error: ' + str(status_code) + "\n")
        print 'Error: ' + str(status_code) + "\n"
        return False
    def on_timeout(self):
        self.log.write("Timeout, sleeping for 60 seconds...\n")
        print "Timeout, sleeping for 60 seconds...\n"
        time.sleep(60)
        return