import httplib2
import oauth2
import urllib3
import types
import psycopg2, psycopg2.extras
from PIL import Image
from resizeimage import resizeimage
import urllib as urllib

class UpworkPhotoSaver:
    
    def __init__(self):
        self.log_filename = "./log_files/log_upwork_save_photos_unitedstates_allskills_2017_12_12.txt"
        self.photo_folder_name = "images_unitedstates_allskills_2017_12_12"
        
        # Write a log
        self.log = open(self.log_filename, 'a')
        self.log.write("We have started saving photo data!" + "\n")

        # Connect to the database 
        self.conn = psycopg2.connect("dbname=eureka01")
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
        
        # Get detailed_info from workers in our database
        self.cur.execute("SELECT detailed_info FROM upwork_unitedstates_allskills_2017_12_12")
        
        # For naming the worker photos we save  
        self.user_count = 1
        
    def collect_photos(self):
        for user in self.cur:
            try: 
                photo_url = user[0]["dev_portrait"]
                local_filename = "profile_photos/" + self.photo_folder_name + "/" + str(self.user_count) + ".jpg"
                urllib.urlretrieve(photo_url, local_filename)
                print "We have saved the photo! " + str(self.user_count)
                self.log.write("We have saved the photo at {0}".format(self.user_count) + "\n")
                self.log.flush()
                self.user_count += 1
            except Exception as err: 
                print "Problem saving the photo at {0}".format(self.user_count)
                print err 
                self.log.write("Problem saving photo at {0}: {1}".format(self.user_count, err) + "\n")
                self.log.flush()
                self.user_count += 1

myObject = UpworkPhotoSaver()
myObject.collect_photos()

