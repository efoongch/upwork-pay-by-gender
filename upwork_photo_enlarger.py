import httplib2
import oauth2
import urllib3
import types
import psycopg2, psycopg2.extras
from PIL import Image
from resizeimage import resizeimage
import urllib as urllib
import cv2
import numpy as np

class UpworkPhotoEnlarger:
    
    def __init__(self):
        self.log_filename = "./log_files/log_upwork_enlarge_photos_unitedstates_allskills_2017_12_12.txt"
        self.input_photo_folder_name = "images_unitedstates_allskills_2017_12_12"
        self.output_photo_folder_name = "resized_images_unitedstates_allskills_2017_12_12"
        
        # Write a log
        self.log = open(self.log_filename, 'a')
        self.log.write("We have started enlarging photo data!" + "\n")

        # Connect to the database 
        self.conn = psycopg2.connect("dbname=eureka01")
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
        
        # Get detailed_info from workers in our database
        self.cur.execute("SELECT detailed_info FROM upwork_unitedstates_allskills_2017_12_12")
        
        # For naming the worker photos we save  
        self.user_count = 1
        
    def enlarge_photos(self):
        for user in self.cur:
            try: 
                local_filename = "profile_photos/" + self.input_photo_folder_name + "/" + str(self.user_count) + ".jpg"
                image = Image.open(local_filename)
                image_data = np.asarray(image)
                output_filename = "resized_profile_photos/" + self.output_photo_folder_name + "/" + str(self.user_count) + ".jpg"
                resized = cv2.resize(image_data, None, fx=2, fy=2, interpolation=cv2.INTER_LINEAR)
                resized = cv2.cvtColor(resized, cv2.COLOR_BGR2RGB) # Changes color back to RGB
                cv2.imwrite(output_filename, resized)
                
                print "We have enlarged the photo! " + str(self.user_count)
                self.log.write("We have enlarged the photo at {0}".format(self.user_count) + "\n")
                self.log.flush()
                self.user_count += 1
                
            except Exception as err: 
                print "Problem saving the photo at {0}".format(self.user_count)
                print err 
                self.log.write("Problem saving photo at {0}: {1}".format(self.user_count, err) + "\n")
                self.log.flush()
                self.user_count += 1

myObject = UpworkPhotoEnlarger()
myObject.enlarge_photos()

