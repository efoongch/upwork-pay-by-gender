import time 
import csv
import datetime 

# Add another function to calculate work experience from the profile description 

def calculateWorkExperience(work_experience_list):
    present_date = "12/2017"
    count = 0
    total_overlap = 0
    work_experience_array = []
    total_experience = 0
    
    for experience in work_experience_list:
        start_date = experience[0]
        end_date = experience[1]
        if (end_date == "Present"):
            end_date = present_date
        start_datetime = datetime.datetime.strptime(start_date, "%m/%Y")
        end_datetime = datetime.datetime.strptime(end_date, "%m/%Y")

        if (start_datetime > end_datetime): #If the dates are somehow flipped
            print "We have an odd job entry"
            return "error"
        else: 
            work_experience_array.append([start_datetime, end_datetime])
            experience_duration = (end_datetime - start_datetime).days
            print "Experience duration in days: {0}".format(experience_duration)
            print "Experience duration in months: {0}".format(experience_duration/30)
            total_experience += experience_duration
            print "Final experience duration in months: {0}".format(experience_duration/30)

        print "Work experience array: {0}".format(work_experience_array)
        print "Work experience array length: {0}".format(len(work_experience_array))
        print "Total experience before overlap: {0}".format(total_experience)

    while (count < len(work_experience_array) - 1): # Check for overlap in work experience 
        # While current index + offset < len
        offset = 1

        while (offset + count < len(work_experience_array)): 

            # Check which of the two end dates are the newest and assign appropriate values
            end_date1 = work_experience_array[count][1]
            end_date2 = work_experience_array[count + offset][1]
            
            if (end_date2 <= end_date1):
                start_newer = work_experience_array[count][0]
                end_newer = work_experience_array[count][1]
                start_older = work_experience_array[count + offset][0]
                end_older = work_experience_array[count + offset][1] 
            
            elif (end_date2 > end_date1):
                start_newer = work_experience_array[count + offset][0]
                end_newer = work_experience_array[count + offset][1]
                start_older = work_experience_array[count][0]
                end_older = work_experience_array[count][1]
            
            print "Start newer: {0}".format(start_newer)
            print "End newer: {0}".format(end_newer)
            print "Start older: {0}".format(start_older)
            print "End older: {0}".format(end_older)
            
            # Check for patterns as usual 

            if (end_newer == end_older): # Pattern 1
                newer_duration = (end_newer - start_newer).days
                older_duration = (end_older - start_older).days
                overlap = min(newer_duration, older_duration)
                total_overlap += overlap 
                offset += 1
                print "Pattern 1"
                print overlap/30

            elif(start_older == start_newer): # Pattern 2
                overlap = (end_older - start_older).days
                total_overlap += overlap 
                offset += 1
                print "Pattern 2"
                print overlap/30

            elif(start_newer < start_older and end_newer > end_older): # Pattern 3
                overlap = (end_older - start_older).days
                total_overlap += overlap 
                offset += 1
                print "Pattern 3"
                print overlap/30

            elif(start_newer > start_older and end_newer > end_older and end_older > start_newer): # Pattern 4
                overlap = (end_older - start_newer).days
                total_overlap += overlap 
                offset += 1
                print "Pattern 4"
                print overlap/30 

            else:
                print "No overlap"
                offset += 1

        count += 1

    print "Total experience before overlap in months: {0}".format(total_experience/30)
    print "Total overlap in months: {0}".format(total_overlap/30)
    total_experience = (total_experience - total_overlap)/30
    print "Total experience after overlap in months: {0}".format(total_experience)
    total_experience = total_experience/12 # Convert to years
    print "Total experience after overlap in years: {0}".format(total_experience)

    #return total_experience

calculateWorkExperience([["12/2016", "04/2017"], ["11/2017", "Present"], ["01/2017", "03/2017"]])
calculateWorkExperience([["01/1999", "11/2017"], ["01/2014", "11/2017"], ["01/2011", "01/2013"], ["01/2003", "01/2011"], ["01/2000", "01/2003"], ["01/1998", "01/1999"], ["01/1995", "01/1998"]])

#calculateWorkExperience([["01/1999", "11/2017"], ["01/2014", "11/2017"]])
#calculateWorkExperience([["01/1999", "11/2017"], ["01/2003", "01/2011"]])

