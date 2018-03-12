import os

from unidecode import unidecode
from unicodeMagic import UnicodeReader

'''Load the male and female name lists for <country>'''
def loadData(country, dataPath, hasHeader=False):
    def loadGenderList(gender, country, dataPath, hasHeader):
        fd = open(os.path.join(dataPath, '%s%sUTF8.csv' % (country, gender)), 'rb')
        reader = UnicodeReader(fd)
        names = {}
        if hasHeader:
            unused_header = reader.next()
        '''Load names as-is, but lower cased'''
        for row in reader:
            name = row[0].lower()
            try:
                '''The second column should be the count
                (number of babies in some year with this name)'''
                count = row[1]
            except:
                '''If second column does not exist, default to count=1'''
                count = 1
                if names.has_key(name):
                    '''If here then I've seen this name before, modulo case.
                    Only count once (there is no frequency information anyway)'''
                    count = 0
            if names.has_key(name):
                names[name] += count
            else:
                names[name] = count
        fd.close()
        
        '''Add versions without diacritics'''
        for name in names.keys():
            dname = unidecode(name)
            if not names.has_key(dname):
                names[dname] = names[name]

        return names

    males = loadGenderList('Male', country, dataPath, hasHeader)
    females = loadGenderList('Female', country, dataPath, hasHeader)	
    return males, females


class SimpleGenderComputer:

    def __init__(self, dataPath):

        self.nameLists = {}
        '''Name lists per country'''
        self.listOfCountries = ['Afghanistan', 'Albania', 'Australia', 'Belgium', 'Brazil', 
                        'Canada', 'Czech', 'Finland', 'Greece', 'Hungary', 'India', 'Iran', 
                        'Ireland', 'Israel', 'Italy', 'Latvia', 'Norway', 'Poland', 'Romania', 
                        'Russia', 'Slovenia', 'Somalia', 'Spain', 'Sweden', 'Turkey', 'UK', 
                        'Ukraine', 'USA']
        for country in self.listOfCountries:
            self.nameLists[country] = {}
            self.nameLists[country]['male'], self.nameLists[country]['female'] = loadData(country, dataPath)
 
    def simpleLookup(self, name):
        mCount = 0
        fCount = 0
        Total = 0
        name = name.lower()
        for country in self.listOfCountries:
            if name in self.nameLists[country]['male']:
                mCount += float(self.nameLists[country]['male'][name])
        for country in self.listOfCountries:
            if name in self.nameLists[country]['female']:
                fCount += float(self.nameLists[country]['female'][name])

        if mCount == 0 and fCount == 0:
            return "Unknown"
        if mCount > fCount * 2:
            return "Male"
        if fCount > mCount * 2:
            return "Female"
        return "Unisex"
