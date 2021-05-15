# dictionary with the schools evaluated in this project

schools = {   
'ironhack' : 10828,
'app-academy' : 10525,
'springboard' : 11035,
'le-wagon' : 10868,
}

# importing libraries
import re
import pandas as pd
from pandas.io.json import json_normalize
import requests

# function to get the comments information
def get_comments_school(school):
    TAG_RE = re.compile(r'<[^>]+>')
    # defines url to make api call to data -> dynamic with school if you want to scrape competition
    url = "https://www.switchup.org/chimera/v1/school-review-list?mainTemplate=school-review-list&path=%2Fbootcamps%2F" + school + "&isDataTarget=false&page=3&perPage=10000&simpleHtml=true&truncationLength=250"
    #makes get request and converts answer to json
    # url defines the page of all the information, request is made, and information is returned to data variable
    data = requests.get(url).json()
    #converts json to dataframe
    reviews =  pd.DataFrame(data['content']['reviews'])
  
    #aux function to apply regex and remove tags
    def remove_tags(x):
        return TAG_RE.sub('',x)
    reviews['review_body'] = reviews['body'].apply(remove_tags)
    reviews['school'] = school
    return reviews



# Create a df with all the comments information
comments = []

for school in schools.keys():
    print(school)
    comments.append(get_comments_school(school))

comments = pd.concat(comments)

#comments

comments['school_id'] = [schools[x] for x in comments['school']]

comments.rename(columns={'id': 'comment_id'}, inplace=True)

comments



#importing more libraries

# function to get the schools information
def get_school_info(school, school_id):
    url = 'https://www.switchup.org/chimera/v1/bootcamp-data?mainTemplate=bootcamp-data%2Fdescription&path=%2Fbootcamps%2F'+ str(school) + '&isDataTarget=false&bootcampId='+ str(school_id) + '&logoTag=logo&truncationLength=250&readMoreOmission=...&readMoreText=Read%20More&readLessText=Read%20Less'

    data = requests.get(url).json()

    data.keys()

    courses = data['content']['courses']
    courses_df = pd.DataFrame(courses, columns= ['courses'])

    locations = data['content']['locations']
    locations_df = json_normalize(locations)

    badges_df = pd.DataFrame(data['content']['meritBadges'])
    
    website = data['content']['webaddr']
    description = data['content']['description']
    logoUrl = data['content']['logoUrl']
    school_df = pd.DataFrame([website,description,logoUrl]).T
    school_df.columns =  ['website','description','LogoUrl']

    locations_df['school'] = school
    courses_df['school'] = school
    badges_df['school'] = school
    school_df['school'] = school
    

    locations_df['school_id'] = school_id
    courses_df['school_id'] = school_id
    badges_df['school_id'] = school_id
    school_df['school_id'] = school_id

    return locations_df, courses_df, badges_df, school_df

locations_list = []
courses_list = []
badges_list = []
schools_list = []

for school, id in schools.items():
    print(school)
    a,b,c,d = get_school_info(school,id)
    
    locations_list.append(a)
    courses_list.append(b)
    badges_list.append(c)
    schools_list.append(d)
    
    
#give location list    
locations_list


#locations
locations = pd.concat(locations_list)
locations.rename(columns={'id': 'location_id','country.id': 'country_id','country.name':'country_name',
                          'country.abbrev':'country_abbrev','city.id':'city_id','city.name':'city_name',
                          'city.keyword':'city_keyword','state.id':'state_id','state.name':'state_name',
                          'state.abbrev':'state_abbrev','state.keyword':'state_keyword'}, inplace=True)

#courses
courses = pd.concat(courses_list)
courses['courses_id'] = range(1, len(courses) + 1)
courses


#badges
badges = pd.concat(badges_list)
badges['schoolbadges_id'] = range(1, len(badges) + 1)
badges


schools = pd.concat(schools_list)
schools.head()



#establish connection
import mysql.connector
cnx = mysql.connector.connect(user = "root", password = input('password:'),host="localhost")
cnx.is_connected()
cursor = cnx.cursor()



# Create database
query = ("""CREATE DATABASE competitive_landscape;""")
cursor.execute(query)


from sqlalchemy import create_engine


#Create a engine
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw=input('password:'),
                               db="competitive_landscape"))

# drop comments because there is a problem in the type of objects that are passed to Mysql
comments = comments.drop('comments', axis = 1)
# the same for the comments
comments = comments.drop('user', axis = 1)


#Create comments table in mysql with all data
comments.to_sql('comments', con = engine, if_exists = 'append', chunksize = 1000)


#Add Primary key in comments table
query = ("""ALTER TABLE competitive_landscape.comments ADD PRIMARY KEY(comment_id);""")
cursor.execute(query)

#Create courses table in mysql with all data
courses.to_sql('courses', con = engine, if_exists = 'append', chunksize = 1000)

#Add Primary key in courses table
query = ("""ALTER TABLE competitive_landscape.courses ADD PRIMARY KEY(courses_id);""")
cursor.execute(query)

#Create locations table in mysql with all data
locations.to_sql('locations', con = engine, if_exists = 'append', chunksize = 1000)

#Add Primary key in locations table
query = ("""ALTER TABLE competitive_landscape.locations ADD PRIMARY KEY(location_id);""")
cursor.execute(query)

#Create badges table in mysql with all data
badges.to_sql('badges', con = engine, if_exists = 'append', chunksize = 1000)

#Add Primary key in badges table
query = ("""ALTER TABLE competitive_landscape.badges ADD PRIMARY KEY(schoolbadges_id);""")
cursor.execute(query)

#Create schools table in mysql with all data
schools.to_sql('schools', con = engine, if_exists = 'append', chunksize = 1000)

#Add Primary key in schools table
query = ("""ALTER TABLE competitive_landscape.schools ADD PRIMARY KEY(school_id);""")
cursor.execute(query)


#CREATE THE FOREIGN KEYS
query = ("""ALTER TABLE competitive_landscape.courses ADD FOREIGN KEY (school_id) REFERENCES competitive_landscape.schools (school_id);""")
cursor.execute(query)

query = ("""ALTER TABLE competitive_landscape.badges ADD FOREIGN KEY (school_id) REFERENCES competitive_landscape.schools (school_id);""")
cursor.execute(query)

query = ("""ALTER TABLE competitive_landscape.locations ADD FOREIGN KEY (school_id) REFERENCES competitive_landscape.schools (school_id);""")
cursor.execute(query)

query = ("""ALTER TABLE competitive_landscape.comments ADD FOREIGN KEY (school_id) REFERENCES competitive_landscape.schools (school_id);""")
cursor.execute(query)

#CREATING ADDITIONAL TABLE WITH INFORMATION REGARDING EACH COUNTRY
country_data = pd.read_csv('country_data.csv')
country_data

# columns without relevant information
country_data = country_data.drop('Country Code', axis = 1)
country_data = country_data.drop('Series Code', axis = 1)
country_data

#lets keep only rows with information that will be evaluated
condition = country_data[((country_data['Series Name'] != 'Population, total') &
                          (country_data['Series Name'] != 'Population growth (annual %)') & 
                          (country_data['Series Name'] != 'Primary completion rate, total (% of relevant age group)') &
                          (country_data['Series Name'] != 'School enrollment, primary (% gross)') & 
                          (country_data['Series Name'] != 'School enrollment, secondary (% gross)') &
                          (country_data['Series Name'] != 'School enrollment, primary and secondary (gross), gender parity index (GPI)') & 
                          (country_data['Series Name'] != 'Mobile cellular subscriptions (per 100 people)') &
                          (country_data['Series Name'] != 'Individuals using the Internet (% of population)') & 
                          (country_data['Series Name'] != 'High-technology exports (% of manufactured exports)'))]
condition

country_data_final = country_data.drop(condition.index, axis=0) # Step 2

country_data_final


#Lets rename de columns (mysql friendly)
country_data_final.rename(columns={'Country Name': 'country_name','Series Name': 'indicator',
                             'Scale (Precision)': 'unit','1990 [YR1990]': 'year_1990',
                             '2000 [YR2000]': 'year_2000','2010 [YR2010]': 'year_2010',
                             '2018 [YR2018]': 'year_2018'}, inplace=True)
country_data_final

#Add a column with a unique value
country_data_final['count_indicator_id'] = range(1, len(country_data_final) + 1)
country_data_final


#Create country_data table in mysql with all data
country_data_final.to_sql('country_data', con = engine, if_exists = 'append', chunksize = 1000)

#Add Primary key in country_data table
query = ("""ALTER TABLE competitive_landscape.country_data ADD PRIMARY KEY(count_indicator_id);""")
cursor.execute(query)