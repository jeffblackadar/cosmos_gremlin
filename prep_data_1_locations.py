import pandas as pd

def clean_string(raw_string):
    if raw_string=="nan":
        raw_string = ""
    raw_string = raw_string.replace("'","&apos;")
    raw_string = raw_string.replace('"',"&quot;")
    raw_string = raw_string.replace('\n',"")
    return(raw_string)


df = pd.read_csv("wwloc.csv", sep=";", header=0)

# open a file to write out the Gremlin statements
f = open("data_load_data_locations.py", "w")

f.write('_gremlin_insert_vertices = [\n')

row_count = 0
for index, row in df.iterrows():
    row_count = row_count + 1
    #print(row['name'], row['name_last'])
    #id must be unique - so prepending org
    per_row = "g.addV('location').property('id', 'loc"+clean_string(str(row['id_entities_location']))+"').property('name', '"+clean_string(row['name'])+"').property('about', '"+clean_string(str(row['about']))+"').property('latitude', '"+clean_string(str(row['latitude']))+"').property('longitude', '"+clean_string(str(row['longitude']))+"').property('partitionKey', 'partitionKey')"
    if row_count < len(df.index):
        print('"'+per_row+'",')
        f.write('"'+per_row+'",\n')
    else:
        print('"'+per_row+'"')
        f.write('"'+per_row+'"\n')
        f.write(']\n')

f.close()