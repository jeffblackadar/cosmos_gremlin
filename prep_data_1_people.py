import pandas as pd

def clean_string(raw_string):
    raw_string = str(raw_string)
    if raw_string=="nan":
        raw_string = ""
    raw_string = raw_string.replace("'","&apos;")
    raw_string = raw_string.replace('"',"&quot;")
    raw_string = raw_string.replace('\n',"")
    return(raw_string)

df = pd.read_csv("wwper.csv", sep=";", header=0)

# open a file to write out the Gremlin statements
f = open("data_load_data_people.py", "w")

f.write('_gremlin_insert_vertices = [\n')
# First line to include William White who is not in the database
per_row = "g.addV('person').property('id', 'per0').property('name', 'Capt. William White').property('name_last', 'White').property('about', 'No. 2 Construction chaplin').property('partitionKey', 'partitionKey')"
f.write('"'+per_row+'",\n')

row_count = 0
for index, row in df.iterrows():
    row_count = row_count + 1
    #print(row['name'], row['name_last'])
    #id must be unique - so prepending per

    per_row = "g.addV('person').property('id', 'per"+clean_string(str(row['id_entities_person']))+"').property('name', '"+clean_string(row['name'])+"').property('name_last', '"+clean_string(row['name_last'])+"').property('about', '"+clean_string(str(row['about']))+"').property('partitionKey', 'partitionKey')"
    if row_count < len(df.index):
        print('"'+per_row+'",')
        f.write('"'+per_row+'",\n')
    else:
        print('"'+per_row+'"')
        f.write('"'+per_row+'"\n')
        f.write(']\n')

f.close()