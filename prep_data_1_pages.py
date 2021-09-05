import pandas as pd
import re
p = re.compile('\A[0-9]+;\"')

def clean_string(raw_string):
    raw_string = str(raw_string)
    if raw_string=="nan":
        raw_string = ""
    raw_string = raw_string.replace("'","&apos;")
    raw_string = raw_string.replace('"',"&quot;")
    raw_string = raw_string.replace('\n',"")
    return(raw_string)


def clean_string_lf(raw_string):
    raw_string = str(raw_string)
    raw_string = raw_string.replace('\n'," ")
    return(raw_string.strip())

writefile = open("wwpages_clean.csv", "w")
with open("wwpages.csv") as readfile:
    for line in readfile:
        #only put a lf back for lines that start with an id number
        clean_line=clean_string_lf(line)
        if(p.match(clean_line)):
            writefile.write("\n"+clean_line)
        else:
            writefile.write(clean_line)

writefile.close()

df = pd.read_csv("wwpages_clean.csv", sep=";", header=0)

# open a file to write out the Gremlin statements
f = open("data_load_data_pages.py", "w")

f.write('_gremlin_insert_vertices = [\n')


row_count = 0
for index, row in df.iterrows():
    row_count = row_count + 1
    #print(row['name'], row['name_last'])
    #id must be unique - so prepending per

    pag_row = "g.addV('page').property('id', 'page"+clean_string(str(row['id']))+"').property('name', '"+clean_string(str(row['date']))+"').property('image', '"+clean_string(row['image'])+"').property('year', '"+clean_string(row['year'])+"').property('month_name', '"+clean_string(str(row['month_name']))+"').property('day_name', '"+clean_string(str(row['day_name']))+"').property('year_day', '"+clean_string(str(row['year_day']))+"').property('date', '"+clean_string(str(row['date']))+"').property('page_text', '"+clean_string(str(row['page_text']))+"').property('partitionKey', 'partitionKey')"




    if row_count < len(df.index):
        print('"'+pag_row+'",')
        f.write('"'+pag_row+'",\n')
    else:
        print('"'+pag_row+'"')
        f.write('"'+pag_row+'"\n')
        f.write(']\n')

f.close()