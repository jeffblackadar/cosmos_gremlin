import pandas as pd

#_gremlin_insert_edges = [
#    "g.V('0').addE('knows').to(g.V('2'))",
#    "g.V('thomas').addE('knows').to(g.V('ben'))",
#    "g.V('ben').addE('knows').to(g.V('robin'))"
#]

def clean_string(raw_string):
    raw_string = str(raw_string)
    if raw_string=="nan":
        raw_string = ""
    raw_string = raw_string.replace("'","&apos;")
    raw_string = raw_string.replace('"',"&quot;")
    raw_string = raw_string.replace('\n',"")
    return(raw_string)

df = pd.read_csv("wwloc_x_page.csv", sep=";", header=0)

# open a file to write out the Gremlin statements
f = open("data_load_data_loc_x_page.py", "w")

f.write('_gremlin_insert_edges = [\n')


row_count = 0
for index, row in df.iterrows():
    row_count = row_count + 1
    #print(row['name'], row['name_last'])
    #id must be unique - so prepending per
    x_row = "g.V('loc"+clean_string(str(row['id_entities_location']))+"').addE('appears').to(g.V('page"+clean_string(str(row['id_source_document']))+"'))"
    
    if row_count < len(df.index):
        print('"'+x_row+'",')
        f.write('"'+x_row+'",\n')
    else:
        print('"'+x_row+'"')
        f.write('"'+x_row+'"\n')
        f.write(']\n')

f.close()