# Code is from: https://github.com/Azure-Samples/azure-cosmos-db-graph-python-getting-started
# Thank you to the contributors
from gremlin_python.driver import client, serializer, protocol
from gremlin_python.driver.protocol import GremlinServerError
import sys
import traceback
import time


_gremlin_cleanup_graph = "g.V().drop()"

_gremlin_insert_vertices = [
"g.addV('person').property('id', '0').property('name', 'Capt. William White').property('name_last', 'White').property('about', 'No. 2 Construction chaplin').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '1').property('name', 'Mlles Thomas').property('name_last', 'Thomas').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '2').property('name', 'Mlles Dole').property('name_last', 'Dole').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '3').property('name', 'Mrs. White').property('name_last', 'White').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '4').property('name', 'Mrs. Fred Borden').property('name_last', 'Borden').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '5').property('name', 'Miss Jackson').property('name_last', 'Jackson').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '6').property('name', 'Mrs. Frank Paris').property('name_last', 'Paris').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '8').property('name', 'Col. Johnson').property('name_last', 'Johnson').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '9').property('name', 'Bro. Milton').property('name_last', 'Milton').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '10').property('name', 'Mme Levy').property('name_last', 'Levy').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '11').property('name', 'Pte Roach').property('name_last', 'Roach').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '12').property('name', 'Sister Campbell').property('name_last', 'Campbell').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '13').property('name', 'Syd Jones').property('name_last', 'Jones').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '15').property('name', 'Col. White').property('name_last', 'White').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '17').property('name', 'Joanne Dubied').property('name_last', 'Dubied').property('about', 'Joanne Dubied lives in Switzerland.').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '18').property('name', 'Miss Walsh').property('name_last', 'Walsh').property('about', 'Miss Walsh lives in Switzerland.').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '19').property('name', 'Frank Stanfield').property('name_last', 'Stanfield').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '21').property('name', 'Capt. Murray').property('name_last', 'Murray').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '22').property('name', 'Pte. Brent').property('name_last', 'Brent').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '23').property('name', 'Fred Dixon').property('name_last', 'Dixon').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '24').property('name', 'Mary Clyke').property('name_last', 'Clyke').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '25').property('name', 'Mr. David-Mauvas').property('name_last', 'David-Mauvas').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '26').property('name', 'Miss Vera Ribbins').property('name_last', 'Ribbins').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '27').property('name', 'Lord Lovett').property('name_last', 'Lovett').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '28').property('name', 'Mrs. Bryant').property('name_last', 'Bryant').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '30').property('name', 'Sadie Lopez').property('name_last', 'Lopez').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '31').property('name', 'Rev. A. W. Thompson').property('name_last', 'Thompson').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '32').property('name', 'Mrs. Alice White').property('name_last', 'White').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '33').property('name', 'Mrs. Henry Paris').property('name_last', 'Paris').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '34').property('name', 'Rev Thomas').property('name_last', 'Thomas').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '37').property('name', 'Ethel Williams').property('name_last', 'Williams').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '38').property('name', 'Lieut. Evans').property('name_last', 'Evans').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '39').property('name', 'Mrs. Talbot').property('name_last', 'Talbot').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '40').property('name', 'Sister Sarah.').property('name_last', 'Sarah').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '41').property('name', 'Hayes').property('name_last', 'Hayes').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '42').property('name', 'Leon Grybon').property('name_last', 'Grybon').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '43').property('name', 'Mr. Hayes').property('name_last', 'Hayes').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '46').property('name', 'Mlle Rochaix').property('name_last', 'Rochaix').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '48').property('name', 'Gabrielle').property('name_last', 'Gabrielle').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '49').property('name', 'Marie').property('name_last', 'Marie').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '51').property('name', 'Mme Mayer').property('name_last', 'Mayer').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '52').property('name', 'Mlle Videlier').property('name_last', 'Vidlelier').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '56').property('name', 'Pte Roachford').property('name_last', 'Roachford').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '57').property('name', 'Bradshaw').property('name_last', 'Bradshow').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '60').property('name', 'Pte. Boone').property('name_last', 'Boone').property('about', 'Pte Boone was sick and died in hospital Dec. 1, 1917.').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '62').property('name', 'Sarah').property('name_last', 'Sarah').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '63').property('name', 'J.H. Desmond').property('name_last', 'Desmond').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '64').property('name', 'Mrs. Anderson').property('name_last', 'Anderson').property('about', 'She lives in gray France and knows Miss Wilford.').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '67').property('name', 'Col. McGreer').property('name_last', 'McGreer').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '68').property('name', 'Rev. Hartley').property('name_last', 'Hartley').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '69').property('name', 'Charley').property('name_last', 'Charley').property('about', 'Lives near Mrs. Henry Paris.').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '70').property('name', 'Capt. Anderson').property('name_last', 'Anderson').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '71').property('name', 'Henri Guyon').property('name_last', 'Guyon').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '73').property('name', 'Mrs. McDougal').property('name_last', 'McDougal').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '74').property('name', 'Mrs. Wilson').property('name_last', 'Wilson').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '75').property('name', 'Misses McCullough').property('name_last', 'McCullough').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '77').property('name', 'Mrs. Oscar Clyke').property('name_last', 'Clyke').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '80').property('name', 'Capt. Livingston').property('name_last', 'Livingston').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '81').property('name', 'Pte. Miller').property('name_last', 'Miller').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '83').property('name', 'Mrs. Izie White').property('name_last', 'White').property('about', 'Mrs. Izie White is married to Capt. William White.').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '84').property('name', 'Mme Parrod').property('name_last', 'Parrod').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '85').property('name', 'Prof. Lapalus').property('name_last', 'Lapalus').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '86').property('name', 'Mme De Geger').property('name_last', 'De Geger').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '87').property('name', 'Mlle Jennie Leben').property('name_last', 'Leben').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '88').property('name', 'Dr. King').property('name_last', 'King').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '89').property('name', 'Rev. Logan').property('name_last', 'Logan').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '90').property('name', 'Mlle Wilford').property('name_last', 'Wilford').property('about', 'She lives in Gray, France and knows Mrs. Anderson.').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '91').property('name', 'Bessie Paris').property('name_last', 'Paris').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '92').property('name', 'Lieut. Stewart').property('name_last', 'Stewart').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '93').property('name', 'Lieut. Colter').property('name_last', 'Colter').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '94').property('name', 'Mme Michault').property('name_last', 'Michault').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '95').property('name', 'Romney').property('name_last', 'Romney').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '96').property('name', 'Marie Gideon').property('name_last', 'Gideon').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '97').property('name', 'Hayel').property('name_last', 'Hayel').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '99').property('name', 'Sgt. Stoute').property('name_last', 'Stoute').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '100').property('name', 'Miss Ford').property('name_last', 'Ford').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '101').property('name', 'Helena White').property('name_last', 'White').property('about', 'Helena is William and Izie White's daughter.').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '102').property('name', 'Portia White').property('name_last', 'White').property('about', 'Portia White is the daughter of William White.').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '103').property('name', 'Major Sutherland').property('name_last', 'Sutherland').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '104').property('name', 'Miss Bontoft').property('name_last', 'Bontoft').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '105').property('name', 'Senior Chaplain').property('name_last', 'Chaplain').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '106').property('name', 'Major MacDonald').property('name_last', 'MacDonald').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '107').property('name', 'Jen').property('name_last', 'Jen').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '108').property('name', 'Pte. Johnson').property('name_last', 'Johnson').property('about', 'He was batman for Capt. White.').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '109').property('name', 'Mrs. Mentis').property('name_last', 'Mentis').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '110').property('name', 'Rev. States').property('name_last', 'States').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '111').property('name', 'Capt. Morrison').property('name_last', 'Morrison').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '112').property('name', 'Capt. Stark').property('name_last', 'Stark').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '113').property('name', 'Lieut. Purdy').property('name_last', 'Purdy').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '114').property('name', 'E. Boudot').property('name_last', 'Boudot').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '115').property('name', 'Madelein').property('name_last', 'Madelein').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '116').property('name', 'Ambassador of Brazil').property('name_last', 'Ambassador of Brazil').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '118').property('name', 'Sgt. Sealy').property('name_last', 'Sealy').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '120').property('name', 'Mlle Grandin').property('name_last', 'Grandin').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '121').property('name', 'Mlle Lapalus').property('name_last', 'Lapalus').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '122').property('name', 'Mrs. Fred Dixon').property('name_last', 'Dixon').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '123').property('name', 'Raymonde').property('name_last', 'Raymonde').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '124').property('name', 'Mrs. Hattie Borden').property('name_last', 'Borden').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '125').property('name', 'Mrs. Carty').property('name_last', 'Carty').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '126').property('name', 'Lieut. Hood').property('name_last', 'Hood').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '127').property('name', 'Mme Menfoo').property('name_last', 'Menfoo').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '129').property('name', 'Major Legere').property('name_last', 'Legere').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '130').property('name', 'Mlle De Byars').property('name_last', 'De Byars').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '131').property('name', 'Mme Chauvin').property('name_last', 'Chauvin').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '132').property('name', 'Lieut. McLean').property('name_last', 'McLean').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '133').property('name', 'Mr. Thrope').property('name_last', 'Thrope').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '134').property('name', 'Mlle Deniset').property('name_last', 'Deniset').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '135').property('name', 'Mme Lea Chapuzot').property('name_last', 'Chapuzot').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '136').property('name', 'Nettie White').property('name_last', 'White').property('about', 'Nettie mentioned in the diary is likely William White's daughter.').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '137').property('name', 'Billie (William) White').property('name_last', 'White').property('about', 'Billie mentioned in the diary is likely William White's son.').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '138').property('name', 'Mrs. Binga').property('name_last', 'Binga').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '139').property('name', 'Commandant, French equipment depot, Gray.').property('name_last', 'Commandant').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '140').property('name', 'Pte. Rochaix').property('name_last', 'Rochaix').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '141').property('name', 'Cpl. Binga').property('name_last', 'Binga').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '143').property('name', 'Capt. Pocunier').property('name_last', 'Pocunier').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '144').property('name', 'Baronne du Bourg').property('name_last', 'Bourg').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '145').property('name', 'Rev. Pusyear').property('name_last', 'Pusyear').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '146').property('name', 'F. B. McCurdy').property('name_last', 'McCurdy').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '147').property('name', 'Nursing Sisters').property('name_last', 'Nursing sisters').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '148').property('name', 'Agnes').property('name_last', 'Agnes').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '149').property('name', 'Major Strong').property('name_last', 'Strong').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '150').property('name', 'Col. Wilson').property('name_last', 'Wilson').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '151').property('name', 'Capt. McDougal').property('name_last', 'McDougal').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '153').property('name', 'Flight Lieut. P. Tostain').property('name_last', 'Tostain').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '154').property('name', 'Dr. Ortion').property('name_last', 'Ortion').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '155').property('name', 'Mr. Turner').property('name_last', 'Turner').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '156').property('name', 'Mrs. Sutherland').property('name_last', 'Sutherland').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '157').property('name', 'Mrs. Straith').property('name_last', 'Straith').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '158').property('name', 'Major Long').property('name_last', 'Long').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '159').property('name', 'Capt. Logan').property('name_last', 'Logan').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '160').property('name', 'Capt. Gordon').property('name_last', 'Gordon').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '161').property('name', 'Capt. Stubbs').property('name_last', 'Stubbs').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '162').property('name', 'Lieut. Breckon').property('name_last', 'Breckon').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '163').property('name', 'Lieut. Lockman').property('name_last', 'Lockman').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '164').property('name', 'Capt. Grant').property('name_last', 'Grant').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '165').property('name', 'Mme Thomas').property('name_last', 'Thomas').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '166').property('name', 'Major Merrett').property('name_last', 'Merrett').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '167').property('name', 'Mme Monin').property('name_last', 'Monin').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '168').property('name', 'Capt. Latimore').property('name_last', 'Latimore').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '169').property('name', 'Major Carew').property('name_last', 'Carew').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '170').property('name', 'Mme Planche').property('name_last', 'Planche').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '171').property('name', 'Miss Blackadar').property('name_last', 'Blackadar').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '172').property('name', 'Miss McCully').property('name_last', 'McCully').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '173').property('name', 'Miss McDorman').property('name_last', 'McDorman').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '174').property('name', 'Sgt.Q.M. Peacock').property('name_last', 'Peacock').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '175').property('name', 'Mrs. Tucker').property('name_last', 'Tucker').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '176').property('name', 'Mrs. Parsons').property('name_last', 'Parsons').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '177').property('name', 'Emily').property('name_last', 'Emily').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '178').property('name', 'Capt. Bourassa').property('name_last', 'Bourassa').property('about', 'nan').property('partitionKey', 'partitionKey')",
"g.addV('person').property('id', '179').property('name', 'Mme Johnson').property('name_last', 'Johnson').property('about', 'nan').property('partitionKey', 'partitionKey')"

]


_gremlin_insert_edges = [
    "g.V('0').addE('knows').to(g.V('2'))",
    "g.V('thomas').addE('knows').to(g.V('ben'))",
    "g.V('ben').addE('knows').to(g.V('robin'))"
]

_gremlin_update_vertices = [
    
]

_gremlin_count_vertices = "g.V().count()"

_gremlin_traversals = {
    "Get all persons older than 40": "g.V().hasLabel('person').has('age', gt(40)).values('firstName', 'age')",
    "Get all persons and their first name": "g.V().hasLabel('person').values('firstName')",
    "Get all persons sorted by first name": "g.V().hasLabel('person').order().by('firstName', incr).values('firstName')",
    "Get all persons that Thomas knows": "g.V('thomas').out('knows').hasLabel('person').values('firstName')",
    "People known by those who Thomas knows": "g.V('thomas').out('knows').hasLabel('person').out('knows').hasLabel('person').values('firstName')",
    "Get the path from Thomas to Robin": "g.V('thomas').repeat(out()).until(has('id', 'robin')).path().by('firstName')"
}

_gremlin_drop_operations = {
    "Drop Edge - Thomas no longer knows Mary": "g.V('thomas').outE('knows').where(inV().has('id', 'mary')).drop()",
    "Drop Vertex - Drop Thomas": "g.V('thomas').drop()"
}

def print_status_attributes(result):
    # This logs the status attributes returned for successful requests.
    # See list of available response status attributes (headers) that Gremlin API can return:
    #     https://docs.microsoft.com/en-us/azure/cosmos-db/gremlin-headers#headers
    #
    # These responses includes total request units charged and total server latency time.
    # 
    # IMPORTANT: Make sure to consume ALL results returend by cliient tothe final status attributes
    # for a request. Gremlin result are stream as a sequence of partial response messages
    # where the last response contents the complete status attributes set.
    #
    # This can be 
    print("\tResponse status_attributes:\n\t{0}".format(result.status_attributes))

def cleanup_graph(client):
    print("\n> {0}".format(
        _gremlin_cleanup_graph))
    callback = client.submitAsync(_gremlin_cleanup_graph)
    if callback.result() is not None:
        callback.result().all().result() 
    print("\n")
    print_status_attributes(callback.result())
    print("\n")


def insert_vertices(client):
    for query in _gremlin_insert_vertices:
        print("\n> {0}\n".format(query))
        # don't go too fast. Wait. Avoid "Request rate is large. More Request Units may be needed, so no changes were made. Please retry this request later. Learn more: http://aka.ms/cosmosdb-error-429"
        time.sleep(1)
        callback = client.submitAsync(query)
        if callback.result() is not None:
            print("\tInserted this vertex:\n\t{0}".format(
                callback.result().all().result()))
        else:
            print("Something went wrong with this query: {0}".format(query))
        print("\n")
        print_status_attributes(callback.result())
        print("\n")

    print("\n")


def insert_edges(client):
    for query in _gremlin_insert_edges:
        print("\n> {0}\n".format(query))
        callback = client.submitAsync(query)
        if callback.result() is not None:
            print("\tInserted this edge:\n\t{0}\n".format(
                callback.result().all().result()))
        else:
            print("Something went wrong with this query:\n\t{0}".format(query))
        print_status_attributes(callback.result())
        print("\n")

    print("\n")


def update_vertices(client):
    for query in _gremlin_update_vertices:
        print("\n> {0}\n".format(query))
        callback = client.submitAsync(query)
        if callback.result() is not None:
            print("\tUpdated this vertex:\n\t{0}\n".format(
                callback.result().all().result()))
        else:
            print("Something went wrong with this query:\n\t{0}".format(query))

        print_status_attributes(callback.result())
        print("\n")

    print("\n")


def count_vertices(client):
    print("\n> {0}".format(
        _gremlin_count_vertices))
    callback = client.submitAsync(_gremlin_count_vertices)
    if callback.result() is not None:
        print("\tCount of vertices: {0}".format(callback.result().all().result()))
    else:
        print("Something went wrong with this query: {0}".format(
            _gremlin_count_vertices))

    print("\n")
    print_status_attributes(callback.result())
    print("\n")


def execute_traversals(client):
    for key in _gremlin_traversals:
        print("{0}:".format(key))
        print("> {0}\n".format(
            _gremlin_traversals[key]))
        callback = client.submitAsync(_gremlin_traversals[key])
        for result in callback.result():
            print("\t{0}".format(str(result)))
        
        print("\n")
        print_status_attributes(callback.result())
        print("\n")


def execute_drop_operations(client):
    for key in _gremlin_drop_operations:
        print("{0}:".format(key))
        print("\n> {0}".format(
            _gremlin_drop_operations[key]))
        callback = client.submitAsync(_gremlin_drop_operations[key])
        for result in callback.result():
            print(result)
        print_status_attributes(callback.result())
        print("\n")


try:
    client = client.Client('wss://graph-william-white.gremlin.cosmosdb.azure.com:443/', 'g',
                           username="/dbs/graphdb/colls/Persons",
                           password="SsMUNfKi9e2QLXaug4Vu1Iyfrb0rcWYHWwxbZiYr615LA1XlpiLwYZW9wC9HgpzVduPR6h42bZgZCqDnKvhKgg==",
                           message_serializer=serializer.GraphSONSerializersV2d0()
                           )

    print("**** Welcome to Azure Cosmos DB + Gremlin on Python!")

    # Drop the entire Graph
    input("We're about to drop whatever graph is on the server. Press any key to continue...")
    cleanup_graph(client)

    # Insert all vertices
    input("Let's insert some vertices into the graph. Press any key to continue...")
    insert_vertices(client)

    # Create edges between vertices
    input("Now, let's add some edges between the vertices. Press any key to continue...")
    insert_edges(client)

    # Update a couple of vertices
    input("Ah, sorry. I made a mistake. Let's change the ages of these two vertices. Press any key to continue...")
    update_vertices(client)

    # Count all vertices
    input("Okay. Let's count how many vertices we have. Press any key to continue...")
    count_vertices(client)

    # Execute traversals and get results
    input("Cool! Let's run some traversals on our graph. Press any key to continue...")
    execute_traversals(client)

    # Drop a few vertices and edges
    input("So, life happens and now we will make some changes to the graph. Press any key to continue...")
    execute_drop_operations(client)

    # Count all vertices again
    input("How many vertices do we have left? Press any key to continue...")
    count_vertices(client)

except GremlinServerError as e:
    print('Code: {0}, Attributes: {1}'.format(e.status_code, e.status_attributes))

    # GremlinServerError.status_code returns the Gremlin protocol status code
    # These are broad status codes which can cover various scenaios, so for more specific
    # error handling we recommend using GremlinServerError.status_attributes['x-ms-status-code']
    # 
    # Below shows how to capture the Cosmos DB specific status code and perform specific error handling.
    # See detailed set status codes than can be returned here: https://docs.microsoft.com/en-us/azure/cosmos-db/gremlin-headers#status-codes
    #
    # See also list of available response status attributes that Gremlin API can return:
    #     https://docs.microsoft.com/en-us/azure/cosmos-db/gremlin-headers#headers
    cosmos_status_code = e.status_attributes["x-ms-status-code"]
    if cosmos_status_code == 409:
        print('Conflict error!')
    elif cosmos_status_code == 412:
        print('Precondition error!')
    elif cosmos_status_code == 429:
        print('Throttling error!');
    elif cosmos_status_code == 1009:
        print('Request timeout error!')
    else:
        print("Default error handling")

    traceback.print_exc(file=sys.stdout) 
    sys.exit(1)

print("\nAnd that's all! Sample complete")
input("Press Enter to continue...")
