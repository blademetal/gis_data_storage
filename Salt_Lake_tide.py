# Könyvtárak betöltése
import numpy as np
import pandas as pd
import psycopg2
import collections
import time


proj = "4326"

# Az publikus adatok betöltése
Salt_Lake_City_data = pd.read_csv("Salt_Lake_City_geocoded.csv")
City_Council_Members = pd.read_excel("City_Council_Members_List.xlsx")

# Felesleges oszlopok törlése
Salt_Lake_City_data = Salt_Lake_City_data.drop(["Unnamed: 0",
                                                "ncic code",
                                                "city",
                                                "y_coordinate",
                                                "x_coordinate",
                                                "police grid"], axis=1).dropna()


# Az összes olyan eset törlése, ami a felsorolt körzeteken kívülre esett 
index = 0
del_list = []
for i in Salt_Lake_City_data.iloc[:,7]:
    
    if(i.isdigit() == False or int(i) not in list(range(1,7))):
        del_list.append(index)
    index+=1
    
Salt_Lake_City_data = Salt_Lake_City_data.drop(Salt_Lake_City_data.index[del_list])



crime_Scenes = list(set(Salt_Lake_City_data.iloc[:,6]))


# Helper function az égtájak jobb olvashatóságának érdekében
def trans_Dir(s):
    if(s == "N"):
        return "North"
    elif (s == "E"):
        return "East"
    elif (s == "S"):
        return "South"
    elif (s == "W"):
        return "West"
    else:
        return s
    

# Az irreálisan hosszú nevű utcák törlése
index = 0
del_list_street = []
for i in Salt_Lake_City_data.iloc[:,6]:
    i = i.split()
    if(len(i) > 5):
        del_list_street.append(index)
    index += 1
        
Salt_Lake_City_data = Salt_Lake_City_data.drop(Salt_Lake_City_data.index[del_list_street])



# Helper function a gyorsabb listához adáshoz
def add_To_List(list1, list2, x1, x2):
    list1.append(x1)
    list2.append(x2)


# Az utcanevek könnyen érthetővé tétele, és visszadása dataframe-ként
def transform_Street_Names(col):
    address_split_list = [word.split() for word in col]
    
    address_1st_st = []
    address_2st_st = []
    
    for i in address_split_list:
        if(len(i) <= 3):
            st = trans_Dir(i[0]) + " " + trans_Dir(i[1]) + " " + trans_Dir(i[2])
            add_To_List(address_1st_st, address_2st_st, st, "")
        elif (len(i) == 4):
            st1 = trans_Dir(i[0]) + " " + trans_Dir(i[1])
            st2 = trans_Dir(i[2]) + " " + trans_Dir(i[3])
            add_To_List(address_1st_st, address_2st_st, st1, st2)
        else:
            st1 = trans_Dir(i[0]) + " " + trans_Dir(i[1])
            st2 = trans_Dir(i[2]) + " " + trans_Dir(i[3]) + " " + trans_Dir(i[4])
            add_To_List(address_1st_st, address_2st_st, st1, st2)
            
    address_1st_st = pd.DataFrame(address_1st_st)
    address_2st_st = pd.DataFrame(address_2st_st)
                
    return address_1st_st, address_2st_st



# Mivel egy helyszínen több bűncselekményt is elkövetnek, ezért a helyszíneket kategokisan rendezzük
unique_places = list(set(Salt_Lake_City_data.iloc[:,6]))
unique_ind = list(range(len(unique_places)))
places_dict = dict(zip(unique_places, unique_ind))
places_col = pd.DataFrame(Salt_Lake_City_data.iloc[:,6].map(places_dict)).reset_index(drop=True)


# Az utcanevek olvashatóbbát tétele, és a felesleges lokációs oszlop elvetése
street_1st, street_2st = transform_Street_Names(Salt_Lake_City_data.iloc[:,6])
Salt_Lake_City_data.iloc[:,6] = Salt_Lake_City_data.iloc[:,6].map(places_dict)
Salt_Lake_City_data = Salt_Lake_City_data.reset_index(drop = True)


# Az esetek esetleges helyeit jelölő tábla létrehozása, és annak konzisztensé tétele
crime_places = pd.concat([places_col,
                             street_1st,
                             street_2st,
                             Salt_Lake_City_data.iloc[:,9],
                             Salt_Lake_City_data.iloc[:,10]], axis = 1)
    
crime_places.columns = ["place_id", "street_1st", "street_2st", "x_coord", "y_coord"]
crime_places = crime_places.sort_values(by = ["place_id"]).drop_duplicates(subset = ["place_id", "street_1st", "street_2st"] ,keep = "last")


# A koordinátákat tartalmazó oszlopok elvetése a főtáblából
Salt_Lake_City_data = Salt_Lake_City_data.drop(["x_gps_coords", "y_gps_coords"], axis = 1).reset_index(drop=True)
  


######################################################################
## Elkészíteni a többi segéd táblának alapul szolgáló dataframe-et
######################################################################

# A rendőrségi zónák segédtáblájának kialakítása
police_zones = (
                pd.DataFrame(pd.Series(list(set(list(Salt_Lake_City_data.iloc[:,8])))))
                       .reset_index()
                       .sort_values(['index'])
                                        )
                                        
police_zones_dict = (
                police_zones.reindex(columns = [0, "index"])
                       .set_index([0])
                       .to_dict()
                                        )
                                        
Salt_Lake_City_data.iloc[:,8] = (
                Salt_Lake_City_data.iloc[:,8]
                       .map(police_zones_dict['index'])
                       .reset_index()
                                        )


# A bűneset leírásának külön táblába való felvétele
descriptions = (
                pd.DataFrame(pd.Series(list(set(list(Salt_Lake_City_data.iloc[:,1])))))
                       .reset_index()
                       .sort_values(['index'])
                                       )

descriptions_dict = (
                descriptions.reindex(columns = [0, "index"])
                       .set_index([0])
                       .to_dict()
                                       )

Salt_Lake_City_data.iloc[:,1] = (
                Salt_Lake_City_data.iloc[:,1]
                       .map(descriptions_dict['index'])
                       .reset_index()
                                       )


# Az IBR kódok külön táblába való felvétele
IBRs = (
        pd.DataFrame(pd.Series(list(set(list(Salt_Lake_City_data.iloc[:,2])))))
                 .reset_index()
                 .sort_values(['index'])
                                 )
                                                        
IBRs_dict = (
        IBRs.reindex(columns = [0, "index"])
                 .set_index([0])
                 .to_dict()
                                 )

Salt_Lake_City_data.iloc[:,2] = (
        Salt_Lake_City_data.iloc[:,2]
                 .map(IBRs_dict['index'])
                 .reset_index()
                                 )

def print_out_upload_state(part, whole):
    print(str(part) + "/" + str(whole))




# Kapcsolódás a szerverhez a psycopg2-vel
conn = psycopg2.connect(dbname="u9y9j3", user="u9y9j3", password="u9y9j3", host="webgis.fmt.bme.hu", port="25432")

cur = conn.cursor()


# Ellenőrizzük, hogy a description_table létezik-e, ha nem akkor létrehozzuk a táblát,
# és feltöltjük adatokkal 
cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('description_table',))
if(cur.fetchone()[0] == False):
    
    cur.execute("CREATE TABLE description_table ("
                                + "id integer PRIMARY KEY, "
                                + "description text"
                                + ")")
    conn.commit()
    
    # description értékek description_table-höz adása
    for i in range(np.shape(descriptions)[0]):
        cur.execute("INSERT INTO description_table (id, description) VALUES ("
                                    + str(descriptions.iloc[i,0]) + ", '"
                                    + str(descriptions.iloc[i,1]) + "')")
        conn.commit()
        
else:
    print('description_table already exists')
    
    

# Ellenőrizzük, hogy a ibr_table létezik-e, ha nem akkor létrehozzuk a táblát,
# és feltöltjük adatokkal 
cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('ibr_table',))
if(cur.fetchone()[0] == False):
    
    cur.execute("CREATE TABLE ibr_table ("
                            + "id integer PRIMARY KEY, "
                            + "ibr text"
                            + ")")
    conn.commit()
    
    # ibr értékek hozzáadás az ibr_table-höz
    for i in range(np.shape(IBRs)[0]):
        cur.execute("INSERT INTO ibr_table VALUES ("
                                    + str(IBRs.iloc[i,0]) + ", '"
                                    + str(IBRs.iloc[i,1]) + "')")
        conn.commit()
        
else:
    print('ibr_table already exists')
    
    

# Ellenőrizzük, hogy a location_table létezik-e, ha nem akkor létrehozzuk a táblát,
# és feltöltjük adatokkal 
cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('location_table',))
if(cur.fetchone()[0] == False):
    
    cur.execute("CREATE TABLE location_table("
                            + "id integer PRIMARY KEY, "
                            + "street_1st text, "
                            + "street_2st text, "
                            + "point geometry"
                            + ")")
    conn.commit()
    
    
    # helységet jelölő értékek hozzáadás a location_table-höz
    counter = 0
    for i in range(0, np.shape(crime_places)[0]):
        cur.execute("INSERT INTO location_table VALUES ("
                                + str(crime_places.iloc[i,0]) + ", '"
                                + str(crime_places.iloc[i,1]) + "', '"
                                + str(crime_places.iloc[i,2]) + "', "
                                + "ST_GeomFromText('POINT(" + str(crime_places.iloc[i,3]) + " "
                                                            + str(crime_places.iloc[i,4]) + ")', "
                                                            + proj + ")"
                                + ")")
        conn.commit()
        print_out_upload_state(counter, np.shape(crime_places)[0])
        counter+=1
        time.sleep(0.05)
        
else:
    print('location_table already exists')    




# Ellenőrizzük, hogy a area_table létezik-e, ha nem akkor létrehozzuk a táblát,
# és feltöltjük adatokkal     
cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('area_table',))
if(cur.fetchone()[0] == False):
    
    cur.execute("CREATE TABLE area_table("
                            + "id integer PRIMARY KEY, "
                            + "name text, "
                            + "area double precision, "
                            + "area_shape geometry"
                            + ")")
    conn.commit()
    
    
    ##########################################################################
    #####  Ittkészítettem el a QGIS-ben a szükséges területi lefedéseket
    #####  és adtam hozzá az adatbázishoz
    ##########################################################################

else:
    print('area_table already exists')    
    


# Ellenőrizzük, hogy a city_council_table létezik-e, ha nem akkor létrehozzuk a táblát,
# és feltöltjük adatokkal  
cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('city_council_table',))
if(cur.fetchone()[0] == False):
    
    cur.execute("CREATE TABLE city_council_table ("
                            + "id integer PRIMARY KEY, "
                            + "name text, "
                            + "website text, "
                            + "phone text, "
                            + "email text, "
                            + "area_id integer REFERENCES area_table"
                            + ")")
    conn.commit()
    
    
    # A városi tanács tagjainak a hozzáadás a city_council_table-höz
    for i in range(np.shape(City_Council_Members)[0]):
        cur.execute("INSERT INTO city_council_table VALUES ("
                                    + str(City_Council_Members.iloc[i,0]) + ", '"
                                    + str(City_Council_Members.iloc[i,1]) + "', '"
                                    + str(City_Council_Members.iloc[i,2]) + "', '"
                                    + str(City_Council_Members.iloc[i,3]) + "', '"
                                    + str(City_Council_Members.iloc[i,4]) + "', "
                                    + str(City_Council_Members.iloc[i,5]) + ")")
        conn.commit()
        
else:
    print('city_council_table already exists') 
    
    
    
    
# Ellenőrizzük, hogy a police_zone_table létezik-e, ha nem akkor létrehozzuk a táblát,
# és feltöltjük adatokkal 
cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('police_zone_table',))
if(cur.fetchone()[0] == False):
    
    cur.execute("CREATE TABLE police_zone_table ("
                            + "id integer PRIMARY KEY, "
                            + "zone_name text"
                            + ")")
    conn.commit()
    
    
    # rendőrségi zónák hozzáadás a police_zone_table-höz
    for i in range(np.shape(police_zones)[0]):
        cur.execute("INSERT INTO police_zone_table VALUES ("
                                    + str(IBRs.iloc[i,0]) + ", '"
                                    + str(IBRs.iloc[i,1]) + "')")
        conn.commit()
        
else:
    print('police_zone_table already exists') 
    



# Ellenőrizzük, hogy a crimes_table létezik-e, ha nem akkor létrehozzuk a táblát,
# és feltöltjük adatokkal 
cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('crimes_table',))
if(cur.fetchone()[0] == False):
    
    cur.execute("CREATE TABLE crimes_table ("
                            + "case_id SERIAL NOT NULL PRIMARY KEY, "
                            + "case_num text, "
                            + "description integer REFERENCES description_table, "
                            + "ibr integer REFERENCES ibr_table, "
                            + "occured_date date, "
                            + "occured_time time, "
                            + "reported_date date, "
                            + "reported_time time, "
                            + "day_of_week integer CHECK (day_of_week > 0 AND day_of_week < 8), "
                            + "location integer REFERENCES location_table, "
                            + "city_council integer REFERENCES city_council_table, "
                            + "police_zone integer REFERENCES police_zone_table"
                            + ");")
    
    conn.commit()
    
    
    # Végül az egyes bűnesetek kerülnek hozzáadásra
    counter = 0
    for i in range(np.shape(Salt_Lake_City_data)[0]):
        cur.execute("INSERT INTO crimes_table VALUES (DEFAULT, '"
                                    + str(Salt_Lake_City_data.iloc[i,0]) + "', "
                                    + str(Salt_Lake_City_data.iloc[i,1]) + ", "
                                    + str(Salt_Lake_City_data.iloc[i,2]) + ", '"
                                    + str(Salt_Lake_City_data.iloc[i,3]) + "', '"
                                    + str(Salt_Lake_City_data.iloc[i,3]) + "', '"
                                    + str(Salt_Lake_City_data.iloc[i,4]) + "', '"
                                    + str(Salt_Lake_City_data.iloc[i,4]) + "', "
                                    + str(Salt_Lake_City_data.iloc[i,5]) + ", "
                                    + str(Salt_Lake_City_data.iloc[i,6]) + ", "
                                    + str(Salt_Lake_City_data.iloc[i,7]) + ", "
                                    + str(Salt_Lake_City_data.iloc[i,8]) + ")")
        conn.commit()
        print_out_upload_state(counter, np.shape(Salt_Lake_City_data)[0])
        counter+=1
        time.sleep(0.03)
    
else:
    print('crimes_table already exists') 
    

conn.close()





