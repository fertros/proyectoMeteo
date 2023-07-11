import requests
import json
import pymongo
import pandas as pd
from datetime import datetime
from unidecode import unidecode

client = pymongo.MongoClient("mongodb+srv://guillermo:guillermo@cluster0.g5jnb9x.mongodb.net/test")


#Obtención de datos de campoo y los valles del sur
urlcampoo = "https://www.icane.es/data/api/climatology-campoo-southern-valleys.json"
#Obtención de datos de la comarca costera oriental
urlcostera = "https://www.icane.es/data/api/climatology-eastern-coastal-region.json"
#Obtención de datos de la comarca de liebana
urlliebana = "https://www.icane.es/data/api/climatology-liebana.json"
#Obtención de datos de Santander y el arco metropolitano
urlsantandermetro = "https://www.icane.es/data/api/climatology-santander-metropolitan-arc.json"
#Obtención de datos del valle de nansa
urlvallenansa = "https://www.icane.es/data/api/climatology-nansa-valley.json"
#Obtención de datos del valle medio de pas y valle del pisueña
urlvallepaspisueña = "https://www.icane.es/data/api/climatology-pas-pisuenna-middle-valley.json"
#Obtención de datos de valles altos de Asón , Pas y Miera
urlasonmiera = "https://www.icane.es/data/api/climatology-ason-pas-miera-high-valley.json"
#Obtención de datos de valles del Saja-Vesalla
urlsajavesalla = "https://www.icane.es/data/api/climatology-saja-besaya-valleys.json"
#Obtención de datos de Santander y el area de influencia urbana
urlsantanderurbana = "https://www.icane.es/data/api/climatology-santander-urban-influence-area.json"
#Obtención de datos del area de Torrelavega y zona costera occidental
urltorrelavega = "https://www.icane.es/data/api/climatology-torrelavega-western-coastal-area.json"



def limpiarjson(url):
    # Descargar el archivo JSON de una URL
    response = requests.get(url)
    data = response.json()

    # Eliminar valores "null"
    def remove_null_values(obj):
        if isinstance(obj, dict):
            return {k: remove_null_values(v) for k, v in obj.items() if v is not None}
        elif isinstance(obj, list):
            return [remove_null_values(item) for item in obj if item is not None]
        else:
            return obj

    data = remove_null_values(data)

    # Agregar corchetes al principio y al final del archivo
    data = [data]
    with open("sin_null.json", "w") as f:
        json.dump(data, f)
        

#############################################################################################


def prepararDF( comarca ,  nombrejson):
    
    df = pd.read_json("sin_null.json")

  
    
    df.rename(columns={"data" : comarca}, inplace=True, errors='raise')

    df.drop(['metadata', 'headers'], axis=1, inplace=True)
    

    df_json = df.to_json(orient = 'records')

    with open(nombrejson, 'w', encoding='utf-8') as f:
          f.write(df_json)


############################################################################################


def estructurajson(nombrejson3):
    with open(nombrejson3, 'r') as file:
        # Cargar el contenido del archivo en un objeto Python
            datos = json.load(file)[0]
            #print (datos)
    lista = []
    for i, comarca in enumerate(datos):
        
    # print(comarca)
        for k,poblacion in enumerate(datos[comarca]):
            
        # print('-'+poblacion)
            if "Precipitación total" in datos[comarca][poblacion]:
                d_poblacion = datos[comarca][poblacion]["Precipitación total"]
                
                for l,ano in enumerate(d_poblacion):
            # print('--' + ano)
                    lista.append({
                        "comarca": comarca,
                        "poblacion": poblacion,
                        "año": ano,
                        "valor": d_poblacion[ano]
                    })
                    
                
    with open("jsonestructurado.json", 'w') as file:
        json.dump(lista, file) 


#############################################################################################
       

def quitar_mayusculas_y_tildes(nombrejson2):
    # Leer el archivo JSON
    with open(nombrejson2, 'r') as f:
        data = json.load(f)

    # Recorrer el objeto JSON y aplicar las transformaciones
    if isinstance(data, dict):
        # Si el objeto es un diccionario
        for key, value in data.items():
            if isinstance(value, str):
                data[key] = unidecode(value.lower())
    elif isinstance(data, list):
        # Si el objeto es una lista
        for obj in data:
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if isinstance(value, str):
                        obj[key] = unidecode(value.lower())

    # Guardar el objeto JSON modificado en un archivo
    with open("sinmayus.json", 'w') as f:
        json.dump(data, f)


############################################################################################


def splitaño(nombrejson5):
    with open("sinmayus.json") as f:
        data = json.load(f)

    for obj in data:
        if len(obj["año"]) == 4:  # verificar si "año" solo tiene 4 caracteres (el año)
            data.remove(obj)  # eliminar el objeto completo

    for obj in data:
        if " - " in obj["año"]:  # verificar si "año" tiene formato "año - mes"
            anio, mes = obj["año"].split(" - ")  # separar "año" y "mes"
            obj["año"] = int(anio)  # actualizar "año" con solo el año como un entero
            obj["mes"] = {
                "enero": 1,
                "febrero": 2,
                "marzo": 3,
                "abril": 4,
                "mayo": 5,
                "junio": 6,
                "julio": 7,
                "agosto": 8,
                "septiembre": 9,
                "octubre": 10,
                "noviembre": 11,
                "diciembre": 12
            }[mes.lower()]  # agregar nuevo campo "mes" con el valor del mes como un entero

    with open(nombrejson5, 'w') as file:
         json.dump(data, file, indent=4)


############################################################################################


def subir_mongo(archivo , comarca):
     db = client['reto']
     collection = db[comarca]
     with open (archivo) as f :
         data = json.load(f)
         collection.insert_many(data)


############################################################################################


json_sin_null = limpiarjson(urlcampoo)
prepararDF( "CampooVallesSur" , "DataFrame.json")
estructurajson("DataFrame.json")
quitar_mayusculas_y_tildes("jsonestructurado.json")
splitaño("CampooVallesSur.json")
subir_mongo ("CampooVallesSur.json" , 'CampooVallesSur')

json_sin_null2 = limpiarjson(urlcostera)
prepararDF('ComarcaEste', 'DataFrame.json')
estructurajson("DataFrame.json")
quitar_mayusculas_y_tildes("jsonestructurado.json")
splitaño("ComarcaEste.json")
subir_mongo ("ComarcaEste.json" , 'ComarcaEste')

json_sin_null3 = limpiarjson(urlliebana)
prepararDF('Liebana', 'DataFrame.json')
estructurajson("DataFrame.json")
quitar_mayusculas_y_tildes("jsonestructurado.json")
splitaño("Liebana.json")
subir_mongo ("Liebana.json" , 'Liebana')

json_sin_null4 = limpiarjson(urlsantandermetro)
prepararDF('SantanderMetro', 'DataFrame.json')
estructurajson("DataFrame.json")
quitar_mayusculas_y_tildes("jsonestructurado.json")
splitaño("SantanderMetro.json")
subir_mongo ("SantanderMetro.json" , 'SantanderMetro')

json_sin_null5 = limpiarjson(urlvallenansa)
prepararDF('ValleNansa', 'DataFrame.json')
estructurajson("DataFrame.json")
quitar_mayusculas_y_tildes("jsonestructurado.json")
splitaño("ValleNansa.json")
subir_mongo ("ValleNansa.json" , 'ValleNansa')

json_sin_null4 = limpiarjson(urlvallepaspisueña)
prepararDF('PasyPisueña', 'DataFrame.json')
estructurajson("DataFrame.json")
quitar_mayusculas_y_tildes("jsonestructurado.json")
splitaño("PasyPisueña.json")
subir_mongo ("PasyPisueña.json" , 'PasyPisueña')

json_sin_null5 = limpiarjson(urlasonmiera)
prepararDF('AsonPasMiera', 'DataFrame.json')
estructurajson("DataFrame.json")
quitar_mayusculas_y_tildes("jsonestructurado.json")
splitaño("CampooVallesSur.json")
subir_mongo ("CampooVallesSur.json" , 'CampooVallesSur')

json_sin_null6 = limpiarjson(urlsajavesalla)
prepararDF('ValleBesaySaja', 'DataFrame.json')
estructurajson("DataFrame.json")
quitar_mayusculas_y_tildes("jsonestructurado.json")
splitaño("ValleBesaySaja.json")
subir_mongo ("ValleBesaySaja.json" , 'ValleBesaySaja')

json_sin_null7 = limpiarjson(urlsantanderurbana)
prepararDF('SantanderUrbano', 'DataFrame.json')
estructurajson("DataFrame.json")
quitar_mayusculas_y_tildes("jsonestructurado.json")
splitaño("SantanderUrbano.json")
subir_mongo ("SantanderUrbano.json" , 'SantanderUrbano')

json_sin_null7 = limpiarjson(urltorrelavega)
prepararDF('Torrelavega', 'DataFrame.json')
estructurajson("DataFrame.json")
quitar_mayusculas_y_tildes("jsonestructurado.json")
splitaño("Torrelavega.json")
subir_mongo ("Torrelavega.json" , 'Torrelavega')



# Crea un archivo vacío para guardar el resultado final
jsonvacio = "cantabria.json"
with open(jsonvacio, "w") as f:
    f.write("")

# Lista de archivos a unir
unionjsons = ["CampooVallesSur.json", "ComarcaEste.json","Liebana.json","PasyPisueña.json",
               "SantanderMetro.json","SantanderUrbano.json","Torrelavega.json","ValleBesaySaja.json","ValleNansa.json" ]

# Lista para guardar el contenido de los archivos a unir
data_to_merge = []

# Carga el contenido de cada archivo en la lista
for file in unionjsons:
    with open(file) as f:
        data = json.load(f)
        data_to_merge.extend(data)

# Escribe el contenido de la lista en el archivo final
with open(jsonvacio, "w") as f:
    json.dump(data_to_merge, f)

db = client['reto']
collection = db['cantabria']
with open ("cantabria.json") as f :
    data = json.load(f)
    collection.insert_many(data)