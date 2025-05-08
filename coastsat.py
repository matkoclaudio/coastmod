#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Shoreline extraction from satellite images
# Modificado de Kilian Vos WRL 2018

# 20240608_run
# 20240609_skip
# 20240610_synopsis
# 20240622_zip
# 20240706_filepath_data

import os
import shutil
import warnings
import numpy as np
import pandas as pd
import geopandas as gpd
from pyproj import Proj, transform
from coastsat import SDS_download, SDS_preprocess, SDS_shoreline, SDS_tools
warnings.filterwarnings('ignore')

# VARIABLES
zona = '02' # llamar al archivo por el código # CARGA ROI
año  = 2023 # seleccionar año correspondiente a la misión
sat  = 'S2' # ['L5', 'L7', 'L8', 'L9', 'S2']

# L5 1984-2012 (2012!)
# L7 1999-2021 
# L8 2013-2023 
# L9 2021-2023 
# S2 2015-2023 (2015!)

# CARGA ROI 
archivo_por_codigo = {
    
    '00' : '00-ORIENTALES_ROi20.geojson',
    '01' : '01-DELTA_ROi20.geojson',
    '02' : '02-RDPLATA_ROi20.geojson',
    '03' : '03-SAMBOROMBON_ROi20.geojson',
    '04' : '04-MEDANOS_ROi20.geojson',
    '05' : '05-NECOCHEA_ROi20.geojson',
    '06' : '06-BBLANCA_ROi20.geojson',
    '07' : '07-VIEDMA_ROi20.geojson',
    '08' : '08-SJULIAN_ROi20.geojson',
    '09' : '09-VALDES_ROi20.geojson',
    '10' : '10-SJORGE_ROi20.geojson',
    '11' : '11-DESEADO_ROi20.geojson',
    '12' : '12-BGRANDE_ROi20.geojson',
    '13' : '13-TDFUEGO_ROi20.geojson',
    '14' : '14-ESTADOS_ROi20.geojson',
    '15' : '15-MALVINAS_ROi20.geojson',
    '16' : '16-GEORGIAS_ROi20.geojson',
    '17' : '17-SANDWICH_ROi20.geojson',
}

codigo = zona
geojson_file = archivo_por_codigo.get(codigo)
gdf = gpd.read_file(geojson_file)

# FECHAS
dates = [f'{año}-01-01', f'{año}-12-31'] # en año ['2015-01-01', '2015-12-31']

# MISIONES
sat_list = [sat] # en sat ['L5', 'L7', 'L8', 'L9', 'S2']
collection = 'C02' # colección Landsat 'C01' o 'C02'(2022/01/01)

# LOOP geojson (ROI) de entrada
for index, feature in gdf.iterrows():
    
    # Extrae coord
    polygon_coords = feature['geometry'].exterior.coords.xy
    polygon = np.array(polygon_coords).T.tolist()

    # Validación rectangular 
    polygon = SDS_tools.smallest_rectangle([polygon])

    # Obtiene id del ROI
    id_value = feature['id']

    # Define nombre del ROI 
    sitename = f'{geojson_file[0:2]}_{(str(dates))[2:6]}_{(str(sat_list))[2:4]}_{id_value}' 

    # Ruta de resultados 
    # filepath_data = os.path.join(os.getcwd(), 'data')
    # filepath_data = os.path.join(os.getcwd(), f'{geojson_file[0:2]}_{(str(dates))[2:6]}_{(str(sat_list))[2:4]}') 
    filepath_data = f'/media/clod/1TB/procesado/{geojson_file[0:2]}_{(str(dates))[2:6]}_{(str(sat_list))[2:4]}' # solo en P50

    # Características 
    inputs = {   
        'polygon': polygon,
        'dates': dates,
        'sat_list': sat_list,
        'sitename': sitename,
        'filepath': filepath_data,
        'landsat_collection': collection
    }

    # Disponibilidad de img S2 y salto 
    try:
        available_images = SDS_download.check_images_available(inputs)
        if not available_images:
            continue

        # Recupera de GEE
        metadata = SDS_download.retrieve_images(inputs)

        # Carga metadatos, si las img ya se descargaron previamente
        metadata = SDS_download.get_metadata(inputs)
    
    except ValueError as e:
        print(f'no hay imágenes S2 para {sitename} skip!..')
        print(f'error : {e}')
        continue

    # Extracción de interfase agua/tierra
    settings = {
        
        # Parametros generales
        'cloud_thresh'  : 1,    # (0.1) Umbral nubosidad máx 1 = 100%
        'dist_clouds'   : 300,  # (300) Distancia alrededor de las nubes donde la costa no se puede mapear
        'output_epsg'   : 3857, # (28356) Código EPSG del SRC para la salida
        
        # Control de calidad
        'check_detection'   : False, # (True)   True, muestra cada detección de costa al usuario para su validación
        'adjust_detection'  : False, # (False)  True, permite al usuario ajustar la posición de cada costa cambiando el umbral
        'save_figure'       : True,  # (True)   True, guarda una figura que muestra la costa mapeada para cada imagen
        
        # Parámetros de detección 
        'min_beach_area'    : 4500,      # (1000) Área mínima (m2) para que un objeto sea etiquetado como playa
        'min_length_sl'     : 200,       # (500) Longitud mínima (en metros) del perímetro de la costa para ser válida
        'cloud_mask_issue'  : False,     # (False) Cambie este parámetro a True si los píxeles de arena están enmascarados (en negro) en muchas imágenes
        'sand_color'        : 'default', # ('default') 'default', 'latest', 'dark' (para playas de arena gris/negra) or 'bright' (para playas de arena blanca)
        'pan_off'           : False,     # (False) True para desactivar el enfoque general de las imágenes Landsat 7/8/9
        's2cloudless_prob'  : 40,        # (40) Umbral para identificar píxeles nublados en la máscara de probabilidad s2cloudless
        
        # Añade datos ingresados
        'inputs': inputs,
    }

    # [OPCIONAL] Preprocesa img (cloud masking, pansharpening/down-sampling) - (enmascaramiento de nubes, enfoque panorámico/muestreo reducido)
    SDS_preprocess.save_jpg(metadata, settings, use_matplotlib=True)

    # Extrae líneas costeras de todas las img (guarda output.pkl y Shorelines.kml?)
    output = SDS_shoreline.extract_shorelines(metadata, settings)

    # Elimina duplicados (img misma fecha, mismo sat)
    output = SDS_tools.remove_duplicates(output)

    # Elimina georreferenciación inexacta (umbral en 10m)
    output = SDS_tools.remove_inaccurate_georef(output, 10)

    # Parámetros de reproyección
    inProj  = Proj(init='epsg:3857')
    outProj = Proj(init='epsg:4326')

    # salida
    data = []
    for i in range(len(output['shorelines'])):
        print ('run rabbit! run!')
        
        sl      = output['shorelines'][i]
        date    = output['dates'][i]
        archivo = output['filename'][i]

        nubes     = output['cloud_cover'][i]
        precision = output['geoaccuracy'][i]
        indice    = output['idx'][i]
        MNDWI     = output['MNDWI_threshold'][i]

        # transformaciones
        x, y     = transform(inProj, outProj, sl[:, 0], sl[:, 1])
        fechas   = [f'{archivo[8:10]}/{archivo[5:7]}/{archivo[0:4]} {archivo[11:13]}:{archivo[14:16]}:{archivo[17:19]}' for _ in range(len(x))]
        archivos = [archivo[20:22]] * len(x)
        
        nubes_list     = [nubes] * len(x)
        precision_list = [precision] * len(x)
        indice_list    = [indice] * len(x)
        MNDWI_list     = [round(MNDWI, 5)] * len(x)

        # concatena listas a 'data'
        data.extend(zip(x, y, fechas, archivos, nubes_list, precision_list, indice_list, MNDWI_list))

    # guarda salida.txt
    out_file_path = os.path.join(filepath_data, f'{geojson_file[0:2]}_{(str(dates))[2:6]}_{(str(sat_list))[2:4]}_{id_value}.txt')
    df = pd.DataFrame(data, columns=['x', 'y', 'fecha', 'archivo', 'nubes', 'precision', 'indice', 'MNDWI'])
    df.to_csv(out_file_path, index=False, header=False)

# synopsis, elimina resto del filepath_data
for root, dirs, files in os.walk(filepath_data, topdown=False):
    for name in files:
        if not name.endswith('.txt'):
            try:
                os.remove(os.path.join(root, name))
            except Exception as e:
                print(f'Error eliminando archivo {os.path.join(root, name)}: {e}')
    for name in dirs:
        try:
            shutil.rmtree(os.path.join(root, name))
        except Exception as e:
            print(f'Error eliminando carpeta {os.path.join(root, name)}: {e}')

# zip salida 
output_zip = f'{filepath_data}.zip'
shutil.make_archive(filepath_data, 'zip', filepath_data)
print(f'zipeado : {output_zip}')

# elimina carpeta 
try:
    shutil.rmtree(filepath_data)
    print(f'eliminado : {filepath_data}')
except Exception as e:
    print(f'Error eliminando carpeta {filepath_data}: {e}')

