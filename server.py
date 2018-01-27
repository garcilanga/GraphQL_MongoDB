#!/usr/bin/env python
# -*- coding: utf-8 -*-
#encoding:utf-8

import sys
import unicodedata
import datetime
import time
import argparse
import ast
import re
from json import dumps
import numpy as np
import pymongo
from bottle import route, run

####################################################################################################
### Configuración
####################################################################################################

### Parámetros de configuración por línea de comandos
_config = { # ¡¡¡ No usar la variable _config, sino la función getConfig() !!!
    'mongo_host': None,
    'mongo_port': None,
    'mongo_dbname': None,
    'mongo_collections': None,
    'mongo_user': None,
    'mongo_password': None,
    'verbose': False,
}
def getConfig():
    return _config
def getVerbose():
    return _config['verbose']

####################################################################################################
### Obtiene un gestor de base de datos MongoDB
####################################################################################################
### Retorna:
### - Gestor de base de datos MongoDB
####################################################################################################
def get_mongodb_mgr(reconnect=False):

    # Obtiene la información de configuración de conexión (indicada por línea de comandos)
    config = getConfig()

    # Abre conexión con el servidor de MongoDB
    client = pymongo.MongoClient(config['mongo_host'], config['mongo_port'])

    # Selecciona la base de datos
    db = client[config['mongo_dbname']]

    # Aplica las credenciales de usuario y contraseña
    if config['mongo_user']:
        db.authenticate(config['mongo_user'], config['mongo_password'])

    # Retorna un gestor para manejar la base de datos
    return db

####################################################################################################
### Formatea un objeto JSON
####################################################################################################
### Parámetros:
### - Diccionario / objeto JSON
### Retorna:
### - String formateado
####################################################################################################
def pretty_json(jsondata):
    return dumps(jsondata, sort_keys=False, indent=4, separators=(',', ': '))

####################################################################################################
### Reemplaza NaN por None en los campos de un diccionario (para evitar errores de parseo).
### Recorre recursivamente los datos del diccionario y reemplaza los valores NaN por None.
####################################################################################################
### Parámetros:
### - Objeto diccionario (en la primera iteración, en las siguientes también otros tipos)
### Retorna:
### - Objeto diccionario con los valores NaN reemplazados por None
####################################################################################################
def nan2none(myobj):
    # diccionario
    if type(myobj) is dict:
        for key, val in myobj.items():
            myobj[key] = nan2none(val)
    # lista
    elif type(myobj) is list:
        myobj = list(map(lambda x: nan2none(x), myobj))
    # dato numerico
    elif type(myobj) is float and np.isnan(myobj):
        myobj = None

    return myobj

####################################################################################################
### Extrae los parámetros de una url y los almacena en un diccionario de pares clave-valor
####################################################################################################
### Parámetros:
### - string con los parámetros de una Url
### Retorna:
### - Objeto diccionario con los pares clave-valor de los parámetros
####################################################################################################
### Por ejemplo, una Url con los parámetros:
###     q={'precio':{'$gt':3.5}}&limit=50&f={'articulo':1,'cantidad':1,'precio':1}
### generaría el siguiente resultado:
###     url_params = {
###         'q': "{'precio':{'$gt':3.5}}", 
###         'limit': '50', 
###         'f': "{'articulo':1,'cantidad':1,'precio':1}"
###     }
####################################################################################################
def get_url_params(strparam):
    # Inicia el diccionario de pares claves-valor
    url_params = {}

    # Si el parámetro no está vacío
    if strparam:

        # Obtiene del string los pares clave-valor de cada parámeto
        strparam_list = strparam.split('&')
        for item in strparam_list:

            # Separa clave y valor de cada parámetro y los añade al diccionario
            # Si sólo hay nombre de parámetro pero no valor, le asigna valor True
            pair_keyval = item.split('=')
            if len(pair_keyval) == 1:
                url_params[pair_keyval[0]] = True
            elif len(pair_keyval) == 2:
                url_params[pair_keyval[0]] = pair_keyval[1]

    # Retorna el diccionario de pares clave-valor
    return url_params

####################################################################################################
### Obtiene el diccionario de parámetos de consulta para MongoDB
####################################################################################################
### Parámetros:
### - Objeto diccionario con los pares clave-valor de los parámetros de la url
### Retorna:
### - Objeto diccionario con los pares clave-valor de los parámetros para la consulta a MongoDB
####################################################################################################
### Por ejemplo, los parámetros:
###     url_params = {
###         'q': "{'precio':{'$gt':3.5}}", 
###         'limit': '50', 
###         'f': "{'articulo':1,'cantidad':1,'precio':1}"
###     }
### generarían el siguiente resultado:
###     qyery_params = {
###         "skip": 0,
###         "limit": 50,
###         "fields": {
###             "articulo": 1,
###             "cantidad": 1,
###             "precio": 1,
###             "_id": 0
###         },
###         "where": {
###             "precio": {
###                 "$gt": 3.5
###             }
###         },
###         "sort": []
###     }
####################################################################################################
def get_mongodb_query_params(url_params):
    # Inicia el diccionario de parámetos de consulta para MongoDB
    qyery_params = {
        'skip': 0,
        'limit': 0,
        'fields': { '_id': 0 },
        'where': {},
        'sort': [],
        'count': True
    }
    
    # Contar registros (Ej.: &count)
    # En este caso solo se tendrá en cuenta el parámetros 'q' 
    if 'count' not in url_params:
        del qyery_params['count']
    
    # Número de documento inicial (Ej.: &skip=120)
    if 'skip' in url_params:
        qyery_params['skip'] = int(url_params.get('skip'))

    # Número total de documentos (Ej.: &limit=10)
    if 'limit' in url_params:
        qyery_params['limit'] = int(url_params.get('limit'))
    
    # Datos para filtrar (Ej.: &q={'precio':{'$gt':3.5}})
    if 'q' in url_params:
        qyery_params['where'] = ast.literal_eval(url_params.get('q'))

    # Datos a mostrar (Ej.: &f={'articulo':1,'cantidad':1,'precio':1})
    if 'f' in url_params:
        qyery_params['fields'] = ast.literal_eval(url_params.get('f'))
    qyery_params['fields']['_id'] = 0

    # Datos para ordenar (Ej.: &s=[('precio',-1)])
    if 's' in url_params:
        qyery_params['sort'] = ast.literal_eval(url_params.get('s'))

    # En modo verbose muestra los parámetros por consola
    if getVerbose():
        print('- get_mongodb_query_params: %s' % pretty_json(qyery_params))

    # Retorna el diccionario de parámetos de consulta para MongoDB
    return qyery_params

####################################################################################################
### Obtiene de la base de datos la información solicitada por la url
####################################################################################################
### Parámetros:
### - Nombre de colección
### - Cadena de parámetros pasados por url
### Retorna:
### - Resultado de la consulta a base de datos y alguna información adicioanl
####################################################################################################
def get_data_from_mongodb_query(collection_name, strparam):
    # Obtiene los parámetros de la url
    url_params = get_url_params(strparam)
    # Obtiene los parámetros de la consulta a base de datos
    qyery_params = get_mongodb_query_params(url_params)

    # Inicia el contador de tiempo de ejecución de la consulta
    init_time = time.time()

    # Comprueba que la colección sea una de las permitidas
    if collection_name in getConfig()['mongo_collections']:
        # Obtiene un gestor de base de datos
        mgr = get_mongodb_mgr()[collection_name]
        # Si la consulta contiene el parámetro 'count'
        if 'count' in qyery_params:
            numero = mgr.find(qyery_params['where'], {}).count()
        # En caso contrario
        else:
            # Si la consulta contiene el parámetro 'sort'
            if len(qyery_params['sort']) == 0:
                cursor = mgr.find (qyery_params['where'], qyery_params['fields']).skip(qyery_params['skip' ]).limit(qyery_params['limit'])
            # En caso contrario
            else:
                cursor = mgr.find (qyery_params['where'], qyery_params['fields']).sort(qyery_params['sort']).skip(qyery_params['skip' ]).limit(qyery_params['limit'])
            # Formatea el resultado
            lista = list(map(lambda doc: nan2none(doc), cursor))
            numero = len(lista)

    # Finaliza el contador de tiempo de ejecución de la consulta
    end_time = time.time()

    # Formatea la respuesta
    url_params['collection'] = collection_name
    response = {
        'params': url_params,
        'date': init_time,
        'time': end_time - init_time,
        'count': numero
    }
    
    # En modo verbose muestra información adicional por consola
    if getVerbose():
        print('- Response:\n%s' % pretty_json(response))

    # Completa la respuesta
    if 'count' not in qyery_params:
        response['list'] = lista

    # Retorna la la respuesta
    return response

####################################################################################################
### Endpoint de bienvenida
####################################################################################################
@route('/')
def hello():
    lsthtml = [
        '<h3>Microservicio de datos MongoDB</h3>',
        '<ul>',
        '<li>Servidor: <b>' + getConfig()['mongo_host'] + '</b>',
        '<li>Puerto: <b>' + str(getConfig()['mongo_port']) + '</b>',
        '<li>Base de datos: <b>' + getConfig()['mongo_dbname'] + '</b>',
        '<li>Usuario: <b>' + str(getConfig()['mongo_user']) + '</b>',
        '<li>Colecciones: <b>' + ', '.join(getConfig()['mongo_collections']) + '</b>',
        '</ul>',
    ]
    return lsthtml

####################################################################################################
### Endpoint general de consulta
####################################################################################################
@route('/<collection>')
@route('/<collection>/')
@route('/<collection>/<strparam>')
def alerta(collection=None, strparam=None):
    return get_data_from_mongodb_query(collection, strparam)

####################################################################################################
### MAIN
####################################################################################################
if __name__ == "__main__":
    
    ### Obtiene los parámetros por línea de comandos
    parser = argparse.ArgumentParser()
    parser.add_argument("-v"   , "--verbose"          , help="Default None.", default=False, action="store_true")
    parser.add_argument("-sh"  , "--server_host"      , help="Server host name. Default 'localhost'.", default="localhost")
    parser.add_argument("-sp"  , "--server_port"      , help="Server port number. Default 8080.", type=int, default=8080)
    parser.add_argument("-mh"  , "--mongo_host"       , help="MongoDB host name. Default 'localhost'.", default="localhost")
    parser.add_argument("-mp"  , "--mongo_port"       , help="MongoDB port number. Default 27017.", type=int, default=27017)
    parser.add_argument("-mu"  , "--mongo_user"       , help="MongoDB user. Default None.", default=None)
    parser.add_argument("-mw"  , "--mongo_password"   , help="MongoDB password. Default None.", default=None)
    parser.add_argument("-mdb" , "--mongo_dbname"     , help="MongoDB database name. Default 'None'.", default=None)
    parser.add_argument("-mcol", "--mongo_collections", help="MongoDB database collection list. Default '_all'.", default='_all')
    args = parser.parse_args()

    ### Comprueba que se haya indicado el nombre de la base de datos
    if not args.mongo_dbname:
        print('¡ATENCIÓN!')
        print('No se ha indicado el nombre de la base de datos. Por ejemplo: -mdb mi_base_de_datos')
        print('Use el parámetro -h para obtener ayuda.')
        exit(1)
    
    ### Asigna los parámetros de configuración
    _config = {
        'mongo_host': args.mongo_host,
        'mongo_port': args.mongo_port,
        'mongo_user': args.mongo_user,
        'mongo_password': args.mongo_password,
        'mongo_dbname': args.mongo_dbname,
        'verbose': args.verbose,
    }

    ### Asigna las colecciones de la base de datos
    if args.mongo_collections.lower() == '_all':
        _config['mongo_collections'] = get_mongodb_mgr().collection_names()
    else:
        _config['mongo_collections'] = args.mongo_collections.split(',')
    
    ### Inicia el servidor
    ### Ej.: run(host='localhost', port=8080, debug=False)
    ### Ej.: run(host='0.0.0.0', port=8080, debug=False)
    run(host=args.server_host, port=args.server_port, debug=args.verbose)

