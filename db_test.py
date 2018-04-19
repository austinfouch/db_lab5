import pymongo
from pymongo import MongoClient
import csv
import configparser

#######################################################################
# IMPORTANT:  You must set your config.ini values!
#######################################################################
# The connection string is provided by mlab.  Log into your account and copy it into the 
# config.ini file.  It should look something like this:
# mongodb://labs:test@ds213239.mlab.com:13239/cmps364
# Make sure you copy the entire thing, exactly as displayed in your account page!
#######################################################################
config = configparser.ConfigParser()
config.read('config.ini')
connection_string = config['database']['mongo_connection']


ships = None
classes = None

def load_data():
    classes = list()
    with open('classes.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile)
        for row in spamreader:
            classes.append(row)

    ships = list()
    with open('ships.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile)
        for row in spamreader:
            ships.append(row)
    return [classes, ships]


def seed_database():
    
    existing = classes.find({})

    if existing.count() < 1:
        data = load_data()
        for ship_class in data[0]:
            add_class(ship_class)
        for ship in data[1]:
            add_ship(ship)

    # If there is already data, there is no need to do anything at all...

# utility lists defining the fields, and the order they are expected to be in by ui.py
class_keys = ('class', 'type', 'country', 'guns', 'bore', 'displacement')
ship_keys = ('class', 'name', 'launched')

# utility function you might find useful.. accepts a key list (see above) and 
# a document returned by pymongo (dictionary) and turns it into a list.     
def to_list(keys, document):
    record = []
    for key in keys:
        record.append(document[key])
    return record  

# utility function you might find useful...  Similar to to_list above, but it's appending
# to a list (record) instead of creating a new one.  Useful for when you already have a
# list, but need to join another dictionary object into it...
def join(keys, document, record):
    for key in keys:
        record.append(document[key])
    return record  

# Return list of all classes.  Important, to receive full credit you
# should use a Python generator to yield each item out of the cursor.
# Field order should be class, type, country, guns, bore, displacement
# Remember, pymongo returns a dictionary, so you need to transform it into a list!
def get_classes():
    all_classes = classes.find({})
    for class_obj in all_classes:
        yield to_list(class_keys, class_obj)

# Return list of all ships, joined with class.  Important, to receive full credit you
# should use a Python generator to yield each item out of the cursor.
# Field order should be ship.class, name, launched, class.class, type, country, guns, bore, displacement
# If class_name is not None, only return ships with the given class_name.  Otherwise, return all ships
def get_ships(class_name):
    all_ships = ships.find({})
    for ship in all_ships:
        for a_class in get_classes():
            ship = to_list(ship_keys, ship)
            ship_class = to

# Data will be a list ordered with class, type, country, guns, bore, displacement.
def add_class(data):
    class_obj = {
        'class' : data[0],
        'type' : data[1],
        'country' : data[2],
        'guns' : data[3],
        'bore' : data[4],
        'displacement' : data[5]
    }
    classes.insert_one(class_obj)

# Data will be a list ordered with class, name, launched.
def add_ship(data):
    ship_obj = {
        'name' : data[0],
        'class' : data[1],
        'launched' : data[2]
    }
    ships.insert_one(ship_obj)

def delete_class(class_name):
    return {}

def delete_ship(ship_name, class_name):
    return {}

# This is called at the bottom of this file.  You can re-use this important function in any python program
# that uses psychopg2.  The connection string parameter comes from the config.json file in this
# particular example.
def connect_to_db(conn_str):
    global classes
    global ships
    client = MongoClient(conn_str)
    classes = client.db_lab4.classes
    ships = client.db_lab4.ships
    return client

# This establishes the connection, conn will be used across the lifetime of the program.
conn = connect_to_db(connection_string)
seed_database()