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
        for a_class in data[0]:
        	add_class(a_class)
        for ship in data[1]:
        	add_ship(ship)
        # insert the data....

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
# -- Remember, pymongo returns a dictionary, so you need to transform it into a list!
def get_classes():
	allClasses = classes.find({})
	for theClass in allClasses:
		yield to_list(class_keys, theClass)

# Return list of all ships, joined with class.  Important, to receive full credit you
# should use a Python generator to yield each item out of the cursor.
# Field order should be ship.class, name, launched, class.class, type, country, guns, bore, displacement
# If class_name is not None, only return ships with the given class_name.  Otherwise, return all ships
def get_ships(class_name):
	allShips = ships.find({})
	for theShip in allShips:
		for a_class in get_classes():
			if(a_class[0] == theShip['class']):
				theShip = to_list(ship_keys, theShip)
				the_class = turnClassToDict(a_class)
				aShip = join(class_keys, the_class, theShip)
				break
		yield aShip

def turnClassToDict(data):
	the_class = dict()
	the_class['class'] = data[0]
	the_class['type'] = data[1]
	the_class['country'] = data[2]
	the_class['guns'] = data[3]
	the_class['bore'] = data[4]
	the_class['displacement'] = data[5]
	return the_class

# Data will be a list ordered with class, type, country, guns, bore, displacement.
def add_class(data):
	the_class = turnClassToDict(data)
	classes.insert_one(the_class)

# Data will be a list ordered with class, name, launched.
def add_ship(data):
	the_ship = dict()
	the_ship['class'] = data[0]
	the_ship['name'] = data[1]
	the_ship['launched'] = data[2]
	ships.insert_one(the_ship)

def delete_class(class_name):
	ships.delete_many({'class':class_name})
	classes.delete_one({'class':class_name})

def delete_ship(ship_name, class_name):
	ships.delete_one({'name' : ship_name, 'class' : class_name})

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