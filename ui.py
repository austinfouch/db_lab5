# import all functions from db.py
import db

def main_menu():
    done = False
    while not done:
        print('Battleship Main Menu')
        print('----------------------------------------')
        print('(C)  Display class list')
        print('(S)  Display ship list')
        print('(AC) Add a new class')
        print('(AS) Add a new ship')
        print('(DC)  Delete a class')
        print('(DS)  Delete a ship')
        print('(X)  Exit')
        
        choice = input('Please make your selection:  ').upper()
        if choice == 'C':
            list_classes()
        elif choice == 'S':
            list_ships()
        elif choice == 'AC':
            add_new_class()
        elif choice == 'AS':
            add_new_ship()
        elif choice == 'DC':
            delete_class()
        elif choice == 'DS':
            delete_ship()
        elif choice == 'X':
            done = True
        else:
            print('¯\_(ツ)_/¯')


def print_ship(ship):
    print(ship[0].ljust(15), end='\t')
    print(ship[1].ljust(15), end='\t')
    print(str(ship[2]).ljust(10), end='\t')
    print(ship[5].ljust(15), end='\t')
    print()

def print_class(cl):
    print(cl[0].ljust(15), end='\t')
    print(cl[1].ljust(6), end='\t')
    print(cl[2].ljust(10), end='\t')
    print(str(cl[3]).ljust(6), end='\t')
    print(str(cl[4]).ljust(6), end='\t')
    print(str(cl[5]).ljust(15), end='\t')
    print()

def class_header():
    print('Class'.ljust(15), end='\t')
    print('Type'.ljust(6), end='\t')
    print('Country'.ljust(10), end='\t')
    print('# Guns'.ljust(6), end='\t')
    print('Bore'.ljust(6), end='\t')
    print('Disp.'.ljust(15), end='\t')
    print('\n-------------------------------------------------------------')

def ship_header():
    print('Class'.ljust(15), end='\t')
    print('Name'.ljust(15), end='\t')
    print('Launched'.ljust(10), end='\t')
    print('Country'.ljust(10), end='\t')
    print('\n-------------------------------------------------------------')

def list_classes():
    print()
    classes = db.get_classes()
    class_header()
    for cl in classes:
        print_class(cl)
    print()


def list_ships():
    print()
    ships = db.get_ships(None)
    ship_header()
    for ship in ships:
        print_ship(ship)
    print()

def add_new_class():
    print()
    c = []
    c.append(input("Please enter the name of class:  "))
    c.append(input("Please enter the type of class (bb or bc):  "))
    c.append(input("Please enter the country:  "))
    c.append(int(input("Please enter number of guns:  ")))
    c.append(int(input("Please enter bore size (inches):  ")))
    c.append(int(input("Please enter displacement (tons):  ")))
    db.add_class(c)

def choose_class():
    print()
    classes = []
    
    class_header()

    for i, cl in enumerate(db.get_classes()):
        print(str(i).rjust(2), end='\t')
        print_class(cl)
        classes.append(cl[0])
    
    class_number = input('Please select class by entering its number:')
    class_number = int(class_number)
    if class_number < 0 or class_number > len(classes)-1:
        return None
    return classes[class_number]

def choose_ship(class_name):
    print()
    ships = []
    
    ship_header()

    for i, ship in enumerate(db.get_ships(class_name)):
        print(str(i).rjust(2), end='\t')
        print_ship(ship)
        ships.append(ship[1])
    
    ship_number = input('Please select ship by entering its number:')
    ship_number = int(ship_number)
    if ship_number < 0 or ship_number > len(ships)-1:
        return None
    return ships[ship_number]


def add_new_ship():
    class_name = choose_class()
    if not class_name:
        print("Invalid class name")
        return

    ship = [class_name]
    ship.append(input("Please enter the name of the ship:  "))
    ship.append(int(input("Please enter the launch year of the ship:  ")))
    db.add_ship(ship)

def delete_class():
    class_name = choose_class()
    if not class_name:
        print("Invalid class name")
        return

    db.delete_class(class_name)

def delete_ship():
    class_name = choose_class()
    if not class_name:
        print("Invalid class name")
        return
    ship_name = choose_ship(class_name)
    if not ship_name:
        print("Invalid ship")

    db.delete_ship(ship_name, class_name)


main_menu()