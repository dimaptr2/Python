#! /usr/bin/python

import sys
import time
import math
import sapnwrfc
import mysql.connector as mydb

# SAP remote functions calls

# Units of measurements
def get_units_of_measurements():
    fd = sap_conn.discover("Z_RFC_GET_UNITS_MEASURE")
    f = fd.create_function_call()
    f.LANGUAGE("RU")
    f.invoke()
    return f.UNITS.value

# Upload customers and vendors master records
def get_vendors_customers():
    fd = sap_conn.discover("Z_FM_DMS_DICT_READ")
    f = fd.create_function_call()
    f.invoke()
    return [f.T_LFA1.value, f.T_KNA1.value]

# Get materials master data
def get_materials():
    fd = sap_conn.discover("BAPI_MATERIAL_GETLIST")
    f = fd.create_function_call()
    f.MATNRSELECTION(
        [{'SIGN': "I", 'OPTION': "BT", 'MATNR_LOW': "0", 'MATNR_HIGH': "999999999999999999"}])
    f.invoke()
    return f.MATNRLIST.value

# Database operations

def refresh_master_data(i_db):
    c = i_db.cursor()
    c.execute('DELETE FROM units')
    c.execute('DELETE FROM materials')
    c.execute('DELETE FROM vendors')
    c.execute('DELETE FROM customers')
    i_db.commit()
    c.close()

# create units of measurements master records
def create_units(i_db, i_units):
    c = i_db.cursor()
    index = 1
    for uom in i_units:
        index = index + 1
        s1 = uom['UOM_SAP']
        s2 = uom['UOM_ISO']
        s3 = uom['UOM_DESCLONG']
        line = []
        line.append(s1.decode('utf-8'))
        line.append(s2.decode('utf-8'))
        line.append(s3.decode('utf-8'))
        c.execute('INSERT INTO units VALUES (%s, %s, %s)', line)
    i_db.commit()
    c.close()
    return index

# create vendors and customers
def create_vendors_customers(i_db, i_data):
    c = i_db.cursor()
    # create vendor's master records
    for vendor in i_data[0]:
        v_id = vendor['LIFNR'].decode('utf-8')
        v_name = vendor['NAME1'].decode('utf-8')
        line = []
        line.append(v_id)
        line.append(v_name)
        c.execute('INSERT INTO vendors VALUES (%s, %s)', line)
    # create customer's master records
    for customer in i_data[1]:
        c_id = customer['KUNNR'].decode('utf-8')
        c_name = customer['NAME1'].decode('utf-8')
        line = []
        line.append(c_id)
        line.append(c_name)
        c.execute('INSERT INTO customers VALUES (%s, %s)', line)

    i_db.commit()
    c.close()

# create material master records
def create_materials(i_db, i_data):
    c = i_db.cursor()
    for material in i_data:
        m_id = material['MATERIAL'].decode('utf-8')
        m_desc = material['MATL_DESC'].decode('utf-8')
        line = []
        line.append(m_id)
        line.append(m_desc)
        c.execute('INSERT INTO materials VALUES (%s, %s)', line)
    i_db.commit()
    c.close()

# --------- MAIN BLOCK ----------

# Set SAP connection
sapnwrfc.base.config_location = 'prd500.yml'
sapnwrfc.base.load_config()

# Start the data reading and processing ...

sap_conn = sapnwrfc.base.rfc_connect()

if sap_conn is not None:

    db_conn = mydb.connect(user="copa", password="12345678", host="localhost", port=3306, database="copadb")

    if db_conn is not None:

        mode_code = int(sys.argv[1])
        # Check a starting timestamp
        ts1 = time.time()

        if mode_code == 1:

            refresh_master_data(db_conn)
            units = get_units_of_measurements()
            counter = create_units(db_conn, units)
            print counter, "Uoms was inserted"
            agents = get_vendors_customers()
            print "The number of vendors is ", len(agents[0])
            print "The number of customers is ", len(agents[1])
            create_vendors_customers(db_conn, agents)
            print "Vendors and customers was created"
            matnrs = get_materials()
            print "Number of materials is ", len(matnrs)
            create_materials(db_conn, matnrs)
            print "Materials was created"

        elif mode_code == 2:
            pass
        elif mode_code == 3:
            pass
        else:
            print "Usage only: 1 (master data) | 2 (full upload) | 3 (delta upload)"

        db_conn.close()

    else:
        print "Cannot connect to database"

    sap_conn.close()

    # Check a final timestamp
    ts2 = time.time()
    # Calculate the time of execution in minutes or in seconds
    diff = math.floor(ts2 - ts1)

    time_units = ""

    if diff >= 0 and diff <= 60:
        time_units = "seconds"
    else:
        time_units = "minutes"
        diff = math.floor(diff / 60)

    print "Time of execution is ", int(diff), time_units

else:
    print("SAP connection is failed")

