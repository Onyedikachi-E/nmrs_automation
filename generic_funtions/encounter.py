import mysql.connector as sql
from mysql.connector import Error
from datetime import timedelta
import random


# To create a Class for the Encounter Data
class EncounterInfo: 
    def __init__(self):
        self.encounter_type = 0
        self.patient_id = 0
        self.location_id = ""
        self.form_id = 0
        self.encounter_datetime = ""
        self.creator = 1
        self.date_created = ""
        self.voided = 0
        self.voided_by = None
        self.date_voided = None
        self.void_reason = None
        self.changed_by = None
        self.date_changed = None
        self.visit_id = 0
        self.uuid = ''



# To create the encounter data mapping each attribute of enct_info instance (EncounterInfo) with a value and append them to form list.
def pharm_encounter_list(connection, id):
    enct_info_list = []
    cursor = connection.cursor(dictionary=True)
    try:
        for patient_id in id:

            query = """SELECT max(visit_id) as visit_id, patient_id, max(date_started) as encounter_datetime, 
                            max(date_created) as date_created, uuid() as uuid FROM visit
                                WHERE patient_id IN (SELECT patient_id FROM patient_identifier WHERE identifier_type = 4 and identifier = %s)"""
            
            cursor.execute(query, (patient_id,))

            for row in cursor:
                enct_info = EncounterInfo()
                enct_info.encounter_type = 13
                enct_info.patient_id = row['patient_id']
                enct_info.location_id = None
                enct_info.form_id = 27
                enct_info.encounter_datetime = row['encounter_datetime'].strftime("%Y-%m-%d %H:%M:%S")
                enct_info.date_created = row['date_created'].strftime("%Y-%m-%d %H:%M:%S")
                enct_info.visit_id = row['visit_id']
                enct_info.uuid = row['uuid']
                enct_info_list.append(enct_info)

    except Error as e:
        print("MySQL Error:", e)
    
    finally:
        cursor.close()

    return enct_info_list


# To create the encounter data mapping each attribute of enct_info instance (EncounterInfo) with a value and append them to form list.
def care_encounter_list(connection, id):
    "To create the encounter data mapping each attribute of enct_info instance (EncounterInfo) with a value and append them to form list."
    enct_info_list = []
    cursor = connection.cursor(dictionary=True)
    try:
        for patient_id in id:

            query = """SELECT max(visit_id) as visit_id, patient_id, max(date_started) as encounter_datetime, 
                            max(date_created) as date_created, uuid() as uuid FROM visit
                                WHERE patient_id IN (SELECT patient_id FROM patient_identifier WHERE identifier_type = 4 and identifier = %s)"""
            
            cursor.execute(query, (patient_id,))

            for row in cursor:
                enct_info = EncounterInfo()
                enct_info.encounter_type = 12
                enct_info.patient_id = row['patient_id']
                enct_info.location_id = 14
                enct_info.form_id = 14
                enct_info.encounter_datetime = row['encounter_datetime'].strftime("%Y-%m-%d %H:%M:%S")
                enct_info.date_created =  (row['date_created'] + timedelta(minutes=random.uniform(3, 7))).strftime("%Y-%m-%d %H:%M:%S")
                enct_info.visit_id = row['visit_id']
                enct_info.uuid = row['uuid']
                enct_info_list.append(enct_info)

    except Error as e:
        print("MySQL Error:", e)
    
    finally:
        cursor.close()

    return enct_info_list


# To create the encounter data mapping each attribute of enct_info instance (EncounterInfo) with a value and append them to form list.
def tracking_encounter_list(connection, id):
    enct_info_list = []
    cursor = connection.cursor(dictionary=True)
    try:
        for patient_id in id:

            query = """SELECT max(visit_id) as visit_id, patient_id, max(date_started) as encounter_datetime, 
                            max(date_created) as date_created, uuid() as uuid FROM visit
                                WHERE patient_id IN (SELECT patient_id FROM patient_identifier WHERE identifier_type = 4 and identifier = %s)"""
            
            cursor.execute(query, (patient_id,))

            for row in cursor:
                enct_info = EncounterInfo()
                enct_info.encounter_type = 15
                enct_info.patient_id = row['patient_id']
                enct_info.location_id = 14
                enct_info.form_id = 13
                enct_info.encounter_datetime = row['encounter_datetime'].strftime("%Y-%m-%d %H:%M:%S")
                enct_info.date_created = row['date_created'].strftime("%Y-%m-%d %H:%M:%S")
                enct_info.visit_id = row['visit_id']
                enct_info.uuid = row['uuid']
                enct_info_list.append(enct_info)

    except Error as e:
        print("MySQL Error:", e)
    
    finally:
        cursor.close()

    return enct_info_list


# To create the encounter data mapping each attribute of enct_info instance (EncounterInfo) with a value and append them to form list.
def lab_encounter_list(connection, id):
    enct_info_list = []
    cursor = connection.cursor(dictionary=True)
    try:
        for patient_id in id:

            query = """SELECT max(visit_id) as visit_id, patient_id, max(date_started) as encounter_datetime, 
                            max(date_created) as date_created, uuid() as uuid FROM visit
                                WHERE patient_id IN (SELECT patient_id FROM patient_identifier WHERE identifier_type = 4 and identifier = %s)"""
            
            cursor.execute(query, (patient_id,))

            for row in cursor:
                enct_info = EncounterInfo()
                enct_info.encounter_type = 11
                enct_info.patient_id = row['patient_id']
                enct_info.location_id = None
                enct_info.form_id = 21
                enct_info.encounter_datetime = row['encounter_datetime'].strftime("%Y-%m-%d %H:%M:%S")
                enct_info.date_created = (row['date_created'] + timedelta(minutes=random.uniform(3, 7))).strftime("%Y-%m-%d %H:%M:%S")
                enct_info.visit_id = row['visit_id']
                enct_info.uuid = row['uuid']
                enct_info_list.append(enct_info)

    except Error as e:
        print("MySQL Error:", e)
    
    finally:
        cursor.close()

    return enct_info_list


# To insert the encounter data into the encounter table using the commit method.
def insert_encounter_data(connection, enct_info): 
    try:
        cursor = connection.cursor()

        cursor.execute("""
                INSERT INTO encounter (encounter_type, patient_id, location_id, 
                         form_id, encounter_datetime, creator,date_created, voided, 
                        voided_by, date_voided, void_reason, changed_by, date_changed, visit_id, uuid
                ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                enct_info.encounter_type, enct_info.patient_id, enct_info.location_id, enct_info.form_id,
                enct_info.encounter_datetime, enct_info.creator, enct_info.date_created, enct_info.voided,
                enct_info.voided_by, enct_info.date_voided, enct_info.void_reason, enct_info.changed_by,
                enct_info.date_changed, enct_info.visit_id, enct_info.uuid
            ))
        connection.commit()
    except Error as e:
        print("MySQL Error:", e)

    finally:
        cursor.close()



