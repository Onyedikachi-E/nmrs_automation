import mysql.connector as sql
from mysql.connector import Error
from datetime import datetime, timedelta


# To create a Class for the Encounter Provider
class EnounterProvider:
    def __init__(self):
        self.encounter_id = 0
        self.provider_id = 1
        self.encounter_role_id = 1
        self.creator = 1
        self.date_created = ''
        self.changed_by = None
        self.date_changed = None
        self.voided = 0
        self.date_voided = None
        self.voided_by = None
        self.void_reason = None
        self.uuid = ''


# To create provider for the Encounter Provider table
def encounter_provider(connection, ids):
    provider_info_list = []
    try:
        for patient_id in ids:
            cursor = connection.cursor(dictionary=True)

            query = """ SELECT max(encounter_id) as encounter_id, max(date_created) as date_created, uuid() as uuid FROM encounter 
                                WHERE patient_id IN (SELECT patient_id from patient_identifier where identifier_type = 4 and identifier = %s)
                    """
            
            cursor.execute(query, (patient_id,))
            for row in cursor:
                enct_provider_info = EnounterProvider()
                enct_provider_info.encounter_id = row['encounter_id']
                enct_provider_info.date_created = row['date_created'].strftime("%Y-%m-%d %H:%M:%S")
                enct_provider_info.uuid = row['uuid']
                provider_info_list.append(enct_provider_info)

    except Error as e:
        print('MySQL Error:', e)
    finally:
        cursor.close()
    return provider_info_list


# To insert the provider data into the Encounter Provider table using the commit method.
def insert_encounter_provider_data(connection, enct_provider_info):
    try:
        cursor = connection.cursor()
        cursor.execute(""" INSERT INTO encounter_provider (encounter_id, provider_id, encounter_role_id, creator, 
                       date_created, changed_by, date_changed, voided, date_voided, voided_by, void_reason, uuid  
                    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (enct_provider_info.encounter_id, enct_provider_info.provider_id, enct_provider_info.encounter_role_id,
                      enct_provider_info.creator, enct_provider_info.date_created, enct_provider_info.changed_by, 
                      enct_provider_info.date_changed, enct_provider_info.voided, enct_provider_info.date_voided, 
                      enct_provider_info.voided_by, enct_provider_info.void_reason, enct_provider_info.uuid))
        connection.commit()
    except Error as e:
        print('MySQL Error:', e)
    finally:
        cursor.close()