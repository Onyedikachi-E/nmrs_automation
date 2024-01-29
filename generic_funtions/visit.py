import mysql.connector as sql
from mysql.connector import Error
import datetime
from datetime import datetime



# To create a Class for the Visit Data
class VisitInfo:
    def __init__(self):
        self.patient_id = 0
        self.visit_type_id = 1
        self.date_started = ""
        self.date_stopped = ""
        self.indication_concept_id = None
        self.location_id = ""
        self.creator = 1
        self.date_created = ""
        self.change_by = None
        self.date_changed = None
        self.voided = 0
        self.voided_by = None
        self.date_voided = None
        self.void_reason = None
        self.uuid = ""



# To create the visit data mapping each attribute of visit_info instance of (VisitInfo Class) with a value and append them to form list.
def visit_list(connection, ids):
    visit_info_list = []
    cursor = connection.cursor(dictionary=True)
    try:
        for patient_id in ids:
           
            query = """SELECT patient_id, uuid() as uuid
                        FROM patient_identifier WHERE identifier_type = 4 and identifier = %s LIMIT 1"""
            
            cursor.execute(query, (patient_id,))
           
            for row in cursor:
                visit_info = VisitInfo()
                visit_info.patient_id = row['patient_id']
                visit_info.date_started = (datetime.strptime(ids[patient_id],"%d/%m/%Y")).strftime("%Y-%m-%d %H:%M:%S")
                visit_info.date_stopped = (datetime.strptime(ids[patient_id],"%d/%m/%Y").replace(hour=23, minute=59, second=59)).strftime("%Y-%m-%d %H:%M:%S")
                visit_info.location_id = None
                visit_info.uuid = row['uuid']
                visit_info_list.append(visit_info)

    except Error as e:
        print("MySQL Error:", e)
    finally:
        cursor.close()

    return visit_info_list


def insert_visit_data(connection, visit_info): # To insert the visit data into the visit table using the commit method.
    try:
        cursor = connection.cursor()
        cursor.execute("""SET FOREIGN_KEY_CHECKS = 0""")
        cursor.execute("""SET SQL_SAFE_UPDATES = 0""")
        cursor.execute("""
            INSERT INTO visit (
                patient_id, visit_type_id, date_started, date_stopped, indication_concept_id,
                location_id, creator, date_created, changed_by, date_changed,	
                voided, voided_by, date_voided, void_reason, uuid
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            visit_info.patient_id, visit_info.visit_type_id, visit_info.date_started, visit_info.date_stopped,
            visit_info.indication_concept_id, visit_info.location_id, visit_info.creator, visit_info.date_created,
            visit_info.change_by, visit_info.date_changed, visit_info.voided, visit_info.voided_by, visit_info.date_voided,
            visit_info.void_reason, visit_info.uuid
        ))
        connection.commit()
    except Error as e:
        print("MySQL Error:", e)
    finally:
        cursor.close()


