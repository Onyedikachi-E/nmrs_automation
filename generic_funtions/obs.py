import mysql.connector as sql
from mysql.connector import Error
import datetime
from datetime import datetime, timedelta
from typing import List



# To create a Class for the Obs Data
class ObsInfo: 
    def __init__(self):
        self.person_id = 0
        self.concept_id = 0
        self.encounter_id = ''
        self.order_id = None
        self.obs_datetime = ""
        self.location_id = None
        self.obs_group_id = None
        self.accession_number = None
        self.value_group_id = None
        self.value_coded = None
        self.value_coded_name_id = None
        self.value_drug = None
        self.value_datetime = None
        self.value_numeric = None
        self.value_modifier = None
        self.value_text = None
        self.value_complex = None
        self.comments = None
        self.creator = 1
        self.date_created = ""
        self.voided = 0    
        self.voided_by = None
        self.date_voided = None
        self.void_reason = None
        self.uuid = ""
        self.previous_version = None
        self.form_namespace_and_path = None
        self.status = "FINAL"
        self.interpretation = None


def check_patient_sex(connection, patient_id):
    """To fetch patient gender"""
    try:
        cursor = connection.cursor(dictionary = True)

        query = f"SELECT gender from person where person_id IN (SELECT patient_id FROM patient_identifier where identifier_type = 4 AND identifier = %s)"
        cursor.execute(query, (patient_id,))
        value =  cursor.fetchone()

        gender = value['gender'].upper()

    except Error as e:
        print("MySQL Error:", e)
    finally:
        cursor.close()
    return gender


# To create the obs data mapping each attribute of obs_info instance (ObsInfo) with a value and append them to form list.
def pharmacy_obs_list(connection, ids): 
    obs_info_list = []

    cursor = connection.cursor(dictionary=True)
    cursor2 = connection.cursor(dictionary=True) # This for the inner query of last (patients next appt)

    try:
        for patient_id in ids:

            # Mapping of all the conccepts in a list.
            concepts = [165945, 164181, 165050, 165774, 166148, 166363, 166278, 165836, 165832, 165720, 165708, 164506, 166406, 162240, 160856, 166120, 165724, 
                        165725, 167209, 159368, 165723, 167218, 1443, 165726, 165727, 160856, 165723, 167218, 1443, 165725, 167209, 159368, 5096, 164989]
            
            counter = 0
            for concept_id in concepts:

                if concept_id == 165050 and check_patient_sex(connection, patient_id) != "F":
                    counter += 1
                    continue

                query = """ SELECT max(encounter_id) as encounter_id, patient_id, max(encounter_datetime) AS obs_datetime, 
                                max(date_created) AS date_created, uuid() AS uuid  FROM encounter
                                    WHERE patient_id IN (SELECT patient_id FROM patient_identifier WHERE identifier_type = 4 and identifier = %s)
                    """

                cursor.execute(query, (patient_id,))
                
                for row in cursor:
                    obs_info = ObsInfo()
                    obs_info.person_id = row['patient_id']
                    obs_info.concept_id = concept_id
                    obs_info.encounter_id = row['encounter_id']
                    obs_info.obs_datetime = row['obs_datetime'].strftime("%Y-%m-%d %H:%M:%S")
                    obs_info.date_created = row['date_created'].strftime("%Y-%m-%d %H:%M:%S")
                    obs_info.uuid = row['uuid']
                    
                    # To loop through the concepts and match concept_id with value when True
                    if concept_id == 165945:
                        obs_info.value_coded = 165303
                        break
                    if concept_id == 164181:
                        obs_info.value_coded = 160530
                        break
                    if concept_id == 165050: 
                        obs_info.value_coded = 165047
                        break  
                    if concept_id == 165774:
                        obs_info.value_coded = 165662
                        break
                    if concept_id == 166148:
                        obs_info.value_coded = 166363
                        break
                    if concept_id == 166363:
                        obs_info.value_coded = 166135
                        break
                    if concept_id == 166278:
                        obs_info.value_coded = 166282
                        break
                    if concept_id == 165836:
                        obs_info.value_coded = 165833
                        break
                    if concept_id == 165832:
                        obs_info.value_coded = 1065
                        break
                    if concept_id == 165720:
                        obs_info.value_coded = 165709
                        break
                    if concept_id == 165708:
                        obs_info.value_coded = 164506
                        break
                    if concept_id == 164506:
                        obs_info.value_coded = 165681
                        break

                    # The logic below is to calculate the pill balance using patients last(next_apt_date) and the current_visit_date
                    if concept_id == 166406: 

                        pill_balance = 0
                        next_apt_date_query = """select value_datetime as next_apt from obs where  concept_id = 5096
                        and encounter_id in (select encounter_id from encounter where form_id = 27) and person_id in 
                                (select patient_id from patient_identifier where identifier_type = 4 and identifier = %s) 
                                order by obs_datetime desc limit 1"""

                        cursor2.execute(next_apt_date_query, (patient_id,))
                        next_apt_row = cursor2.fetchone()
                        
                        if next_apt_row is not None:

                            next_apt = next_apt_row['next_apt']

                            pill_balance = next_apt - row['obs_datetime']
                            
                            if pill_balance.days > 0:
                                obs_info.value_text = pill_balance.days
                            else:
                                obs_info.value_text = 0
                        else:
                            pill_balance = 0
                            obs_info.value_text = 0
                        break

                    if concept_id == 160856:
                        obs_info.value_numeric = 90
                        break
                    if concept_id == 166120:
                        obs_info.value_numeric = 1
                        break
                    if concept_id == 165724:
                        obs_info.value_coded = 166043
                        break
                    if concept_id == 165725 and counter == 17:
                        obs_info.value_coded = 166044
                        break
                    if concept_id == 167209:
                        obs_info.value_numeric = 90
                        break
                    if concept_id == 159368:
                        obs_info.value_numeric = 90
                        break
                    if concept_id == 165723:
                        obs_info.value_coded = 160862
                        break
                    if concept_id == 167218:
                        obs_info.value_coded = 1513
                        break
                    if concept_id == 1443:
                        obs_info.value_numeric = 90
                        break
                    if concept_id == 165727:
                        obs_info.value_coded = 165257
                        break
                    if concept_id == 160856:
                        obs_info.value_numeric = 90
                        break
                    if concept_id == 165723:
                        obs_info.value_coded = 160862
                        break
                    if concept_id == 167218:
                        obs_info.value_coded = 1513
                    if concept_id == 1443:
                        obs_info.value_numeric = 90
                        break
                    if concept_id == 165725:
                        obs_info.value_coded = 165062
                        break
                    if concept_id == 167209:
                        obs_info.value_numeric = 90
                        break
                    if concept_id == 159368:
                        obs_info.value_numeric = 90
                        break

                    # The logic below is to calculate the next_apt_date using (Visit_date , Drug_Duration and Pill_Balance)
                    if concept_id == 5096 and next_apt_row is not None:
                        obs_info.value_datetime = (row['obs_datetime'] + timedelta(days=90) + timedelta(days=(pill_balance.days))).strftime("%Y-%m-%d %H:%M:%S")
                        break
                        
                    if concept_id == 5096 and next_apt_row is None:
                        obs_info.value_datetime = (row['obs_datetime'] + timedelta(days=90) + timedelta(days=(pill_balance))).strftime("%Y-%m-%d %H:%M:%S")
                        break
                    
                    if concept_id == 164989:
                        obs_info.value_datetime = row['obs_datetime'].strftime("%Y-%m-%d %H:%M:%S")
                        break
                    
                counter += 1

                obs_info_list.append(obs_info)

    except Error as e:
        print("MySQL Error:", e)
    finally:
        cursor.close()

    return obs_info_list


# To create the care_obs data mapping each attribute of obs_info instance (ObsInfo) with a value and append them to form list.
def care_obs_list(connection, ids):
    obs_info_list = []

    cursor = connection.cursor(dictionary=True)
    cursor2 = connection.cursor(dictionary=True)
    try:
        
        for patient_id in ids:

             # Mapping of all the conccepts in a list.
            concepts = [165050, 5089, 5090, 5085, 5086, 1342, 165039, 5356, 1659, 167126, 165771, 165708, 164506, 165290, 165069, 161652, 5096]

            for concept_id in concepts:

                if concept_id == 165050 and check_patient_sex(connection, patient_id) != "F":
                    continue

                query = """ SELECT max(encounter_id) as encounter_id, patient_id, max(encounter_datetime) AS obs_datetime, 
                                 max(date_created) AS date_created, uuid() AS uuid  FROM encounter
                                     WHERE form_id = 14 AND patient_id IN (SELECT patient_id FROM patient_identifier 
                                        WHERE identifier_type = 4 and identifier = %s)
                     """
                
                cursor.execute(query, (patient_id,))

                for row in cursor:
                    
                    obs_info = ObsInfo()
                    obs_info.person_id = row['patient_id']
                    obs_info.concept_id = concept_id
                    obs_info.encounter_id = row['encounter_id']
                    obs_info.obs_datetime = row['obs_datetime'].strftime("%Y-%m-%d %H:%M:%S")
                    obs_info.location_id = 14
                    obs_info.date_created = row['date_created'].strftime("%Y-%m-%d %H:%M:%S")
                    obs_info.uuid = row['uuid']
                    
                    # To loop through the concepts and match concept_id with value when True
                    if concept_id == 165050: 
                        obs_info.value_coded = 165047
                        break     

                    if concept_id == 5089:
                        obs_info.value_numeric = get_max_vitals(connection, patient_id, concept_id)
                        break
                    if concept_id == 5090:
                        obs_info.value_numeric = get_max_vitals(connection, patient_id, concept_id)
                        break
                    if concept_id == 5085:
                        obs_info.value_numeric = get_max_vitals(connection, patient_id, concept_id)
                        break
                    if concept_id == 5086:
                        obs_info.value_numeric = get_max_vitals(connection, patient_id, concept_id)
                        break
                    if concept_id == 1342:
                        if get_max_vitals(connection, patient_id, concept_id=5089) == None or get_max_vitals(connection, patient_id, concept_id=5090) == None:
                            obs_info.value_numeric = 0
                        else:
                            obs_info.value_numeric = round(get_max_vitals(connection, patient_id, concept_id=5089) / ((get_max_vitals(connection, patient_id, concept_id=5090) * 0.01) ** 2), 2)
                        break
                    if concept_id == 165039:
                        obs_info.value_coded = get_max_info(connection, patient_id, concept_id)
                        break
                    if concept_id == 5356:
                        obs_info.value_coded = get_max_info(connection, patient_id, concept_id)
                        break
                    if concept_id == 1659:
                        obs_info.value_coded = get_max_info(connection, patient_id, concept_id)
                        break
                    if concept_id == 167126:
                        obs_info.value_coded = 167128
                        break
                    if concept_id == 165771:
                        obs_info.value_coded = 1257 #get_max_info(connection, patient_id, concept_id)
                        break
                    if concept_id == 165708:
                        obs_info.value_coded = 164506 #get_max_info(connection, patient_id, concept_id)
                        break
                    if concept_id == 164506:
                        obs_info.value_coded = 165681 #get_max_info(connection, patient_id, concept_id)
                        break
                    if concept_id == 165290:
                        obs_info.value_coded = 165287 #get_max_info(connection, patient_id, concept_id)
                        break
                    if concept_id == 165069:
                        obs_info.value_coded = 165062
                        break
                    if concept_id == 161652:
                        obs_info.value_coded = 165287
                        break
                    if  concept_id == 5096:

                        query = """select value_datetime as next_apt from obs where  concept_id = 5096
                                            and encounter_id in (select encounter_id from encounter where form_id = 27) and person_id in 
                                                (select patient_id from patient_identifier where identifier_type = 4 and identifier = %s) 
                                                         order by obs_datetime desc limit 1"""

                        cursor2.execute(query, (patient_id,))
                        apt_date = cursor2.fetchone()
                        
                        obs_info.value_datetime = apt_date['next_apt'].strftime("%Y-%m-%d %H:%M:%S")
                        break
                
                obs_info_list.append(obs_info)
    

    except Error as e:
        print('MySQL Error:', e)
    finally:
        cursor.close()

    return obs_info_list


def get_max_vitals(connection, patient_id, concept_id):
    """To Fetch the last vital sign (Weight, Height) from patient last visit"""

    try:

        cursor = connection.cursor(dictionary=True)

        query = f""" SELECT  o.value_numeric as value_numeric from obs o 
                        inner join encounter e on(e.encounter_id=o.encounter_id and e.voided=0)
                            left join patient_identifier pi on pi.patient_id = o.person_id and  pi.identifier_type = 4 and pi.voided = 0
                                inner join patient p on pi.patient_id = p.patient_id and p.voided = 0
                                    where pi.identifier = %s and o.concept_id={concept_id} and o.voided = 0 ORDER BY person_id, o.obs_datetime DESC 
                                        LIMIT 1"""
        
        cursor.execute(query, (patient_id,))
        value = cursor.fetchone()

        if value is not None and 'value_numeric' in value:
            result = value['value_numeric']
            return int(result)
        else:
            return None

    except Error as e:
        print("MySQL Error:", e)
    finally:
        cursor.close()


def get_max_info(connection, patient_id, concept_id):
    """To Fetch the last vital sign (Functional status, WHO) from patient last visit"""

    try:

        cursor = connection.cursor(dictionary=True)

        query = f""" SELECT  o.value_coded as value_numeric from obs o 
                        inner join encounter e on(e.encounter_id=o.encounter_id and e.voided=0)
                            left join patient_identifier pi on pi.patient_id = o.person_id and  pi.identifier_type = 4 and pi.voided = 0
                                inner join patient p on pi.patient_id = p.patient_id and p.voided = 0
                                    where pi.identifier = %s and o.concept_id={concept_id} and o.voided = 0 ORDER BY person_id, o.obs_datetime DESC 
                                        LIMIT 1"""
        
        cursor.execute(query, (patient_id,))
        value = cursor.fetchone()

        if value is not None and 'value_numeric' in value:
            result = value['value_numeric']
            return int(result)
        else:
            return None

    except Error as e:
        print("MySQL Error:", e)
    finally:
        cursor.close()


# To create the obs data mapping each attribute of obs_info instance (ObsInfo) with a value and append them to form list.
def tracking_obs_list(connection:str, ids:dict) -> List[ObsInfo]:
    obs_info_list = []
    cursor = connection.cursor(dictionary = True)

    try:
        for patient_id, patient_data in ids.items():
                track_reason, verify_indication, track_date, who_attempt, mode_comm, person_contacted, default_reason, discontinue_care, discontinue_reason, discontinue_date, referred_service, return_date = patient_data


            # Mapping of all the conccepts in a list.
                concepts = [165460, 166138, 165461, 165778, 167221, 167222, 165902, 165463, 165464, 165465, 166139, 165466, 165467, 165586, 165470, 165469, 165775, 165776, 165459, 165777]


                counter = 0
                for concept_id in concepts:
                    if concept_id == 166138 and get_concept_definition(track_reason) != 5622:
                        continue
                    if concept_id == 166139 and get_concept_definition(default_reason) != 5622:
                        continue
                    if concept_id == 165470 and discontinue_reason == '':
                        continue
                    if concept_id == 165469 and discontinue_date == '':
                        continue
                    if concept_id == 165775 and return_date == '':
                        continue

                    query = """ SELECT max(encounter_id) as encounter_id, patient_id, max(encounter_datetime) AS obs_datetime, 
                                    max(date_created) AS date_created, uuid() AS uuid  FROM encounter
                                        WHERE form_id = 13 AND patient_id IN (SELECT patient_id FROM patient_identifier 
                                            WHERE identifier_type = 4 and identifier = %s)
                        """
                    
                    cursor.execute(query, (patient_id,))

                    # To loop through the concepts and match concept_id with value when True
                    for row in cursor:

                        obs_info = ObsInfo()
                        obs_info.person_id = row['patient_id']
                        obs_info.concept_id = concept_id
                        obs_info.encounter_id = row['encounter_id']
                        obs_info.obs_datetime = row['obs_datetime'].strftime("%Y-%m-%d %H:%M:%S")
                        obs_info.location_id = 14
                        obs_info.date_created = row['date_created'].strftime("%Y-%m-%d %H:%M:%S")
                        obs_info.uuid = row['uuid']


                        # Concepts IDs Condition Mapping when True
                        if concept_id == 165460: 
                            obs_info.value_coded = get_concept_definition(track_reason)
                            break
                        if concept_id == 166138:
                            obs_info.value_text = "Client Verification"
                            break
                        if concept_id ==  165461:
                            obs_info.value_datetime = last_appt_date(connection, patient_id).strftime("%Y-%m-%d %H:%M:%S")
                            break
                        if concept_id == 165778:
                            if next_appt_date(connection, patient_id) < row['obs_datetime']:
                                missed_appt_date = next_appt_date(connection, patient_id).strftime("%Y-%m-%d %H:%M:%S")
                                obs_info.value_datetime = missed_appt_date
                            else:
                                missed_appt_date = None
                                obs_info.value_datetime = missed_appt_date
                            break
                        if concept_id == 167221:
                            obs_info.value_coded = 1065
                            break
                        if concept_id == 167222:
                            obs_info.value_coded = get_concept_definition(verify_indication)
                            break
                        if concept_id == 165463:
                            obs_info.value_datetime = (datetime.strptime(track_date, "%d/%m/%Y")).strftime("%Y-%m-%d %H:%M:%S")
                            break
                        if concept_id == 165464:
                            obs_info.value_text = who_attempt
                            break
                        if concept_id == 165465:
                            obs_info.value_coded = get_concept_definition(mode_comm)
                            break
                        if concept_id == 165466:
                            obs_info.value_coded = get_concept_definition(person_contacted)
                            break
                        if concept_id == 165467:
                            obs_info.value_coded = get_concept_definition(default_reason)
                            break
                        if concept_id == 166139:
                            obs_info.value_text = "Client Verification"
                            break
                        if concept_id == 165586:
                            obs_info.value_coded = get_concept_definition(discontinue_care)
                            break


                        if concept_id == 165470: #Dicontinue Reason
                            obs_info.value_coded = get_concept_definition(discontinue_reason)
                            break
                        if concept_id == 165469: #Discontinue Date
                            obs_info.value_datetime = (datetime.strptime(discontinue_date, "%d/%m/%Y")).strftime("%Y-%m-%d %H:%M:%S")
                            break
                        if concept_id == 165776: #Referred_service
                            obs_info.value_coded = get_concept_definition(referred_service)
                            break
                        if concept_id == 165775: #Return Date
                            obs_info.value_datetime = (datetime.strptime(return_date, "%d/%m/%Y")).strftime("%Y-%m-%d %H:%M:%S")
                            break 


                        if concept_id == 165459:
                            obs_info.value_text = who_attempt
                            break
                        if concept_id == 165777:
                            obs_info.value_datetime = row['obs_datetime'].strftime("%Y-%m-%d %H:%M:%S")
                            break
                    
                    counter += 1

                    obs_info_list.append(obs_info)
              
    except Error as e:
        print("MySQL Error:", e)
    finally:
        cursor.close()

    return obs_info_list

# To create the obs data mapping each attribute of obs_info instance (ObsInfo) with a value and append them to form list.
def lab_obs_list(connection, ids):
    obs_info_list = []
    cursor = connection.cursor(dictionary = True)

    try:
        for patient_id in ids:
            concepts = []

            # Mapping of all the conccepts in a list.
            for concept_id in concepts:

                query = """ SELECT max(encounter_id) as encounter_id, patient_id, max(encounter_datetime) AS obs_datetime, 
                                    max(date_created) AS date_created, uuid() AS uuid  FROM encounter
                                        WHERE form_id = 13 AND patient_id IN (SELECT patient_id FROM patient_identifier 
                                            WHERE identifier_type = 4 and identifier = %s)
                        """
                
                cursor = cursor.execute(query, (patient_id,))
                
                # To loop through the concepts and match concept_id with value when True
                for row in cursor:

                    obs_info = ObsInfo()
                    obs_info.person_id = row['patient_id']
                    obs_info.concept_id = concept_id
                    obs_info.encounter_id = row['encouter_id']
                    obs_info.obs_datetime = row['obs_datetime'].strftime("%Y-%m-%d %H:%M:%S")
                    obs_info.location_id = 14
                    obs_info.date_created = row['date_created'].strftime("%Y-%m-%d %H:%M:%S")
                    obs_info.uuid = row['uuid']


                    # Concepts IDs Condition Mapping when True







                obs_info_list.append(obs_info)

    

    except Error as e:
        print("MySQL Error", e)
    finally:
        cursor.close()

    return obs_info_list



# Dictionary of concept names and concept IDs
def get_concept_definition(concept_name):

    try:
        concept_dcitionary = {
            'Yes' : 1065,
            'No' : 1066,
            'Other': 5622,
            'Others(Specify)' : 5622,
            'Couple testing' : 165789,
            'Missed Appointment' : 165462,
            'Missed Pharmacy Refill' : 165473,
            'Consistently had drug pickup by proxy without viral load sample collection for two quarters' : 167223,
            'duplicated demographic and clinical variables' : 167224,
            'No biometrics recapture' : 167225,
            'Batched ARV pickup dates' : 167226,
            'Last clinical visit is over 18 months prior' : 167227,
            'Batched ART start and pickup dates' : 167228,
            'No initial biometric capture' : 167229,
            'Mobile Phone' : 1650,
            'Home Visit' : 165791,
            'Patient' : 162571,
            'Guardian' : 160639,
            'Treatment Supporter' : 161642,
            'No transport fare' : 1737,
            'Transferred to new site' : 159492,
            'Forgot' : 162192,
            'Felt better' : 160586,
            'Not permitted to leave work' : 165896,
            'Lost appointment card' : 165897,
            'Still had drugs' : 165898,
            'Taking herbal treatment' : 165899,
            'Could not verify client' : 167231,
            'Duplicate Record' : 167230,
            'Death' : 165889,
            'Transferred out to another facility' : 159492,
            'Discontinued care' : 165916,
            'Lost_to_followup' : 5240,
            'Adhrence Councelling' : 5488
        }

        for concept_key, concept_value in concept_dcitionary.items():
            if concept_key == concept_name:
                concept_id = concept_value

                return concept_id

    except Error as e:
        print("Function Error:", e)


def last_appt_date(connection, patient_id):
    try:
        cursor = connection.cursor(dictionary = True)

        query = f"""select encounter_datetime from encounter where patient_id in (
                            select patient_id from patient_identifier where identifier = %s and 
                                    identifier_type = 4)order by encounter_datetime desc limit 1,1
                       """
        cursor.execute(query, (patient_id,))
                       
        value = cursor.fetchone()
        last_appt_date = value['encounter_datetime']
    except Error as e:
        print("MySQL Error:", e)
    finally:
        cursor.close()

    return last_appt_date


def next_appt_date(connection:str, patient_id:str):
    try:
        cursor = connection.cursor(dictionary = True)

        query = f"""select value_datetime from obs where concept_id = 5096 and person_id in (
                                select patient_id from patient_identifier where identifier = %s and 
                                        identifier_type = 4) order by obs_datetime desc limit 1
                       """
        cursor.execute(query, (patient_id, ))

        value = cursor.fetchone()
        next_appt_date = value['value_datetime']
    except Error as e:
        print("MySQL Error:", e)
    finally:
        cursor.close()

    return next_appt_date


# To update the obs table with the obs_group_id for arv
def update_arv_group_id(connection, ids): 
    
    try:
        for patient_id in ids:
            cursor = connection.cursor(dictionary=True)
            
            # To pull the group_id and person_id from the dB using nested select statement
            query = ("""SELECT max(obs_id) as group_id, person_id from obs where concept_id = 162240 
                                AND person_id in (select patient_id from patient_identifier where identifier_type = 4 and identifier = %s)""")
            
            cursor.execute(query, (patient_id,))
            for row in cursor:
                group_id = row['group_id']
                patient_id = row['person_id']
                if group_id is not None:

                    # Update of the obs records using place holders to accept value within the query statement
                    cursor.execute(f"UPDATE obs set obs_group_id = {group_id} where person_id = {patient_id} and obs_id > {group_id} and concept_id in (160856, 166120, 165724, 165725, 167209, 159368, 165723, 167218, 1443) LIMIT 9")
                    connection.commit()

    except Error as e:
        print("MySQL Error:", e)
        
    finally:
        cursor.close()


# To update the obs table with the obs_group_id for ctx
def update_ctx_group_id(connection, ids): 
    
    try:
        for patient_id in ids:
            cursor = connection.cursor(dictionary=True)
            
            # To pull the group_id and person_id from the dB using nested select statement
            query = ("""SELECT max(obs_id) as group_id, person_id from obs where concept_id = 165726 
                                AND person_id in (select patient_id from patient_identifier where identifier_type = 4 and identifier = %s)""")
            
            cursor.execute(query, (patient_id,))
            for row in cursor:
                group_id = row['group_id']
                patient_id = row['person_id']
                if group_id is not None:

                    # Update of the obs records using place holders to accept value within the query statement
                    cursor.execute(f"UPDATE obs set obs_group_id = {group_id} where person_id = {patient_id} and obs_id > {group_id} and concept_id in (165727, 160856, 165723, 167218, 1443, 165725, 167209, 159368) ORDER BY obs_id DESC LIMIT 8")
                    connection.commit()

    except Error as e:
        print("MySQL Error:", e)
        
    finally:
        cursor.close()


# To update the obs table with the obs_group_id for tracking attempt
def update_tracking_group_id(connection, ids): 
    
    try:
        for patient_id in ids:
            cursor = connection.cursor(dictionary=True)
            
            # To pull the group_id and person_id from the dB using nested select statement
            query = ("""SELECT max(obs_id) as group_id, person_id from obs where concept_id = 165902 
                                AND person_id in (select patient_id from patient_identifier where identifier_type = 4 and identifier = %s)""")
            
            cursor.execute(query, (patient_id,))
            for row in cursor:
                group_id = row['group_id']
                patient_id = row['person_id']
                if group_id is not None:

                    # Update of the obs records using place holders to accept value within the query statement
                    cursor.execute(f"UPDATE obs set obs_group_id = {group_id} where person_id = {patient_id} and obs_id > {group_id} and concept_id in (165465, 165463, 165466, 165464, 165467, 166139) LIMIT 6")
                    connection.commit()

    except Error as e:
        print("MySQL Error:", e)
        
    finally:
        cursor.close()
        

# To insert the obs data into the obs table using the commit method.
def insert_obs_data(connection, obs_info): 
    try:
        cursor = connection.cursor()
        cursor.execute("""
                INSERT INTO obs (person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, 
                       accession_number, value_group_id, value_coded, value_coded_name_id, value_drug, value_datetime, 
                            value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, 
                                voided_by, date_voided, void_reason, uuid, previous_version, form_namespace_and_path, status, interpretation
                       
                ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
            obs_info.person_id, obs_info.concept_id, obs_info.encounter_id, obs_info.order_id,obs_info.obs_datetime, obs_info.location_id, 
            obs_info.obs_group_id, obs_info.accession_number, obs_info.value_group_id, obs_info.value_coded, obs_info.value_coded_name_id, 
            obs_info.value_drug, obs_info.value_datetime, obs_info.value_numeric, obs_info.value_modifier, obs_info.value_text, obs_info.value_complex,
            obs_info.comments, obs_info.creator, obs_info.date_created, obs_info.voided, obs_info.voided_by, obs_info.date_voided,
            obs_info.void_reason, obs_info.uuid, obs_info.previous_version, obs_info.form_namespace_and_path, obs_info.status, obs_info.interpretation
            ))
        connection.commit()

    except Error as e:
        print("MySQL Error:", e)
    finally:
        cursor.close()
