# ///////// Pharmacy Form Creation Program using Python ///////// #
# ////////  Created By Onyedikachi Ezeobi on 8th December 2023 //////// #


import mysql.connector as sql
import random
import csv
import datetime
from mysql.connector import Error
from datetime import datetime, timedelta
from tqdm import tqdm
from generic_funtions import visit, encounter, provider, obs



def connect_to_database(): 
    "To create a connection string to the database using the correct credentials."
    try:
        connection = sql.connect(
            host='localhost',
            user='admin',
            password='Admin123',
            database='st_anthony_aro'
        )
        if connection.is_connected():
            return connection
        else:
            print("Connection failed.")
            return None
    except Error as e:
        print("Error:", e)
        return None



# To read patients_id and visit date from CSV to form a dictionary of keyword arguments (**kwargs)
def read_patient_ids_from_csv(csv_file): 
    patient_ids = {}

    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)

        for row in reader:
            key = row[0]
            value = row[1]
            patient_ids[key] = value

    return patient_ids


# This is the Base Function that starts the execution of the program.
def main():

    connection = connect_to_database()

    if connection:
        try:
            # Point to your CSV file
            csv_file = 'pharmacy_test.csv'

            # Read patient IDs from the CSV file
            patient_ids = read_patient_ids_from_csv(csv_file)

            # Record the start time
            current_time = datetime.now() - timedelta(days=0)
            pharmacy_forms_created = 0

            # Create a tqdm progress bar
            progress_bar = tqdm(patient_ids.items(), desc="Processing Pharmacy and Care Card Data")

            # Iteration of the Dictionary to access key and value of each patients records.
            for patient_id, visit_date in progress_bar:
            
                visit_data = visit.visit_list(connection, {patient_id: visit_date})
                delay_minutes=random.uniform(8, 12)
                date_created = (current_time + timedelta(minutes=delay_minutes)).strftime("%Y-%m-%d %H:%M:%S")
                
                for visit_info in visit_data:
                    delay_seconds = random.uniform(8, 12)
                    current_time += timedelta(seconds=delay_seconds)

                    visit_info.date_created = current_time.strftime("%Y-%m-%d %H:%M:%S")
                    visit.insert_visit_data(connection, visit_info)



                # To Create Pharmacy Form using same Visit ID
                pharm_encounter_data = encounter.pharm_encounter_list(connection, [patient_id])           
                for encounter_info in pharm_encounter_data:
                    encounter.insert_encounter_data(connection, encounter_info)

                provider_data = provider.encounter_provider(connection, [patient_id])
                for provider_info in provider_data:
                    provider.insert_encounter_provider_data(connection, provider_info)   

                pharmacy_obs_data = obs.pharmacy_obs_list(connection, [patient_id])
                for observation_info in pharmacy_obs_data:
                    obs.insert_obs_data(connection, observation_info)

                obs.update_arv_group_id(connection, [patient_id])
                obs.update_ctx_group_id(connection, [patient_id])

                

                # To Create Care Card using same Visit ID 
                care_encounter_data = encounter.care_encounter_list(connection, [patient_id])
                for encounter_info in care_encounter_data:
                    encounter.insert_encounter_data(connection, encounter_info)

                provider_data = provider.encounter_provider(connection, [patient_id])
                for provider_info in provider_data:
                    provider.insert_encounter_provider_data(connection, provider_info)    

                care_obs_data = obs.care_obs_list(connection, [patient_id])
                for  observation_info in care_obs_data:
                    obs.insert_obs_data(connection, observation_info)


               
                pharmacy_forms_created +=1
                current_time = datetime.strptime(date_created, "%Y-%m-%d %H:%M:%S")

            progress_bar.set_postfix(Patients_Processed=pharmacy_forms_created)

            print(f"Number of patients that Pharmacy Form and Care Card was Created: {pharmacy_forms_created}")

        except Error as e:
            print("Error:", e)
        finally:
            connection.close()
   

if __name__ == "__main__":
    main()