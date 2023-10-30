import pandas as pd
import csv
from FlightRadar24 import FlightRadar24API
fr_api = FlightRadar24API()
import os
from datetime import datetime
from BRONZE import DataRetriever

data_retriever = DataRetriever(fr_api)  # Create an instance of DataRetriever

class DataFrame: 
    def datafame_data(self, data_type) :
        data = data_retriever.get_data(data_type)
        
        if data_type == "flights":
            data_save = []
            item = data[0]  # Take the first flight as an example
            columns= [attr for attr in dir(item) if not callable(getattr(item, attr)) and not attr.startswith('_')]
            for item in data:
                data_dict = {attr: getattr(item, attr) for attr in columns}
                data_save.append(data_dict)
            df = pd.DataFrame(data_save)
        elif  data_type == "airports" :
            data_save = []
            item = data[0]  # Take the first flight as an example
            columns= [attr for attr in dir(item) if not callable(getattr(item, attr)) and not attr.startswith('_')]
            for item in data:
                data_dict = {attr: getattr(item, attr) for attr in columns}
                 # Handle conversion to float for the "altitude" column
                data_save.append(data_dict)
            df = pd.DataFrame(data_save)
                
        elif data_type == "airlines":
            df= pd.DataFrame(data)
            
        elif data_type == "zones" : 
            rows=[]
            for zone, details in data.items():
                row = {'Zone': zone, 'tl_y': details['tl_y'], 'tl_x': details['tl_x'], 'br_y': details['br_y'], 'br_x': details['br_x']}
                if 'subzones' in details:
                    for subzone, coords in details['subzones'].items():
                        sub_row = row.copy()
                        sub_row.update({'Subzone': subzone, 'tl_y': coords['tl_y'], 'tl_x': coords['tl_x'], 'br_y': coords['br_y'], 'br_x': coords['br_x']})
                        rows.append(sub_row)
                else:
                    row.update({'Subzone': None})
                    rows.append(row)

            # Create the DataFrame
            df = pd.DataFrame(rows)
        return df
            
    def dataframe_distance(self, airports,flights):
        airport_distance_details = data_retriever.get_distance("flights", "airports")
            # Convert the list of dictionaries to a DataFrame
        df = pd.DataFrame(airport_distance_details)   
        return df
    
dataframe_instance = DataFrame()   

class DataTransformer: 
        
    def transform_data(self,data_type): 
        df = dataframe_instance.datafame_data(data_type)
        if data_type == "flights":
            # Rename columns for flights
            df_transform = df.rename(columns= {"icao_24bit":"ID_aircraft", "id":"flight_ID", "number": "flight_number","registration": "number_registration_aircraft"})
        elif data_type == "airlines":
            # Rename columns for airlines
            df_transform = df.rename(columns= {"ICAO":"airline_icao", "code":"airline_iata", "Name":"airline_name"})
        elif data_type == "airports":
            # Rename columns for airports
            df_transform = df.rename(columns= {"iata": "airport_iata" ,"icao": "airport_icao", "name":"airport_name"})
            df_transform= df_transform.drop(columns= ["altitude"], axis=1)
        elif data_type == "zones":
            # Rename columns for zones
            df_transform = df.rename(columns= {"Zone": "Continent" ,"Subzone": "Country"})
    
        return df_transform
      
    def transform_distance(self, data1, data2) :
        df_transform = dataframe_instance.dataframe_distance(data1,data2)
            
        return df_transform 


transform= DataTransformer()

class Save_data_transform :
    def save_to_parquet(self, data_type):
        df= transform.transform_data(data_type)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        directory_path = "./silver_container" 
        folder_name = f"{data_type}_{timestamp}.parquet"
        file_path = os.path.join(directory_path, folder_name)
        os.makedirs(directory_path, exist_ok=True)
        # Save the DataFrame to a Parquet file
        df.to_parquet(file_path, index=False)

        #return file_path

    def save_distance_transform_to_parquet(self, data1, data2) :
        df= transform.transform_distance(data1, data2)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        directory_path = "./silver_container" 
        folder_name = f"distance_{timestamp}.parquet"
        file_path = os.path.join(directory_path, folder_name)
        # Save the DataFrame to a Parquet file
        os.makedirs(directory_path, exist_ok=True)
        df.to_parquet(file_path, index=False)

        #return file_path
