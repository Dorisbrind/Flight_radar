import pandas as pd
import csv
from FlightRadar24 import FlightRadar24API
fr_api = FlightRadar24API()
from SILVER import DataTransformer
import os
from datetime import datetime
transform = DataTransformer()

class cleaning_data:        
    def clean_data(self,data_type):        
        df =transform.transform_data(data_type)
        
        if data_type == "flights":
            column_drop = ["altitude", "callsign", "ground_speed", "heading",
                    "latitude", "longitude", "squawk","time","vertical_speed"]
            df_flights = df.drop(columns=column_drop, axis=1)
            
            df_clean= df_flights.applymap(lambda x: None if x == "N/A" else x)
            df_clean = df_clean.dropna(subset=["flight_number"])

               
        elif data_type == "airlines" :
            df_clean= df  
        elif data_type == "airports":
            df_clean = df.drop(columns=["latitude","longitude"], axis=1)          
        elif data_type == "zones":
            columns_to_drop = ["tl_y", "tl_x", "br_y", "br_x"]
            df_clean = df.drop(columns=columns_to_drop, axis=1)
    
        return df_clean
    
    def clean_distance(self, data1, data2) :
        df = transform.transform_distance(data1,data2)
        df_clean =df
        
        return  df_clean
    
clean= cleaning_data()

class Save_data_gold_transform :
    def save_data_to_parquet(self, data_type):
        df= clean.clean_data(data_type)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        directory_path = "./Gold_container" 
        folder_name = f"{data_type}_{timestamp}.parquet"
        os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, folder_name)
        # Save the DataFrame to a Parquet file
        df.to_parquet(file_path, index=False)

       

    def save_distance_to_parquet(self, data1, data2) :
        df= clean.clean_distance(data1, data2)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        directory_path = "./Gold_container" 
        folder_name = f"distance_{timestamp}.parquet"
        os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, folder_name)
        # Save the DataFrame to a Parquet file
        df.to_parquet(file_path, index=False)

        
