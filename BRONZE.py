import json
import csv
import pyarrow
import os
from datetime import datetime
from FlightRadar24 import FlightRadar24API
fr_api = FlightRadar24API()


class DataRetriever:
    def __init__(self, api):
        self.api = api

    def get_data(self, data_type):
        try:
            if data_type == "flights":
                return self.api.get_flights()
            elif data_type == "airports":
                return self.api.get_airports()
            elif data_type == "airlines":
                return self.api.get_airlines()
            elif data_type == "zones":
                return self.api.get_zones()
            else:
                raise ValueError("Invalid data type")
        except Exception as e:
            print(f"Error while fetching data: {e}")
            return None

    def get_flight_details(self, flight):
        return self.api.get_flight_details(flight)

    def get_airport_details(self, airport_icao):
        return self.api.get_airport_details(airport_icao)

    def get_distance(self, flights, airports):
        
        airport_distance_details = []
        flights= self.api.get_flights()
        airports =  self.api.get_airports()
        for airport in airports:
            for flight in flights:
                distance = flight.get_distance_from(airport)
                details = {
                            "Flightnumber": str(flight.number),  
                            "Airport_icao": airport.icao,  # Or airport.name 
                            "Distance": distance       # The distance value
                            }

                airport_distance_details.append(details)  
                
        return airport_distance_details

data_retriever = DataRetriever(fr_api)

class savedataraw:
    def save_raw(self, data_type):
        data = data_retriever.get_data(data_type)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        directory_path = "./Bronze_contenair"
        folder_name = f"{data_type}_{timestamp}.csv"
        file_path = os.path.join(directory_path, folder_name)
# Ensure the directory exists or create it


        os.makedirs(directory_path, exist_ok=True)
        if data_type == "flights" or data_type == "airports" :
            fieldnames = data[0].__dict__.keys()
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for item in data:
                    writer.writerow(item.__dict__)

        elif data_type == "airlines" :
            column_names = list(data[0].keys())
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=column_names, delimiter=',')
                writer.writeheader()
                for item in data:
                    writer.writerow(item)

        elif data_type == "zones" :
            folder_name = f"{data_type}_{timestamp}.json"
            file_path = os.path.join(directory_path, folder_name)
            with open(file_path, 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)

        #return file_path
    def save_distance (self, data1, data2):
        data = data_retriever.get_distance(data1 , data2)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        directory_path = "./Bronze_contenair"
        folder_name = f"distance_{timestamp}.csv"
        os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, folder_name)
        column_names = list(data[0].keys())
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=column_names, delimiter=',')
            writer.writeheader()
            for item in data:
                writer.writerow(item)
        #return file_path