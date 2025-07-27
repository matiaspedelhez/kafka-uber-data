import uuid
import datetime
import time
import random
import json
import math
import os
from config import config
from kafka import KafkaProducer 

# --- Configuration and Helper Functions ---

EARTH_RADIUS_KM = 6371 
POIS_FOLDER = "data"   

# --- Kafka Configuration ---
KAFKA_BROKER = config['kafka_broker'] 
KAFKA_TOPIC = config['kafka_topic'] 

def load_pois_from_folder(folder_path):
    """
    Loads all Points of Interest (POIs) from JSON files in the specified folder.
    Returns a dictionary where the key is the province name (e.g., 'corrientes')
    and the value is a list of its POIs.
    """
    all_pois = {}
    if not os.path.exists(folder_path):
        print(f"Error: POI folder '{folder_path}' does not exist. Creating it...")
        os.makedirs(folder_path)
        return all_pois

    for filename in os.listdir(folder_path):
        if filename.startswith("pois_") and filename.endswith(".json"):
            province_name = filename[len("pois_"): -len(".json")]
            filepath = os.path.join(folder_path, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    pois_data = json.load(f)
                    if isinstance(pois_data, list):
                        all_pois[province_name] = pois_data
                        print(f"Loaded {len(pois_data)} POIs for province: {province_name}")
                    else:
                        print(f"Warning: File {filename} does not contain a list of POIs.")
            except json.JSONDecodeError:
                print(f"Error: Could not decode JSON from {filename}.")
            except Exception as e:
                print(f"Error reading file {filename}: {e}")
    return all_pois

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculates the Haversine distance between two points in kilometers."""
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad

    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = EARTH_RADIUS_KM * c
    return distance

def interpolate_route(start_lat, start_lon, end_lat, end_lon, steps=100):
    """
    Generates a simplified route by linearly interpolating between two points.
    This simulates a path without considering actual streets.
    """
    route = []
    for i in range(steps + 1):
        fraction = i / steps
        lat = start_lat + fraction * (end_lat - start_lat)
        lon = start_lon + fraction * (end_lon - start_lon)
        route.append({"latitude": lat, "longitude": lon})
    return route

# --- UberVehicle Class ---

class UberVehicle:
    def __init__(self, vehicle_id, province, all_pois):
        self.vehicleId = vehicle_id
        self.province = province
        self.province_pois = all_pois.get(province, [])
        
        if not self.province_pois:
            raise ValueError(f"No POIs found for province '{province}'. Ensure 'pois_{province}.json' exists and is correctly formatted in the '{POIS_FOLDER}' folder.")

        self.tripId = None
        self.vehicleState = "AVAILABLE"
        self.currentPassengers = 0
        self.speedKPH = 0
        self.orientationDegrees = 0
        self.fuelPercentage = random.randint(70, 100)
        self.predictedDestination = None
        self.kilometersRemainingTrip = 0
        self.estimatedTimeRemainingMinutes = 0

        self.planned_route = []
        self.route_index = 0

        initial_poi = random.choice(self.province_pois)
        self.currentLocation = {"latitude": initial_poi["latitude"], "longitude": initial_poi["longitude"]}
        print(f"Vehicle {self.vehicleId} ({self.province}) initialized at {initial_poi['name']} ({self.currentLocation['latitude']:.4f}, {self.currentLocation['longitude']:.4f})")


    def assign_trip(self):
        """
        Assigns a new trip to the vehicle, restricted to its province,
        ensuring the destination is not the current location and has a minimum length.
        """
        if not self.province_pois:
            print(f"Vehicle {self.vehicleId} ({self.province}): No POIs available to assign a trip.")
            self.vehicleState = "AVAILABLE" # Vehicle stays available
            self.tripId = None
            self.currentPassengers = 0
            return

        self.tripId = str(uuid.uuid4())
        self.vehicleState = "IN_TRIP"
        self.currentPassengers = random.randint(1, 4)

        current_lat = self.currentLocation["latitude"]
        current_lon = self.currentLocation["longitude"]
        
        MIN_TRIP_DISTANCE_KM = 0.5 
        
        available_destinations = [
            p for p in self.province_pois 
            if haversine_distance(current_lat, current_lon, p["latitude"], p["longitude"]) >= MIN_TRIP_DISTANCE_KM 
        ]
        
        if not available_destinations:
            print(f"Warning: Vehicle {self.vehicleId} ({self.province}) cannot find a suitable destination within its province. All available POIs are too close or too far for a valid trip.")
            self.vehicleState = "AVAILABLE"
            self.tripId = None
            self.currentPassengers = 0
            return

        destination_poi = random.choice(available_destinations)
        self.predictedDestination = {"latitude": destination_poi["latitude"], "longitude": destination_poi["longitude"]}
        
        initial_trip_distance = haversine_distance(
            self.currentLocation["latitude"], self.currentLocation["longitude"],
            self.predictedDestination["latitude"], self.predictedDestination["longitude"]
        )

        min_route_steps = 20 
        steps_per_km = 100   
        steps = max(min_route_steps, int(initial_trip_distance * steps_per_km)) 
        
        self.planned_route = interpolate_route(
            self.currentLocation["latitude"], self.currentLocation["longitude"],
            self.predictedDestination["latitude"], self.predictedDestination["longitude"],
            steps=steps
        )
        self.route_index = 0
        
        self.speedKPH = random.randint(30, 80)

        self.kilometersRemainingTrip = haversine_distance(
            self.currentLocation["latitude"], self.currentLocation["longitude"],
            self.predictedDestination["latitude"], self.predictedDestination["longitude"]
        )
        if self.speedKPH > 0:
            self.estimatedTimeRemainingMinutes = (self.kilometersRemainingTrip / self.speedKPH) * 60
        else:
            self.estimatedTimeRemainingMinutes = 0

        print(f"Vehicle {self.vehicleId} ({self.province}) assigned to new trip ({self.tripId}) to {destination_poi['name']} (Dist: {self.kilometersRemainingTrip:.2f} km, Steps: {len(self.planned_route)}).")


    def move(self):
        """Moves the vehicle along its planned route."""
        if self.vehicleState == "IN_TRIP" and self.route_index < len(self.planned_route) - 1:
            distance_can_travel_km = self.speedKPH * (1 / 3600) 

            next_target_index = self.route_index + 1 
            
            if next_target_index >= len(self.planned_route):
                self.route_index = len(self.planned_route) - 1
                self.currentLocation = self.planned_route[self.route_index]
            else:
                target_lat = self.planned_route[next_target_index]["latitude"]
                target_lon = self.planned_route[next_target_index]["longitude"]
                
                dist_to_next_route_point = haversine_distance(
                    self.currentLocation["latitude"], self.currentLocation["longitude"],
                    target_lat, target_lon
                )

                if dist_to_next_route_point <= distance_can_travel_km:
                    self.route_index = next_target_index 
                    self.currentLocation = self.planned_route[self.route_index]
                else:
                    fraction_of_segment = distance_can_travel_km / dist_to_next_route_point
                    
                    new_lat = self.currentLocation["latitude"] + fraction_of_segment * (target_lat - self.currentLocation["latitude"])
                    new_lon = self.currentLocation["longitude"] + fraction_of_segment * (target_lon - self.currentLocation["longitude"])
                    self.currentLocation = {"latitude": new_lat, "longitude": new_lon}

            if len(self.planned_route) > 1 and self.route_index > 0:
                if self.route_index < len(self.planned_route) - 1 and \
                   haversine_distance(self.currentLocation["latitude"], self.currentLocation["longitude"], 
                                      self.planned_route[self.route_index + 1]["latitude"], 
                                      self.planned_route[self.route_index + 1]["longitude"]) > 1e-6:
                    ref_lat = self.planned_route[self.route_index + 1]["latitude"]
                    ref_lon = self.planned_route[self.route_index + 1]["longitude"]
                else:
                    ref_lat = self.planned_route[self.route_index]["latitude"]
                    ref_lon = self.planned_route[self.route_index]["longitude"]
                    if self.route_index > 0:
                        prev_lat = self.planned_route[self.route_index - 1]["latitude"]
                        prev_lon = self.planned_route[self.route_index - 1]["longitude"]
                        
                        delta_lon_prev = ref_lon - prev_lon
                        delta_lat_prev = ref_lat - prev_lat
                        if abs(delta_lat_prev) > 1e-7 or abs(delta_lon_prev) > 1e-7:
                            self.orientationDegrees = (math.degrees(math.atan2(delta_lon_prev, delta_lat_prev)) + 360) % 360
                    
            self.fuelPercentage = max(0, self.fuelPercentage - 0.01)

            if self.predictedDestination:
                self.kilometersRemainingTrip = haversine_distance(
                    self.currentLocation["latitude"], self.currentLocation["longitude"],
                    self.predictedDestination["latitude"], self.predictedDestination["longitude"]
                )
                if self.speedKPH > 0:
                    self.estimatedTimeRemainingMinutes = (self.kilometersRemainingTrip / self.speedKPH) * 60
                else:
                    self.estimatedTimeRemainingMinutes = 0
            
            self.speedKPH += random.uniform(-1, 1)
            self.speedKPH = max(10, min(120, self.speedKPH))
        
        elif self.vehicleState == "AVAILABLE":
            self.speedKPH = 0
            self.orientationDegrees = 0
            self.currentPassengers = 0
            self.kilometersRemainingTrip = 0
            self.estimatedTimeRemainingMinutes = 0

    def update_state(self):
        """Updates the overall state of the vehicle at each 'tick'."""
        if self.vehicleState == "AVAILABLE":
            if random.random() < 0.05:
                if self.fuelPercentage < 20:
                    print(f"Vehicle {self.vehicleId} ({self.province}): Low fuel ({self.fuelPercentage:.1f}%). Entering PAUSED state for refueling.")
                    self.vehicleState = "PAUSED"
                else:
                    self.assign_trip()
        
        elif self.vehicleState == "IN_TRIP":
            self.move()
            ARRIVAL_THRESHOLD_KM = 0.05 
            if self.predictedDestination and self.kilometersRemainingTrip < ARRIVAL_THRESHOLD_KM:
                print(f"Vehicle {self.vehicleId} ({self.province}) has arrived at its destination. Trip {self.tripId} finished.")
                self.vehicleState = "AVAILABLE"
                self.currentPassengers = 0
                self.predictedDestination = None
                self.planned_route = []
                self.route_index = 0
                self.speedKPH = 0
                self.orientationDegrees = 0
                self.kilometersRemainingTrip = 0
                self.estimatedTimeRemainingMinutes = 0
            
            if self.fuelPercentage <= 0:
                print(f"Vehicle {self.vehicleId} ({self.province}): Out of fuel during trip! Entering OUT_OF_SERVICE state.")
                self.vehicleState = "OUT_OF_SERVICE"
                self.speedKPH = 0
        
        elif self.vehicleState == "PAUSED":
            self.fuelPercentage = min(100, self.fuelPercentage + random.uniform(0.5, 2))
            if self.fuelPercentage >= 95:
                print(f"Vehicle {self.vehicleId} ({self.province}): Fuel recharged. Returning to AVAILABLE.")
                self.vehicleState = "AVAILABLE"
        
        elif self.vehicleState == "OUT_OF_SERVICE":
            if self.fuelPercentage > 10:
                if random.random() < 0.01:
                    print(f"Vehicle {self.vehicleId} ({self.province}): Repaired/Rescued. Returning to AVAILABLE.")
                    self.vehicleState = "AVAILABLE"
            else:
                self.fuelPercentage = min(100, self.fuelPercentage + random.uniform(0.1, 0.5))
                if self.fuelPercentage > 50:
                    print(f"Vehicle {self.vehicleId} ({self.province}): Fuel assistance received. Returning to AVAILABLE.")
                    self.vehicleState = "AVAILABLE"

    def generate_json_state(self):
        """Generates the JSON with the current vehicle state."""
        data = {
            "vehicleId": self.vehicleId,
            "province": self.province,
            "tripId": self.tripId,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "currentLocation": self.currentLocation,
            "vehicleState": self.vehicleState,
            "currentPassengers": self.currentPassengers,
            "speedKPH": round(self.speedKPH, 2),
            "orientationDegrees": round(self.orientationDegrees, 2),
            "fuelPercentage": round(self.fuelPercentage, 2),
            "predictedDestination": self.predictedDestination,
            "kilometersRemainingTrip": round(self.kilometersRemainingTrip, 2),
            "estimatedTimeRemainingMinutes": round(self.estimatedTimeRemainingMinutes, 2)
        }
        return json.dumps(data, indent=None)

# --- Main Simulation Function ---

def simulate_vehicles_in_real_time(vehicles_per_province, kafka_producer, duration_seconds=60):
    """
    Simulates vehicle movement, publishing their state to Kafka every second.
    Args:
        vehicles_per_province (dict): A dictionary in the format
                                        {'province_name': num_vehicles, ...}
        kafka_producer (KafkaProducer): An initialized kafka-python Producer instance.
        duration_seconds (int): Total duration of the simulation in seconds.
    """
    all_pois = load_pois_from_folder(POIS_FOLDER)
    if not all_pois:
        print("Critical Error! No POIs loaded. Ensure you have JSON files in the 'data' folder.")
        return

    vehicles = []
    total_vehicles = 0
    
    for province, num_vehicles in vehicles_per_province.items():
        if province not in all_pois:
            print(f"Warning: No POIs loaded for province '{province}'. No vehicles will be created for this province.")
            continue
        for _ in range(num_vehicles):
            vehicle_id = str(uuid.uuid4())
            vehicles.append(UberVehicle(vehicle_id, province, all_pois))
            total_vehicles += 1

    if not vehicles:
        print("No vehicles were created for the simulation. Check your province and POI configuration.")
        return

    print(f"\nStarting simulation for {total_vehicles} vehicle(s) across {len(vehicles_per_province)} province(s) for {duration_seconds} seconds...")
    print("--------------------------------------------------")

    start_time = time.time()
    while (time.time() - start_time) < duration_seconds:
        for vehicle in vehicles:
            vehicle.update_state()
            vehicle_state_json = vehicle.generate_json_state()
            
           # fire-and-forget for high throughput.
            kafka_producer.send(KAFKA_TOPIC, key=vehicle.vehicleId.encode('utf-8'), value=vehicle_state_json.encode('utf-8'))
            
            # Debug
            # print(vehicle_state_json) 
        
        time.sleep(1) # Pause for 1 second

    print("--------------------------------------------------")
    print(f"Simulation finished after {duration_seconds} seconds.")
    kafka_producer.flush() # Ensure all messages are sent before exiting

if __name__ == "__main__":
    # --- STEP 1: Ensure Kafka Broker is Running ---
    # Before running this script, make sure your Kafka broker is accessible at KAFKA_BROKER (e.g., localhost:9092).
    # Create the topic if it doesn't exist.

    # --- Initialize Kafka Producer (kafka-python) ---
    # value_serializer converts the Python object (our JSON string) to bytes
    # key_serializer converts the key (vehicle ID) to bytes
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER.split(','), 
            value_serializer=lambda v: v.encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8'),
            acks='all', 
            retries=5  
        )
        print(f"Kafka Producer (kafka-python) initialized for broker(s): {KAFKA_BROKER}")
    except Exception as e:
        print(f"Failed to initialize Kafka Producer: {e}")
        producer = None 

    if producer:
        # --- STEP 2: Configure POI data (as previously instructed) ---
        # Ensure you have a 'data' folder with 'pois_corrientes.json', etc.

        # --- STEP 3: CONFIGURE THE SIMULATION (config.py) ---
        
        simulate_vehicles_in_real_time(
            vehicles_per_province=config['vehicles_per_province'],
            kafka_producer=producer, 
            duration_seconds=config['duration_seconds']
        )
        
    else:
        print("Skipping simulation due to Kafka Producer initialization failure.")