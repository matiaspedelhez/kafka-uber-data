# Uber Trips Data Simulator

## Overview

This simulation models the basic behavior of ride-hailing vehicles, generating real-time status updates. Each vehicle operates independently, transitioning through different states and publishing its data to Kafka.

### Vehicle Actions & States

`AVAILABLE`: Vehicles are ready for a new trip. They have a small chance each second to assign_trip() if their fuel is above 20%.

`IN_TRIP`: Once a trip is assigned, vehicles enter this state and begin moving.

- Movement (move()): They continuously update their currentLocation along a simulated route based on speedKPH. Movement is interpolated for smoothness.

- Trip Progress: kilometersRemainingTrip and estimatedTimeRemainingMinutes are updated in real-time.

- Fuel Consumption: Fuel decreases with movement.

- Arrival: The trip concludes when the vehicle is within 50 meters of its destination, returning it to the AVAILABLE state.

- Out of Fuel: If fuel hits 0% during a trip, the vehicle goes OUT_OF_SERVICE.

`PAUSED`: Vehicles in this state are typically refueling, with fuelPercentage gradually increasing until they return to AVAILABLE (at 95%+ fuel).

`OUT_OF_SERVICE`: This state signifies a vehicle is unoperational (e.g., ran out of fuel or broke down). It will eventually return to AVAILABLE after simulated recovery or fuel assistance.

### Data Publishing
Crucially, after every state update, each vehicle's status (location, speed, state, trip ID, etc.) is formatted into a JSON string and published to a Kafka topic. This continuous stream of real-time data serves as the foundation for downstream analytics, dashboards, and data marts (e.g., via dbt).

## Intended use cases

- **Simulation**: Generate random trips with different attributes.
- **Analysis/Visualization**: Create visualizations to analyze the generated data.

## Customization options

You can customize the number of concurrent simulated trips, and add your own POIs (points of interest) by creating a JSON file in the `data` folder.

## How to run

1. Install Python 3
2. Clone this repository
3. Install Kafka and create a topic in your Kafka broker
4. Adjust the configuration parameters in `config.py`
```python config.py
config = {
    'kafka_broker': 'localhost:9092',
    'topic_name': 'uber-trips',
    'vehicles_per_province': {'corrientes': 50},
    'duration_seconds': 120,
}
```
5. Run the script: `python uber_simulator.py`
6. Check your Kafka topic for the generated data

## License

MIT
