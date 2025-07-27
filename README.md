# Uber Trips Data Simulator

## About the project

This is a small script that generates simulated real-time data for Uber services following POIs (points of interest) and pushes it into a Kafka topic.

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