import flwr as fl
import numpy as np

# Define a custom strategy for industrial aggregation
class IndustrialStrategy(fl.server.strategy.FedAvg):
    def aggregate_fit(self, rnd, results, failures):
        print(f"--- Round {rnd} Industrial Model Aggregation ---")
        return super().aggregate_fit(rnd, results, failures)

def run_fl_server():
    print("Starting Central Federated Learning Aggregator...")
    
    # Define strategy
    strategy = IndustrialStrategy(
        min_fit_clients=2,
        min_available_clients=2,
    )

    # Start Flower server
    fl.server.start_server(
        server_address="0.0.0.0:8080",
        config=fl.server.ServerConfig(num_rounds=3),
        strategy=strategy,
    )

if __name__ == "__main__":
    run_fl_server()
