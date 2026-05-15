import flwr as fl
import numpy as np
import time

# Define a simple Industrial Model (e.g. for RUL prediction)
class IndustrialModel:
    def __init__(self):
        # Simulated weights
        self.weights = [np.random.randn(3, 3)]

    def get_weights(self):
        return self.weights

    def set_weights(self, weights):
        self.weights = weights

    def train(self):
        print("Training local model on factory private data...")
        # Simulate training time
        time.sleep(2)
        # Add small updates to weights
        self.weights = [w + 0.01 for w in self.weights]
        return len(self.weights)

# Define Flower client
class FactoryClient(fl.client.NumPyClient):
    def __init__(self):
        self.model = IndustrialModel()

    def get_parameters(self, config):
        return self.model.get_weights()

    def fit(self, parameters, config):
        self.model.set_weights(parameters)
        num_examples = self.model.train()
        return self.model.get_weights(), num_examples, {}

    def evaluate(self, parameters, config):
        self.model.set_weights(parameters)
        loss = 0.5  # Simulated loss
        accuracy = 0.9  # Simulated accuracy
        return loss, 1, {"accuracy": accuracy}

def run_fl_client():
    print("Starting Factory Edge Federated Learning Client...")
    fl.client.start_numpy_client(server_address="localhost:8080", client=FactoryClient())

if __name__ == "__main__":
    run_fl_client()
