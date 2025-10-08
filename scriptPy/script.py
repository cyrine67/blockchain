import csv
import random
import numpy as np
from collections import deque

class SystemState:
    """Encapsulates the current state of the data generator."""
    def __init__(self):
        self.regime = 'NORMAL_LOW'
        self.state_counter = 0
        self.params = {}

def _create_row_from_params(params, block_index, anomaly_type):
    """Helper function to create a data row from a dictionary of parameters."""
    is_anomaly = 0 if anomaly_type == 'Normal' else 1
    
    num_tx = max(1, int(random.uniform(*params['num_tx_range'])))
    time_since_last_block = random.uniform(*params['time_range']) + (num_tx * params.get('time_tx_factor', 0.01))

    avg_energy = random.uniform(*params['avg_energy_range'])
    std_dev_energy = avg_energy * random.uniform(*params['std_energy_factor_range'])
    
    avg_amount = random.uniform(*params['avg_amount_range'])
    std_dev_amount = avg_amount * random.uniform(*params['std_amount_factor_range'])

    if num_tx == 1:
        std_dev_energy = 0.0
        std_dev_amount = 0.0
    
    noise_factor = 1 + random.uniform(-0.02, 0.02)
    total_energy = avg_energy * num_tx * noise_factor
    total_amount = avg_amount * num_tx * noise_factor

    num_theft_attempts = 1 if anomaly_type == 'Theft' else 0
    num_alerts = 1 if is_anomaly else random.choice([0, 0, 0, 1])

    if num_tx > 1:
        if anomaly_type == 'DoS':
            unique_senders = params.get('unique_senders', random.randint(2, 5))
            unique_recipients = params.get('unique_recipients', random.randint(2, 5))
        else:
            unique_senders = max(1, int(num_tx * random.uniform(0.4, 0.8)))
            unique_recipients = max(1, int(num_tx * random.uniform(0.5, 0.9)))
    else:
        unique_senders = 1
        unique_recipients = 1

    if unique_senders > 0:
        if anomaly_type == 'DoS':
            max_tx_per_sender = int(num_tx / unique_senders * random.uniform(1.8, 3.0))
        else:
            max_tx_per_sender = int(num_tx / unique_senders * random.uniform(1.1, 1.6))
        max_tx_per_sender = min(num_tx, max(1, max_tx_per_sender))
        sender_concentration = max_tx_per_sender / num_tx
    else:
        max_tx_per_sender = 0
        sender_concentration = 0.0
    
    if num_tx == 1:
        sender_concentration = 1.0

    tx_rate = num_tx / time_since_last_block if time_since_last_block > 0.01 else 0

    num_deliveries = int(num_tx * params.get('delivery_ratio', 0.5) + random.randint(-1, 1))
    num_payments = int(num_tx * params.get('payment_ratio', 0.5) + random.randint(-1, 1))
    num_deliveries = max(0, num_deliveries)
    num_payments = max(0, num_payments)

    avg_energy_per_sender_mean = avg_energy * random.uniform(0.9, 1.1)
    avg_energy_per_sender_std = std_dev_energy * random.uniform(0.9, 1.1)
    avg_amount_per_sender_mean = avg_amount * random.uniform(0.9, 1.1)
    avg_amount_per_sender_std = std_dev_amount * random.uniform(0.9, 1.1)

    return [
        block_index, 0, num_tx, time_since_last_block, avg_energy,
        std_dev_energy, total_energy, avg_amount, std_dev_amount, total_amount,
        num_deliveries, num_payments, num_theft_attempts, num_alerts, unique_senders,
        unique_recipients, tx_rate, max_tx_per_sender, sender_concentration,
        avg_energy_per_sender_mean, avg_energy_per_sender_std, avg_amount_per_sender_mean,
        avg_amount_per_sender_std, is_anomaly, anomaly_type
    ]

def generate_lstm_ready_data(num_lines=50000):
    """
    Generates stateful, sequential data suitable for LSTM models and writes it to a CSV file.
    """
    header = [
        'block_index', 'miner_id', 'num_tx', 'time_since_last_block', 'avg_energy',
        'std_dev_energy', 'total_energy', 'avg_amount', 'std_dev_amount', 'total_amount',
        'num_deliveries', 'num_payments', 'num_theft_attempts', 'num_alerts', 'unique_senders',
        'unique_recipients', 'tx_rate', 'max_tx_per_sender', 'sender_concentration',
        'avg_energy_per_sender_mean', 'avg_energy_per_sender_std', 'avg_amount_per_sender_mean',
        'avg_amount_per_sender_std', 'is_anomaly', 'anomaly_type'
    ]
    
    with open(r'C:\Users\Cyrinechalghoumi\Desktop\blockchain\data1.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)

        state = SystemState()
        target_counts = { 'Theft': num_lines//4, 'Breakdown': num_lines//4, 'DoS': num_lines//4 }
        actual_counts = { 'Theft': 0, 'Breakdown': 0, 'DoS': 0 }

        for i in range(1, num_lines + 1):
            state.state_counter += 1
            current_regime = state.regime
            
            if current_regime in ['NORMAL_LOW', 'NORMAL_HIGH']:
                possible_next_states = ['DEGRADING', 'RAMP_UP_DOS', 'THEFT_EVENT', current_regime]
                weights = [
                    (target_counts['Breakdown'] - actual_counts['Breakdown']) * 0.05,
                    (target_counts['DoS'] - actual_counts['DoS']) * 0.05,
                    (target_counts['Theft'] - actual_counts['Theft']) * 0.05,
                    98
                ]
                weights = [max(0, w) for w in weights]
                
                if sum(weights) > 98:
                    next_regime = random.choices(possible_next_states, weights=weights, k=1)[0]
                    if next_regime != current_regime:
                        state.regime = next_regime
                        state.state_counter = 0

            if current_regime == 'RAMP_UP_DOS' and state.state_counter > random.randint(3, 6):
                state.regime = 'FULL_DOS'
                state.state_counter = 0
            elif current_regime == 'FULL_DOS' and state.state_counter > random.randint(2, 5):
                state.regime = 'NORMAL_LOW'
                state.state_counter = 0
            elif current_regime == 'DEGRADING' and state.state_counter > random.randint(4, 8):
                state.regime = 'BREAKDOWN_EVENT'
                state.state_counter = 0
            elif current_regime in ['THEFT_EVENT', 'BREAKDOWN_EVENT']:
                state.regime = 'NORMAL_LOW'
                state.state_counter = 0
                
            if state.state_counter > random.randint(50, 100):
                if current_regime == 'NORMAL_LOW':
                    state.regime = 'NORMAL_HIGH'
                elif current_regime == 'NORMAL_HIGH':
                    state.regime = 'NORMAL_LOW'
                state.state_counter = 0

            params = {}
            anomaly_type = 'Normal'

            if state.regime == 'NORMAL_LOW':
                params = {
                    'num_tx_range': (2, 20), 'time_range': (3.0, 15.0),
                    'avg_energy_range': (0.1, 0.4), 'std_energy_factor_range': (0.7, 1.3),
                    'avg_amount_range': (0.05, 0.2), 'std_amount_factor_range': (0.7, 1.3),
                }
            elif state.regime == 'NORMAL_HIGH':
                params = {
                    'num_tx_range': (50, 150), 'time_range': (2.0, 8.0),
                    'avg_energy_range': (0.2, 0.8), 'std_energy_factor_range': (0.6, 1.1),
                    'avg_amount_range': (0.1, 0.3), 'std_amount_factor_range': (0.6, 1.1),
                }
            elif state.regime == 'DEGRADING':
                params = {
                    'num_tx_range': (10, 50), 'time_range': (2.0, 10.0),
                    'avg_energy_range': (0.2, 0.6), 'std_energy_factor_range': (1.5, 2.5),
                    'avg_amount_range': (0.1, 0.3), 'std_amount_factor_range': (1.5, 2.5),
                }
            elif state.regime == 'BREAKDOWN_EVENT':
                anomaly_type = 'Breakdown'
                actual_counts['Breakdown'] += 1
                params = {
                    'num_tx_range': (20, 100), 'time_range': (40.0, 150.0),
                    'avg_energy_range': (0.1, 1.5), 'std_energy_factor_range': (3.0, 6.0),
                    'avg_amount_range': (0.1, 1.0), 'std_amount_factor_range': (3.0, 6.0),
                }
            elif state.regime == 'RAMP_UP_DOS':
                params = {
                    'num_tx_range': (150, 280), 'time_range': (5.0, 15.0),
                    'avg_energy_range': (0.01, 0.05), 'std_energy_factor_range': (1.0, 2.0),
                    'avg_amount_range': (0.01, 0.05), 'std_amount_factor_range': (1.0, 2.0),
                }
            elif state.regime == 'FULL_DOS':
                anomaly_type = 'DoS'
                actual_counts['DoS'] += 1
                params = {
                    'num_tx_range': (400, 1200), 'time_range': (20.0, 100.0),
                    'avg_energy_range': (0.0001, 0.005), 'std_energy_factor_range': (1.0, 2.5),
                    'avg_amount_range': (0.0001, 0.005), 'std_amount_factor_range': (1.0, 2.5),
                    'delivery_ratio': 0.0, 'payment_ratio': 0.0,
                }
            elif state.regime == 'THEFT_EVENT':
                anomaly_type = 'Theft'
                actual_counts['Theft'] += 1
                params = {
                    'num_tx_range': (1, 3), 'time_range': (10.0, 80.0),
                    'avg_energy_range': (0.0, 0.05), 'std_energy_factor_range': (0.1, 0.5),
                    'avg_amount_range': (20.0, 40.0), 'std_amount_factor_range': (0.0, 0.1),
                    'delivery_ratio': 0.0
                }

            row = _create_row_from_params(params, i, anomaly_type)
            writer.writerow(row)

if __name__ == '__main__':
    generate_lstm_ready_data(num_lines=50000)
