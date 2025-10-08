import pandas as pd

# === Étape 1 : Chemin de travail ===
output_path = r"C:\Users\Cyrinechalghoumi\Desktop\blockchain\smart_grid_blockchain_fusion.csv"

# === Étape 2 : Chargement des fichiers CSV ===
df_energy = pd.read_csv(r"C:\Users\Cyrinechalghoumi\Desktop\blockchain\smart_grid_dataset1_clean.csv")
df_chain = pd.read_csv(r"C:\Users\Cyrinechalghoumi\Desktop\blockchain\data1_clean_simple.csv")

# === Étape 3 : Réindexation et fusion ===
df_energy.reset_index(inplace=True)
df_energy.rename(columns={'index': 'block_index'}, inplace=True)
df_merged = pd.merge(df_chain, df_energy, on='block_index', how='left')

# === Étape 4 : Suppression des colonnes à faible corrélation ===
columns_to_drop = [
    'Voltage(V)', 'Humidity(%)', 'Transformer_Fault', 
    'Voltage_Fluctuation(%)', 'Power_Factor',
    'unique_senders', 'unique_recipients', 'tx_rate',
    'avg_energy', 'avg_energy_per_sender_mean', 'avg_energy_per_sender_std',
    'std_dev_amount', 'std_dev_energy', 'avg_amount_per_sender_std'
]
df_merged.drop(columns=[col for col in columns_to_drop if col in df_merged.columns], inplace=True)

# === Étape 5 supprimée : pas de génération de hash
# === Étape 6 supprimée : pas de previous_hash

# === Étape 7 : Sélection et réorganisation des colonnes ===
features_to_keep = [
    'block_index',
    'time_since_last_block',
    'num_tx',
    'max_tx_per_sender',
    'sender_concentration',
    'avg_amount',
    'total_amount',
    'Reactive_Power(kVAR)',
    'Electricity_Price(USD/kWh)',
    'avg_amount_per_sender_mean',
    'Power_Consumption(kW)',
    'Energy_Consumption(kWh)',
    'is_anomaly',
    'anomaly_type'
]

# Colonnes prioritaires à placer en début
ordered_cols = [
    'block_index',
    'num_tx',
    'Timestamp'
]

# Ajouter les autres colonnes restantes (en conservant leur ordre dans features_to_keep)
remaining_cols = [col for col in features_to_keep if col not in ordered_cols]
cols_final = ordered_cols + remaining_cols
df_final = df_merged[[col for col in cols_final if col in df_merged.columns]]

# === Étape 8 : Export CSV final ===
df_final.to_csv(output_path, index=False)
print(f"✅ Fichier fusionné enregistré ici : {output_path}")
