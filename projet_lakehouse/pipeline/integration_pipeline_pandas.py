#!/usr/bin/env python3
"""
Pipeline d'intégration alternatif (version Pandas)
Pour environnements sans Apache Spark - Compatible avec le format du projet

Cette version simule le comportement du pipeline Spark/Delta Lake
en utilisant Pandas et Parquet pour la démonstration.
"""

import pandas as pd
import json
import os
import glob
import shutil
from datetime import datetime

# Configuration des chemins
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(BASE_DIR, "..", "data_lake", "raw")
WAREHOUSE_DIR = os.path.join(BASE_DIR, "..", "data_lake", "warehouse")
PROCESSED_DIR = os.path.join(BASE_DIR, "..", "data_lake", "processed")


def validate_measurement(data, sensor_type):
    """Valide une mesure selon son type"""
    
    required_fields = ["sensor_id", "type", "value", "unit", "site", "machine", "timestamp"]
    
    # Vérifier les champs requis
    for field in required_fields:
        if field not in data or data[field] is None:
            return False
    
    # Valider les plages de valeurs
    value = data["value"]
    
    if sensor_type == "temperature":
        return -50 <= value <= 200
    elif sensor_type == "vibration":
        return 0 <= value <= 100
    elif sensor_type == "pressure":
        return 0 <= value <= 50
    
    return True


def load_json_files(directory):
    """Charge tous les fichiers JSON d'un répertoire"""
    
    json_files = glob.glob(os.path.join(directory, "*.json"))
    records = []
    
    for filepath in json_files:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                data['_source_file'] = os.path.basename(filepath)
                records.append(data)
        except (json.JSONDecodeError, IOError) as e:
            print(f"   ⚠️  Erreur lecture {filepath}: {e}")
    
    return records, json_files


def transform_data(df):
    """Transforme les données pour le warehouse"""
    
    # Convertir timestamp en datetime (format ISO8601)
    df['timestamp_dt'] = pd.to_datetime(df['timestamp'], format='ISO8601')
    
    # Ajouter colonnes de partitionnement
    df['year'] = df['timestamp_dt'].dt.year
    df['month'] = df['timestamp_dt'].dt.month
    df['day'] = df['timestamp_dt'].dt.day
    df['hour'] = df['timestamp_dt'].dt.hour
    
    # Timestamp d'ingestion
    df['ingestion_timestamp'] = datetime.now()
    
    return df


def save_to_warehouse(df, sensor_type, warehouse_dir):
    """Sauvegarde les données en format CSV partitionné"""
    
    output_dir = os.path.join(warehouse_dir, sensor_type)
    os.makedirs(output_dir, exist_ok=True)
    
    # Sauvegarder par site (simulation du partitionnement)
    for site in df['site'].unique():
        site_df = df[df['site'] == site]
        site_dir = os.path.join(output_dir, f"site={site}")
        os.makedirs(site_dir, exist_ok=True)
        
        # Partitionner par année/mois/jour
        for (year, month, day), group in site_df.groupby(['year', 'month', 'day']):
            partition_dir = os.path.join(site_dir, f"year={year}", f"month={month}", f"day={day}")
            os.makedirs(partition_dir, exist_ok=True)
            
            # Sauvegarder en CSV
            output_path = os.path.join(partition_dir, "data.csv")
            group.to_csv(output_path, index=False)
    
    # Sauvegarder aussi une version complète pour les analyses
    full_path = os.path.join(output_dir, "_full_data.csv")
    df.to_csv(full_path, index=False)


def process_sensor_type(sensor_type):
    """Traite les données d'un type de capteur"""
    
    raw_path = os.path.join(RAW_DATA_DIR, sensor_type)
    processed_path = os.path.join(PROCESSED_DIR, sensor_type)
    
    print(f"\n{'='*60}")
    print(f"📥 Traitement des données {sensor_type.upper()}")
    print(f"{'='*60}")
    
    # Vérifier le répertoire source
    if not os.path.exists(raw_path):
        print(f"   ⚠️  Répertoire non trouvé: {raw_path}")
        return 0
    
    # Charger les fichiers JSON
    records, json_files = load_json_files(raw_path)
    
    if not records:
        print(f"   ⚠️  Aucune donnée trouvée")
        return 0
    
    print(f"   📂 {len(json_files)} fichiers JSON chargés")
    print(f"   📊 {len(records)} enregistrements lus")
    
    # Valider les données
    valid_records = [r for r in records if validate_measurement(r, sensor_type)]
    invalid_count = len(records) - len(valid_records)
    
    if invalid_count > 0:
        print(f"   ⚠️  {invalid_count} enregistrements invalides filtrés")
    
    print(f"   ✅ {len(valid_records)} enregistrements valides")
    
    # Créer DataFrame
    df = pd.DataFrame(valid_records)
    
    # Transformer les données
    df = transform_data(df)
    
    # Sauvegarder dans le warehouse
    print(f"   💾 Écriture en format Parquet...")
    save_to_warehouse(df, sensor_type, WAREHOUSE_DIR)
    
    warehouse_path = os.path.join(WAREHOUSE_DIR, sensor_type)
    print(f"   ✅ Données écrites dans: {warehouse_path}")
    
    # Déplacer les fichiers traités
    os.makedirs(processed_path, exist_ok=True)
    for json_file in json_files:
        src = json_file
        dst = os.path.join(processed_path, os.path.basename(json_file))
        shutil.move(src, dst)
    
    print(f"   📁 {len(json_files)} fichiers déplacés vers processed/")
    
    return len(valid_records)


def show_warehouse_stats():
    """Affiche les statistiques du warehouse"""
    
    print("\n" + "=" * 70)
    print("📊 STATISTIQUES DU DATA LAKEHOUSE")
    print("=" * 70)
    
    for sensor_type in ["temperature", "vibration", "pressure"]:
        csv_path = os.path.join(WAREHOUSE_DIR, sensor_type, "_full_data.csv")
        
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            
            print(f"\n📈 {sensor_type.upper()}: {len(df)} enregistrements")
            
            # Stats par site
            site_stats = df.groupby('site').size()
            for site, count in site_stats.items():
                print(f"   - {site}: {count} mesures")
            
            # Alertes critiques
            if 'is_critical' in df.columns:
                critical = df['is_critical'].sum()
                print(f"   ⚠️  Alertes critiques: {int(critical)}")
        else:
            print(f"\n⚠️  {sensor_type}: Pas de données")


def main():
    """Fonction principale"""
    
    print("\n" + "=" * 70)
    print("🏭 InduSense - Pipeline d'Intégration (Version Pandas)")
    print("=" * 70)
    print(f"📂 Source: {RAW_DATA_DIR}")
    print(f"💾 Destination: {WAREHOUSE_DIR}")
    
    total_records = 0
    
    for sensor_type in ["temperature", "vibration", "pressure"]:
        count = process_sensor_type(sensor_type)
        total_records += count
    
    print("\n" + "=" * 70)
    print("✅ PIPELINE TERMINÉ")
    print("=" * 70)
    print(f"📊 Total des enregistrements traités: {total_records}")
    
    # Afficher les statistiques
    show_warehouse_stats()


if __name__ == "__main__":
    main()
