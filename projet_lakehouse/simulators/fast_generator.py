#!/usr/bin/env python3
"""
Générateur rapide de données pour les tests
Génère toutes les mesures sans délai pour tester rapidement le pipeline
"""

import json
import uuid
import random
import os
from datetime import datetime, timedelta

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_LAKE_DIR = os.path.join(BASE_DIR, "..", "data_lake", "raw")

SITES = ["Site_Paris", "Site_Lyon", "Site_Marseille", "Site_Toulouse", "Site_Bordeaux"]
MACHINES = ["Machine_A1", "Machine_A2", "Machine_B1", "Machine_B2", "Machine_C1"]

# Nombre de mesures par type
NUM_MEASUREMENTS = 1000

# Seuils critiques
TEMP_CRITICAL = 90.0
VIBRATION_CRITICAL = 10.0
PRESSURE_CRITICAL_LOW = 0.5
PRESSURE_CRITICAL_HIGH = 10.0


def generate_temperature():
    """Génère une mesure de température"""
    if random.random() < 0.95:
        value = round(random.uniform(20.0, 85.0), 2)
    else:
        value = round(random.uniform(90.0, 120.0), 2)
    
    return {
        "sensor_id": str(uuid.uuid4()),
        "type": "temperature",
        "value": value,
        "unit": "Celsius",
        "site": random.choice(SITES),
        "machine": random.choice(MACHINES),
        "is_critical": value > TEMP_CRITICAL
    }


def generate_vibration():
    """Génère une mesure de vibration"""
    machine = random.choice(MACHINES)
    
    if machine in ["Machine_B1", "Machine_C1"]:
        base_value = random.uniform(0.5, 10.0)
        variability = random.uniform(0.8, 1.5)
    else:
        base_value = random.uniform(0.5, 7.0)
        variability = random.uniform(0.9, 1.1)
    
    value = round(base_value * variability, 3)
    
    if random.random() < 0.05:
        value = round(random.uniform(10.0, 18.0), 3)
    
    return {
        "sensor_id": str(uuid.uuid4()),
        "type": "vibration",
        "value": value,
        "unit": "mm/s",
        "site": random.choice(SITES),
        "machine": machine,
        "is_critical": value > VIBRATION_CRITICAL,
        "frequency_hz": round(random.uniform(10, 1000), 1)
    }


def generate_pressure():
    """Génère une mesure de pression"""
    if random.random() < 0.93:
        value = round(random.uniform(1.0, 8.0), 3)
        is_critical = False
    else:
        if random.random() < 0.5:
            value = round(random.uniform(0.1, 0.5), 3)
        else:
            value = round(random.uniform(10.0, 15.0), 3)
        is_critical = True
    
    return {
        "sensor_id": str(uuid.uuid4()),
        "type": "pressure",
        "value": value,
        "unit": "Bar",
        "site": random.choice(SITES),
        "machine": random.choice(MACHINES),
        "is_critical": is_critical,
        "value_pascal": round(value * 100000, 2)
    }


def save_measurement(measurement, sensor_type, index, base_time):
    """Sauvegarde une mesure dans un fichier JSON"""
    
    # Calculer un timestamp simulé (réparti sur 24h)
    time_offset = timedelta(seconds=index * (86400 / NUM_MEASUREMENTS))
    timestamp = base_time + time_offset
    measurement["timestamp"] = timestamp.isoformat()
    
    # Créer le répertoire
    output_dir = os.path.join(DATA_LAKE_DIR, sensor_type)
    os.makedirs(output_dir, exist_ok=True)
    
    # Nom du fichier
    prefix = {"temperature": "temp", "vibration": "vib", "pressure": "press"}[sensor_type]
    filename = f"{prefix}_{timestamp.strftime('%Y%m%d_%H%M%S')}_{measurement['sensor_id'][:8]}.json"
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(measurement, f, indent=2, ensure_ascii=False)
    
    return filepath


def main():
    """Génère toutes les données rapidement"""
    
    print("=" * 70)
    print("⚡ Générateur Rapide de Données - InduSense")
    print("=" * 70)
    
    # Base time pour les timestamps (aujourd'hui à minuit)
    base_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    stats = {
        "temperature": {"total": 0, "critical": 0},
        "vibration": {"total": 0, "critical": 0},
        "pressure": {"total": 0, "critical": 0}
    }
    
    generators = {
        "temperature": generate_temperature,
        "vibration": generate_vibration,
        "pressure": generate_pressure
    }
    
    for sensor_type, generator in generators.items():
        print(f"\n🔄 Génération des données {sensor_type}...")
        
        for i in range(NUM_MEASUREMENTS):
            measurement = generator()
            save_measurement(measurement, sensor_type, i, base_time)
            
            stats[sensor_type]["total"] += 1
            if measurement["is_critical"]:
                stats[sensor_type]["critical"] += 1
            
            if (i + 1) % 200 == 0:
                print(f"   ✅ {i + 1}/{NUM_MEASUREMENTS} mesures générées")
        
        print(f"   ✅ {sensor_type}: {stats[sensor_type]['total']} mesures "
              f"({stats[sensor_type]['critical']} critiques)")
    
    print("\n" + "=" * 70)
    print("📊 RÉSUMÉ FINAL")
    print("=" * 70)
    
    total_measurements = sum(s["total"] for s in stats.values())
    total_critical = sum(s["critical"] for s in stats.values())
    
    print(f"Total des mesures générées: {total_measurements}")
    print(f"Total des alertes critiques: {total_critical}")
    print(f"\nRépartition par type:")
    for sensor_type, s in stats.items():
        print(f"  - {sensor_type}: {s['total']} mesures ({s['critical']} critiques)")
    
    print(f"\n📁 Données stockées dans: {os.path.abspath(DATA_LAKE_DIR)}")
    print("=" * 70)


if __name__ == "__main__":
    main()
