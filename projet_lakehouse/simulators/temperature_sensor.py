#!/usr/bin/env python3
"""
Simulateur de capteur de température pour InduSense
Génère des mesures de température et les dépose dans le Data Lake brut
"""

import json
import uuid
import random
import time
import os
from datetime import datetime

# Configuration
OUTPUT_DIR = "../data_lake/raw/temperature/"
SITES = ["Site_Paris", "Site_Lyon", "Site_Marseille", "Site_Toulouse", "Site_Bordeaux"]
MACHINES = ["Machine_A1", "Machine_A2", "Machine_B1", "Machine_B2", "Machine_C1"]
MIN_INTERVAL = 1  # secondes
MAX_INTERVAL = 3  
NUM_MEASUREMENTS = 1000  # Nombre de mesures à générer

# Plages de température réalistes (en Celsius)
TEMP_NORMAL_MIN = 20.0
TEMP_NORMAL_MAX = 85.0
TEMP_CRITICAL_THRESHOLD = 90.0  # Seuil d'alerte critique


def generate_temperature_measurement():
    """Génère une mesure de température simulée"""
    
    # Générer une valeur de température
    # 95% des mesures sont normales, 5% sont critiques
    if random.random() < 0.95:
        value = round(random.uniform(TEMP_NORMAL_MIN, TEMP_NORMAL_MAX), 2)
    else:
        # Valeur critique (hors seuil)
        value = round(random.uniform(TEMP_CRITICAL_THRESHOLD, 120.0), 2)
    
    measurement = {
        "sensor_id": str(uuid.uuid4()),
        "type": "temperature",
        "value": value,
        "unit": "Celsius",
        "site": random.choice(SITES),
        "machine": random.choice(MACHINES),
        "timestamp": datetime.now().isoformat(),
        "is_critical": value > TEMP_CRITICAL_THRESHOLD
    }
    
    return measurement


def save_measurement(measurement, output_dir):
    """Sauvegarde la mesure dans un fichier JSON"""
    
    # Créer le répertoire si nécessaire
    os.makedirs(output_dir, exist_ok=True)
    
    # Générer un nom de fichier unique
    filename = f"temp_{measurement['timestamp'].replace(':', '-')}_{measurement['sensor_id'][:8]}.json"
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(measurement, f, indent=2, ensure_ascii=False)
    
    return filepath


def main():
    """Fonction principale du simulateur"""

    print("=" * 60)
    print("🌡️  Simulateur de Capteur de Température - InduSense")
    print("=" * 60)
    print(f"Répertoire de sortie: {os.path.abspath(OUTPUT_DIR)}")
    print(f"Nombre de mesures à générer: {NUM_MEASUREMENTS}")
    print(f"Intervalle entre mesures: {MIN_INTERVAL}-{MAX_INTERVAL} secondes")
    print("=" * 60)
    
    # Obtenir le chemin absolu du répertoire de sortie
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, OUTPUT_DIR)
    
    measurements_generated = 0
    critical_count = 0
    
    try:
        for i in range(NUM_MEASUREMENTS):
            # Générer la mesure
            measurement = generate_temperature_measurement()
            
            # Sauvegarder la mesure
            filepath = save_measurement(measurement, output_dir)
            
            measurements_generated += 1
            if measurement['is_critical']:
                critical_count += 1
                print(f"⚠️  [{i+1}/{NUM_MEASUREMENTS}] ALERTE CRITIQUE: {measurement['value']}°C "
                      f"- {measurement['site']}/{measurement['machine']}")
            else:
                print(f"✅ [{i+1}/{NUM_MEASUREMENTS}] Mesure: {measurement['value']}°C "
                      f"- {measurement['site']}/{measurement['machine']}")
            
            # Attendre avant la prochaine mesure (sauf pour la dernière)
            if i < NUM_MEASUREMENTS - 1:
                interval = random.uniform(MIN_INTERVAL, MAX_INTERVAL)
                time.sleep(interval)
                
    except KeyboardInterrupt:
        print("\n\n🛑 Simulation interrompue par l'utilisateur")
    
    print("\n" + "=" * 60)
    print("📊 Résumé de la simulation")
    print("=" * 60)
    print(f"Mesures générées: {measurements_generated}")
    print(f"Alertes critiques: {critical_count}")
    print(f"Fichiers créés dans: {os.path.abspath(output_dir)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
