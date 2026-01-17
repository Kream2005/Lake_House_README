#!/usr/bin/env python3
"""
Simulateur de capteur de vibration pour InduSense
Génère des mesures de vibration et les dépose dans le Data Lake brut
"""

import json
import uuid
import random
import time
import os
from datetime import datetime

# Configuration
OUTPUT_DIR = "../data_lake/raw/vibration/"
SITES = ["Site_Paris", "Site_Lyon", "Site_Marseille", "Site_Toulouse", "Site_Bordeaux"]
MACHINES = ["Machine_A1", "Machine_A2", "Machine_B1", "Machine_B2", "Machine_C1"]
MIN_INTERVAL = 1  # secondes
MAX_INTERVAL = 3  # secondes
NUM_MEASUREMENTS = 1000  # Nombre de mesures à générer

# Plages de vibration réalistes (en mm/s - vélocité de vibration)
VIBRATION_NORMAL_MIN = 0.5
VIBRATION_NORMAL_MAX = 7.0
VIBRATION_CRITICAL_THRESHOLD = 10.0  # Seuil d'alerte critique (ISO 10816)


def generate_vibration_measurement():
    """Génère une mesure de vibration simulée"""
    
    # Générer une valeur de vibration
    # Certaines machines ont plus de variabilité
    machine = random.choice(MACHINES)
    
    # Machine_B1 et Machine_C1 ont plus de variabilité (pour l'analyse top 5)
    if machine in ["Machine_B1", "Machine_C1"]:
        base_value = random.uniform(VIBRATION_NORMAL_MIN, VIBRATION_NORMAL_MAX + 3)
        variability_factor = random.uniform(0.8, 1.5)
    else:
        base_value = random.uniform(VIBRATION_NORMAL_MIN, VIBRATION_NORMAL_MAX)
        variability_factor = random.uniform(0.9, 1.1)
    
    value = round(base_value * variability_factor, 3)
    
    # 5% de chance d'avoir une valeur critique
    if random.random() < 0.05:
        value = round(random.uniform(VIBRATION_CRITICAL_THRESHOLD, 18.0), 3)
    
    measurement = {
        "sensor_id": str(uuid.uuid4()),
        "type": "vibration",
        "value": value,
        "unit": "mm/s",
        "site": random.choice(SITES),
        "machine": machine,
        "timestamp": datetime.now().isoformat(),
        "is_critical": value > VIBRATION_CRITICAL_THRESHOLD,
        "frequency_hz": round(random.uniform(10, 1000), 1)  # Fréquence de vibration
    }
    
    return measurement


def save_measurement(measurement, output_dir):
    """Sauvegarde la mesure dans un fichier JSON"""
    
    # Créer le répertoire si nécessaire
    os.makedirs(output_dir, exist_ok=True)
    
    # Générer un nom de fichier unique
    filename = f"vib_{measurement['timestamp'].replace(':', '-')}_{measurement['sensor_id'][:8]}.json"
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(measurement, f, indent=2, ensure_ascii=False)
    
    return filepath


def main():
    """Fonction principale du simulateur"""
    
    print("=" * 60)
    print("📳 Simulateur de Capteur de Vibration - InduSense")
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
            measurement = generate_vibration_measurement()
            
            # Sauvegarder la mesure
            filepath = save_measurement(measurement, output_dir)
            
            measurements_generated += 1
            if measurement['is_critical']:
                critical_count += 1
                print(f"⚠️  [{i+1}/{NUM_MEASUREMENTS}] ALERTE CRITIQUE: {measurement['value']} mm/s "
                      f"- {measurement['site']}/{measurement['machine']}")
            else:
                print(f"✅ [{i+1}/{NUM_MEASUREMENTS}] Mesure: {measurement['value']} mm/s "
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
