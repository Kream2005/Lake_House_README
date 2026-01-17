#!/usr/bin/env python3
"""
Simulateur de capteur de pression pour InduSense
Génère des mesures de pression et les dépose dans le Data Lake brut
"""

import json
import uuid
import random
import time
import os
from datetime import datetime

# Configuration
OUTPUT_DIR = "../data_lake/raw/pressure/"
SITES = ["Site_Paris", "Site_Lyon", "Site_Marseille", "Site_Toulouse", "Site_Bordeaux"]
MACHINES = ["Machine_A1", "Machine_A2", "Machine_B1", "Machine_B2", "Machine_C1"]
MIN_INTERVAL = 1  # secondes
MAX_INTERVAL = 3  # secondes
NUM_MEASUREMENTS = 1000  # Nombre de mesures à générer

# Plages de pression réalistes (en Pascal/Bar)
PRESSURE_NORMAL_MIN = 1.0  # Bar
PRESSURE_NORMAL_MAX = 8.0  # Bar
PRESSURE_CRITICAL_LOW = 0.5  # Seuil critique bas
PRESSURE_CRITICAL_HIGH = 10.0  # Seuil critique haut


def generate_pressure_measurement():
    """Génère une mesure de pression simulée"""
    
    # Générer une valeur de pression
    # 93% des mesures sont normales, 7% sont critiques
    if random.random() < 0.93:
        value = round(random.uniform(PRESSURE_NORMAL_MIN, PRESSURE_NORMAL_MAX), 3)
        is_critical = False
    else:
        # Valeur critique (hors seuil - trop bas ou trop haut)
        if random.random() < 0.5:
            value = round(random.uniform(0.1, PRESSURE_CRITICAL_LOW), 3)
        else:
            value = round(random.uniform(PRESSURE_CRITICAL_HIGH, 15.0), 3)
        is_critical = True
    
    site = random.choice(SITES)
    
    measurement = {
        "sensor_id": str(uuid.uuid4()),
        "type": "pressure",
        "value": value,
        "unit": "Bar",
        "site": site,
        "machine": random.choice(MACHINES),
        "timestamp": datetime.now().isoformat(),
        "is_critical": is_critical,
        "value_pascal": round(value * 100000, 2)  # Conversion en Pascal pour référence
    }
    
    return measurement


def save_measurement(measurement, output_dir):
    """Sauvegarde la mesure dans un fichier JSON"""
    
    # Créer le répertoire si nécessaire
    os.makedirs(output_dir, exist_ok=True)
    
    # Générer un nom de fichier unique
    filename = f"press_{measurement['timestamp'].replace(':', '-')}_{measurement['sensor_id'][:8]}.json"
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(measurement, f, indent=2, ensure_ascii=False)
    
    return filepath


def main():
    """Fonction principale du simulateur"""
    
    print("=" * 60)
    print("🔵 Simulateur de Capteur de Pression - InduSense")
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
            measurement = generate_pressure_measurement()
            
            # Sauvegarder la mesure
            filepath = save_measurement(measurement, output_dir)
            
            measurements_generated += 1
            if measurement['is_critical']:
                critical_count += 1
                print(f"⚠️  [{i+1}/{NUM_MEASUREMENTS}] ALERTE CRITIQUE: {measurement['value']} Bar "
                      f"- {measurement['site']}/{measurement['machine']}")
            else:
                print(f"✅ [{i+1}/{NUM_MEASUREMENTS}] Mesure: {measurement['value']} Bar "
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
