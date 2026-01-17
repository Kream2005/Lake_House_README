#!/usr/bin/env python3
"""
Module d'analyse décisionnelle (version Pandas)
Alternative au module Spark SQL pour environnements sans Apache Spark

Produit les mêmes analyses que la version Spark:
1. Température moyenne par site et par machine
2. Alertes critiques par type de capteur
3. Top 5 variabilité vibration
4. Évolution horaire pression
"""

import pandas as pd
import os
from datetime import datetime

# Configuration des chemins
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE_DIR = os.path.join(BASE_DIR, "..", "data_lake", "warehouse")
REPORTS_DIR = os.path.join(BASE_DIR, "..", "reports")


def load_data():
    """Charge les données du warehouse"""
    
    data = {}
    
    for sensor_type in ["temperature", "vibration", "pressure"]:
        csv_path = os.path.join(WAREHOUSE_DIR, sensor_type, "_full_data.csv")
        
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            data[sensor_type] = df
            print(f"✅ {sensor_type}: {len(df)} enregistrements chargés")
        else:
            print(f"⚠️  {sensor_type}: données non trouvées")
            data[sensor_type] = None
    
    return data


def analyse_temperature_moyenne(df_temp):
    """
    Analyse 1: Température moyenne par site et par machine sur la journée
    """
    
    print("\n" + "=" * 70)
    print("📊 ANALYSE 1: Température moyenne par site et par machine")
    print("=" * 70)
    
    if df_temp is None:
        print("⚠️  Pas de données de température disponibles")
        return None
    
    # Grouper par site et machine
    result = df_temp.groupby(['site', 'machine']).agg({
        'value': ['mean', 'min', 'max', 'count'],
        'is_critical': 'sum'
    }).round(2)
    
    # Aplatir les colonnes
    result.columns = ['temperature_moyenne', 'temperature_min', 'temperature_max', 
                      'nombre_mesures', 'alertes_critiques']
    result = result.reset_index()
    
    print("\nRésultats:")
    print(result.to_string(index=False))
    
    return result


def analyse_alertes_critiques(data):
    """
    Analyse 2: Nombre total d'alertes critiques par type de capteur
    """
    
    print("\n" + "=" * 70)
    print("📊 ANALYSE 2: Alertes critiques par type de capteur")
    print("=" * 70)
    
    results = []
    
    for sensor_type, df in data.items():
        if df is not None and 'is_critical' in df.columns:
            critical_count = df['is_critical'].sum()
            total_count = len(df)
            percentage = round((critical_count / total_count) * 100, 2) if total_count > 0 else 0
            
            results.append({
                'type_capteur': sensor_type,
                'alertes_critiques': int(critical_count),
                'total_mesures': total_count,
                'pourcentage_critique': percentage
            })
    
    result_df = pd.DataFrame(results)
    
    print("\nRésumé global:")
    print(result_df.to_string(index=False))
    
    # Détail par site
    print("\nDétail par site:")
    
    detail_results = []
    for sensor_type, df in data.items():
        if df is not None:
            site_stats = df.groupby('site').agg({
                'is_critical': 'sum',
                'sensor_id': 'count'
            }).rename(columns={'is_critical': 'alertes_critiques', 'sensor_id': 'total_mesures'})
            site_stats['type_capteur'] = sensor_type
            site_stats = site_stats.reset_index()
            detail_results.append(site_stats)
    
    if detail_results:
        detail_df = pd.concat(detail_results, ignore_index=True)
        detail_df = detail_df[['type_capteur', 'site', 'alertes_critiques', 'total_mesures']]
        detail_df = detail_df.sort_values(['type_capteur', 'site'])
        print(detail_df.to_string(index=False))
    
    return result_df


def analyse_variabilite_vibration(df_vibration):
    """
    Analyse 3: Top 5 des machines présentant la plus forte variabilité de vibration
    """
    
    print("\n" + "=" * 70)
    print("📊 ANALYSE 3: Top 5 machines - Variabilité de vibration")
    print("=" * 70)
    
    if df_vibration is None:
        print("⚠️  Pas de données de vibration disponibles")
        return None
    
    # Calculer les statistiques par machine et site
    result = df_vibration.groupby(['machine', 'site']).agg({
        'value': ['count', 'mean', 'std', 'min', 'max']
    }).round(3)
    
    result.columns = ['nombre_mesures', 'vibration_moyenne', 'ecart_type', 
                      'valeur_min', 'valeur_max']
    result = result.reset_index()
    
    # Filtrer les machines avec au moins 10 mesures
    result = result[result['nombre_mesures'] >= 10]
    
    # Calculer l'amplitude et le coefficient de variation
    result['amplitude'] = (result['valeur_max'] - result['valeur_min']).round(3)
    result['coefficient_variation_pct'] = (
        (result['ecart_type'] / result['vibration_moyenne']) * 100
    ).round(2)
    
    # Top 5 par écart-type
    top5_std = result.nlargest(5, 'ecart_type')
    
    print("\nTop 5 des machines par variabilité (écart-type):")
    print(top5_std[['machine', 'site', 'nombre_mesures', 'vibration_moyenne', 
                    'ecart_type', 'valeur_min', 'valeur_max', 'amplitude']].to_string(index=False))
    
    # Top 5 par coefficient de variation
    result_cv = result[result['vibration_moyenne'] > 0]
    top5_cv = result_cv.nlargest(5, 'coefficient_variation_pct')
    
    print("\nTop 5 des machines par coefficient de variation:")
    print(top5_cv[['machine', 'site', 'nombre_mesures', 'vibration_moyenne', 
                   'ecart_type', 'coefficient_variation_pct']].to_string(index=False))
    
    return top5_std


def analyse_evolution_pression(df_pressure):
    """
    Analyse 4: Évolution horaire de la pression moyenne par site
    """
    
    print("\n" + "=" * 70)
    print("📊 ANALYSE 4: Évolution horaire de la pression par site")
    print("=" * 70)
    
    if df_pressure is None:
        print("⚠️  Pas de données de pression disponibles")
        return None
    
    # Grouper par site et heure
    result = df_pressure.groupby(['site', 'hour']).agg({
        'value': ['mean', 'min', 'max', 'count'],
        'is_critical': 'sum'
    }).round(3)
    
    result.columns = ['pression_moyenne', 'pression_min', 'pression_max', 
                      'nombre_mesures', 'alertes']
    result = result.reset_index()
    result = result.rename(columns={'hour': 'heure'})
    result = result.sort_values(['site', 'heure'])
    
    print("\nÉvolution horaire de la pression:")
    print(result.to_string(index=False))
    
    # Résumé par site
    print("\nRésumé par site:")
    summary = df_pressure.groupby('site').agg({
        'value': ['mean', 'std', 'count']
    }).round(3)
    summary.columns = ['pression_moyenne_globale', 'variabilite', 'total_mesures']
    summary['heures_couvertes'] = df_pressure.groupby('site')['hour'].nunique()
    summary = summary.reset_index()
    print(summary.to_string(index=False))
    
    return result


def generate_summary_report(data):
    """Génère un rapport résumé global"""
    
    print("\n" + "=" * 70)
    print("📋 RAPPORT RÉSUMÉ GLOBAL")
    print("=" * 70)
    
    total_mesures = 0
    total_critiques = 0
    
    for sensor_type, df in data.items():
        if df is not None:
            count = len(df)
            critical = df['is_critical'].sum() if 'is_critical' in df.columns else 0
            total_mesures += count
            total_critiques += critical
            
            print(f"\n{sensor_type.upper()}:")
            print(f"  - Total mesures: {count}")
            print(f"  - Alertes critiques: {int(critical)} ({round(critical/count*100, 2)}%)")
            
            sites = df['site'].nunique()
            machines = df['machine'].nunique()
            print(f"  - Sites: {sites}")
            print(f"  - Machines: {machines}")
    
    print(f"\n{'='*40}")
    print(f"TOTAL GLOBAL:")
    print(f"  - Mesures: {total_mesures}")
    print(f"  - Alertes critiques: {int(total_critiques)}")
    if total_mesures > 0:
        print(f"  - Taux d'alerte: {round(total_critiques/total_mesures*100, 2)}%")


def export_to_csv(data, results):
    """Exporte les résultats en CSV pour Power BI"""
    
    print("\n" + "=" * 70)
    print("💾 EXPORT DES DONNÉES POUR POWER BI")
    print("=" * 70)
    
    os.makedirs(REPORTS_DIR, exist_ok=True)
    
    # 1. Température par site et machine
    if results.get('temperature') is not None:
        path = os.path.join(REPORTS_DIR, "temperature_par_site_machine.csv")
        results['temperature'].to_csv(path, index=False)
        print(f"✅ Exporté: {path}")
    
    # 2. Alertes critiques
    if results.get('alertes') is not None:
        path = os.path.join(REPORTS_DIR, "alertes_critiques.csv")
        results['alertes'].to_csv(path, index=False)
        print(f"✅ Exporté: {path}")
    
    # 3. Variabilité vibration
    if results.get('vibration') is not None:
        path = os.path.join(REPORTS_DIR, "variabilite_vibration.csv")
        results['vibration'].to_csv(path, index=False)
        print(f"✅ Exporté: {path}")
    
    # 4. Évolution pression
    if results.get('pressure') is not None:
        path = os.path.join(REPORTS_DIR, "evolution_pression_horaire.csv")
        results['pressure'].to_csv(path, index=False)
        print(f"✅ Exporté: {path}")
    
    # 5. Données complètes par type
    for sensor_type, df in data.items():
        if df is not None:
            path = os.path.join(REPORTS_DIR, f"{sensor_type}_full_data.csv")
            df.to_csv(path, index=False)
            print(f"✅ Exporté: {path}")
    
    print(f"\n📁 Tous les fichiers exportés dans: {REPORTS_DIR}")


def main():
    """Fonction principale"""
    
    print("\n" + "=" * 70)
    print("🔬 InduSense - Module d'Analyse Décisionnelle (Pandas)")
    print("=" * 70)
    
    # Charger les données
    print("\n📂 Chargement des données du Lakehouse...")
    data = load_data()
    
    # Vérifier qu'il y a des données
    if all(df is None for df in data.values()):
        print("\n❌ Aucune donnée disponible. Exécutez d'abord le pipeline d'intégration.")
        return
    
    # Exécuter les analyses
    print("\n" + "=" * 70)
    print("🔍 EXÉCUTION DES ANALYSES")
    print("=" * 70)
    
    results = {}
    
    # Analyse 1: Température moyenne
    results['temperature'] = analyse_temperature_moyenne(data["temperature"])
    
    # Analyse 2: Alertes critiques
    results['alertes'] = analyse_alertes_critiques(data)
    
    # Analyse 3: Top 5 variabilité vibration
    results['vibration'] = analyse_variabilite_vibration(data["vibration"])
    
    # Analyse 4: Évolution pression
    results['pressure'] = analyse_evolution_pression(data["pressure"])
    
    # Rapport résumé
    generate_summary_report(data)
    
    # Export pour Power BI
    export_to_csv(data, results)
    
    print("\n✅ Analyse terminée")


if __name__ == "__main__":
    main()
