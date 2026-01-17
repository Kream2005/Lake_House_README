#!/usr/bin/env python3
"""
Module d'analyse décisionnelle pour InduSense
Utilise Spark SQL pour produire les analyses sur les données du Lakehouse
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, stddev, sum as spark_sum,
    hour, date_format, when, desc, asc,
    round as spark_round, variance
)
from delta import configure_spark_with_delta_pip
import os

# Configuration des chemins
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE_DIR = os.path.join(BASE_DIR, "..", "data_lake", "warehouse")
REPORTS_DIR = os.path.join(BASE_DIR, "..", "reports")


def create_spark_session():
    """Crée une session Spark configurée pour Delta Lake"""
    
    builder = SparkSession.builder \
        .appName("InduSense-Analytics") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def load_data(spark):
    """Charge les données du Lakehouse"""
    
    data = {}
    
    for sensor_type in ["temperature", "vibration", "pressure"]:
        path = os.path.join(WAREHOUSE_DIR, sensor_type)
        if os.path.exists(path):
            df = spark.read.format("delta").load(path)
            data[sensor_type] = df
            print(f"{sensor_type}: {df.count()} enregistrements chargés")
        else:
            print(f"{sensor_type}: données non trouvées")
            data[sensor_type] = None
    
    return data


def register_temp_views(spark, data):
    """Enregistre les DataFrames comme vues temporaires SQL"""
    
    for sensor_type, df in data.items():
        if df is not None:
            df.createOrReplaceTempView(sensor_type)
            print(f"Vue SQL créée: {sensor_type}")


def analyse_temperature_moyenne_par_site_machine(spark, df_temp):
    """
    Analyse 1: Température moyenne par site et par machine sur la journée
    """
    
    print("\n" + "=" * 70)
    print("ANALYSE 1: Température moyenne par site et par machine")
    print("=" * 70)
    
    if df_temp is None:
        print("Pas de données de température disponibles")
        return None
    
    # Utilisation de Spark SQL
    result = spark.sql("""
        SELECT 
            site,
            machine,
            ROUND(AVG(value), 2) as temperature_moyenne,
            ROUND(MIN(value), 2) as temperature_min,
            ROUND(MAX(value), 2) as temperature_max,
            COUNT(*) as nombre_mesures,
            SUM(CASE WHEN is_critical = true THEN 1 ELSE 0 END) as alertes_critiques
        FROM temperature
        GROUP BY site, machine
        ORDER BY site, machine
    """)
    
    print("\nRésultats:")
    result.show(50, truncate=False)
    
    return result


def analyse_alertes_critiques_par_type(spark, data):
    """
    Analyse 2: Nombre total d'alertes critiques par type de capteur
    """
    
    print("\n" + "=" * 70)
    print("📊 ANALYSE 2: Alertes critiques par type de capteur")
    print("=" * 70)
    
    results = []
    
    for sensor_type, df in data.items():
        if df is not None:
            critical_count = df.filter(col("is_critical") == True).count()
            total_count = df.count()
            percentage = round((critical_count / total_count) * 100, 2) if total_count > 0 else 0
            
            results.append({
                "type_capteur": sensor_type,
                "alertes_critiques": critical_count,
                "total_mesures": total_count,
                "pourcentage_critique": percentage
            })
    
    # Créer un DataFrame avec les résultats
    result_df = spark.createDataFrame(results)
    
    print("\nRésultats:")
    result_df.show(truncate=False)
    
    # Aussi avec SQL pur (union des trois types)
    print("\nDétail par site (SQL):")
    
    sql_result = spark.sql("""
        SELECT 
            'temperature' as type_capteur,
            site,
            SUM(CASE WHEN is_critical = true THEN 1 ELSE 0 END) as alertes_critiques,
            COUNT(*) as total_mesures
        FROM temperature
        GROUP BY site
        
        UNION ALL
        
        SELECT 
            'vibration' as type_capteur,
            site,
            SUM(CASE WHEN is_critical = true THEN 1 ELSE 0 END) as alertes_critiques,
            COUNT(*) as total_mesures
        FROM vibration
        GROUP BY site
        
        UNION ALL
        
        SELECT 
            'pressure' as type_capteur,
            site,
            SUM(CASE WHEN is_critical = true THEN 1 ELSE 0 END) as alertes_critiques,
            COUNT(*) as total_mesures
        FROM pressure
        GROUP BY site
        
        ORDER BY type_capteur, site
    """)
    
    sql_result.show(50, truncate=False)
    
    return result_df


def analyse_top5_variabilite_vibration(spark, df_vibration):
    """
    Analyse 3: Top 5 des machines présentant la plus forte variabilité de vibration
    """
    
    print("\n" + "=" * 70)
    print("ANALYSE 3: Top 5 machines - Variabilité de vibration")
    print("=" * 70)
    
    if df_vibration is None:
        print("Pas de données de vibration disponibles")
        return None
    
    # La variabilité est mesurée par l'écart-type et le coefficient de variation
    result = spark.sql("""
        SELECT 
            machine,
            site,
            COUNT(*) as nombre_mesures,
            ROUND(AVG(value), 3) as vibration_moyenne,
            ROUND(STDDEV(value), 3) as ecart_type,
            ROUND(MIN(value), 3) as valeur_min,
            ROUND(MAX(value), 3) as valeur_max,
            ROUND(MAX(value) - MIN(value), 3) as amplitude,
            ROUND((STDDEV(value) / AVG(value)) * 100, 2) as coefficient_variation_pct
        FROM vibration
        GROUP BY machine, site
        HAVING COUNT(*) >= 10
        ORDER BY ecart_type DESC
        LIMIT 5
    """)
    
    print("\nTop 5 des machines par variabilité (écart-type):")
    result.show(truncate=False)
    
    # Analyse alternative: par coefficient de variation
    result_cv = spark.sql("""
        SELECT 
            machine,
            site,
            COUNT(*) as nombre_mesures,
            ROUND(AVG(value), 3) as vibration_moyenne,
            ROUND(STDDEV(value), 3) as ecart_type,
            ROUND((STDDEV(value) / AVG(value)) * 100, 2) as coefficient_variation_pct
        FROM vibration
        GROUP BY machine, site
        HAVING COUNT(*) >= 10 AND AVG(value) > 0
        ORDER BY coefficient_variation_pct DESC
        LIMIT 5
    """)
    
    print("\nTop 5 des machines par coefficient de variation:")
    result_cv.show(truncate=False)
    
    return result


def analyse_evolution_horaire_pression(spark, df_pressure):
    """
    Analyse 4: Évolution horaire de la pression moyenne par site
    """
    
    print("\n" + "=" * 70)
    print("📊 ANALYSE 4: Évolution horaire de la pression par site")
    print("=" * 70)
    
    if df_pressure is None:
        print("Pas de données de pression disponibles")
        return None
    
    result = spark.sql("""
        SELECT 
            site,
            hour as heure,
            ROUND(AVG(value), 3) as pression_moyenne,
            ROUND(MIN(value), 3) as pression_min,
            ROUND(MAX(value), 3) as pression_max,
            COUNT(*) as nombre_mesures,
            SUM(CASE WHEN is_critical = true THEN 1 ELSE 0 END) as alertes
        FROM pressure
        GROUP BY site, hour
        ORDER BY site, hour
    """)
    
    print("\nÉvolution horaire de la pression:")
    result.show(100, truncate=False)
    
    # Résumé par site
    print("\nRésumé par site:")
    summary = spark.sql("""
        SELECT 
            site,
            ROUND(AVG(value), 3) as pression_moyenne_globale,
            ROUND(STDDEV(value), 3) as variabilite,
            COUNT(DISTINCT hour) as heures_couvertes,
            COUNT(*) as total_mesures
        FROM pressure
        GROUP BY site
        ORDER BY site
    """)
    summary.show(truncate=False)
    
    return result


def generate_summary_report(spark, data):
    """Génère un rapport résumé global"""
    
    print("\n" + "=" * 70)
    print("RAPPORT RÉSUMÉ GLOBAL")
    print("=" * 70)
    
    total_mesures = 0
    total_critiques = 0
    
    for sensor_type, df in data.items():
        if df is not None:
            count = df.count()
            critical = df.filter(col("is_critical") == True).count()
            total_mesures += count
            total_critiques += critical
            
            print(f"\n{sensor_type.upper()}:")
            print(f"  - Total mesures: {count}")
            print(f"  - Alertes critiques: {critical} ({round(critical/count*100, 2)}%)")
            
            # Sites uniques
            sites = df.select("site").distinct().count()
            machines = df.select("machine").distinct().count()
            print(f"  - Sites: {sites}")
            print(f"  - Machines: {machines}")
    
    print(f"\n{'='*40}")
    print(f"TOTAL GLOBAL:")
    print(f"  - Mesures: {total_mesures}")
    print(f"  - Alertes critiques: {total_critiques}")
    if total_mesures > 0:
        print(f"  - Taux d'alerte: {round(total_critiques/total_mesures*100, 2)}%")


def export_results_to_csv(spark, data):
    """Exporte les résultats d'analyse en CSV pour Power BI"""
    
    print("\n" + "=" * 70)
    print("EXPORT DES DONNÉES POUR POWER BI")
    print("=" * 70)
    
    os.makedirs(REPORTS_DIR, exist_ok=True)
    
    # 1. Température moyenne par site et machine
    if data["temperature"] is not None:
        temp_analysis = spark.sql("""
            SELECT 
                site, machine,
                ROUND(AVG(value), 2) as temperature_moyenne,
                ROUND(MIN(value), 2) as temperature_min,
                ROUND(MAX(value), 2) as temperature_max,
                COUNT(*) as nombre_mesures,
                SUM(CASE WHEN is_critical = true THEN 1 ELSE 0 END) as alertes_critiques
            FROM temperature
            GROUP BY site, machine
        """)
        
        temp_path = os.path.join(REPORTS_DIR, "temperature_par_site_machine.csv")
        temp_analysis.toPandas().to_csv(temp_path, index=False)
        print(f"Exporté: {temp_path}")
    
    # 2. Alertes critiques par type
    alerts_data = []
    for sensor_type, df in data.items():
        if df is not None:
            stats = df.groupBy("site").agg(
                count("*").alias("total"),
                spark_sum(when(col("is_critical"), 1).otherwise(0)).alias("critiques")
            ).collect()
            
            for row in stats:
                alerts_data.append({
                    "type_capteur": sensor_type,
                    "site": row["site"],
                    "total_mesures": row["total"],
                    "alertes_critiques": row["critiques"]
                })
    
    if alerts_data:
        alerts_df = spark.createDataFrame(alerts_data)
        alerts_path = os.path.join(REPORTS_DIR, "alertes_critiques.csv")
        alerts_df.toPandas().to_csv(alerts_path, index=False)
        print(f"Exporté: {alerts_path}")
    
    # 3. Variabilité vibration
    if data["vibration"] is not None:
        variability = spark.sql("""
            SELECT 
                machine, site,
                COUNT(*) as nombre_mesures,
                ROUND(AVG(value), 3) as vibration_moyenne,
                ROUND(STDDEV(value), 3) as ecart_type,
                ROUND(MIN(value), 3) as valeur_min,
                ROUND(MAX(value), 3) as valeur_max
            FROM vibration
            GROUP BY machine, site
        """)
        
        var_path = os.path.join(REPORTS_DIR, "variabilite_vibration.csv")
        variability.toPandas().to_csv(var_path, index=False)
        print(f"Exporté: {var_path}")
    
    # 4. Évolution horaire pression
    if data["pressure"] is not None:
        hourly_pressure = spark.sql("""
            SELECT 
                site, hour as heure,
                ROUND(AVG(value), 3) as pression_moyenne,
                COUNT(*) as nombre_mesures
            FROM pressure
            GROUP BY site, hour
        """)
        
        pressure_path = os.path.join(REPORTS_DIR, "evolution_pression_horaire.csv")
        hourly_pressure.toPandas().to_csv(pressure_path, index=False)
        print(f"Exporté: {pressure_path}")
    
    # 5. Données complètes par type pour analyse détaillée
    for sensor_type, df in data.items():
        if df is not None:
            full_path = os.path.join(REPORTS_DIR, f"{sensor_type}_full_data.csv")
            df.toPandas().to_csv(full_path, index=False)
            print(f"Exporté: {full_path}")
    
    print(f"\nTous les fichiers exportés dans: {REPORTS_DIR}")


def main():
    """Fonction principale"""
    
    print("\n" + "=" * 70)
    print("InduSense - Module d'Analyse Décisionnelle")
    print("=" * 70)
    
    # Créer la session Spark
    print("\nInitialisation de Spark...")
    spark = create_spark_session()
    print("Session Spark initialisée")
    
    # Charger les données
    print("\nChargement des données du Lakehouse...")
    data = load_data(spark)
    
    # Enregistrer les vues SQL
    print("\nCréation des vues SQL...")
    register_temp_views(spark, data)
    
    # Exécuter les analyses
    print("\n" + "=" * 70)
    print("EXÉCUTION DES ANALYSES")
    print("=" * 70)
    
    # Analyse 1: Température moyenne
    analyse_temperature_moyenne_par_site_machine(spark, data["temperature"])
    
    # Analyse 2: Alertes critiques
    analyse_alertes_critiques_par_type(spark, data)
    
    # Analyse 3: Top 5 variabilité vibration
    analyse_top5_variabilite_vibration(spark, data["vibration"])
    
    # Analyse 4: Évolution pression
    analyse_evolution_horaire_pression(spark, data["pressure"])
    
    # Rapport résumé
    generate_summary_report(spark, data)
    
    # Export pour Power BI
    export_results_to_csv(spark, data)
    
    # Fermer la session
    spark.stop()
    print("\nAnalyse terminée")


if __name__ == "__main__":
    main()
