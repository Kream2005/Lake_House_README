#!/usr/bin/env python3
"""
Pipeline d'intégration des données dans le Data Lakehouse
Utilise Apache Spark et Delta Lake pour transformer et stocker les données
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lit, year, month, dayofmonth, hour,
    when, input_file_name, current_timestamp,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    BooleanType, TimestampType
)
from delta import DeltaTable
import os
import time
import shutil

# Configuration des chemins
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(BASE_DIR, "..", "data_lake", "raw")
WAREHOUSE_DIR = os.path.join(BASE_DIR, "..", "data_lake", "warehouse")
CHECKPOINT_DIR = os.path.join(BASE_DIR, "..", "data_lake", "checkpoints")
PROCESSED_DIR = os.path.join(BASE_DIR, "..", "data_lake", "processed")


def create_spark_session():
    """Crée une session Spark configurée pour Delta Lake"""
    
    spark = SparkSession.builder \
        .appName("InduSense-DataLakehouse-Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0") \
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR) \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def get_sensor_schema(sensor_type):
    """Retourne le schéma pour un type de capteur"""
    
    base_schema = StructType([
        StructField("sensor_id", StringType(), False),
        StructField("type", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("unit", StringType(), False),
        StructField("site", StringType(), False),
        StructField("machine", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("is_critical", BooleanType(), True)
    ])
    
    if sensor_type == "vibration":
        return StructType(base_schema.fields + [
            StructField("frequency_hz", DoubleType(), True)
        ])
    elif sensor_type == "pressure":
        return StructType(base_schema.fields + [
            StructField("value_pascal", DoubleType(), True)
        ])
    
    return base_schema


def validate_data(df, sensor_type):
    """Valide les données selon le type de capteur"""
    
    # Filtrer les enregistrements avec des valeurs nulles critiques
    df_valid = df.filter(
        col("sensor_id").isNotNull() &
        col("type").isNotNull() &
        col("value").isNotNull() &
        col("site").isNotNull() &
        col("machine").isNotNull() &
        col("timestamp").isNotNull()
    )
    
    # Validation des plages de valeurs selon le type
    if sensor_type == "temperature":
        df_valid = df_valid.filter(
            (col("value") >= -50) & (col("value") <= 200)
        )
    elif sensor_type == "vibration":
        df_valid = df_valid.filter(
            (col("value") >= 0) & (col("value") <= 100)
        )
    elif sensor_type == "pressure":
        df_valid = df_valid.filter(
            (col("value") >= 0) & (col("value") <= 50)
        )
    
    return df_valid


def transform_data(df):
    """Transforme les données pour le Lakehouse"""
    
    # Convertir le timestamp string en timestamp
    df_transformed = df.withColumn(
        "timestamp_dt",
        col("timestamp").cast(TimestampType())
    )
    
    # Ajouter les colonnes de partitionnement
    df_transformed = df_transformed \
        .withColumn("year", year(col("timestamp_dt"))) \
        .withColumn("month", month(col("timestamp_dt"))) \
        .withColumn("day", dayofmonth(col("timestamp_dt"))) \
        .withColumn("hour", hour(col("timestamp_dt")))
    
    # Ajouter un timestamp d'ingestion
    df_transformed = df_transformed.withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )
    
    return df_transformed


def process_sensor_data_batch(spark, sensor_type):
    """Traite les données d'un type de capteur en mode batch"""
    
    raw_path = os.path.join(RAW_DATA_DIR, sensor_type)
    warehouse_path = os.path.join(WAREHOUSE_DIR, sensor_type)
    processed_path = os.path.join(PROCESSED_DIR, sensor_type)
    
    print(f"\n{'='*60}")
    print(f"Traitement des données {sensor_type.upper()}")
    print(f"{'='*60}")
    
    # Vérifier si le répertoire source existe et contient des fichiers
    if not os.path.exists(raw_path):
        print(f"Répertoire source non trouvé: {raw_path}")
        return 0
    
    json_files = [f for f in os.listdir(raw_path) if f.endswith('.json')]
    if not json_files:
        print(f"Aucun fichier JSON trouvé dans {raw_path}")
        return 0
    
    print(f"{len(json_files)} fichiers JSON trouvés")
    
    # Lire les fichiers JSON
    schema = get_sensor_schema(sensor_type)

    try:
        # Spark 4.x : utiliser le répertoire directement ou une liste de fichiers
        # Option 1: Passer le répertoire directement
        df_raw = spark.read \
            .schema(schema) \
            .option("multiLine", True) \
            .json(raw_path)
        
        initial_count = df_raw.count()
        print(f"{initial_count} enregistrements lus")
        
        if initial_count == 0:
            print("Aucun enregistrement valide trouvé")
            return 0
        
        # Validation des données
        df_valid = validate_data(df_raw, sensor_type)
        valid_count = df_valid.count()
        invalid_count = initial_count - valid_count
        
        if invalid_count > 0:
            print(f"{invalid_count} enregistrements invalides filtrés")
        
        print(f"{valid_count} enregistrements valides")
        
        if valid_count == 0:
            print("Aucun enregistrement valide après filtrage")
            return 0
        
        # Transformation des données
        df_transformed = transform_data(df_valid)
        
        # Écrire en format Delta Lake avec partitionnement
        print(f"Écriture en format Delta Lake...")
        
        df_transformed.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("site", "year", "month", "day") \
            .save(warehouse_path)
        
        print(f"Données écrites dans: {warehouse_path}")
        
        # Déplacer les fichiers traités
        os.makedirs(processed_path, exist_ok=True)
        for json_file in json_files:
            src = os.path.join(raw_path, json_file)
            dst = os.path.join(processed_path, json_file)
            shutil.move(src, dst)
        
        print(f"{len(json_files)} fichiers déplacés vers processed/")
        
        return valid_count
        
    except Exception as e:
        print(f"Erreur lors du traitement: {str(e)}")
        import traceback
        traceback.print_exc()
        return 0


def process_sensor_data_streaming(spark, sensor_type):
    """Traite les données d'un type de capteur en mode streaming"""
    
    raw_path = os.path.join(RAW_DATA_DIR, sensor_type)
    warehouse_path = os.path.join(WAREHOUSE_DIR, sensor_type)
    checkpoint_path = os.path.join(CHECKPOINT_DIR, sensor_type)
    
    print(f"\n{'='*60}")
    print(f"Démarrage du streaming pour {sensor_type.upper()}")
    print(f"{'='*60}")
    
    # Créer les répertoires si nécessaire
    os.makedirs(raw_path, exist_ok=True)
    os.makedirs(checkpoint_path, exist_ok=True)
    
    schema = get_sensor_schema(sensor_type)
    
    # Lecture en streaming
    df_stream = spark.readStream \
        .schema(schema) \
        .option("maxFilesPerTrigger", 100) \
        .json(raw_path)
    
    # Transformation
    df_transformed = transform_data(df_stream)
    
    # Écriture en streaming vers Delta Lake
    query = df_transformed.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("site", "year", "month", "day") \
        .start(warehouse_path)
    
    return query


def show_warehouse_stats(spark):
    """Affiche les statistiques du warehouse"""
    
    print("\n" + "=" * 70)
    print("STATISTIQUES DU DATA LAKEHOUSE")
    print("=" * 70)
    
    sensor_types = ["temperature", "vibration", "pressure"]
    
    for sensor_type in sensor_types:
        warehouse_path = os.path.join(WAREHOUSE_DIR, sensor_type)
        
        if os.path.exists(warehouse_path):
            try:
                df = spark.read.format("delta").load(warehouse_path)
                count = df.count()
                
                # Statistiques par site
                print(f"\n{sensor_type.upper()}: {count} enregistrements")
                
                sites_stats = df.groupBy("site").count().collect()
                for row in sites_stats:
                    print(f"   {row['site']}: {row['count']} mesures")
                
                # Nombre d'alertes critiques
                critical_count = df.filter(col("is_critical") == True).count()
                print(f"   Alertes critiques: {critical_count}")
                
            except Exception as e:
                print(f"\nErreur lecture {sensor_type}: {str(e)}")
        else:
            print(f"\n{sensor_type}: Pas de données")


def run_batch_pipeline(spark):
    """Exécute le pipeline en mode batch"""
    
    print("\n" + "=" * 70)
    print("DÉMARRAGE DU PIPELINE D'INTÉGRATION (MODE BATCH)")
    print("=" * 70)
    print(f"Source: {RAW_DATA_DIR}")
    print(f"Destination: {WAREHOUSE_DIR}")
    
    total_records = 0
    
    for sensor_type in ["temperature", "vibration", "pressure"]:
        count = process_sensor_data_batch(spark, sensor_type)
        total_records += count
    
    print("\n" + "=" * 70)
    print("PIPELINE TERMINÉ")
    print("=" * 70)
    print(f"Total des enregistrements traités: {total_records}")
    
    # Afficher les statistiques
    show_warehouse_stats(spark)


def run_streaming_pipeline(spark, duration_seconds=60):
    """Exécute le pipeline en mode streaming"""
    
    print("\n" + "=" * 70)
    print("DÉMARRAGE DU PIPELINE D'INTÉGRATION (MODE STREAMING)")
    print("=" * 70)
    print(f"Surveillance de: {RAW_DATA_DIR}")
    print(f"Destination: {WAREHOUSE_DIR}")
    print(f"Durée: {duration_seconds} secondes")
    
    queries = []
    
    
    for sensor_type in ["temperature", "vibration", "pressure"]:
        query = process_sensor_data_streaming(spark, sensor_type)
        queries.append(query)
    
    print("\nStreaming actif... (Ctrl+C pour arrêter)")
    
    try:
        time.sleep(duration_seconds)
    except KeyboardInterrupt:
        print("\nArrêt demandé...")
    
    # Arrêter les streams
    for query in queries:
        query.stop()
    
    print("\nStreaming arrêté")
    show_warehouse_stats(spark)


def main():
    """Fonction principale"""
    
    import sys
    
    print("\n" + "=" * 70)
    print("InduSense - Pipeline d'Intégration Data Lakehouse")
    print("=" * 70)
    
    # Créer la session Spark
    print("\nInitialisation de Spark avec Delta Lake...")
    spark = create_spark_session()
    print("Session Spark initialisée")
    
    # Mode d'exécution
    mode = sys.argv[1] if len(sys.argv) > 1 else "batch"
    
    if mode == "streaming":
        duration = int(sys.argv[2]) if len(sys.argv) > 2 else 60
        run_streaming_pipeline(spark, duration)
    else:
        run_batch_pipeline(spark)
    
    # Fermer la session
    spark.stop()
    print("\nSession Spark fermée")


if __name__ == "__main__":
    main()