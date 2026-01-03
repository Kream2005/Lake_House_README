# 🏭 Big Data Lakehouse - InduSense

## Mini-Projet Data Warehouse et Big Data Warehouse
**Université Sultan Moulay Slimane - ENSA Khouribga**  
**Filière: Informatique et Ingénierie de Données (2ème année)**  
**Module: Data Warehouse et Big Data Warehouse**  
**Professeur: M. Mostafa SAADI**

---

## 📋 Description du Projet

Ce projet simule un environnement industriel IoT pour la société **InduSense**, qui opère plusieurs sites équipés de capteurs collectant des mesures de température, vibration et pression. Le système implémente une architecture **Data Lakehouse** basée sur **Apache Spark** et **Delta Lake**.

---

## 🏗️ Architecture du Projet

```
projet_lakehouse/
├── simulators/                    # Partie 1: Simulateurs de capteurs
│   ├── temperature_sensor.py      # Simulateur température
│   ├── vibration_sensor.py        # Simulateur vibration
│   ├── pressure_sensor.py         # Simulateur pression
│   └── fast_generator.py          # Générateur rapide (tests)
│
├── pipeline/                      # Partie 2: Pipeline d'intégration
│   └── integration_pipeline.py    # Pipeline Spark + Delta Lake
│
├── analytics/                     # Partie 3: Analyses décisionnelles
│   └── spark_analytics.py         # Analyses Spark SQL
│
├── data_lake/                     # Stockage des données
│   ├── raw/                       # Données brutes (JSON)
│   │   ├── temperature/
│   │   ├── vibration/
│   │   └── pressure/
│   ├── warehouse/                 # Delta Lake (données transformées)
│   ├── processed/                 # Fichiers traités
│   └── checkpoints/               # Checkpoints streaming
│
├── reports/                       # Fichiers exportés pour Power BI
│
├── requirements.txt               # Dépendances Python
├── GUIDE_POWERBI.md              # Instructions Power BI
└── README.md                      # Ce fichier
```

---

## 🚀 Installation et Configuration

### Prérequis
- Python 3.8+
- Java 8 ou 11 (pour Spark)
- Apache Spark 3.4+
- Power BI Desktop (pour la visualisation)

### Installation des dépendances

```bash
# Créer un environnement virtuel (optionnel mais recommandé)
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate  # Windows

# Installer les dépendances
pip install -r requirements.txt
```

### Configuration de Spark

Assurez-vous que les variables d'environnement sont configurées:
```bash
export SPARK_HOME=/path/to/spark
export PATH=$PATH:$SPARK_HOME/bin
export JAVA_HOME=/path/to/java
```

---

## 📖 Guide d'Utilisation

### Partie 1: Génération des Données

#### Option A: Génération rapide (recommandé pour les tests)
```bash
cd simulators
python fast_generator.py
```
Génère 1000 mesures par type de capteur instantanément.

#### Option B: Simulation en temps réel
```bash
# Terminal 1
python temperature_sensor.py

# Terminal 2
python vibration_sensor.py

# Terminal 3
python pressure_sensor.py
```
Chaque script génère des mesures toutes les 1-3 secondes.

### Partie 2: Pipeline d'Intégration

#### Mode Batch (traitement par lots)
```bash
cd pipeline
python integration_pipeline.py batch
```

#### Mode Streaming (surveillance continue)
```bash
python integration_pipeline.py streaming 120  # 120 secondes
```

### Partie 3: Analyses Décisionnelles

```bash
cd analytics
python spark_analytics.py
```

Les analyses produites:
1. **Température moyenne** par site et par machine
2. **Alertes critiques** par type de capteur
3. **Top 5 machines** avec la plus forte variabilité de vibration
4. **Évolution horaire** de la pression par site

### Partie 4: Reporting Power BI

1. Les fichiers CSV sont exportés dans le dossier `reports/`
2. Suivre les instructions dans `GUIDE_POWERBI.md`
3. Importer les fichiers dans Power BI Desktop
4. Créer les visualisations selon le guide

---

## 📊 Structure des Données

### Format JSON des mesures

```json
{
  "sensor_id": "uuid-unique",
  "type": "temperature|vibration|pressure",
  "value": 45.67,
  "unit": "Celsius|mm/s|Bar",
  "site": "Site_Paris",
  "machine": "Machine_A1",
  "timestamp": "2026-01-08T10:30:00",
  "is_critical": false
}
```

### Seuils Critiques

| Type | Seuil Critique |
|------|----------------|
| Température | > 90°C |
| Vibration | > 10 mm/s |
| Pression | < 0.5 Bar ou > 10 Bar |

### Sites et Machines

- **Sites**: Site_Paris, Site_Lyon, Site_Marseille, Site_Toulouse, Site_Bordeaux
- **Machines**: Machine_A1, Machine_A2, Machine_B1, Machine_B2, Machine_C1

---

## 🔧 Configuration Delta Lake

Le partitionnement des données dans le warehouse:
```
warehouse/
├── temperature/
│   └── site=Site_Paris/
│       └── year=2026/
│           └── month=1/
│               └── day=8/
│                   └── *.parquet
```

---

## 📈 Analyses Spark SQL

### Exemple de requête - Température moyenne
```sql
SELECT 
    site,
    machine,
    ROUND(AVG(value), 2) as temperature_moyenne,
    COUNT(*) as nombre_mesures,
    SUM(CASE WHEN is_critical THEN 1 ELSE 0 END) as alertes
FROM temperature
GROUP BY site, machine
ORDER BY site, machine
```

### Exemple de requête - Top 5 variabilité vibration
```sql
SELECT 
    machine,
    site,
    ROUND(STDDEV(value), 3) as ecart_type,
    ROUND(AVG(value), 3) as moyenne
FROM vibration
GROUP BY machine, site
ORDER BY ecart_type DESC
LIMIT 5
```

---

## 🎯 Livrables du Projet

- [x] **Partie 1**: 3 scripts simulateurs de capteurs
- [x] **Partie 2**: Pipeline d'intégration Spark + Delta Lake
- [x] **Partie 3**: Module d'analyses Spark SQL
- [x] **Partie 4**: Guide de création du tableau de bord Power BI
- [x] Documentation complète

---

## 📝 Notes Importantes

1. **Volume de données**: Minimum 1000 mesures par type (configurable)
2. **Delta Lake**: Format de stockage optimisé avec support ACID
3. **Partitionnement**: Par site, année, mois, jour pour performances optimales
4. **Power BI**: Utiliser les fichiers CSV exportés pour l'import

---

## 🐛 Dépannage

### Erreur Java non trouvé
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

### Erreur mémoire Spark
Augmenter la mémoire dans le script:
```python
.config("spark.driver.memory", "4g")
```

### Fichiers Delta corrompus
Supprimer le dossier `warehouse/` et relancer le pipeline.

---

## 👥 Auteur

Projet réalisé dans le cadre du module Data Warehouse et Big Data Warehouse.

---

## 📄 Licence

Projet académique - ENSA Khouribga 2025-2026
