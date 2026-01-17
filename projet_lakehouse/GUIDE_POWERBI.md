# Guide Power BI - Tableau de Bord InduSense

## 1. Connexion aux Données

### Option A: Import des fichiers CSV
1. Ouvrir Power BI Desktop
2. **Accueil** → **Obtenir des données** → **Texte/CSV**
3. Importer les fichiers du dossier `reports/`:
   - `temperature_par_site_machine.csv`
   - `alertes_critiques.csv`
   - `variabilite_vibration.csv`
   - `evolution_pression_horaire.csv`

### Option B: Connexion Delta Lake (avancé)
Pour une connexion directe au Delta Lake:
1. Utiliser le connecteur **Azure Data Lake Storage** ou **Databricks**
2. Pointer vers le dossier `data_lake/warehouse/`

---

## 2. Création du Tableau de Bord

### Page 1: Vue d'ensemble Température

**Visualisation 1: Température moyenne par site (Graphique à barres groupées)**
- Axe X: `site`
- Axe Y: `temperature_moyenne`
- Légende: `machine`
- Titre: "Température Moyenne par Site et Machine"

**Visualisation 2: Carte de chaleur température**
- Lignes: `site`
- Colonnes: `machine`
- Valeurs: `temperature_moyenne`
- Format: Mise en forme conditionnelle (vert → rouge)

**Visualisation 3: Jauge d'alertes**
- Valeur: Somme de `alertes_critiques`
- Maximum: Somme de `nombre_mesures`
- Titre: "Taux d'Alertes Critiques"

---

### Page 2: Alertes Critiques

**Visualisation 1: Graphique à secteurs par type**
- Valeurs: `alertes_critiques`
- Légende: `type_capteur`
- Titre: "Répartition des Alertes par Type de Capteur"

**Visualisation 2: Graphique à barres empilées**
- Axe X: `site`
- Axe Y: `alertes_critiques`
- Légende: `type_capteur`
- Titre: "Alertes par Site et Type"

**Visualisation 3: Tableau détaillé**
- Colonnes: `type_capteur`, `site`, `total_mesures`, `alertes_critiques`
- Ajouter colonne calculée: `% Critique = alertes_critiques / total_mesures * 100`

**KPI Cards:**
- Total alertes température
- Total alertes vibration
- Total alertes pression

---

### Page 3: Analyse Vibration

**Visualisation 1: Top 5 variabilité (Graphique à barres horizontales)**
- Axe Y: `machine`
- Axe X: `ecart_type`
- Filtre: Top 5 par `ecart_type`
- Titre: "Top 5 Machines - Plus Forte Variabilité de Vibration"

**Visualisation 2: Scatter Plot**
- Axe X: `vibration_moyenne`
- Axe Y: `ecart_type`
- Taille: `nombre_mesures`
- Détails: `machine`, `site`
- Titre: "Moyenne vs Variabilité par Machine"

**Visualisation 3: Tableau comparatif**
- Colonnes: `machine`, `site`, `vibration_moyenne`, `ecart_type`, `valeur_min`, `valeur_max`
- Tri: par `ecart_type` décroissant

---

### Page 4: Évolution Pression

**Visualisation 1: Graphique linéaire (Évolution horaire)**
- Axe X: `heure`
- Axe Y: `pression_moyenne`
- Légende: `site`
- Titre: "Évolution Horaire de la Pression par Site"

**Visualisation 2: Graphique à aires empilées**
- Axe X: `heure`
- Axe Y: `nombre_mesures`
- Légende: `site`
- Titre: "Volume de Mesures par Heure"

**Visualisation 3: Small multiples**
- Créer un graphique par site (5 graphiques)
- Chaque graphique montre l'évolution horaire

---

## 3. Mesures DAX à Créer

```dax
// Taux d'alerte global
Taux Alerte = 
DIVIDE(
    SUM('alertes_critiques'[alertes_critiques]),
    SUM('alertes_critiques'[total_mesures]),
    0
) * 100

// Température moyenne globale
Temp Moyenne Globale = 
AVERAGE('temperature_par_site_machine'[temperature_moyenne])

// Écart max vibration
Ecart Max Vibration = 
MAX('variabilite_vibration'[ecart_type])

// Pression moyenne par site (mesure dynamique)
Pression Moyenne = 
AVERAGE('evolution_pression_horaire'[pression_moyenne])
```

---

## 4. Filtres et Slicers Recommandés

Ajouter sur chaque page:
- **Slicer Site**: Filtre multi-sélection par site
- **Slicer Machine**: Filtre par machine
- **Slicer Type Capteur**: Pour les pages multi-capteurs
- **Slicer Période**: Si données sur plusieurs jours

---

## 5. Thème et Mise en Forme

### Palette de couleurs suggérée:
- Température: Orange/Rouge (#FF6B35, #FF4500)
- Vibration: Bleu (#4169E1, #1E90FF)
- Pression: Vert (#228B22, #32CD32)
- Alertes critiques: Rouge vif (#DC143C)
- Fond: Gris clair (#F5F5F5)

### Configuration des seuils visuels:
- Température > 90°C: Rouge
- Vibration > 10 mm/s: Rouge
- Pression < 0.5 ou > 10 Bar: Rouge

---

## 6. Export et Partage

1. **Publier sur Power BI Service**:
   - Fichier → Publier → Power BI Service
   
2. **Actualisation automatique**:
   - Configurer une passerelle si les CSV sont mis à jour
   
3. **Alertes Power BI**:
   - Créer des alertes sur les KPIs critiques

---

## 7. Modèle de Données

```
temperature_par_site_machine
├── site (texte)
├── machine (texte)
├── temperature_moyenne (décimal)
├── temperature_min (décimal)
├── temperature_max (décimal)
├── nombre_mesures (entier)
└── alertes_critiques (entier)

alertes_critiques
├── type_capteur (texte)
├── site (texte)
├── total_mesures (entier)
└── alertes_critiques (entier)

variabilite_vibration
├── machine (texte)
├── site (texte)
├── nombre_mesures (entier)
├── vibration_moyenne (décimal)
├── ecart_type (décimal)
├── valeur_min (décimal)
└── valeur_max (décimal)

evolution_pression_horaire
├── site (texte)
├── heure (entier)
├── pression_moyenne (décimal)
└── nombre_mesures (entier)
```

### Relations:
- `site` est la clé commune entre toutes les tables
- Créer une table de dimension `Sites` si nécessaire

---

## 8. Captures d'écran attendues

Le tableau de bord final devrait inclure:
1. ✅ Page température avec graphiques par site/machine
2. ✅ Page alertes avec répartition par type
3. ✅ Page vibration avec Top 5 machines
4. ✅ Page pression avec évolution horaire
5. ✅ Filtres interactifs sur toutes les pages
