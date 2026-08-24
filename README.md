# Airline_Loyalty_Program

Ce projet met en place une infrastructure d'analyse de données de bout en bout (**ETL & Data Visualization**) dédiée à l'analyse du programme de fidélité d'une compagnie aérienne. 

Il permet d'évaluer l'impact des campagnes d'adhésion (notamment la **Promotion 2018**), de comprendre le profil démographique des membres et d'analyser la réservation de vols pendant la période estivale.

---

## 🛠️ Architecture Technique

L'infrastructure s'appuie sur une architecture multi-conteneurs orchestrée avec **Docker Compose** :

* **Database (`PostgreSQL 15`)** : Stocke à la fois les données brutes ingérées depuis les fichiers CSV et les tables d'insights précalculées.
* **Engine de Calcul (`PySpark 3.5.0`)** : Cluster Spark distribué (Master + Worker) pour exécuter les traitements ETL et les agrégations complexes.
* **Visualisation (`Streamlit`)** : Application web interactive exposant les résultats sous forme de tableaux et de graphiques Plotly.
* **Orchestration (`Makefile`)** : Commande unique pour construire, lancer et nettoyer l'ensemble des services.

              ┌──────────────────────────────┐
              │    Fichiers CSV Bruts        │
              └──────────────┬───────────────┘
                             │ Ingestion (Pandas / SQLAlchemy)
                             ▼
              ┌──────────────────────────────┐
              │    PostgreSQL (raw_*)        │
              └──────────────┬───────────────┘
                             │ Spark JDBC Reader
                             ▼
              ┌──────────────────────────────┐
              │   Spark Master / Worker      │
              │   (Transformations SQL)      │
              └──────────────┬───────────────┘
                             │ Spark JDBC Writer
                             ▼
              ┌──────────────────────────────┐
              │   PostgreSQL (insight_*)     │
              └──────────────┬───────────────┘
                             │ Query SQL
                             ▼
              ┌──────────────────────────────┐
              │    Dashboard Streamlit       │
              └──────────────────────────────┘


## Insights & Analyses Traitées
Le pipeline répond à 3 problématiques métier fondamentales :

Inscriptions Brutes vs Nettes : Comparaison du volume global d'adhésions et du taux de rétention entre les membres issus de la 2018 Promotion et les adhésions Standard.

Profil Démographique (Promo 2018) : Répartition par genre et par niveau d'études des membres ayant rejoint le programme durant la promotion de 2018.

Réservation de Vols en Été : Comparaison du nombre total de vols réservés durant la saison estivale (juin, juillet, août) entre 2017 et 2018 par type d'engagement.

## Lancement Rapide
Prérequis
Docker et Docker Compose installés.

Make disponible dans le terminal.

Les fichiers CSV bruts placés dans le dossier ./data/ à la racine du projet.

1. Démarrer l'application
Pour exécuter l'ensemble du pipeline (ingestion, calcul Spark et lancement du dashboard) : make run
Note : Si tes données se trouvent dans un autre dossier, tu peux spécifier son chemin : make run DATA_PATH=/chemin/vers/tes/donnees

2. Accéder au Dashboard
Une fois le conteneur spark-pipeline terminé avec succès, le tableau de bord Streamlit devient accessible à l'adresse suivante : http://localhost:8501

Pour observer le cluster Spark en fonctionnement : http://localhost:8080 (UI Spark Master)

3. Nettoyer l'environnement
Pour arrêter tous les conteneurs et supprimer les volumes (notamment pour réinitialiser la base de données PostgreSQL) : make clean

## 📂 Structure du Dépôt

```text
.
├── docker-compose.yml         # Définition des 5 services Docker (Postgres, Spark Master/Worker, Pipeline, Streamlit)
├── Makefile                   # Script d'automatisation des commandes Docker
├── README.md                  # Documentation du projet
├── data/                      # Dossier contenant les fichiers CSV bruts (exclus du Git via .gitignore)
├── spark_app/
│   ├── main.py                # Script ETL PySpark (Ingestion, connexions JDBC, calculs SQL)
│   └── requirements.txt       # Dépendances Python pour le job Spark
└── dashboard/
    ├── app.py                 # Application Streamlit pour la restitution des résultats
    └── requirements.txt       # Dépendances Python pour le dashboard (Streamlit, Plotly, etc.) ```

