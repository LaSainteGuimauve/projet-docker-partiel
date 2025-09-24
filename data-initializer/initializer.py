import os
import time
import random
import psycopg2
from datetime import datetime, timedelta
from faker import Faker
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

fake = Faker('fr_FR')  # Données françaises pour plus de réalisme

def wait_for_postgres():
    """Attend que PostgreSQL soit prêt à recevoir des connexions"""
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST'),
                database=os.getenv('POSTGRES_DB'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD')
            )
            conn.close()
            logger.info("✅ PostgreSQL est prêt !")
            return True
        except psycopg2.OperationalError:
            retry_count += 1
            logger.info(f"⏳ Attente de PostgreSQL... Tentative {retry_count}/{max_retries}")
            time.sleep(2)
    
    logger.error("❌ Impossible de se connecter à PostgreSQL")
    return False

def create_reference_data():
    """Crée les données de référence nécessaires au fonctionnement du système"""
    
    if not wait_for_postgres():
        return False
    
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        cursor = conn.cursor()
        
        logger.info("🏗️ Création des données de référence...")
        
        # 1. Création des magasins (10 magasins)
        logger.info("📍 Création des magasins...")
        magasins_data = []
        for i in range(10):
            nom = f"SmartRetail {fake.city()}"
            adresse = fake.street_address()
            ville = fake.city()
            code_postal = fake.postcode()
            region = fake.region()
            
            cursor.execute("""
                INSERT INTO magasin (nom, adresse, ville, code_postal, region)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING magasin_id
            """, (nom, adresse, ville, code_postal, region))
            
            magasin_id = cursor.fetchone()[0]
            magasins_data.append(magasin_id)
        
        logger.info(f"✅ {len(magasins_data)} magasins créés")
        
        # 2. Création des produits (200 produits)
        logger.info("🛍️ Création des produits...")
        categories = ['Électronique', 'Vêtements', 'Alimentation', 'Maison', 'Sport', 'Beauté', 'Livres', 'Jouets']
        produits_data = []
        
        for i in range(200):
            nom = fake.word().capitalize() + " " + fake.word().capitalize()
            categorie = random.choice(categories)
            prix = round(random.uniform(5.99, 999.99), 2)
            stock = random.randint(0, 100)
            
            cursor.execute("""
                INSERT INTO produit (nom, categorie, prix, stock)
                VALUES (%s, %s, %s, %s)
                RETURNING produit_id
            """, (nom, categorie, prix, stock))
            
            produit_id = cursor.fetchone()[0]
            produits_data.append(produit_id)
        
        logger.info(f"✅ {len(produits_data)} produits créés")
        
        # 3. Création des clients (1000 clients)
        logger.info("👥 Création des clients...")
        clients_data = []
        
        for i in range(1000):
            nom = fake.last_name()
            prenom = fake.first_name()
            email = fake.email()
            date_naissance = fake.date_of_birth(minimum_age=18, maximum_age=80)
            sexe = random.choice(['H', 'F', 'AUTRE'])
            ville = fake.city()
            date_inscription = fake.date_time_between(start_date='-2y', end_date='now')
            
            cursor.execute("""
                INSERT INTO client (nom, prenom, email, date_naissance, sexe, ville, date_inscription)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING client_id
            """, (nom, prenom, email, date_naissance, sexe, ville, date_inscription))
            
            client_id = cursor.fetchone()[0]
            clients_data.append(client_id)
        
        logger.info(f"✅ {len(clients_data)} clients créés")
        
        # 4. Création des capteurs (50 capteurs)
        logger.info("📡 Création des capteurs...")
        types_capteurs = ['Stock RFID', 'Compteur clients', 'Capteur température', 'Détecteur mouvement']
        
        for i in range(50):
            type_capteur = random.choice(types_capteurs)
            emplacement = f"Rayon {random.randint(1, 20)}"
            
            cursor.execute("""
                INSERT INTO capteur (type_capteur, emplacement)
                VALUES (%s, %s)
            """, (type_capteur, emplacement))
        
        logger.info("✅ 50 capteurs créés")
        
        # 5. Création des caméras (30 caméras)
        logger.info("📹 Création des caméras...")
        
        for i in range(30):
            magasin_id = random.choice(magasins_data)
            
            cursor.execute("""
                INSERT INTO camera (magasin_id)
                VALUES (%s)
            """, (magasin_id,))
        
        logger.info("✅ 30 caméras créées")
        
        # Validation et commit
        conn.commit()
        
        # Affichage des statistiques finales
        cursor.execute("SELECT COUNT(*) FROM magasin")
        nb_magasins = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM produit")
        nb_produits = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM client")
        nb_clients = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM capteur")
        nb_capteurs = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM camera")
        nb_cameras = cursor.fetchone()[0]
        
        logger.info("🎉 Initialisation terminée avec succès !")
        logger.info(f"📊 Statistiques finales:")
        logger.info(f"   - Magasins: {nb_magasins}")
        logger.info(f"   - Produits: {nb_produits}")
        logger.info(f"   - Clients: {nb_clients}")
        logger.info(f"   - Capteurs: {nb_capteurs}")
        logger.info(f"   - Caméras: {nb_cameras}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'initialisation: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("🚀 Démarrage de l'initialisation des données de référence...")
    
    success = create_reference_data()
    
    if success:
        logger.info("✅ Initialisation réussie - Le container va maintenant s'arrêter")
        exit(0)
    else:
        logger.error("❌ Initialisation échouée")
        exit(1)