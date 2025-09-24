import os
import json
import time
import random
import psycopg2
from datetime import datetime
from kafka import KafkaProducer
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TransactionGenerator:
    def __init__(self):
        self.postgres_conn = None
        self.kafka_producer = None
        self.reference_data = {
            'clients': [],
            'magasins': [],
            'produits': []
        }
        
    def connect_to_postgres(self):
        """Se connecte à PostgreSQL et charge les données de référence"""
        max_retries = 30
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.postgres_conn = psycopg2.connect(
                    host=os.getenv('POSTGRES_HOST'),
                    database=os.getenv('POSTGRES_DB'),
                    user=os.getenv('POSTGRES_USER'),
                    password=os.getenv('POSTGRES_PASSWORD')
                )
                logger.info("✅ Connexion à PostgreSQL établie")
                return True
            except psycopg2.OperationalError:
                retry_count += 1
                logger.info(f"⏳ Attente de PostgreSQL... Tentative {retry_count}/{max_retries}")
                time.sleep(2)
        
        logger.error("❌ Impossible de se connecter à PostgreSQL")
        return False
    
    def connect_to_kafka(self):
        """Se connecte à Kafka"""
        max_retries = 30
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=os.getenv('KAFKA_BROKERS').split(','),
                    value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                    key_serializer=lambda x: str(x).encode('utf-8') if x else None
                )
                logger.info("✅ Connexion à Kafka établie")
                return True
            except Exception as e:
                retry_count += 1
                logger.info(f"⏳ Attente de Kafka... Tentative {retry_count}/{max_retries}: {str(e)}")
                time.sleep(2)
        
        logger.error("❌ Impossible de se connecter à Kafka")
        return False
    
    def load_reference_data(self):
        """Charge les données de référence depuis PostgreSQL"""
        try:
            cursor = self.postgres_conn.cursor()
            
            # Charger les clients
            cursor.execute("SELECT client_id FROM client")
            self.reference_data['clients'] = [row[0] for row in cursor.fetchall()]
            
            # Charger les magasins
            cursor.execute("SELECT magasin_id FROM magasin")
            self.reference_data['magasins'] = [row[0] for row in cursor.fetchall()]
            
            # Charger les produits avec leurs prix
            cursor.execute("SELECT produit_id, prix FROM produit WHERE stock > 0")
            self.reference_data['produits'] = [(row[0], float(row[1])) for row in cursor.fetchall()]
            
            cursor.close()
            
            logger.info(f"✅ Données de référence chargées:")
            logger.info(f"   - {len(self.reference_data['clients'])} clients")
            logger.info(f"   - {len(self.reference_data['magasins'])} magasins")
            logger.info(f"   - {len(self.reference_data['produits'])} produits en stock")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du chargement des données de référence: {str(e)}")
            return False
    
    def generate_transaction(self):
        """Génère une transaction aléatoire réaliste"""
        # Sélection aléatoire des références
        client_id = random.choice(self.reference_data['clients'])
        magasin_id = random.choice(self.reference_data['magasins'])
        
        # Génération des détails de transaction (1 à 5 produits)
        nb_produits = random.randint(1, 5)
        produits_transaction = random.sample(self.reference_data['produits'], min(nb_produits, len(self.reference_data['produits'])))
        
        details = []
        montant_total = 0
        
        for produit_id, prix_unitaire in produits_transaction:
            quantite = random.randint(1, 3)
            sous_total = prix_unitaire * quantite
            montant_total += sous_total
            
            details.append({
                'produit_id': produit_id,
                'quantite': quantite,
                'prix_unitaire': prix_unitaire
            })
        
        # Mode de paiement aléatoire avec probabilités réalistes
        modes_paiement = ['CB', 'CB', 'CB', 'ESPECE', 'VIREMENT']  # CB plus probable
        mode_paiement = random.choice(modes_paiement)
        
        transaction = {
            'client_id': client_id,
            'magasin_id': magasin_id,
            'date_achat': datetime.now().isoformat(),
            'montant_total': round(montant_total, 2),
            'mode_paiement': mode_paiement,
            'details': details,
            'metadata': {
                'source': 'transaction-generator',
                'timestamp': datetime.now().timestamp()
            }
        }
        
        return transaction
    
    def send_to_kafka(self, transaction):
        """Envoie la transaction vers Kafka"""
        try:
            # Utiliser le client_id comme clé pour partitioning
            key = str(transaction['client_id'])
            
            # Envoyer vers le topic 'transactions'
            future = self.kafka_producer.send('transactions', key=key, value=transaction)
            
            # Attendre confirmation (optionnel, pour debug)
            result = future.get(timeout=10)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'envoi vers Kafka: {str(e)}")
            return False
    
    def generate_other_events(self):
        """Génère d'autres types d'événements (stock, fréquentation)"""
        # Événement de stock (capteurs)
        if random.random() < 0.3:  # 30% de chance à chaque cycle
            stock_event = {
                'type': 'stock_update',
                'capteur_id': random.randint(1, 50),
                'produit_id': random.choice([p[0] for p in self.reference_data['produits']]),
                'magasin_id': random.choice(self.reference_data['magasins']),
                'niveau_stock': random.randint(0, 100),
                'timestamp': datetime.now().isoformat(),
                'metadata': {
                    'source': 'stock-sensor',
                    'timestamp': datetime.now().timestamp()
                }
            }
            
            try:
                self.kafka_producer.send('stock-events', value=stock_event)
                logger.debug("📦 Événement stock envoyé")
            except Exception as e:
                logger.error(f"❌ Erreur envoi stock: {str(e)}")
        
        # Événement de fréquentation (caméras)
        if random.random() < 0.2:  # 20% de chance à chaque cycle
            frequentation_event = {
                'type': 'frequentation',
                'camera_id': random.randint(1, 30),
                'nombre_clients': random.randint(0, 15),
                'timestamp': datetime.now().isoformat(),
                'metadata': {
                    'source': 'camera-sensor',
                    'timestamp': datetime.now().timestamp()
                }
            }
            
            try:
                self.kafka_producer.send('frequentation-events', value=frequentation_event)
                logger.debug("👥 Événement fréquentation envoyé")
            except Exception as e:
                logger.error(f"❌ Erreur envoi fréquentation: {str(e)}")
    
    def run(self):
        """Boucle principale de génération"""
        logger.info("🚀 Démarrage du générateur de transactions")
        
        # Initialisation des connexions
        if not self.connect_to_postgres():
            return False
            
        if not self.connect_to_kafka():
            return False
            
        if not self.load_reference_data():
            return False
        
        logger.info("🎯 Génération de 5 transactions par seconde...")
        
        transaction_count = 0
        start_time = time.time()
        
        try:
            while True:
                cycle_start = time.time()
                
                # Générer 5 transactions
                for i in range(5):
                    transaction = self.generate_transaction()
                    
                    if self.send_to_kafka(transaction):
                        transaction_count += 1
                        if transaction_count % 100 == 0:  # Log toutes les 100 transactions
                            elapsed = time.time() - start_time
                            rate = transaction_count / elapsed
                            logger.info(f"📈 {transaction_count} transactions générées (moyenne: {rate:.1f}/s)")
                    
                    # Petit délai entre chaque transaction du batch
                    time.sleep(0.05)  # 50ms entre chaque transaction
                
                # Générer d'autres événements
                self.generate_other_events()
                
                # S'assurer qu'on respecte le rythme d'1 seconde par cycle
                cycle_duration = time.time() - cycle_start
                if cycle_duration < 1.0:
                    time.sleep(1.0 - cycle_duration)
                
        except KeyboardInterrupt:
            logger.info("⏹️ Arrêt demandé par l'utilisateur")
        except Exception as e:
            logger.error(f"❌ Erreur dans la boucle principale: {str(e)}")
        finally:
            if self.kafka_producer:
                self.kafka_producer.close()
            if self.postgres_conn:
                self.postgres_conn.close()
            logger.info("🔚 Générateur arrêté")

if __name__ == "__main__":
    generator = TransactionGenerator()
    generator.run()