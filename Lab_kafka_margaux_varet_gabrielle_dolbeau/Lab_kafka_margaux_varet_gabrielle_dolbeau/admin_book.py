from confluent_kafka.admin import AdminClient, NewTopic

config = {'bootstrap.servers': 'localhost:9092'}
admin_client = AdminClient(config)

# Nouveau topic pour le livre
topic = 'book'

# create_topics renvoie un dictionnaire {nom_du_topic: future}
futures = admin_client.create_topics(
    [NewTopic(topic, num_partitions=1, replication_factor=1)]
)

for t, f in futures.items():
    try:
        f.result()  # attend que le topic soit réellement créé
        print(f"Topic '{t}' créé")
    except Exception as e:
        # Par exemple si le topic existe déjà
        print(f"Topic '{t}' non créé : {e}")

# Vérification : liste de tous les topics
print("Topics existants :")
for t in admin_client.list_topics(timeout=10).topics:
    print(" -", t)

# Pour supprimer le topic, décommenter la ligne suivante :
# admin_client.delete_topics([topic])