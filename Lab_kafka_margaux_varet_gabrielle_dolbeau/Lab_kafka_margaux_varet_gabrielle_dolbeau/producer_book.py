import os
import socket
import time
from confluent_kafka import Producer

# Le livre doit être dans le même dossier que ce script, sous le nom book.txt
BOOK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "book.txt")
topic = 'book'
DELAY = 0.01  # pause entre deux lignes (en secondes) ; 0 pour tout envoyer d'un coup

conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}
producer = Producer(conf)

# utf-8-sig gère le caractère invisible (BOM) au début de certains fichiers Gutenberg
with open(BOOK_FILE, encoding='utf-8-sig') as f:
    lines = f.readlines()

# Les livres Gutenberg ont un en-tête et une licence à la fin :
# on n'envoie que le texte situé entre les marqueurs START et END
start = next((i + 1 for i, l in enumerate(lines) if l.startswith("*** START OF")), 0)
end = next((i for i, l in enumerate(lines) if l.startswith("*** END OF")), len(lines))

sent = 0
for line in lines[start:end]:
    line = line.strip()
    if not line:  # on saute les lignes vides
        continue

    producer.produce(topic=topic, value=line.encode('utf-8'))
    producer.poll(0)  # laisse Kafka traiter les envois en arrière-plan
    sent += 1

    if sent % 500 == 0:
        print(f"{sent} lignes envoyées...")
    time.sleep(DELAY)

producer.flush()  # attend que tous les messages soient bien partis
print(f"Terminé : {sent} lignes envoyées dans le topic '{topic}'")