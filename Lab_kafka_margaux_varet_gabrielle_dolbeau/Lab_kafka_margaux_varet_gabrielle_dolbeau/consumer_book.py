import os
import re
from collections import Counter
from confluent_kafka import Consumer

OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "book_clean.txt")
topic = 'book'

conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'book-reader',        # nouveau groupe = relit le topic depuis le début
        'auto.offset.reset': 'earliest'}
consumer = Consumer(conf)
consumer.subscribe([topic])

# Mots vides (anglais) à retirer : à adapter selon ton lab word count / la langue du livre
STOP_WORDS = {
    "the", "and", "a", "an", "of", "to", "in", "is", "it", "that", "was", "he",
    "she", "i", "you", "for", "on", "with", "as", "his", "her", "at", "be", "had",
    "not", "but", "they", "this", "so", "all", "or", "by", "from", "have", "were",
    "my", "me", "what", "there", "which", "him", "them", "we", "are", "if", "no",
    "would", "could", "been", "their", "its", "do", "did", "then", "into", "out",
}


def clean(line):
    """Nettoyage de base : minuscules, garde seulement les mots
    (lettres uniquement, sans ponctuation ni chiffres), retire les mots vides."""
    words = re.findall(r"[^\W\d_]+", line.lower())
    return [w for w in words if w not in STOP_WORDS and len(w) > 1]


MAX_EMPTY_POLLS = 10  # s'arrête après ~10 secondes sans message
MAX_ERRORS = 5        # s'arrête après 5 erreurs consécutives
empty_polls = 0
error_count = 0
word_counts = Counter()
nb_lines = 0

with open(OUTPUT_FILE, 'w', encoding='utf-8') as out:
    while True:
        msg = consumer.poll(1.0)

        # 1. Pas de message
        if msg is None:
            empty_polls += 1
            if empty_polls >= MAX_EMPTY_POLLS:
                print("Closing: No new messages received.")
                break
            continue

        # 2. Erreur
        if msg.error():
            error_count += 1
            print(f"Consumer error: {msg.error()}")
            if error_count >= MAX_ERRORS:
                print("Closing: Too many consecutive errors.")
                break
            continue

        # 3. Message reçu : on remet les compteurs à zéro
        empty_polls = 0
        error_count = 0

        line = msg.value().decode('utf-8')
        words = clean(line)
        if words:
            out.write(" ".join(words) + "\n")
            word_counts.update(words)
            nb_lines += 1

consumer.close()

print(f"{nb_lines} lignes nettoyées écrites dans {OUTPUT_FILE}")
print("Top 10 des mots les plus fréquents :")
for word, count in word_counts.most_common(10):
    print(f"  {word}: {count}")