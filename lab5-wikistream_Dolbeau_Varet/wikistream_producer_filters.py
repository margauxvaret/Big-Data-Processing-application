"""
Producteur Wikimedia EventStreams -> Kafka avec filtres configurables (étape 11 du lab).

Exemples :
  python wikistream_producer_filters.py                          # comme la démo : fr.wikipedia, éditions, 10 min
  python wikistream_producer_filters.py --wikis en.wikipedia.org de.wikipedia.org --minutes 5
  python wikistream_producer_filters.py --exclude-bots --namespace 0      # articles, humains uniquement
  python wikistream_producer_filters.py --titles "Paris" "Lyon"           # suivre des pages précises
  python wikistream_producer_filters.py --types edit new --min-size 1000  # grosses modifs + créations
  python wikistream_producer_filters.py --since-minutes 120               # rejouer les 2 dernières heures

Notes :
- Le filtrage est fait côté client par pywikibot : le flux reçoit tous les événements
  `recentchange` de tous les wikis et ne garde que ceux qui passent les filtres.
- Avec un filtre très restrictif (une seule page), next(stream) peut attendre longtemps
  le prochain événement : la durée --minutes n'est vérifiée qu'à chaque événement reçu.
- `revision-create` (présent dans la démo) est retiré : ses événements n'ont ni
  `server_name` ni `type`, donc le filtre de la démo les rejetait tous.
"""
import argparse
import json
import socket
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from pywikibot.comms.eventstreams import EventStreams


def parse_args():
    p = argparse.ArgumentParser(description="Wikimedia recentchange -> Kafka")
    p.add_argument("--bootstrap", default="localhost:9092", help="broker Kafka (listener EXTERNAL)")
    p.add_argument("--topic", default="wikistreams")
    p.add_argument("--minutes", type=float, default=10, help="durée de collecte en minutes")
    p.add_argument("--since-minutes", type=int, default=0,
                   help="rejouer l'historique des N dernières minutes (0 = temps réel)")
    p.add_argument("--wikis", nargs="+", default=["fr.wikipedia.org"],
                   help="valeurs de server_name à suivre, ou 'all' pour tous les wikis")
    p.add_argument("--types", nargs="+", default=["edit"], help="edit, new, log, categorize")
    p.add_argument("--titles", nargs="+", help="ne suivre que ces pages (titre exact)")
    p.add_argument("--namespace", type=int, nargs="+",
                   help="0 = articles, 1 = discussions, 2 = pages utilisateur…")
    bots = p.add_mutually_exclusive_group()
    bots.add_argument("--exclude-bots", action="store_true", help="ignorer les robots")
    bots.add_argument("--only-bots", action="store_true", help="ne garder que les robots")
    p.add_argument("--min-size", type=int, default=0,
                   help="ne garder que les modifs avec |nouvelle taille - ancienne| >= N octets")
    p.add_argument("--verbose", action="store_true", help="afficher chaque événement envoyé")
    return p.parse_args()


def build_stream(args):
    kwargs = {"streams": ["recentchange"]}
    if args.since_minutes > 0:
        since = datetime.now(timezone.utc) - timedelta(minutes=args.since_minutes)
        kwargs["since"] = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    stream = EventStreams(**kwargs)

    # Filtres 'all' (par défaut) : toutes les conditions doivent être vraies
    if args.wikis != ["all"]:
        stream.register_filter(server_name=tuple(args.wikis))
    stream.register_filter(type=tuple(args.types))
    if args.titles:
        stream.register_filter(title=tuple(args.titles))
    if args.namespace:
        stream.register_filter(namespace=tuple(args.namespace))
    if args.only_bots:
        stream.register_filter(bot=True)

    # Filtre 'none' : l'événement est rejeté si la condition est vraie
    if args.exclude_bots:
        stream.register_filter(ftype="none", bot=True)

    # Filtre écrit comme une fonction (n'importe quelle règle Python)
    if args.min_size > 0:
        def assez_gros(event):
            length = event.get("length") or {}
            return abs((length.get("new") or 0) - (length.get("old") or 0)) >= args.min_size
        stream.register_filter(assez_gros)

    return stream


def main():
    args = parse_args()
    print("Filtres :", {k: v for k, v in vars(args).items() if v not in (None, False, 0)})

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "client.id": socket.gethostname(),
        "compression.type": "lz4",
    })
    stream = build_stream(args)

    # Premier événement : aperçu brut et formaté, comme dans la démo
    change = next(stream)
    print("Raw Message: " + str(change))
    print('\nFormatted Message: {type} on page "{title}" by "{user}" at {meta[dt]}.\n'.format(**change))

    stop_time = datetime.now() + timedelta(minutes=args.minutes)
    stats = {"envoyes": 0, "erreurs": 0}

    def on_delivery(err, msg):
        if err is not None:
            stats["erreurs"] += 1
            print("Échec de livraison :", err)

    try:
        while datetime.now() < stop_time:
            producer.produce(
                args.topic,
                key=(change.get("server_name") or "").encode("utf-8"),  # même wiki -> même partition
                value=json.dumps(change).encode("utf-8"),
                on_delivery=on_delivery,
            )
            producer.poll(0)  # traite les accusés de réception, évite de saturer la file locale
            stats["envoyes"] += 1
            if args.verbose:
                print(change)
            elif stats["envoyes"] % 100 == 0:
                dt = (change.get("meta") or {}).get("dt")
                print(f"{stats['envoyes']} messages envoyés — dernier : {dt} | {change.get('title')}")
            change = next(stream)
    except KeyboardInterrupt:
        print("\nInterruption (Ctrl+C)")
    finally:
        print("Envoi des derniers messages…")
        producer.flush(30)
        print(f"Terminé : {stats['envoyes']} messages envoyés vers '{args.topic}', "
              f"{stats['erreurs']} erreur(s).")


if __name__ == "__main__":
    main()
