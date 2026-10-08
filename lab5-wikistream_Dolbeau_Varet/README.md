# Wikipedia Edits Streaming with Kafka and Spark

This lab streams live Wikipedia edits from Wikimedia EventStreams into Kafka, then processes them with Spark Structured Streaming to count and analyse edits over time windows.

## Files

- `compose.yaml`: starts Kafka and a Jupyter/PySpark container
- `admin.py`: creates the `wikistreams` Kafka topic
- `wikistream_producer.py`: sends French Wikipedia edits to Kafka
- `wikistream_producer_filters.py`: same producer with command-line filters (other languages, pages, bots…)
- `notebooks/wikistream_pyspark.ipynb`: demo that counts bot vs human edits per hour
- `notebooks/wikistream_pyspark_enrichi.ipynb`: additional aggregations with tumbling and sliding windows
- `results/`: console outputs and screenshots

## How to run

1. Install the Python dependencies:
   ```
   py -m pip install confluent-kafka pywikibot requests-sse
   ```
2. Start the containers:
   ```
   docker compose up -d
   ```
3. Get the Jupyter link (with its token) from the logs:
   ```
   docker logs pyspark_notebook
   ```
4. Create the topic:
   ```
   py admin.py
   ```
5. Start the producer in a first terminal:
   ```
   py wikistream_producer.py
   ```
6. In Jupyter, open `work/wikistream_pyspark.ipynb` and run all cells.
7. Follow the Spark output in a second terminal:
   ```
   docker logs -f --tail 50 pyspark_notebook
   ```

## What was done

The demo notebook reads the Kafka topic, parses each message with a JSON schema, and counts edits per one-hour window, split between bots and humans.

The enriched notebook adds several explorations:

- edits, distinct users, and bytes added or removed per 10-minute window
- the same metrics over sliding windows
- edits grouped by size (small, medium, large) and alerts for edits larger than 5,000 bytes
- the top 3 most edited pages per 30-minute window
- a sliding window with a watermark in update mode
- activity per wiki when the producer follows several languages

## Tumbling vs sliding windows

A tumbling window splits time into consecutive blocks that do not overlap, so each edit is counted once:

```python
window("event_time", "10 minutes")
```

A sliding window starts a new window every 5 minutes, so windows overlap and each edit is counted in two windows:

```python
window("event_time", "10 minutes", "5 minutes")
```

The notebook compares both on the same data: the sliding version gives about twice as many windows and exactly twice the total count, which confirms the overlap.

## Producer filters

The producer uses pywikibot filters to choose which events are sent to Kafka. For example:

```
py wikistream_producer_filters.py --wikis en.wikipedia.org de.wikipedia.org
py wikistream_producer_filters.py --exclude-bots
py wikistream_producer_filters.py --titles "Paris"
```

## Issues and fixes

- `pip` was not recognised in PowerShell, so I used `py -m pip` instead.
- `admin.py` printed nothing because topic creation is asynchronous. The fixed version waits for the creation before listing the topics.
- Spark failed with a `NoSuchMethodError` because the Kafka connector version (4.1.0) did not match the Spark version in the Docker image. The notebook now builds the connector version from `pyspark.__version__`, and the kernel must be restarted after the change.

## Results

Console outputs and screenshots are in the `results/` folder.
