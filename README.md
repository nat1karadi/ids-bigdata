# Cross-Dataset Generalisable Intrusion Detection Using Distributed Big-Data Analytics

DTS 301 Big Data Computing group project, Redeemer's University.

Authors:

- Bankole Oluwanifemi Ogo-oluwakitan (RUN/CMP/24/17179)
- Bunmi Edith Adeola (RUN/CMP/24/17180)
- Dada Temiloluwa (RUN/CMP/24/17183)
- Daramola Eyinjuoluwa Hephzibah (RUN/CMP/24/17184)
- David Esther Ebunrereoluwa (RUN/CMP/24/17185)
- Diya Oluwatobi Oyefeso (RUN/CMP/24/17186)
- Ebenuwah David Obichukwu (RUN/CMP/24/17187)

## What this project does

Trains and tests intrusion detection models on two public datasets, CICIoT2023 and UNSW-NB15, using a distributed pipeline (HDFS, Spark MLlib, MongoDB). It measures how much a model's performance drops when it is trained on one dataset and tested on the other, explains why, and tests whether a small labelled sample from the new dataset closes the gap.

Main findings (details in `docs/results.md`):

| Test | Balanced accuracy (0.5 is chance) |
|---|---|
| Train and test on CICIoT2023 | 0.976 |
| Train and test on UNSW-NB15 | 0.78 to 0.79 |
| Train on one dataset, test on the other | 0.44 to 0.50 |
| Train on one dataset plus about 100 labelled rows of the other | 0.95 (UNSW to CIC) and 0.75 (CIC to UNSW) |

## Architecture

```
raw CSV files (data/)
      |  hdfs put
      v
HDFS  /ids/raw  ->  Spark: clean  ->  /ids/clean (Parquet)
                         |
                         v
                   Spark: harmonise (5 shared features, unified labels)
                         |
                         v
                  /ids/harmonised (Parquet)
                         |
                         v
          Spark MLlib: experiments A to D, scalability tests
                         |
                         v
        /ids/results (JSON)  ->  MongoDB (ids database)
```

All services run in Docker containers on one machine: HDFS (1 namenode, 1 datanode), Spark standalone (1 master, 1 worker), MongoDB.

## Repository layout

| Path | Contents |
|---|---|
| `docker-compose.yml`, `docker/spark/Dockerfile`, `configs/hadoop.env` | Cluster definition (the custom Spark image adds numpy, which MLlib needs) |
| `src/` | All Spark and Python scripts |
| `scripts/run_scale.sh` | Runs the scalability tests |
| `docs/` | Dataset notes, cleaning log, harmonisation plan, results |
| `results/` | Exported result files |
| `data/` | Raw datasets (not in Git) |

## Requirements

- A machine with at least 4 CPU cores, 8 GB RAM and about 15 GB free disk space (more for the full scalability set)
- Docker with Docker Compose. On Windows, use WSL2 and run all commands in the Ubuntu terminal
- Python 3 with `pymongo` (only for loading results into MongoDB)

## Step 1: Get the data

Create the folders `data/ciciot2023`, `data/unsw` and `data/ciciot2023_scale`.

CICIoT2023 (Canadian Institute for Cybersecurity, University of New Brunswick), `CSV/MERGED_CSV` folder:
- Modelling: `Merged01.csv`, `Merged15.csv`, `Merged30.csv`, `Merged45.csv`, `Merged63.csv` into `data/ciciot2023`
- Scalability tests: `Merged01.csv` to `Merged15.csv` into `data/ciciot2023_scale`

UNSW-NB15 (UNSW Canberra), into `data/unsw`:
- `UNSW-NB15_1.csv` to `UNSW-NB15_4.csv`
- `NUSW-NB15_features.csv`
- `UNSW_NB15_training-set.csv` and `UNSW_NB15_testing-set.csv`

Check the training set is complete: `wc -l data/unsw/UNSW_NB15_training-set.csv` must print 175342. A truncated download gives a smaller number.

See `docs/datasets.md` for sources, sizes and citations.

## Step 2: Start the cluster

```bash
docker compose up -d --build
docker compose ps
```

Check `localhost:9870` (HDFS) and `localhost:8080` (Spark: 1 worker, 4 cores).

Use `docker compose stop` and `docker compose start` to pause the cluster. Do not use `docker compose down`: HDFS data lives inside the containers and would be deleted.

## Step 3: Load data into HDFS

```bash
docker compose exec namenode hdfs dfs -mkdir -p /ids/raw/ciciot2023 /ids/raw/unsw /ids/clean
docker compose exec namenode hdfs dfs -chmod -R 777 /ids
docker compose exec namenode bash -c "hdfs dfs -put -f /data/ciciot2023/*.csv /ids/raw/ciciot2023/"
docker compose exec namenode bash -c "hdfs dfs -put -f /data/unsw/*.csv /ids/raw/unsw/"
```

The `chmod` step is needed because Spark runs as a different user from the one that created the folders.

For the scalability tests only:
```bash
docker compose exec namenode hdfs dfs -mkdir -p /ids/raw/ciciot2023_scale
docker compose exec namenode bash -c "hdfs dfs -put -f /data/ciciot2023_scale/*.csv /ids/raw/ciciot2023_scale/"
```

## Step 4: Run the pipeline

Each script runs with:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit /src/<script>.py
```

| Order | Script | What it does | Time (approx.) |
|---|---|---|---|
| 1 | `clean_ciciot.py`, `clean_unsw.py` | Remove duplicates and invalid rows, write Parquet to `/ids/clean` | 5 to 10 min |
| 2 | `harmonise.py` | Build 5 shared features and unified labels, remove 940 train/test duplicates, write `/ids/harmonised` | 5 min |
| 3 | `experiment_a.py` | Within and cross-dataset binary tests (Logistic Regression and Random Forest) | 10 min |
| 4 | `experiment_b.py` | AUC, rank normalisation, leave-one-feature-out | 10 to 15 min |
| 5 | `experiment_c.py` | Three seeds, joint training, multiclass | 15 to 25 min |
| 6 | `experiment_d.py` | Few-shot adaptation | 10 to 20 min |
| 7 | `scripts/run_scale.sh` | Timing tests (see below) | 1 to 2 hours |

Optional helper scripts: `smoke.py` (checks HDFS, Spark), `inspect_data.py` (row counts and label distributions), `verify_features.py` (feature checks used to choose the shared features).

Scalability tests:
```bash
bash scripts/run_scale.sh "1 4" "5 4" "10 4" "15 4" "5 2" "5 1"
```
Each argument is "number of files, number of cores". Results are appended to `results/scale_results.jsonl`. Close other programs while it runs, or the timings are distorted.

## Step 5: Store results in MongoDB

```bash
for e in a b c d; do
  docker compose exec -T namenode bash -c "hdfs dfs -cat /ids/results/experiment_$e/part-*.json" > results/experiment_$e.jsonl
done
python3 -m venv .venv && source .venv/bin/activate
pip install pymongo
python3 src/load_mongo.py
```

This fills two collections in the `ids` database: `experiment_results` (128 documents) and `scale_runs`.

## Known issues and fixes

- `Permission denied` when Spark writes to HDFS: run the `chmod -R 777 /ids` command in Step 3.
- `No module named numpy`: the Spark containers must use the custom image (`docker compose up -d --build`).
- The CICIoT2023 `rate` column contains the text `inf`, so Spark reads it as a string. The scripts cast all non-label columns to numbers before cleaning.
- Scalability timings vary a lot between runs on a laptop (up to five-fold at 4 cores), and 4 cores were not faster than 1 or 2. See `docs/results.md`.

## Documentation

- `docs/datasets.md`: sources, sizes, licences and citations
- `docs/cleaning_log.md`: cleaning steps and row counts
- `docs/harmonisation.md`: shared features, unified labels and the reasons for each choice
- `docs/results.md`: all experiment results, findings and limitations

## Data licences and citations

Both datasets are public research datasets. See `docs/datasets.md` for the terms and the papers to cite.
