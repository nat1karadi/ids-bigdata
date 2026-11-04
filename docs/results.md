# Results: Experiments A and B (draft for the report)

Date: 2 November 2026
Scripts: `src/experiment_a.py`, `src/experiment_b.py`. Raw results in HDFS: `/ids/results/experiment_a`, `/ids/results/experiment_b`.

## Setup

- Data: harmonised CICIoT2023 sample (2,017,802 rows) and UNSW-NB15 (162,745 rows). See `harmonisation.md`.
- Features (5): `rate`, `ttl`, `avg_pkt_size`, `proto_tcp`, `proto_udp`.
- Task: binary attack detection (`is_attack`).
- Splits: CICIoT2023 random 70/30. UNSW-NB15 uses the published train and test sets (940 duplicate test rows removed).
- Training sets are balanced by undersampling. Test sets keep their natural class distribution (about 96% attack in CICIoT2023, roughly 40% attack in UNSW-NB15).
- Cross-dataset tests use the same test sets as the within-dataset baselines, so the gap is a like-for-like comparison.
- Models: Logistic Regression (LR) and Random Forest (RF) in Spark MLlib. Experiment B uses RF only (30 trees, depth 8).
- Primary metric: balanced accuracy (0.5 is chance), because test base rates differ. Macro-F1 depends on the base rate and is reported as a secondary metric.

## Experiment A: within-dataset and cross-dataset

Balanced accuracy:

| Preprocessing and model | Within CIC | Within UNSW | UNSW to CIC | CIC to UNSW |
|---|---|---|---|---|
| naive, LR | 0.915 | 0.692 | 0.303 | 0.328 |
| naive, RF | 0.977 | 0.802 | 0.456 | 0.461 |
| log + standardise, LR | 0.956 | 0.690 | 0.172 | 0.495 |
| log + standardise, RF | 0.977 | 0.809 | 0.492 | 0.483 |

Macro-F1:

| Preprocessing and model | Within CIC | Within UNSW | UNSW to CIC | CIC to UNSW |
|---|---|---|---|---|
| naive, LR | 0.749 | 0.672 | 0.187 | 0.326 |
| naive, RF | 0.817 | 0.762 | 0.467 | 0.453 |
| log + standardise, LR | 0.766 | 0.673 | 0.246 | 0.304 |
| log + standardise, RF | 0.817 | 0.771 | 0.491 | 0.346 |

Generalisation gap in balanced accuracy (within minus cross, same test set):

| Preprocessing and model | On CIC test set | On UNSW test set |
|---|---|---|
| naive, LR | 0.612 | 0.364 |
| naive, RF | 0.521 | 0.341 |
| log + standardise, LR | 0.784 | 0.195 |
| log + standardise, RF | 0.485 | 0.326 |

## Why the cross-dataset models fail

Median feature values by class (0 = benign, 1 = attack):

| Dataset | Class | rate | ttl | avg_pkt_size | TCP share | UDP share |
|---|---|---|---|---|---|---|
| CICIoT2023 | benign | 171 | 98.9 | 247.9 | 0.916 | 0.082 |
| CICIoT2023 | attack | 16,787 | 64 | 60.0 | 0.569 | 0.240 |
| UNSW-NB15 | benign | 943 | 31 | 109.2 | 0.754 | 0.235 |
| UNSW-NB15 | attack | 43.9 | 254 | 100.0 | 0.677 | 0.192 |

- `rate`: attacks have a much higher rate than benign traffic in CICIoT2023 (about 100 times) and a much lower rate in UNSW-NB15 (about 20 times lower). The relationship is inverted.
- `ttl`: attacks have a lower TTL than benign traffic in CICIoT2023 (64 against 99) and a much higher one in UNSW-NB15 (254 against 31). Inverted again. TTL probably reflects which devices generated the traffic in each testbed rather than attack behaviour.
- `avg_pkt_size` separates the classes strongly in CICIoT2023 (60 against 248 bytes) and hardly at all in UNSW-NB15 (100 against 109).
- A model trained on one dataset therefore learns rules that point the wrong way on the other. This matches the below-chance results (balanced accuracy 0.17 and AUC 0.17 to 0.33 in several runs).

## Experiment B: AUC, rank normalisation and feature ablation (RF)

Balanced accuracy / AUC on the cross-dataset tests (0.5 is chance for both):

| Preprocessing | Features | UNSW to CIC | CIC to UNSW |
|---|---|---|---|
| log + standardise | all 5 | 0.505 / 0.772 | 0.476 / 0.618 |
| log + standardise | without rate | 0.477 / 0.569 | 0.567 / 0.549 |
| log + standardise | without ttl | 0.170 / 0.192 | 0.492 / 0.545 |
| log + standardise | without avg_pkt_size | 0.419 / 0.257 | 0.443 / 0.504 |
| log + standardise | without protocol | 0.498 / 0.308 | 0.496 / 0.546 |
| rank | all 5 | 0.192 / 0.248 | 0.461 / 0.341 |
| rank | without rate | 0.187 / 0.246 | 0.528 / 0.424 |
| rank | without ttl | 0.287 / 0.264 | 0.522 / 0.561 |
| rank | without avg_pkt_size | 0.194 / 0.170 | 0.455 / 0.392 |
| rank | without protocol | 0.201 / 0.190 | 0.340 / 0.282 |

Within-dataset results for the same runs stay stable: balanced accuracy 0.96 to 0.98 on CICIoT2023 (AUC about 0.98 to 0.99) and 0.75 to 0.80 on UNSW-NB15 (AUC 0.85 to 0.93).

## Experiment C: three seeds, joint training and multiclass

Script: `src/experiment_c.py`, raw results in HDFS `/ids/results/experiment_c`. Random Forest (30 trees, depth 8), all 5 features. The seed changes the CICIoT2023 70/30 split and the undersampling. Values are mean and standard deviation over 3 seeds.

Binary task, balanced accuracy and AUC:

| Preprocessing | Within CIC | Within UNSW | UNSW to CIC | CIC to UNSW |
|---|---|---|---|---|
| naive, balanced accuracy | 0.976 +/- 0.000 | 0.783 +/- 0.011 | 0.495 +/- 0.042 | 0.441 +/- 0.008 |
| naive, AUC | 0.989 | 0.928 | 0.149 +/- 0.071 | 0.426 +/- 0.030 |
| log + standardise, balanced accuracy | 0.976 +/- 0.000 | 0.788 +/- 0.002 | 0.484 +/- 0.036 | 0.476 +/- 0.002 |
| log + standardise, AUC | 0.989 | 0.924 | 0.513 +/- 0.393 | 0.598 +/- 0.089 |

Joint training (one model trained on both datasets, log + standardise), tested on each test set:

| Test set | Balanced accuracy | AUC | Within-dataset model for comparison |
|---|---|---|---|
| CICIoT2023 | 0.974 +/- 0.000 | 0.989 | 0.976 |
| UNSW-NB15 | 0.783 +/- 0.005 | 0.902 | 0.788 |

Multiclass on the shared classes (BENIGN, DOS_DDOS, RECON), log + standardise. Chance level for macro recall is 0.333.

| Setup | Macro recall | Benign recall | DoS/DDoS recall | Recon recall | Macro-F1 |
|---|---|---|---|---|---|
| Within CIC | 0.816 +/- 0.001 | 0.646 | 0.983 | 0.820 | 0.752 |
| Within UNSW | 0.841 +/- 0.003 | 0.733 | 0.918 | 0.874 | 0.608 |
| UNSW to CIC | 0.387 +/- 0.085 | 0.163 +/- 0.260 | 0.998 | 0.000 | 0.385 |
| CIC to UNSW | 0.377 +/- 0.003 | 0.019 | 0.356 | 0.755 | 0.117 |

Observations:
- Naive UNSW to CIC has an AUC of 0.149 +/- 0.071 (seeds: 0.194, 0.068, 0.186). The ranking is consistently reversed, which fits the inverted feature relationships shown above.
- The log + standardise UNSW to CIC AUC varies enormously between seeds (0.878, 0.098, 0.563). The 0.772 seen in Experiment B was one such draw and is not evidence of transfer.
- A jointly trained model matches the within-dataset models on both test sets. A single model can serve both datasets when it has seen both. This does not show generalisation to an unseen dataset.
- Multiclass cross-dataset models collapse to one class (DoS/DDoS for UNSW to CIC, mostly recon for CIC to UNSW), with macro recall close to chance.

## Experiment D: few-shot adaptation

Script: `src/experiment_d.py`, raw results in HDFS `/ids/results/experiment_d`. Random Forest (30 trees, depth 8), 5 features, log + standardise, 3 seeds (mean and standard deviation).

Method: train on the balanced source training set plus a small number of labelled rows from the target dataset (a class-balanced sample of the target training set). Target rows are weighted so that their total weight equals the source total. Test on the same target test set as before. This is supervised adaptation, not zero-shot generalisation.

UNSW-NB15 to CICIoT2023 (within-dataset reference: balanced accuracy 0.976):

| Labelled CIC rows | Balanced accuracy | AUC |
|---|---|---|
| 0 | 0.484 +/- 0.036 | 0.513 +/- 0.393 |
| about 95 | 0.953 +/- 0.006 | 0.981 +/- 0.001 |
| about 550 | 0.967 +/- 0.004 | 0.984 +/- 0.001 |
| about 2,137 | 0.971 +/- 0.001 | 0.986 +/- 0.001 |
| about 10,524 | 0.973 +/- 0.001 | 0.988 +/- 0.000 |

CICIoT2023 to UNSW-NB15 (within-dataset reference: balanced accuracy 0.788):

| Labelled UNSW rows | Balanced accuracy | AUC |
|---|---|---|
| 0 | 0.476 +/- 0.002 | 0.598 +/- 0.089 |
| about 94 | 0.754 +/- 0.014 | 0.820 +/- 0.047 |
| about 509 | 0.756 +/- 0.011 | 0.887 +/- 0.006 |
| about 2,047 | 0.774 +/- 0.017 | 0.908 +/- 0.011 |
| about 10,364 | 0.771 +/- 0.011 | 0.895 +/- 0.006 |

Observations:
- About 100 labelled target rows lift balanced accuracy from chance to 0.953 (UNSW to CIC) and 0.754 (CIC to UNSW), which is 98% and 96% of the within-dataset reference. More labels give small further gains and plateau near the within-dataset level.
- On UNSW-NB15 benign recall stays around 0.5 to 0.6 even with 10,000 labelled rows, matching the ceiling set by the 4.7% of rows whose five-feature vector appears with both labels.

## Scalability (Stage F)

Script `src/scale_test.py`, runner `scripts/run_scale.sh`, raw results `results/scale_results.jsonl`.

Job: read CICIoT2023 CSV files, cast columns to numbers, remove duplicates and invalid rows, write Parquet; build the 5 shared features and write Parquet; balance, train a Random Forest (30 trees, depth 8) and evaluate on a 30% split.

Environment: one laptop with 4 logical cores and 7.7 GiB RAM (2 GiB swap), Docker, Spark standalone with 1 worker (4 GB executor memory), HDFS with 1 datanode.

Runtime against data size (4 cores):

| Input files | Input size | Clean rows | Clean (s) | Features (s) | Train (s) | Total (s) | Rows per second |
|---|---|---|---|---|---|---|---|
| 1 | 147 MB | 544,929 | 68.0 | 4.1 | 32.5 | 104.5 | 5,200 |
| 5 (best of 6 runs) | 739 MB | 2,207,344 | 157.3 | 4.9 | 57.5 | 219.7 | 10,000 |
| 10 | 1,510 MB | 4,128,223 | 244.5 | 7.2 | 68.8 | 320.5 | 12,900 |
| 15 | 2,364 MB | 6,144,389 | 374.3 | 9.1 | 95.7 | 479.1 | 12,800 |

The model's balanced accuracy stayed between 0.973 and 0.980 across all sizes.

Runtime against cores (5 files, 739 MB):

| Cores | Total time (s) | Speedup against 1 core |
|---|---|---|
| 1 | 283.6 | 1.00 |
| 2 | 207.7 | 1.37 |
| 4 | six runs: 219.7, 343.8, 383.8, 405.2, 444.6 and 1,100.6 | 0.72 (median 394.5 s), 1.29 (best), 0.26 (worst) |

Observations:
- Duplicate removal and parsing dominate: cleaning takes 76% to 78% of the time at 10 and 15 files.
- From 10 to 15 files the input grows 1.57 times and the runtime 1.49 times, so growth is close to linear.
- Extrapolating the 12,800 rows per second to all 63 files (about 26 million clean rows) gives roughly 35 minutes. This is an estimate, not a measurement.
- Going from 1 to 2 cores gives a 1.37 times speedup (68% efficiency). Four cores gave no further gain: six runs took between 220 and 1,101 s (median 394.5 s), slower than the one-core and two-core runs, which are single measurements. Stopping MongoDB for three of the four-core runs did not make them faster. The cause is unknown. Possible explanations, none tested: the four logical cores are shared with HDFS, Docker and Windows, and four concurrent tasks sharing a 4 GB executor spill heavily to disk (the machine has 7.7 GiB of memory and swap was in use).

Limitations of the scalability test: one machine and one worker, a single run for most configurations, and run-to-run variation large enough that the throughput figures are approximate. A multi-node cluster would be needed to show real horizontal scaling.

## Findings

1. Within-dataset detection works with only the five shared features (RF balanced accuracy 0.98 on CICIoT2023, 0.79 to 0.81 on UNSW-NB15).
2. Cross-dataset detection does not work. Balanced accuracy is at chance or below (0.17 to 0.57) and models mostly answer "attack".
3. The failure is explained by inverted feature-to-label relationships (table above), not by a scale difference alone. Log transforms, per-dataset standardisation and rank normalisation did not fix it.
4. The one cross-dataset result with AUC clearly above 0.5 in Experiment B (0.772, UNSW to CIC, all features, log + standardise) is not stable. Across three seeds the same configuration gives AUC 0.878, 0.098 and 0.563. Dropping any single feature or switching to rank normalisation also removes it. It is not evidence of transfer.
5. The best balanced accuracy in the ablation is 0.567 (CIC to UNSW, log + standardise, without `rate`), which is only slightly above chance.
6. Training one model on both datasets removes the gap on the tests it has seen (balanced accuracy 0.974 and 0.783), so the datasets are compatible when the model sees both but not when it must transfer.
7. The multiclass task on the shared classes fails the same way across datasets (macro recall 0.38 to 0.39 against a chance level of 0.33).
8. A small labelled sample from the new environment fixes most of the problem. About 100 labelled target rows recover 98% (UNSW to CIC) and 96% (CIC to UNSW) of within-dataset balanced accuracy. The practical recommendation is an onboarding step for each new environment: label a few hundred rows and retrain, or pool data from several environments.
9. The pipeline's runtime grows roughly linearly with data size (about 12,800 rows per second on one laptop), and doubling from 1 to 2 cores gives a 1.37 times speedup. Four cores gave no gain on this machine (six runs, median 394.5 s, slower than one and two cores); the cause was not established.

## Limitations

- CICIoT2023 within-dataset results use a random split of windows from the same captures, so they are probably optimistic.
- Experiments A and B used one random seed per configuration. Experiment C repeats the key configurations over 3 seeds. Seeds vary only the CICIoT2023 split and the undersampling, so the standard deviations understate other sources of variation.
- Only 5 features could be shared. CICIoT2023 features describe windows of packets, while UNSW-NB15 features describe whole flows.
- The CICIoT2023 sample is 5 of 63 files (3.2 million raw rows).
- Experiment D uses a class-balanced labelled target sample and a weighting scheme chosen by the authors. A real labelling effort would give a more imbalanced sample.
- The CICIoT2023 test split is a random split of windows from the same captures, which probably flatters the UNSW to CIC adaptation results in Experiment D as well. The UNSW-NB15 results use the published split and are the safer estimate.
- 4.7% of UNSW-NB15 rows have a feature vector that appears with both labels, which limits within-dataset accuracy on UNSW-NB15.

## Possible next experiments

1. Split CICIoT2023 by source file (or capture) instead of at random, to check that the within-dataset and adaptation results are not inflated by near-identical neighbouring windows.
2. Aggregate UNSW-NB15 flows into packet windows (using source address and time in the raw files) so both datasets describe the same unit of observation.
