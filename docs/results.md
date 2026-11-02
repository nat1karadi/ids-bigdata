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

## Findings

1. Within-dataset detection works with only the five shared features (RF balanced accuracy 0.98 on CICIoT2023, 0.79 to 0.81 on UNSW-NB15).
2. Cross-dataset detection does not work. Balanced accuracy is at chance or below (0.17 to 0.57) and models mostly answer "attack".
3. The failure is explained by inverted feature-to-label relationships (table above), not by a scale difference alone. Log transforms, per-dataset standardisation and rank normalisation did not fix it.
4. The only cross-dataset result with AUC clearly above 0.5 (0.772, UNSW to CIC, all features, log + standardise) is not stable. Removing any single feature, or switching to rank normalisation, drops it to 0.17 to 0.57. It should not be presented as evidence of transfer.
5. The best balanced accuracy in the ablation is 0.567 (CIC to UNSW, log + standardise, without `rate`), which is only slightly above chance.

## Limitations

- CICIoT2023 within-dataset results use a random split of windows from the same captures, so they are probably optimistic.
- Each configuration was run once with one random seed. Repeat key configurations with several seeds and report mean and standard deviation.
- Only 5 features could be shared. CICIoT2023 features describe windows of packets, while UNSW-NB15 features describe whole flows.
- The CICIoT2023 sample is 5 of 63 files (3.2 million raw rows).
- 4.7% of UNSW-NB15 rows have a feature vector that appears with both labels, which limits within-dataset accuracy on UNSW-NB15.

## Possible next experiments

1. Multiclass on shared classes (BENIGN, DOS_DDOS, RECON), and a jointly trained model evaluated on both test sets.
2. Aggregate UNSW-NB15 flows into packet windows (using source address and time in the raw files) so both datasets describe the same unit of observation.
3. Repeat the key runs with several seeds.
