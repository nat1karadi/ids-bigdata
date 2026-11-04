# Cleaning Log (Stage C)

Date: 1 November 2026
Scripts: `src/clean_ciciot.py`, `src/clean_unsw.py`
Environment: Spark 3.5.1 (1 worker, 4 cores, 4 GB executor memory), HDFS (Hadoop 3.3.6)

## Cleaning steps (both datasets)

1. Column names converted to lower snake_case (e.g. `Protocol Type` becomes `protocol_type`).
2. Exact duplicate rows removed (`dropDuplicates`).
3. Rows with null, NaN or infinite values in any numeric column removed.
4. Labels trimmed and upper-cased (CICIoT2023). Attack category trimmed (UNSW-NB15).
5. CICIoT2023: binary column `is_attack` added (0 for BENIGN, 1 otherwise).
6. UNSW-NB15: `id` column dropped before duplicate removal.
7. Output saved as Parquet in HDFS under `/ids/clean/`.

## Results

| Dataset | Raw rows | After duplicates removed | After bad values removed | Removed |
|---|---|---|---|---|
| CICIoT2023 (5-file sample) | 3,228,175 | 2,017,860 | 2,017,818 | 37.5% |
| UNSW-NB15 training set | 175,341 | 107,740 | 107,740 | 38.6% |
| UNSW-NB15 testing set | 82,332 | 55,945 | 55,945 | 32.0% |

Output paths: `/ids/clean/ciciot2023`, `/ids/clean/unsw_train`, `/ids/clean/unsw_test`.

## Class balance after cleaning

| Dataset | Benign / Normal | Attack |
|---|---|---|
| CICIoT2023 | 75,655 (3.7%) | 1,942,163 (96.3%) |
| UNSW-NB15 training | 51,890 (48.2%) | 55,850 (51.8%) |
| UNSW-NB15 testing | 34,206 (61.1%) | 21,739 (38.9%) |

## Observations

- Most duplicates in CICIoT2023 are flood attack traffic. Benign rows barely changed (75,744 before, 75,655 after), while classes such as DDOS-ICMP_FLOOD roughly halved (493,494 before, 221,128 after).
- UNSW-NB15 duplicates are concentrated in some categories. In the testing set, Generic fell from 18,871 to 3,657 rows and DoS from 4,089 to 1,718.
- The datasets have very different base rates: about 4% benign in CICIoT2023 against 48% to 61% normal in UNSW-NB15. Accuracy alone is not a meaningful metric. Use macro-F1, per-class precision and recall, and balanced sampling or class weights.
- Removing duplicates before splitting avoids identical rows appearing in both training and test data.

## Corrections and follow-up checks (1 November 2026)

- The infinite-value filter in `clean_ciciot.py` skipped the CICIoT2023 `rate` column, because Spark read it as text (the CSV writes infinity as the word `inf`, which Spark's type inference does not accept as a number). The numeric-only filter therefore never saw it. 16 further CICIoT2023 rows with infinite `rate` were removed in `src/harmonise.py`, after `rate` was cast to a number. `clean_ciciot.py` was patched afterwards to cast all non-label columns to numbers before filtering.
- Identical rows shared between the UNSW training and testing sets: 940. They were removed from the testing set in `src/harmonise.py`.
- CICIoT2023 feature vectors (all 39 features) carrying more than one raw label: 82,420. Most are likely near-identical variants of the same attack type, which merge under the unified labels. After mapping to unified classes and keeping the 5 shared features, only 966 rows lie in vectors with both benign and attack labels.
