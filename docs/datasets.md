# Datasets

Project: Cross-Dataset Generalisable Intrusion Detection Framework Using Distributed Big-Data Analytics (DTS 301)

Download date: 31 October 2026

Raw data is stored under `data/` and is excluded from Git (see `.gitignore`).

---

## 1. CICIoT2023

| Item | Detail |
|---|---|
| Publisher | Canadian Institute for Cybersecurity (CIC), University of New Brunswick |
| Source | https://www.cicresearch.ca/IOTDataset/CIC_IOT_Dataset2023/ (folder `CSV/MERGED_CSV`) |
| Format used | Flow-based CSV (merged version). Raw PCAP files not used |
| Full dataset size | 63 files (`Merged01.csv` to `Merged63.csv`). The paper describes about 46.7 million records. The merged CSV files have 40 columns (39 features plus `Label`) |
| Content | 105 IoT devices, 33 attacks grouped into 7 classes (DDoS, DoS, Recon, Web-based, Brute Force, Spoofing, Mirai) plus benign traffic |
| Local path | `data/ciciot2023/` |
| Licence / use | Academic research use. Cite the paper below |

### Files downloaded (development sample)

| File | Size | Rows |
|---|---|---|
| Merged01.csv | 141 MB | TODO |
| Merged15.csv | 142 MB | TODO |
| Merged30.csv | 138 MB | TODO |
| Merged45.csv | 133 MB | TODO |
| Merged63.csv | 85 MB | TODO |

Combined development sample: 3,228,175 rows, 40 columns, all 34 labels present (checked with Spark on 1 November 2026).

Files were chosen from across the range so the sample is not limited to one section of the capture. Remaining files will be added later for the scalability tests.

Also downloaded: `README.pdf` (stored as `docs/ciciot2023-README.pdf`).

### Citation

E. C. P. Neto, S. Dadkhah, R. Ferreira, A. Zohourian, R. Lu, A. A. Ghorbani, "CICIoT2023: A Real-Time Dataset and Benchmark for Large-Scale Attacks in IoT Environment," *Sensors*, vol. 23, no. 13, 5941, 2023. DOI: 10.3390/s23135941

---

## 2. UNSW-NB15

| Item | Detail |
|---|---|
| Publisher | Cyber Range Lab, UNSW Canberra |
| Source | https://research.unsw.edu.au/projects/unsw-nb15-dataset |
| Format used | CSV files. PCAP, Bro and Argus files not used |
| Full dataset size | 2,540,044 records in four CSV files, 49 features including the label |
| Content | Normal traffic plus nine attack types (Fuzzers, Analysis, Backdoors, DoS, Exploits, Generic, Reconnaissance, Shellcode, Worms) |
| Predefined split | Published sizes: training set 175,341 records, testing set 82,332 records |
| Local path | `data/unsw/` |
| Licence / use | Free for academic research. Cite the papers below |

### Files downloaded

| File | Purpose | Size | Rows |
|---|---|---|---|
| UNSW-NB15_1.csv | Full data, part 1 | 162 MB | TODO |
| UNSW-NB15_2.csv | Full data, part 2 | 158 MB | TODO |
| UNSW-NB15_3.csv | Full data, part 3 | 148 MB | TODO |
| UNSW-NB15_4.csv | Full data, part 4 | 94 MB | TODO |
| NUSW-NB15_features.csv | Column names and descriptions (the official file name has this spelling) | 4 KB | TODO |
| UNSW_NB15_training-set.csv | Predefined training split | 14 MB | TODO |
| UNSW_NB15_testing-set.csv | Predefined testing split | 15 MB | TODO |

Check needed: the local `UNSW_NB15_training-set.csv` loaded with 67,688 rows (published size is 175,341), while the testing set loaded with 82,332 rows as expected. The training file may be incomplete and should be verified or re-downloaded before use.

Note: the four full CSV files have no header row. Column names come from `NUSW-NB15_features.csv`.

### Citation

N. Moustafa and J. Slay, "UNSW-NB15: a comprehensive data set for network intrusion detection systems (UNSW-NB15 network data set)," Military Communications and Information Systems Conference (MilCIS), IEEE, 2015.

N. Moustafa and J. Slay, "The evaluation of Network Anomaly Detection Systems: Statistical analysis of the UNSW-NB15 data set and the comparison with the KDD99 data set," *Information Security Journal: A Global Perspective*, 2016.

---

## 3. Notes for later stages

- Label column names, class counts and feature lists for both datasets: TODO after first Spark inspection.
- Unified label map and shared feature schema: see harmonisation documentation (to be added).
- Any additional CICIoT2023 files downloaded later should be added to the table above with the date.
