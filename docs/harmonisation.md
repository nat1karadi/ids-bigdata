# Harmonisation Design (Stage D, draft v1)

Goal: define a shared feature schema and a unified label map so that a model trained on one dataset can be tested on the other.

Sources: CICIoT2023 README (feature table and attack list), `NUSW-NB15_features.csv`, and the column lists seen in Spark for the cleaned data.

## 1. Key caveat: different units of observation

- **CICIoT2023**: each row summarises a **window of packets** (means, sums and standard deviations over the packets in the window).
- **UNSW-NB15**: each row is one **network flow (connection record)**.

Features with the same name or similar meaning are therefore not identical measurements. This is a main source of any generalisation gap and must be stated as a limitation in the report.

## 2. Shared feature candidates

Column names below are the cleaned (lower snake_case) names.

| Shared feature | CICIoT2023 source | UNSW-NB15 source | Comparability | Notes |
|---|---|---|---|---|
| `rate` | `rate` (packets/sec in window) | `rate` | Medium | `rate` is not in the UNSW features description file (it appears only in the train/test split files). Confirm its definition before use |
| `ttl` | `time_to_live` | `sttl` (source TTL) | Good | CICIoT2023 value is a window summary |
| `proto_tcp`, `proto_udp`, `proto_icmp` | `protocol_type` (mode of protocols in the window, numeric code) | `proto` (string) | Good | Check the CICIoT2023 numeric coding with value counts (expected IANA numbers: 6 TCP, 17 UDP, 1 ICMP). Everything else is "other" |
| `avg_pkt_size` | `avg` (mean packet length in window) | `(sbytes + dbytes) / (spkts + dpkts)` | Good to medium | Guard against division by zero |

These four groups give about six model inputs. This is the v1 shared set.

### Optional v2 additions (test separately)

| Shared feature | CICIoT2023 source | UNSW-NB15 source | Concern |
|---|---|---|---|
| `app_http`, `app_dns`, `app_ssh`, `app_smtp`, `app_irc` | column value greater than 0 | `service` equals the same name | Different meaning: protocol present in a window vs service label of a flow |
| `iat` | `iat` | `sinpkt` / `dinpkt` | Units must be checked (UNSW is in milliseconds) |

### Not used in the shared set

| Feature(s) | Reason |
|---|---|
| CICIoT2023: TCP flag proportions and counts, `header_length`, `min`, `max`, `std`, `variance`, `tot_size`, ARP, LLC, IGMP, DHCP | No UNSW-NB15 equivalent |
| CICIoT2023: `number`, `tot_sum` | Window totals, not comparable with flow totals (check the `number` distribution; it may be nearly constant by design) |
| UNSW-NB15: `dur`, `state`, jitter, load, window sizes, sequence numbers, `ct_*` counters, loss, retransmission, TCP timings | No CICIoT2023 equivalent |
| UNSW-NB15: `srcip`, `dstip`, `sport`, `dsport`, `stime`, `ltime` | Present only in the four raw CSV files (listed in `NUSW-NB15_features.csv`). Drop them when those files are loaded: they identify hosts and times, so a model could memorise them instead of learning behaviour |

## 3. Unified label map

| Unified class | CICIoT2023 labels | UNSW-NB15 `attack_cat` | Cleaned rows (CICIoT2023) | Cleaned rows (UNSW train / test) |
|---|---|---|---|---|
| BENIGN | BENIGN | Normal | 75,655 | 51,890 / 34,206 |
| DOS_DDOS | all DDOS-*, all DOS-*, MIRAI-GREETH_FLOOD, MIRAI-GREIP_FLOOD, MIRAI-UDPPLAIN | DoS | 1,859,781 | 3,806 / 1,718 |
| RECON | RECON-HOSTDISCOVERY, RECON-OSSCAN, RECON-PORTSCAN, RECON-PINGSWEEP, VULNERABILITYSCAN | Reconnaissance | 47,574 | 7,522 / 2,703 |
| OTHER_ATTACK | MITM-ARPSPOOFING, DNS_SPOOFING, DICTIONARYBRUTEFORCE, BROWSERHIJACKING, SQLINJECTION, COMMANDINJECTION, XSS, BACKDOOR_MALWARE, UPLOADING_ATTACK | Exploits, Fuzzers, Generic, Analysis, Backdoor, Shellcode, Worms | 34,808 | remainder |

Judgement calls to confirm as a group:
- Mirai floods are placed in DOS_DDOS because they are flood attacks, even though CICIoT2023 lists Mirai as its own category.
- UNSW "Analysis" is left in OTHER_ATTACK because it mixes scanning with other activity.
- UNSW "Exploits" and the CICIoT2023 web-based attacks are not treated as equivalent.

## 4. Experiment design implications

1. **Binary task (main cross-dataset result)**: BENIGN vs attack (any attack). Needs no class matching.
2. **Shared multiclass task**: BENIGN, DOS_DDOS, RECON only. Rows from OTHER_ATTACK are excluded for this task.
3. **Ceiling baselines**: full-feature within-dataset models for each dataset (not cross-dataset), to show how much is lost by moving to the shared features.
4. **Transfer technique to test**: per-dataset standardisation of the shared features (z-score computed inside each dataset), log transform of heavy-tailed features (`rate`, `avg_pkt_size`), and class weights or balanced sampling.
5. **Metrics**: macro-F1, per-class precision and recall, and the generalisation gap (within-dataset score minus cross-dataset score). Do not rely on accuracy: CICIoT2023 is about 96% attack, UNSW-NB15 is about 40% to 50% attack.

## 5. Checks before finalising

- [ ] Value counts of `protocol_type` in CICIoT2023 and of `proto` in UNSW-NB15.
- [ ] Definition of UNSW `rate` and the units of CICIoT2023 `iat`.
- [ ] Distribution summary (min, max, mean) of each shared feature in both datasets.
- [ ] Identical rows shared between UNSW training and testing sets.
- [ ] Group agreement on the label map in section 3.
- [ ] Naming differences when loading the four raw UNSW files: the features file uses `smeansz`, `dmeansz`, `Sintpkt`, `Dintpkt`, `res_bdy_len`, while the train and test files use `smean`, `dmean`, `sinpkt`, `dinpkt`, `response_body_len`. Rename to the train/test names.

## 6. Verified decisions (v1.1, 1 November 2026)

This section supersedes the feature lists above where they differ.

**Final v1 shared features (5 inputs):** `rate`, `ttl`, `avg_pkt_size`, `proto_tcp`, `proto_udp`.

**Extras kept as separate columns for ablation tests:** `proto_icmp`, `proto_other`, `app_http`, `app_dns`, `iat_ms`.

Findings from the feature check:
- UNSW `rate` equals (spkts + dpkts - 1) / dur, and is 0 when `dur` is 0 (484 training rows). Checked against sample rows. It is packets per second, so it is comparable with the CICIoT2023 `rate`.
- CICIoT2023 `number` is capped at 100 (median 100, max 100) and is dropped.
- CICIoT2023 `protocol_type` uses protocol numbers: 6 (TCP) 58%, 17 (UDP) 23%, 1 (ICMP) 12%, 47 (GRE) 6%, 0 and 2 negligible.
- ICMP is almost absent in UNSW-NB15 and GRE appears only in CICIoT2023, so `proto_icmp` and `proto_other` could act as shortcuts. They are excluded from v1 and tested as an ablation.
- `ssh`, `smtp` and `irc` are present in under 0.5% of CICIoT2023 rows and are dropped. `http` and `dns` are kept as extras.
- CICIoT2023 `iat` is in seconds and was converted to milliseconds (`iat_ms`). The medians still differ by about 300 times, so it stays an extra.
- Scale gap: median `rate` is about 15,900 packets per second in CICIoT2023 and about 82 in UNSW-NB15. Median `avg_pkt_size` is 60 against 100 bytes. This motivates the log transform and per-dataset standardisation.

Data fixes applied in `src/harmonise.py`:
- 16 CICIoT2023 rows with infinite `rate` removed (the earlier cleaning filter missed them).
- 940 UNSW-NB15 test rows identical to a training row removed.

Harmonised datasets (HDFS `/ids/harmonised/`):

| Dataset | Rows | BENIGN | DOS_DDOS | RECON | OTHER_ATTACK |
|---|---|---|---|---|---|
| CICIoT2023 sample | 2,017,802 | 75,655 | 1,859,765 | 47,574 | 34,808 |
| UNSW-NB15 (train 107,740 + test 55,005) | 162,745 | 85,722 | 5,500 | 9,991 | 61,532 |

Ambiguity of the 5 shared features (rows whose exact feature vector appears with both a benign and an attack label):
- CICIoT2023: 966 rows (0.05%). 1,141,993 distinct vectors.
- UNSW-NB15: 7,678 rows (4.7%). 123,085 distinct vectors.

The UNSW figure sets a limit on how well any model can do on UNSW-NB15 with only these features, and belongs in the discussion section.
