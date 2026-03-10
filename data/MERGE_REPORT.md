# Interconnection Queue Master Dataset — Merge Report
Generated: 2026-03-09 20:28

## Summary
| Metric | Value |
|--------|-------|
| Total unique projects | 41,750 |
| With developer name | 32,406 (77.6%) |
| Missing developer | 9,344 (22.4%) |

## Merge Breakdown
| Source | Count |
|--------|-------|
| both | 27,544 |
| scrape_only | 11,407 |
| owen_only | 2,799 |

## Developer Coverage by ISO/Region
| Region | Total | Has Developer | Coverage |
|--------|-------|---------------|----------|
| AESO | 230 | 0 | 0.0% |
| BC | 10 | 0 | 0.0% |
| CAISO | 8,770 | 1,797 | 20.5% |
| ERCOT | 3,849 | 3,791 | 98.5% |
| IESO | 1,061 | 1,061 | 100.0% |
| ISO-NE | 365 | 365 | 100.0% |
| MISO | 5,449 | 5,368 | 98.5% |
| Maritimes | 57 | 0 | 0.0% |
| NYISO | 1,841 | 1,841 | 100.0% |
| PJM | 9,187 | 8,093 | 88.1% |
| Quebec | 10 | 0 | 0.0% |
| SPP | 2,842 | 2,769 | 97.4% |
| Saskatchewan | 11 | 0 | 0.0% |
| Southeast | 2,531 | 2,379 | 94.0% |
| West | 5,537 | 4,942 | 89.3% |

## Enrichment Sources
| Source | Count |
|--------|-------|
| owen_authority | 30,099 |
| interconnecting_entity | 2,228 |
| eia_state_cap | 59 |
| eia_name_match | 17 |
| ferc_filing | 3 |

## Field Coverage
| Field | Non-null | Coverage |
|-------|----------|----------|
| name | 32,663 | 78.2% |
| developer | 32,426 | 77.7% |
| capacity_mw | 40,886 | 97.9% |
| state | 40,801 | 97.7% |
| county | 38,327 | 91.8% |
| status | 41,750 | 100.0% |
| region | 41,750 | 100.0% |
| poi | 35,816 | 85.8% |
| type | 37,840 | 90.6% |
| transmission_owner | 34,944 | 83.7% |
| proposed_completion_date | 29,655 | 71.0% |
| interconnecting_entity | 6,047 | 14.5% |

## Sample Enriched Records (EIA/FERC matches)
|   queue_id | name                    | developer                    | developer_source       | enrichment_confidence   | capacity_mw   | state   | region    |
|-----------:|:------------------------|:-----------------------------|:-----------------------|:------------------------|:--------------|:--------|:----------|
|       0001 | Middletown Station      | Con Edison                   | interconnecting_entity | high                    |               | NY      | NYISO     |
|       0002 | Athens Gen              | Athens Gen Co./ PG&E         | interconnecting_entity | high                    | 1080.0        | NY      | NYISO     |
|       0003 | Bethlehem Energy Center | PSEG Power NY                | interconnecting_entity | high                    | 350.0         | NY      | NYISO     |
|       0004 | CT-LI DC Tie-line       | TransEnergie US, Ltd         | interconnecting_entity | high                    | 330.0         | NY      | NYISO     |
|       0005 | Torne Valley Station    | Sithe Energies               | interconnecting_entity | high                    | 860.0         |         | NYISO     |
|       0006 | Sunset Energy Fleet     | Sunset Energy Fleet LLC      | interconnecting_entity | high                    | 520.0         |         | NYISO     |
|       0007 | Ramapo Energy           | American National Power      | interconnecting_entity | high                    | 1100.0        |         | NYISO     |
|       0008 | Grassy Point            | Columbia Electric Corp.      | interconnecting_entity | high                    | 1100.0        |         | NYISO     |
|       0009 | Millennium 1            | Millennium Power Gen Co. LLC | interconnecting_entity | high                    | 160.0         |         | NYISO     |
|        001 |                         | Duke Energy Florida, LLC     | interconnecting_entity | high                    | 567.0         | FL      | Southeast |
