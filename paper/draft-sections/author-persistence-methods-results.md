# Author Persistence and Terminology Substitution: Methods and Results Draft

## Integration Notes (Not For Manuscript)

- This section uses the author-persistence dashboard export generated on 2026-06-09 from `dashboard/author_persistence.json`.
- The author-persistence export covered 106,388 processed article records, which is newer than the older `greg-methods-results.md` draft. Reconcile corpus counts before merging these sections.
- The current author analysis used PostgreSQL Crossref metadata because the optional focused SQLite stage was not present on `raksasa`.
- The journal target should be named consistently as European Journal of Human Genetics if that is the intended venue.

## Materials and Methods

### Author Matching and Longitudinal Terminology Analysis

To examine whether changes in terminology reflected changes within continuing authors or replacement by newer authors, we linked processed articles by matched Crossref author identity. Author identity was defined using ORCID when an ORCID was available. For authors without ORCID, we used a conservative normalized name key based on family name plus a non-initial-only given name. Initial-only names without ORCID were excluded from author-linkage analyses to reduce false matches between distinct authors with common surnames. This approach favours specificity over sensitivity: it may fail to link some publications by the same person when ORCID is absent or naming varies, but it reduces the risk of spurious same-author pairs.

The author-linkage analysis was restricted to records already processed by the title/abstract terminology classifier. For each matched author identity, publications were ordered by publication year. Same-year pairs were not used for temporal prediction because no within-year publication order could be inferred reliably. We evaluated three related temporal units. First, we counted all earlier-to-later paper pairs that shared a matched author. Second, we counted each paper against the same author's next later analysed paper. Third, we collapsed each author to active publication years and counted transitions from one active publication year to the next. The all-pairs analysis uses more information but includes correlated observations when prolific authors contribute many papers; the next-paper and author-year analyses were therefore used as robustness checks with less duplication.

For each temporal unit, we tested whether earlier use of `Caucasian` predicted later use of `Caucasian`. Two-sided Fisher exact tests were used as the primary tests for 2 x 2 tables, with Pearson chi-square tests as secondary checks. Odds ratios were Haldane-adjusted by adding 0.5 to each cell to avoid zero-cell instability. We also examined whether authors who had previously used `Caucasian` later used `white` or European-origin terminology without continuing to use `Caucasian`.

### Author Attrition and Decomposition of the Decline After Peak Use

The centred moving average of `Caucasian` use peaked in 2006 in the main temporal analysis, so 2006 was used as the peak year for the author-decomposition analysis. We defined "legacy Caucasian authors" as matched authors who had at least one `Caucasian`-positive analysed article published in or before 2006. We then compared the peak year, 2006, with the recent period beginning in 2015.

To quantify how much of the decline was due to behaviour change within author groups versus changing author composition, we expressed the article-level `Caucasian` rate in a period as

```text
R = s r + (1 - s) n
```

where `s` is the share of analysed articles with at least one legacy `Caucasian` author, `r` is the `Caucasian` rate among those articles, and `n` is the rate among all other articles. The difference between 2006 and 2015+ was decomposed using a symmetric Oaxaca-Kitagawa decomposition. The behaviour component was the weighted change in `r` and `n`, averaged over peak-year and recent-period group shares. The composition component was the change in the legacy-author article share, averaged over peak-year and recent-period within-group rates. Components were reported as percentages of the total reduction in the `Caucasian` rate.

As a complementary author-level attrition analysis, we identified authors with at least one pre-2015 analysed paper and compared recent activity between authors who had used `Caucasian` before 2015 and authors who had not. Among active pre-2015 `Caucasian` users, we quantified recent use of `Caucasian`, `white`, European-origin language, and `white` or European-origin language without `Caucasian`.

### Tests of Interchangeability Among Terms

Because the focal categories are not mutually exclusive, we did not treat `Caucasian`, `white`, and European-origin terminology as a single multinomial article label. Instead, we used complementary tests. First, we compared peak-year and recent-period counts of articles positive for `Caucasian`, `white`, and European-origin terms using a Pearson chi-square test over the three term-positive count columns. This tests whether the distribution of focal terminology changed over time. Second, we tested the article-level decline in `Caucasian` use from 2006 to 2015+ using Fisher exact testing. Third, we tested article-level co-occurrence between each pair of focal categories (`Caucasian` with `white`, `Caucasian` with European-origin terminology, and `white` with European-origin terminology). These analyses cannot prove semantic equivalence or non-equivalence in a philosophical sense, but they test the null expectation that the observed modern pattern could arise from random interchangeable use drawn from a stable focal vocabulary.

## Results

### Author Matching Coverage

The processed corpus contained 106,388 analysed article records. Crossref author metadata was present for 101,546 records, and 74,099 records had at least one author identity that met the conservative matching criteria. In total, 236,290 matched author identities were identified, including 21,120 ORCID-based identities and 215,170 strict-name identities. Across all matched identities, 3,470 authors had at least one `Caucasian`-positive analysed article.

### Prior Use of `Caucasian` Strongly Predicted Later Use

Across all temporal shared-author paper pairs, earlier use of `Caucasian` was associated with a substantially higher probability of later `Caucasian` use. Later papers used `Caucasian` in 727 of 22,462 pairs (3.24%) when the earlier paper used `Caucasian`, compared with 15,538 of 4,226,150 pairs (0.37%) when the earlier paper did not use `Caucasian` (odds ratio [OR] 9.07, Fisher exact p = 1.83 x 10^-15).

The association was stronger in the less-duplicated transition analyses. In next-paper transitions, a later paper used `Caucasian` in 142 of 2,343 transitions (6.06%) following a `Caucasian`-positive paper, compared with 1,653 of 358,456 transitions (0.46%) following a `Caucasian`-negative paper (OR 13.97, Fisher exact p = 7.51 x 10^-16). In adjacent author-year transitions, the next active publication year used `Caucasian` in 226 of 2,168 transitions (10.42%) following a `Caucasian`-positive active year, compared with 1,651 of 221,988 transitions (0.74%) following a `Caucasian`-negative active year (OR 15.56, Fisher exact p = 9.08 x 10^-16). These results indicate that `Caucasian` use was not randomly distributed across authors: authors who had used the term were much more likely to use it again.

### The Decline Was Driven Mainly by Authors and Author Groups Stopping Use

In 2006, the peak year, 39 of 2,635 analysed articles used `Caucasian` (1.48%). In the 2015+ period, 106 of 30,103 analysed articles used `Caucasian` (0.35%). We identified 1,211 legacy `Caucasian` authors, defined as matched authors with at least one `Caucasian`-positive article through 2006. In 2006, articles with at least one legacy `Caucasian` author represented 277 of 2,635 articles (10.5%) and had a `Caucasian` rate of 31 of 277 (11.2%). By 2015+, articles with at least one legacy `Caucasian` author represented only 1,774 of 30,103 articles (5.9%), and their `Caucasian` rate had fallen to 7 of 1,774 (0.39%). Articles without a legacy `Caucasian` author had a low `Caucasian` rate in both periods: 8 of 2,358 (0.34%) in 2006 and 99 of 28,329 (0.35%) in 2015+.

The symmetric decomposition attributed 77.7% of the peak-to-recent reduction to changed behaviour within author groups, principally the near-disappearance of `Caucasian` among articles by legacy `Caucasian` authors. The remaining 22.3% was attributable to changing author composition. Thus, the reduction was not explained primarily by earlier `Caucasian` users ceasing to publish and being replaced by new authors. Rather, the dominant pattern was that author groups that had previously used `Caucasian` largely stopped using it.

The author-level attrition analysis supported this interpretation. Among authors with pre-2015 analysed publications, those who had used `Caucasian` before 2015 were more likely, not less likely, to publish again in 2015 or later: 801 of 2,358 (34.0%) versus 26,329 of 120,145 (21.9%) among authors who had not used `Caucasian` before 2015 (OR 1.83, Fisher exact p = 4.71 x 10^-15). Therefore, the decline in `Caucasian` was not due to pre-2015 `Caucasian` users disproportionately disappearing from the analysed literature.

### Continuing Previous Users Rarely Used `Caucasian` Recently, but Often Used `white` or European-Origin Terminology

Among the 801 authors who had used `Caucasian` before 2015 and remained active in 2015+, only 23 (2.9%) used `Caucasian` in the recent period. By contrast, 152 (19.0%) used either `white` or European-origin terminology in the recent period, and 143 (17.9%) used `white` or European-origin terminology without using `Caucasian`. At the paper level, recent articles with at least one pre-2015 `Caucasian` author used `Caucasian` in 20 of 3,646 papers (0.55%), but used `white` or European-origin terminology without `Caucasian` in 110 of 3,646 papers (3.02%). These findings are consistent with a shift away from `Caucasian` among continuing authors, with some replacement by `white` and especially European-origin terminology.

Active pre-2015 `Caucasian` users were also more likely than other recently active pre-2015 authors to use `white` or European-origin terminology in 2015+: 152 of 801 (19.0%) versus 2,468 of 26,329 (9.4%) (OR 2.27, Fisher exact p = 1.67 x 10^-15). They were only modestly more likely to continue using `Caucasian`: 23 of 801 (2.9%) versus 457 of 26,329 (1.7%) (OR 1.71, Fisher exact p = 0.0278). The much larger recent difference for `white` or European-origin terminology suggests that the recent vocabulary of previous `Caucasian` users was not simply continued use of the same term.

### Focal Terms Were Not Used as a Stable Randomly Interchangeable Vocabulary

The distribution of focal terminology changed significantly between 2006 and 2015+. In 2006, the term-positive counts were 39 `Caucasian`, 4 `white`, and 37 European-origin articles. In 2015+, the corresponding counts were 106, 149, and 440. A Pearson chi-square test over these term-positive count columns rejected equality of the peak-year and recent-period distributions (chi-square = 56.24, df = 2, p = 6.13 x 10^-13). The article-level rate of `Caucasian` also fell significantly from 39 of 2,635 (1.48%) in 2006 to 106 of 30,103 (0.35%) in 2015+ (OR 4.28 for 2006 versus 2015+, Fisher exact p = 1.11 x 10^-11).

Article-level co-occurrence tests further argued against a simple random interchangeable-label model. `Caucasian` and `white` co-occurred more often than expected under independence (OR 6.07, Fisher exact p = 2.30 x 10^-6), as did `Caucasian` and European-origin terminology (OR 9.35, Fisher exact p = 4.68 x 10^-16) and `white` with European-origin terminology (OR 8.25, Fisher exact p = 3.36 x 10^-16). Because the categories can overlap, these tests do not imply that the words are semantically identical. Rather, they show that the terms are clustered in a non-random subset of articles and that their temporal distribution changed sharply after the peak of `Caucasian` use.

## Proposed Tables and Figures

### Table: Author-Persistence Tests

| Temporal unit | Later `Caucasian` after prior `Caucasian` | Later `Caucasian` after no prior `Caucasian` | OR | Fisher exact p |
| --- | ---: | ---: | ---: | ---: |
| All temporal shared-author paper pairs | 727/22,462 (3.24%) | 15,538/4,226,150 (0.37%) | 9.07 | 1.83 x 10^-15 |
| Next later shared-author paper transition | 142/2,343 (6.06%) | 1,653/358,456 (0.46%) | 13.97 | 7.51 x 10^-16 |
| Adjacent author-year transition | 226/2,168 (10.42%) | 1,651/221,988 (0.74%) | 15.56 | 9.08 x 10^-16 |

### Table: Decomposition of the Decline in `Caucasian`

| Measure | 2006 | 2015+ |
| --- | ---: | ---: |
| Analysed articles | 2,635 | 30,103 |
| Articles using `Caucasian` | 39 (1.48%) | 106 (0.35%) |
| Articles with at least one legacy `Caucasian` author | 277 (10.5%) | 1,774 (5.9%) |
| `Caucasian` rate among legacy-author articles | 31/277 (11.2%) | 7/1,774 (0.39%) |
| `Caucasian` rate among other articles | 8/2,358 (0.34%) | 99/28,329 (0.35%) |
| Articles by new never-`Caucasian` author groups | 0 | 26,866 (89.2%) |

The symmetric decomposition attributed 77.7% of the peak-to-recent reduction to changed behaviour within author groups and 22.3% to author-composition change.

### Table: Recent Activity and Substitution Among Previous Users

| Comparison | Previous `Caucasian` users | Other pre-2015 authors | OR | Fisher exact p |
| --- | ---: | ---: | ---: | ---: |
| Active in 2015+ | 801/2,358 (34.0%) | 26,329/120,145 (21.9%) | 1.83 | 4.71 x 10^-15 |
| Used `Caucasian` in 2015+, among active authors | 23/801 (2.9%) | 457/26,329 (1.7%) | 1.71 | 0.0278 |
| Used `white` or European-origin terminology in 2015+, among active authors | 152/801 (19.0%) | 2,468/26,329 (9.4%) | 2.27 | 1.67 x 10^-15 |

Among recent papers with at least one pre-2015 `Caucasian` author, 20 of 3,646 (0.55%) used `Caucasian`; 110 of 3,646 (3.02%) used `white` or European-origin terminology without `Caucasian`.

### Table: Term-Distribution and Co-Occurrence Tests

| Test | Estimate | p value |
| --- | ---: | ---: |
| 2006 versus 2015+ term-positive distribution (`Caucasian`, `white`, European-origin) | chi-square = 56.24, df = 2 | 6.13 x 10^-13 |
| Decline in article-level `Caucasian` rate, 2006 versus 2015+ | OR = 4.28 | 1.11 x 10^-11 |
| Article-level `Caucasian` and `white` co-occurrence | OR = 6.07 | 2.30 x 10^-6 |
| Article-level `Caucasian` and European-origin co-occurrence | OR = 9.35 | 4.68 x 10^-16 |
| Article-level `white` and European-origin co-occurrence | OR = 8.25 | 3.36 x 10^-16 |

The term-positive count table for the first row was 39 `Caucasian`, 4 `white`, and 37 European-origin articles in 2006, compared with 106, 149, and 440 articles respectively in 2015+.

### Figure: First and Last Use of `Caucasian` by Author

Each point represents a matched author identity with at least one `Caucasian`-positive analysed article. The x-axis shows the first year in which the author used `Caucasian`; the y-axis shows the last year in which the author used `Caucasian`. Points on the diagonal indicate authors observed using the term in a single year only. Points below recent years after early first-use years illustrate that many early users did not continue using the term into the recent period.

### Figure: Relationship Between Focal Terms Over Time and Across Authors

Annual scatter plots compare article-level `Caucasian` rates with `white` and European-origin rates by year. Author-level scatter plots compare each matched author's count of `Caucasian`-positive papers with their counts of `white`-positive and European-origin-positive papers. These plots should be interpreted together with the formal term-distribution and co-occurrence tests because the term categories are overlapping rather than mutually exclusive.

## Files Used

- [dashboard/author_persistence.json](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/dashboard/author_persistence.json)
- [dashboard/author_persistence.csv](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/dashboard/author_persistence.csv)
- [dashboard/author_persistence_annual.csv](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/dashboard/author_persistence_annual.csv)
- [extractor/author_persistence_stats.py](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/extractor/author_persistence_stats.py)
