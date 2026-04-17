# Greg Draft: Methods, Results, and Figure Text

## Integration Notes (Not For Manuscript)

- This draft uses the current live project scope: 17 enabled journals, 98,699 in-scope records, and 96,489 processed records. Sally's PDF still reflects an older 9-journal plan.
- These counts match the public dashboard at `lig.symmachus.org` as checked on 2026-04-17: 98,699 total, 96,489 processed, 2,243 skipped for missing titles.
- Trend reporting stops at 2024. Only 468 records from 2025 had been processed at export time, so 2025 is incomplete and should not be interpreted as a full year.
- The current code in [extractor/bulkquery.py](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/extractor/bulkquery.py) uses the OpenAI batch API with structured function-calling output. The database does not preserve the exact model name used for every historical batch, so check the batch logs before naming a model in the paper.
- The stored `other` category is broader than intended. In practice it contains many non-European population labels (for example `Japanese`, `Chinese`, `Ashkenazi Jewish`). The manuscript text below therefore focuses on `caucasian`, `white`, and the broader `european` / European-origin category.
- Figures generated from the current export are in [paper/figures](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/paper/figures), and the supporting CSV files are in [paper/data](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/paper/data).

## Materials and Methods

### Title and Abstract Analysis

We assembled article metadata from CrossRef for the 17 human genetics journals enabled in the project database at the time of analysis. The in-scope corpus contained 98,699 records published between 1947 and 2025. Of these, 96,489 records (97.8%) had been processed by the automated classifier when the present export was generated, while 2,243 records were skipped because no title was available. Processed records included 26,221 articles with abstracts and 70,268 title-only records.

Each record was classified from its title and, when available, its abstract using an OpenAI batch-processing pipeline with structured function-calling output. The classifier was prompted to determine whether the article used terms such as "Caucasian", "white", or "European ancestry" in a way that referred to race, ancestry, ethnicity, or population. The stored outputs were article-level boolean fields for `caucasian`, `white`, and `european`, together with the extracted phrase where relevant. Articles were allowed to be positive for more than one category, but each category was counted at most once per article regardless of repetition within the title or abstract.

Although the original research question referred to the phrase "European ancestry", the implemented `european` field captured a broader class of European-origin descriptors rather than the exact phrase alone. In the processed corpus, this category included phrases such as "European populations", "European ancestry", "European Americans", "Europeans", and "European descent".

### Data Validation

Because `white` is the most ambiguous of the focal categories, we conducted a manual spot-check of 80 randomly sampled `white`-positive records from the processed corpus. Seventy-eight of the 80 sampled records were clear references to human race, ethnicity, ancestry, or population. Two were clear false positives: one Heredity article on the Drosophila `white` gene and one case report using `white` to describe hypopigmented skin. This spot-check suggests that obvious false positives are uncommon but not absent, and that the remaining errors are concentrated in non-human genetics and colour-description contexts.

### Data Analysis

The primary outcome was article-level prevalence rather than raw token frequency. For each publication year, we calculated the proportion of processed articles that were positive for each focal category. To reduce year-to-year noise, especially in early decades with smaller yearly volumes, we overlaid the annual percentages with centred 5-year moving averages. Because 2025 processing was incomplete at export time, all temporal figures and trend statements were restricted to 1947-2024.

## Results

### Corpus Coverage

At export time, the project database contained 98,699 in-scope records from 17 journals, of which 96,489 had been processed by the title/abstract classifier and 2,243 had been skipped because no title was available. After excluding the incomplete 2025 tail, the trend analysis covered 95,941 processed records published between 1947 and 2024. Only 26,027 of these 95,941 records (27.3%) included an abstract in CrossRef, meaning that the majority of classifications relied on titles alone.

Across 1947-2024, the three focal categories appeared in 1,667 processed records (1.7%). A broader set of population descriptors, including the exploratory `other` category, appeared in 5,188 records (5.4%).

### Temporal Trends in Focal Terminology

Across the 1947-2024 series, European-origin descriptors were the most common focal category (933 articles), followed by `Caucasian` (530) and `white` (282). The term `Caucasian` increased through the 1990s and early 2000s, peaking at 1.42% of processed articles in 2007 (36/2,539), then declining to 0.13% in 2024 (3/2,328). Its centred 5-year moving average peaked in 2006 at 1.22% and fell steadily thereafter.

`White` remained relatively uncommon through the earlier decades, then rose in the late 2010s and early 2020s. The annual percentage peaked at 0.74% in 2020 (20/2,685) and remained 0.73% in 2021 (20/2,736). The centred 5-year moving average peaked in 2022 at 0.65% and remained elevated at 0.59% in 2024.

European-origin descriptors were both more frequent and more persistent than the other two focal categories. The annual percentage peaked at 1.83% in 2021 (50/2,736). The centred 5-year moving average continued to rise into the most recent complete years, peaking at 1.56% in 2023 and remaining high at 1.46% in 2024. The most common stored formulations in this category were `European populations` (n = 52), `European ancestry` (n = 33), `European Americans` (n = 21), `Europeans` (n = 21), and `European descent` (n = 15), indicating that the category captured a broader European-origin vocabulary rather than the exact phrase `European ancestry` alone.

Taken together, these patterns suggest a shift away from `Caucasian`, a later but smaller rise in `white`, and sustained growth in European-origin language.

### Manual Spot-Check of `white`

In the manual review of 80 randomly sampled `white`-positive records, 78 were judged to be true positives and 2 were clear false positives. The false positives were a Heredity paper about the Drosophila `white` gene and a clinical case report that used `white` as a colour descriptor for skin changes. These findings support the use of the title/abstract classifier for descriptive trend analysis, while also showing that ambiguous uses of `white` are not fully eliminated.

## Figure Legends

### Figure 1

Article-level prevalence of focal terminology in titles and abstracts, 1947-2024. Each panel shows the annual percentage of processed articles classified as using `Caucasian`, `white`, or a broader European-origin descriptor. Thin lines show annual values; thick lines show centred 5-year moving averages. The 2025 data were excluded because processing was incomplete at export time.

### Figure 2

Corpus scope by journal at export time. Bar length shows the number of in-scope records per journal, and bar colour shows the percentage of records for which CrossRef supplied an abstract. This figure illustrates the uneven abstract coverage across journals and explains why the classifier frequently operated on titles alone.

## Files Used

- [paper/data/journal_scope.csv](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/paper/data/journal_scope.csv)
- [paper/data/annual_processed_trends.csv](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/paper/data/annual_processed_trends.csv)
- [paper/data/european_phrases_top50.csv](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/paper/data/european_phrases_top50.csv)
- [paper/data/other_phrases_top50.csv](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/paper/data/other_phrases_top50.csv)
- [paper/data/white_positive_spotcheck_sample.csv](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/paper/data/white_positive_spotcheck_sample.csv)
- [paper/scripts/generate_paper_figures.py](/Users/gregb/Documents/devel/Word-Frequency-Analysis-/paper/scripts/generate_paper_figures.py)
