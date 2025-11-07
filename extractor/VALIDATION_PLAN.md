# Validation Sample Plan

## Overview

This document outlines the plan for validating the reliability of abstract-only terminology analysis by comparing it with full-body article analysis.

## Background

Currently, the project analyzes article **titles and abstracts only** to identify racial/ethnic terminology usage. This is efficient and cost-effective, but we need to validate whether abstract-only analysis reliably captures terminology usage compared to analyzing the full article body.

## Validation Approach

### 1. Random Sample Selection

**Status**: ✅ Complete

- Created `quick_random_sample.py` tool for reproducible random sampling
- Generated a reproducible 500-paper sample using seed 42
- Sample spans the full range of journals and years in the dataset

**Output**: `sample_500.csv` (generated with `quick_random_sample.py --seed 42 --sample-size 500`)

### 2. Database Storage for Validation Sample

**Status**: ⏳ Pending

**Requirements**:
- Create new table `languageingenetics.validation_sample` to store:
  - Sample metadata (article_id, DOI, selection criteria, seed used)
  - Abstract-only analysis results (current baseline)
  - Full-body analysis results (to be collected)
  - Comparison metrics

**Proposed Schema**:
```sql
CREATE TABLE languageingenetics.validation_sample (
    id SERIAL PRIMARY KEY,
    article_id INTEGER NOT NULL REFERENCES raw_text_data(id),

    -- Sample metadata
    selected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    selection_seed INTEGER NOT NULL,
    sample_batch TEXT,  -- e.g., "seed42_500papers"

    -- Original abstract-only results (from languageingenetics.files)
    abstract_caucasian BOOLEAN,
    abstract_white BOOLEAN,
    abstract_european BOOLEAN,
    abstract_european_phrase TEXT,
    abstract_other BOOLEAN,
    abstract_other_phrase TEXT,

    -- Full-body results (to be populated)
    fullbody_caucasian BOOLEAN,
    fullbody_white BOOLEAN,
    fullbody_european BOOLEAN,
    fullbody_european_phrase TEXT,
    fullbody_other BOOLEAN,
    fullbody_other_phrase TEXT,
    fullbody_processed BOOLEAN DEFAULT FALSE,
    fullbody_processed_at TIMESTAMP,

    -- Comparison flags
    matches_caucasian BOOLEAN,  -- TRUE if abstract and fullbody agree
    matches_white BOOLEAN,
    matches_european BOOLEAN,
    matches_other BOOLEAN,

    -- Full-body fetch metadata
    fullbody_fetch_method TEXT,  -- e.g., 'unpaywall', 'crossref', 'manual'
    fullbody_available BOOLEAN,
    fullbody_fetch_attempted_at TIMESTAMP,

    UNIQUE(article_id, sample_batch)
);

CREATE INDEX idx_validation_sample_article ON validation_sample(article_id);
CREATE INDEX idx_validation_sample_batch ON validation_sample(sample_batch);
CREATE INDEX idx_validation_sample_processed ON validation_sample(fullbody_processed);
```

### 3. Upload Capability

**Status**: ⏳ Pending

**Task**: Build a script to upload the CSV sample to the validation table

```bash
# Example usage (to be implemented)
./upload_validation_sample.py \
    --csv sample_500.csv \
    --batch "seed42_500papers" \
    --seed 42
```

**Implementation considerations**:
- Read CSV from `quick_random_sample.py` output
- Join with existing `languageingenetics.files` to get abstract-only results
- Insert into `validation_sample` table with metadata
- Handle duplicates gracefully (upsert)

### 4. Full-Body Article Collection

**Status**: ⏳ Pending

**Challenges**:
- Need to obtain full-text PDFs or HTML for 500 articles
- Many articles may be behind paywalls
- Will need to use APIs like:
  - Unpaywall API (for open access articles)
  - CrossRef text-mining endpoints
  - Direct publisher APIs where available
  - Manual download for remaining articles

**Proposed workflow**:
1. Try Unpaywall API first (best for open access)
2. Try CrossRef text-mining endpoint
3. Check if DOI redirects to open repository
4. Flag remaining articles for manual review/download

**Expected Success Rate**:
- ~30-50% may be openly available via Unpaywall
- Additional 10-20% via other automated methods
- Remaining 30-60% may require institutional access or manual download

### 5. Full-Body Processing

**Status**: ⏳ Pending

**Task**: Process full article bodies through OpenAI using same prompt as abstract analysis

**Considerations**:
- Full articles are much longer than abstracts (may need chunking or summarization)
- Token costs will be significantly higher
- May need to adjust prompt to handle longer texts
- Consider using batch API for cost savings

**Proposed approach**:
```
For each article with available full body:
1. Extract text from PDF/HTML
2. Clean and normalize text
3. Submit to OpenAI batch API with same terminology detection prompt
4. Store results in validation_sample.fullbody_* columns
5. Update comparison flags (matches_*)
```

### 6. Comparison Analysis

**Status**: ⏳ Pending

**Key Questions**:
1. **Sensitivity**: Does the abstract miss terminology that appears in the full body?
2. **Specificity**: Does the abstract falsely indicate terminology not in the full body?
3. **Agreement Rate**: What percentage of papers show the same results?
4. **False Negatives**: How many papers have terminology in body but not abstract?
5. **False Positives**: How many papers have terminology in abstract but not body?

**Metrics to Calculate**:
- Overall agreement rate (%)
- Cohen's Kappa for inter-rater reliability
- Sensitivity and specificity per terminology type
- Confusion matrix for each terminology category

### 7. Documentation and Reporting

**Status**: ⏳ Pending

**Deliverables**:
- Statistical analysis of abstract vs full-body agreement
- Recommendations on whether abstract-only analysis is sufficient
- If not sufficient, recommendations for improved methodology
- Publication-ready validation report

## Timeline Estimate

| Phase | Estimated Time | Dependencies |
|-------|---------------|--------------|
| Database table creation | 1 hour | None |
| Upload script development | 2-3 hours | Database table |
| Full-body collection (automated) | 1-2 days | Upload complete |
| Full-body collection (manual) | 1-2 weeks | Automated attempts complete |
| Full-body processing | 3-5 days | Full texts collected |
| Analysis and comparison | 1-2 days | Processing complete |
| Documentation | 2-3 days | Analysis complete |

**Total**: 2-4 weeks (depending on manual collection effort)

## Cost Estimate

### OpenAI API Costs

**Current abstract-only processing**:
- Average tokens per abstract: ~400 tokens (prompt + completion)
- Cost per 1M tokens (GPT-4o): ~$5 input, ~$15 output
- Cost per article: ~$0.001-0.002

**Full-body processing estimate**:
- Average tokens per full article: ~5,000-15,000 tokens
- Will likely need to chunk or summarize long articles
- Estimated cost per article: ~$0.02-0.05
- **Total for 500 articles**: ~$10-25

**Recommendation**: Use batch API for 50% discount: **~$5-13 total**

## Next Steps

1. ✅ Create random sampling tool (`quick_random_sample.py`)
2. ✅ Generate 500-paper sample
3. ⏳ Create `validation_sample` database table
4. ⏳ Build upload script
5. ⏳ Implement full-body fetching (Unpaywall, CrossRef, etc.)
6. ⏳ Process full bodies through OpenAI
7. ⏳ Compare results and calculate agreement metrics
8. ⏳ Document findings and make recommendations

## Open Questions

1. Should we stratify the sample by:
   - Journal?
   - Year?
   - Whether abstract analysis found terminology?

2. If agreement is poor, do we:
   - Switch to full-body analysis for all papers?
   - Use abstract as screening + full-body for positives?
   - Adjust the OpenAI prompt?

3. How do we handle articles where we can't obtain full text?
   - Exclude from analysis?
   - Mark as limitation?
   - Use partial text if available?

## References

- [Unpaywall API Documentation](https://unpaywall.org/products/api)
- [CrossRef Text Mining](https://www.crossref.org/services/text-and-data-mining/)
- OpenAI Batch API pricing and documentation
