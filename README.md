# Word-Frequency-Analysis-

So far, this is just a program to extract data from the CrossRef database.

In the `database` directory, there's an alternate way of processing/manipulating the data.

## Usage (full details)

Skip this if you just want the JSON data.

1. Download the latest CrossRef database from https://academictorrents.com/browse.php?search=Crossref
(You will need a torrent client to do this)

2. Compile the `jsonreader` program. You will need a `golang` compiler to do this. It has only
been tested on Linux, where it was run from `make`

3. Run `./bin/jsonreader -dir "the cross ref db dir" -output "articles"`

4. You will end up with a huge number of files in the `articles` directory and subdirectories.
Run `find articles -type f -exec git add '{}' ';'`

5. Remember to `git commit` and `git push`

## We now have the JSON data

Next stop: fetching the PDFs for each of those DOIs.

## Computational Analysis Methodology

The `extractor/bulkquery.py` script implements an automated approach to analyze genetics articles for specific racial terminology. It processes metadata.json files containing article titles and abstracts, then submits them to OpenAI's API for analysis.

The core of the analysis uses a carefully constructed prompt:
```
"Does this article use any terms like \"Caucasian\" or \"white\" or \"European ancestry\" in a way that refers to race, ancestry, ethnicity or population?\n\n"
"TITLE: {title}\n"
"ABSTRACT: {abstract}\n"
```

This prompt is deliberately framed in a neutral manner to avoid biasing the language model's analysis. It specifically asks about terms related to European ancestry without suggesting preference for any particular terminology.

The analysis is structured through a function-calling API that forces OpenAI to return standardized responses across all articles. The analysis function includes parameters for detecting:
- "caucasian" terminology
- "white" racial descriptors
- "European ancestry" phrasing
- Other phrases describing European populations

When phrases are detected, the system also captures the exact terminology used, enabling detailed analysis of language variations across the literature.

The batch processing system allows efficient processing of thousands of articles with proper error handling and progress tracking, making large-scale analysis feasible within reasonable time and cost constraints.


