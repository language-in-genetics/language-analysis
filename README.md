# Word-Frequency-Analysis-

So far, this is just a program to extract data from the CrossRef database.


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


## Database

Long-term, it's better for this to be in a database. Compile and run `pgjsontool`.

Then run `database/indexing.sql` 
