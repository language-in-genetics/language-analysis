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

## Extracting information

You have either run the program from scratch, or you are looking at the articles that were in the git repo in the `articles/` directory.

Follow the instructions in `extractor/README.md` to extract race terms out of the data.


