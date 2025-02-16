# Background

Sally Way's research wanted to know how often "white" race words were used.

Rather than walking through 35,000 articles by hand, we delegate this to OpenAI.

The prompt is:


```
prompt = "Does this article use any terms like \"Caucasian\" or \"white\" or \"European ancestry\" in a way that refers to race, ancestry, ethnicity or population?\n\n"
    prompt += f"TITLE: {title}\n"
    if abstract:
        prompt += f"ABSTRACT: {abstract}\n"
```


It is forced to provide a tool-calling answer, for this tool:

```
tools = [{
    "type": "function",
    "function": {
        "name": "analyze_text",
        "description": "Analyze text for racial/ethnic terminology",
        "parameters": {
            "type": "object",
            "properties": {
                "caucasian": {
                    "type": "boolean",
                    "description": "uses the word Caucasian, or similar"
                },
                "white": {
                    "type": "boolean",
                    "description": "uses the word 'white' to refer to race, ancestry, ethnicity, population or equivalent"
                },
                "european": {
                    "type": "boolean",
                    "description": "uses a phrase like 'European ancestry'"
                },
                "european_phrase_used": {
                    "type": "string",
                    "description": "the actual phrase used if european is true, blank otherwise"
                },
                "other": {
                    "type": "boolean",
                    "description": "uses some other phrase to describe someone with European/Caucasian/white ancestry, race, ethnicity or population"
                },
                "other_phrase_used": {
                    "type": "string",
                    "description": "what phrase was used if 'other' is true, blank otherwise"
                }
            },
            "required": ["caucasian", "white", "european", "european_phrase_used", "other", "other_phrase_used"]
        }
    }
}]
```



# How to use it

It requires the Python OpenAI library. Create a virtualenv if you haven't already, and install dependencies.

`virtualenv ../.venv`

`. ../.venv/bin/activate`

`pip install -r ../requirements.txt`

Then you should be able to run

`./bulkquery.py --limit 1000 --database openai-batch.sqlite`

If you want to add `--batch-id-save-file` you can, which might make it easier to monitor.

`./batchcheck.py --database openai-batch.sqlite`

Or, you can use `--monitor` and `--batch-id`. That will let you watch how quickly it processes them.

Finally, run

`./batchfetch.py --database openai-batch.sqlite`

Repeat that process as many times as required.

Everything should now be in the `openai-batch.sqlite` database.

Now run:

`sqlite3 openai-batch.sqlite -header -csv "SELECT * FROM files;" > ancestry-full.csv`



