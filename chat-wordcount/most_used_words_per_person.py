"""
Input to this pipeline will be a raw file that has the chats of multiple people

Stage 1(Generate long string per person from raw file):
======================================================
For each line in the raw_file, join all the lines to produce a single string per person

Output:
    {
        "<Person1>": "some long string",
        ...
    }


Stage 2(Get word count of each word in the string):
=================================================
For each person's string of words, get the count of each word.

Output:
    {
        "<Person1>": [(word, count), ..],
        ...
    }

Stage 3(Sort the word-count list per person by the count of the words)
======================================================================
Simple sort the word count list by the count of the tuple

Output:
    {
        "<Person1>": sorted([(word, count), ...]),
        ...
    }
"""

import apache_beam as beam

with beam.Pipeline(beam.PipelineOoptions()) as p:

    most_used_words_per_person = (p
                                  | 'ReadChatFile' >> beam.ReadChatFile(filepath)
                                  | 'CountWordsPerPerson' >> CountWordsPerPerson()
                                  | 'SortWordCount' >> SortWordCount())


class ReadChatFile(beam.io.filebasedsource.FileBasedSource):
    """From the raw text, add transforms to seperate the strings by person"""
    # Add the parsing logic here while reading lines from file
    # Reference: https://stackoverflow.com/questions/41170997/how-to-convert-csv-into-a-dictionary-in-apache-beam-dataflow/41171867#41171867
    pass


@beam.ptransform_fn
def CountWordsPerPerson(pcoll):
    """Count the words used be each person"""
    pass


@beam.ptransform_fn
def SortWordCount(pcoll):
    """Sort the word-counts of each peron by the count in ascending order"""
    pass
