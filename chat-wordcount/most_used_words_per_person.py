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

#with beam.Pipeline(beam.PipelineOptions()) as p:

#    most_used_words_per_person = (p
#                                  | 'ReadChatFile' >> beam.io.Read(ChatFileSource(filepath))
#                                  | 'GroupMsgPerPerson' >> beam.GroupByKey()
#                                  | 'JoinMsgStrings' >> beam.Map(lambda x: (x[0], ' '.join(x[1])))
#                                  | 'SplitMsgStringToWords' >> beam.FlatMap(string_to_words)
#                                  | 'MakeWordOnePairs' >> beam.FlatMap(lambda x: (x[0], ()))
#                                  | 'CountWordsPerPerson' >> beam.FlatMap(count_words_in_msg))

# TODO: Think of a better way to represent each sender, set of words.
# Eg: ((sender, word), count) may be an option. Using this, it is easy
# to count occurences per (sender, word) which is intended as we need
# Count of words per person. So, (sender, word) pair may be a logical key.

with beam.Pipeline(beam.PipelineOptions()) as p:
    most_used_words_per_person = (p
                                  | 'ReadChatFile' >> beam.io.Read(ChatFileSource(filepath))
                                  | 'SplitWordsPerLine' >> beam.ParDo(split_words)
                                  | 'MakeWordOnePairs' >> beam.ParDo(lambda x: ((x), 1))
                                  | 'GroupByWordSenderPairs' >> beam.GroupByKey()
                                  | 'CountWordSender' >> beam.Map(lambda x: (x[0], sum(x[1]))))

class ChatFileSource(beam.io.filebasedsource.FileBasedSource):
    """From the raw text, add transforms to seperate the strings by person"""

    RE_TIMESTAMP = r'[\d]+/[\d]+/[\d]+, [\d]+:[\d]+ -'
    RE_SENDER = r'[\D]+:'
    RE_MSG = r'[\w\']+'

    RE_SINGLE_MSG_LINE = RE_TIMESTAMP + RE_SENDER + RE_MSG
    RE_CONTINUED_MSG = RE_MSG
    RE_SYSTEM_MSG = RE_TIMESTAMP + RE_MSG

    def parse_message(self, line):
        sender = ''
        for line in file:
            if re.match(RE_SINGLE_MSG, line):
                sender, msg = map(str.strip, line.split('-', 1)[1].split(':', 1))
            elif re.match(RE_CONTINUED_MSG, line):
                msg = line
            elif re.match(RE_SYSTEM_MSG, line):
                _, msg = map(str.strip, line.split('-', 1))
                sender = 'SYSTEM'
            yield (sender, msg)

    def read_records(self, file_name, range_tracker):
        with open(file_name, 'r') as file:
            return parse_message(file)


def split_words(element):
    sender = element[0]
    msg = element[1]
    words = re.findall(r'[\w\']+', msg)
    return [(sender, word) for word in words]


# REMOVE
def string_to_words(element):
    string = element[1]
    words = re.findall(r'[\w\']+', string)
    return (element[0], words)


# REMOVE
def make_word_one_pairs(element):
    words = element[1]
    # TODO: Is it more efficient if the map is done
    # with another Beam PTransform? Since each user
    # will have loads of words.
    word_one_pairs = map(words, lambda x: (x, 1))
    return (element[0], word_one_pairs)
