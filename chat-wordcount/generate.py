import re
import pprint
import time

def generate_dictionary(filename):
    with open(filename, 'r') as file:
        dictionary = {}
        pattern = r'[\d]+/[\d]+/[\d]+, [\d]+:[\d]+ - '
        current_message = ''
        for line in file:
            line = line.strip()
            if re.match(pattern, line):
                if current_message:
                    t = re.split(pattern, current_message)[1].split(':')
                    if len(t) > 1:
                        if t[0] not in dictionary.keys():
                            dictionary[t[0]] = [t[1:]]
                        else:
                            dictionary[t[0]].append(t[1:])
                current_message = line
            else:
                current_message = current_message + ' ' + line

        for key in dictionary.keys():
            dictionary[key] = ' '.join(dictionary[key])
    return dictionary

def parse_message(file):
    RE_TIMESTAMP = r'[\d]+/[\d]+/[\d]+, [\d]+:[\d]+'
    RE_SENDER = r'[\D]+'
    RE_MSG = r'[\D]+'

    RE_SINGLE_MSG = RE_TIMESTAMP + r' - ' + RE_SENDER + r': ' + RE_MSG
    RE_CONTINUED_MSG = RE_MSG
    RE_SYSTEM_MSG = RE_TIMESTAMP + r' - ' + RE_MSG

    sender = ''
    for line in file:
        if re.match(RE_SINGLE_MSG, line):
            sender, msg = map(str.strip, line.split('-', 1)[1].split(':', 1))
        elif re.match(RE_CONTINUED_MSG, line):
            msg = line
        elif re.match(RE_SYSTEM_MSG, line):
            _, msg = map(str.strip, line.split('-', 1))
            sender = 'SYSTEM'
        print(line)
        yield (sender, msg)

def dummy(file):
    return parse_message(file)

if __name__ == '__main__':
    #pprint.pprint(generate_dictionary('../WhatsApp_Chat_with_Yashaswini.txt'))
    file = open('../../WhatsApp_Chat_with_Yashaswini.txt', 'r')
    for message in dummy(file):
        print(message)
        print('\n\n')
        time.sleep(1)