def generate_dictionary(filename):
    with open(filename, 'r'):
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
