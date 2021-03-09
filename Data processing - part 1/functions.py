import pandas as pd
import json
import os
import re


# Slice large file to plenty of smaller ones

def slicer(input_path_to_file, output_file_path, output_lines):
    out = []
    i = 0
    with open(input_path_to_file) as file:
        for line in file:
            out.append(line)
            if len(out) > output_lines:
                print(i)
                with open(output_file_path.format(i), mode='a') as file2:
                    file2.write(json.dumps(out))
                    i += 1
                    out = []
        if len(out) != 0:
            with open(output_file_path.format(i), mode='a') as file2:
                file2.write(json.dumps(out))


def json_to_csv(file_name):
    print('\t{}'.format(file_name))
    # Process data
    # Please notice that we using same name of variable to decrease memory usage

    with open(file_name) as f:
        file = json.load(f)

    # Unpack json rows
    file = [json.loads(x) for x in file]

    # Create data frame
    file = pd.DataFrame(file)

    # Remove unnecessary columns
    file = file.filter(['overall', 'reviewText'])

    # Rename columns
    file = file.rename(columns={'overall': 'sentiment', 'reviewText': 'text'})

    # Remove noises
    # Make all to string
    file['text'] = file['text'].apply(str)

    # We can remove some duplicates also at this point
    file = file.drop_duplicates()

    # Declare the pattern s
    patterns = [
        r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)",
        r"[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)",
        r"(@[A-Za-z0-9_]+)",
        r"(#[A-Za-z0-9_]+)"]

    # Remove patterns from the sentences
    for pattern in patterns:
        file['text'] = file['text'].apply(lambda x: re.sub(pattern, '', str(x)))

    file.text = file.text.apply(lambda x: x if 0 < len(str(x).split()) < 100 else None)

    # Transform 0-5 score, into positive/negative values

    file = file[file.sentiment != 3]
    file.sentiment = file.sentiment.apply(lambda x: 'positive' if x > 3 else 'negative')

    # Remove nones
    file = file.dropna()

    # Write into csv file
    file.to_csv(file_name, mode='w', index=False)


def duplicate_remover(input_file_name,
                      output_file_name_neg,
                      output_file_name_pos,
                      files_in_folder,
                      auto_remove_files=False):
    # Drop duplicates
    list_files_1 = [x for x in range(files_in_folder)]
    list_files_2 = [x for x in range(files_in_folder)]

    for i in list_files_1:
        print('duplicate_remover {}'.format(i))
        list_files_2.remove(i)
        df1 = pd.read_csv(input_file_name.format(i))
        # Remove duplicated rows in data frame
        df1 = df1.drop_duplicates()

        # Prepare positive list of sentences and filter them
        pos1 = set(df1[df1.sentiment == 'positive']['text'])
        pos1 = set(filter(lambda x: type(x) == str, pos1))

        # Prepare negative list of sentences and filter them
        neg1 = set(df1[df1.sentiment == 'negative']['text'])
        neg1 = set(filter(lambda x: type(x) == str, neg1))

        for i2 in list_files_2:
            print('\t{}'.format(i2))
            df2 = pd.read_csv(input_file_name.format(i2))
            df2 = df2.drop_duplicates()
            pos2 = set(df2[df2.sentiment == 'positive']['text'])
            neg2 = set(df2[df2.sentiment == 'negative']['text'])

            # Remove duplicates sentences
            pos1 = pos1 - pos2
            neg1 = neg1 - neg2

        # If you would like to don't use separate files use this lines below
        # pos1 = [{'sentiment': 'positive', 'text': x} for x in pos1]
        # neg1 = [{'sentiment': 'negative', 'text': x} for x in neg1]
        # df1 = pd.DataFrame(pos1 + neg1)

        df1 = pd.DataFrame(list(pos1))
        df2 = pd.DataFrame(list(neg1))
        df1.to_csv(output_file_name_pos.format(i), mode='a', index=False)
        df2.to_csv(output_file_name_neg.format(i), mode='a', index=False)
        if auto_remove_files:
            os.remove(input_file_name.format(i))


def count_files_in_dir(input_file_path_with_name):
    counted_files_in_folder = [x for x in os.listdir(os.path.split(input_file_path_with_name)[0]) if
                               os.path.split(input_file_path_with_name)[-1].replace("{", '').replace("}", '') in x
                               and os.path.split(input_file_path_with_name)[-1].replace("{", '').replace("}", '') ==
                               re.sub(r'\d', '', x)].__len__()

    return counted_files_in_folder


def count_rows_in_files(input_file, files: int):
    out = []
    out_sum = 0
    for i in range(files):
        df = pd.read_csv(input_file.format(i))
        out.append(df.__len__())
    for elem in out:
        out_sum += elem
    return out_sum
