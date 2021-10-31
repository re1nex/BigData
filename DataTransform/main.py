import argparse
import re
import os
import json
import pandas as pd


class TaskTransformer:
    def __init__(self, task_name: str):
        self._task_name = task_name
        self.__titles = set()

    def consume(self, json_dict):
        if json_dict['media_type'] == 'MOVIE' and json_dict['title'] not in self.__titles:
            self._consume_impl(json_dict)
            self.__titles.add(json_dict['title'])

    def flush(self, target_dir):
        pass

    def which_task(self):
        return self._task_name

    def _consume_impl(self, json_dict):
        pass


class Task1(TaskTransformer):
    """
    Reviews analysis and correlation with rating
    """
    def __init__(self):
        super().__init__('task_1')
        self.__data = {
            'reviews': [],
            'rating': [],
            'film_title': [],
        }

    def _consume_impl(self, json_dict):
        self.__data['reviews'].append([review['content'] for review in json_dict['reviews']])
        self.__data['rating'].append(json_dict['vote_average'])
        self.__data['film_title'].append(json_dict['title'])

    def flush(self, target_dir):
        pd.DataFrame(self.__data).to_csv(os.path.join(target_dir, self._task_name + '.csv'), encoding='utf-8')


class Task2(TaskTransformer):
    """
    Predict popularity having actors, director and budget
    """
    def __init__(self):
        super().__init__('task_2')
        self.__data = {
            'popularity': [],
            'directors': [],
            'actors': [],
            'budget': [],
            'runtime': [],
            'film_title': [],
        }
        self.__prune = 100

    def _consume_impl(self, json_dict):
        self.__data['film_title'].append(json_dict['title'])
        self.__data['popularity'].append(json_dict['popularity'])
        self.__data['actors'].append([actor['name'] for actor in json_dict['cast'] if actor['order'] < self.__prune])
        self.__data['directors'].append([worker['name'] for worker in json_dict['crew'] if worker['job'] == 'Director'])
        self.__data['budget'].append(json_dict['budget'])
        self.__data['runtime'].append(json_dict['runtime'])

    def flush(self, target_dir):
        pd.DataFrame(self.__data).to_csv(os.path.join(target_dir, self._task_name + '.csv'), encoding='utf-8')


class Task5(TaskTransformer):
    """
    Popularity dependency on production company name & country
    """
    def __init__(self):
        super().__init__('task_5')
        self.__data = {
            'media_type': [],
            'production_countries': [],
            'production_companies': [],
            'popularity': [],
            'rating': [],
            'title': [],
        }

    def _consume_impl(self, json_dict):
        self.__data['media_type'].append(json_dict['media_type'])
        self.__data['production_countries'].append([country['name'] for country in json_dict['production_countries']])
        self.__data['production_companies'].append([company['name'] for company in json_dict['production_companies']])
        self.__data['popularity'].append(json_dict['popularity'])
        self.__data['rating'].append(json_dict['vote_average'])
        self.__data['title'].append(json_dict['title'])

    def flush(self, target_dir):
        pd.DataFrame(self.__data).to_csv(os.path.join(target_dir, self._task_name + '.csv'), encoding='utf-8')


def ensure_dir(transformed_data_dir):
    # Make the directory if it does not exist
    if not os.path.exists(transformed_data_dir):
        os.makedirs(transformed_data_dir)


def repair_json(in_str: str):
    # Firstly, need to connect dictionaries into a single list
    old_string, new_string = r'\n\}[\n$]', r'\n},\n'
    repaired = re.sub(old_string, new_string, in_str)
    # Remove the last comma
    comma_pos = len(repaired) - 1
    while repaired[comma_pos] != ',':
        comma_pos -= 1
    # Wrap into top-level dictionary
    repaired = r'{ "contents": [' + repaired[:comma_pos] + r']}'
    return json.loads(repaired)


class Reporter:
    def __init__(self, verbose: bool):
        self.__verbose = verbose

    def write(self, message: str):
        if self.__verbose:
            print(message)


def process_dir(raw_data_dir, transformed_data_dir, verbose):
    log = Reporter(verbose)
    ensure_dir(transformed_data_dir)
    file_ending = '.txt'  # ignore other files
    transformers = [Task1(), Task2(), Task5()]
    for file in os.listdir(raw_data_dir):
        if not file.endswith(file_ending):
            continue
        log.write(f'Processing file {file}')
        with open(os.path.join(raw_data_dir, file), 'r', encoding='utf-8') as handle:
            contents = handle.read()
        jsoned = repair_json(contents)
        for batch in jsoned['contents']:
            for transformer in transformers:
                transformer.consume(batch)
        log.write(f'File {file} has been processed')
    for transformer in transformers:
        transformer.flush(transformed_data_dir)
        log.write(f'Data for the task {transformer.which_task()} has been prepared')
    log.write(f'Finish')


def aloha(cmd_args):
    process_dir(cmd_args.src_dir, cmd_args.dest_dir, cmd_args.verbose)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find intersections of lines and polygon')
    parser.add_argument('--src_dir', '-i', default='raw_data', help='Source files directory')
    parser.add_argument('--dest_dir', '-o', default='transformed_data', help='Destination directory')
    parser.add_argument('--verbose', '-v', action='store_true', help='Write status in stdout')
    args = parser.parse_args()
    aloha(args)
