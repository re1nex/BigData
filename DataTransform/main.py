import argparse
import re
import os
import json
import pandas as pd


class Reporter:
    def __init__(self, verbose: bool):
        self.__verbose = verbose

    def write(self, message: str):
        if self.__verbose:
            print(message)


def unused(some):
    return some  # Kill IDE warnings


class TaskTransformer:
    def __init__(self, task_name: str, for_movies_only: bool, title_key: str = 'title'):
        self._task_name = task_name
        self.__titles = set()
        self.__key = title_key
        self.__movies = for_movies_only

    def consume(self, json_dict, meta):
        if (not self.__movies or json_dict['media_type'] == 'MOVIE') and json_dict[self.__key] not in self.__titles:
            self._consume_impl(json_dict, meta)
            self.__titles.add(json_dict[self.__key])

    def flush(self, target_dir):
        pass

    def which_task(self):
        return self._task_name

    def _consume_impl(self, json_dict, meta):
        pass


class Task1(TaskTransformer):
    """
    Reviews analysis and correlation with rating
    """

    def __init__(self):
        super().__init__('task_1', True)
        self.__data = {
            'reviews': [],
            'rating': [],
            'film_title': [],
        }

    def _consume_impl(self, json_dict, meta):
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
        super().__init__('task_2', True)
        self.__data = {
            'popularity': [],
            'directors': [],
            'actors': [],
            'budget': [],
            'runtime': [],
            'film_title': [],
        }
        self.__prune = 100

    def _consume_impl(self, json_dict, meta):
        self.__data['film_title'].append(json_dict['title'])
        self.__data['popularity'].append(json_dict['popularity'])
        self.__data['actors'].append([actor['name'] for actor in json_dict['cast'] if actor['order'] < self.__prune])
        self.__data['directors'].append([worker['name'] for worker in json_dict['crew'] if worker['job'] == 'Director'])
        self.__data['budget'].append(json_dict['budget'])
        self.__data['runtime'].append(json_dict['runtime'])

    def flush(self, target_dir):
        pd.DataFrame(self.__data).to_csv(os.path.join(target_dir, self._task_name + '.csv'), encoding='utf-8')


class Task3(TaskTransformer):
    """
    Find the most popular genre for each region
    """

    def __init__(self):
        super().__init__('task_3', False)
        self.__data = {
            'region': [],
            'popularity': [],
            'media_type': [],
            'title': [],
            'genres': [],
        }

    def _consume_impl(self, json_dict, meta):
        self.__data['title'].append(json_dict['title'])
        self.__data['region'].append(meta['region'])
        self.__data['popularity'].append(json_dict['popularity'])
        self.__data['media_type'].append(json_dict['media_type'])
        self.__data['genres'].append([genre for genre in json_dict['genre_ids']])

    def flush(self, target_dir):
        pd.DataFrame(self.__data).to_csv(os.path.join(target_dir, self._task_name + '.csv'), encoding='utf-8')


class Task4(TaskTransformer):
    """
    TV show season count dependency on crew, cast and some other features
    """

    def __init__(self):
        super().__init__('task_4', False, 'name')
        self.__data = {
            'title': [],
            'seasons': [],
            'actors': [],
            'created_by': [],
            'status': [],
            'popularity': [],
            'rating': [],
        }
        self.__actors_prune = 20

    def _consume_impl(self, json_dict, meta):
        if json_dict['number_of_seasons'] is not None and json_dict['number_of_seasons'] >= 1:
            self.__data['title'].append(json_dict['name'])
            self.__data['seasons'].append(json_dict['number_of_seasons'])
            cast = [actor['name'] for actor in json_dict['credits']['cast'] if actor['order'] < self.__actors_prune]
            self.__data['actors'].append(cast)
            self.__data['created_by'].append([creator['name'] for creator in json_dict['created_by']])
            self.__data['status'].append(json_dict['status'])
            self.__data['popularity'].append(json_dict['popularity'])
            self.__data['rating'].append(json_dict['vote_average'])

    def flush(self, target_dir):
        pd.DataFrame(self.__data).to_csv(os.path.join(target_dir, self._task_name + '.csv'), encoding='utf-8')


class Task5(TaskTransformer):
    """
    Popularity dependency on production company name & country
    """

    def __init__(self):
        super().__init__('task_5', True)
        self.__data = {
            'media_type': [],
            'production_countries': [],
            'production_companies': [],
            'popularity': [],
            'rating': [],
            'title': [],
        }

    def _consume_impl(self, json_dict, meta):
        self.__data['media_type'].append(json_dict['media_type'])
        self.__data['production_countries'].append([country['name'] for country in json_dict['production_countries']])
        self.__data['production_companies'].append([company['name'] for company in json_dict['production_companies']])
        self.__data['popularity'].append(json_dict['popularity'])
        self.__data['rating'].append(json_dict['vote_average'])
        self.__data['title'].append(json_dict['title'])

    def flush(self, target_dir):
        pd.DataFrame(self.__data).to_csv(os.path.join(target_dir, self._task_name + '.csv'), encoding='utf-8')


class TaskGroupTransformer:
    def __init__(self, raw_data_subdir: str, task_group_name: str, log: Reporter):
        self._raw_data_subdir = raw_data_subdir
        self._log = log
        self._file_ending = '.txt'
        self._task_group_name = task_group_name

    def _get_transformers_list(self):
        unused(self)  # virtual method
        return []

    def _extract_meta(self, file):
        unused(self)  # virtual method
        return {}

    def process(self, raw_data_dir: str, transformed_data_dir: str):
        ensure_dir(transformed_data_dir)
        transformers = self._get_transformers_list()
        subdir_path = os.path.join(raw_data_dir, self._raw_data_subdir)
        for file in os.listdir(subdir_path):
            if not file.endswith(self._file_ending):
                continue
            self._log.write(f'Processing file {file}')
            with open(os.path.join(subdir_path, file), 'r', encoding='utf-8') as handle:
                contents = handle.read()
            jsoned = repair_json(contents)
            for batch in jsoned['contents']:
                for transformer in transformers:
                    transformer.consume(batch, self._extract_meta(file))
            self._log.write(f'File {file} has been processed')
        for transformer in transformers:
            transformer.flush(transformed_data_dir)
            self._log.write(f'Data for the task {transformer.which_task()} has been prepared')
        self._log.write(f'Finish data preparations for task group {self._task_group_name}')


class MovieTasksTransformer(TaskGroupTransformer):
    def __init__(self, log: Reporter):
        super().__init__('tasks_1_2_5', 'movies', log)

    def _get_transformers_list(self):
        unused(self)
        return [Task1(), Task2(), Task5()]


class RegionTasksTransformer(TaskGroupTransformer):
    def __init__(self, log: Reporter):
        super().__init__('task_3', 'regions', log)

    def _get_transformers_list(self):
        unused(self)
        return [Task3()]

    def _extract_meta(self, file):
        unused(self)
        extension = len('.txt')
        return {'region': file[:-extension]}  # file name without extension == region


class ShowTasksTransformer(TaskGroupTransformer):
    def __init__(self, log: Reporter):
        super().__init__('task_4', 'tv_shows', log)

    def _get_transformers_list(self):
        unused(self)
        return [Task4()]


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


def process_task_group(task_group: TaskGroupTransformer, raw_data_dir: str, transformed_data_dir: str):
    try:
        task_group.process(raw_data_dir, transformed_data_dir)
    except RuntimeError as err:
        print(f'Task group {task_group.__class__} failed: {err}')


def run_task_transformers(cmd_args):
    log = Reporter(cmd_args.verbose)
    task_groups = [
        MovieTasksTransformer(log),
        RegionTasksTransformer(log),
        ShowTasksTransformer(log)
    ]
    for task_group in task_groups:
        process_task_group(task_group, cmd_args.src_dir, cmd_args.dest_dir)


def aloha(cmd_args):
    run_task_transformers(cmd_args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find intersections of lines and polygon')
    parser.add_argument('--src_dir', '-i', default='raw_data', help='Source files directory')
    parser.add_argument('--dest_dir', '-o', default='transformed_data', help='Destination directory')
    parser.add_argument('--verbose', '-v', action='store_true', help='Write status in stdout')
    args = parser.parse_args()
    aloha(args)
