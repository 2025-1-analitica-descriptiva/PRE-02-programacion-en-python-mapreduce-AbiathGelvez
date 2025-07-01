"""Taller evaluable"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os.path
import time
from itertools import groupby
import shutil
import re


def copy_raw_files_to_input_folder(n):
    """Funcion copy_files"""
    raw_files = glob.glob("files/raw/*.txt")
    os.makedirs("files/input", exist_ok=True)
    for file in raw_files:
        base = os.path.basename(file)
        name, ext = os.path.splitext(base)
        for i in range(1, n + 1):
            new_name = f"{name}_{i}{ext}"
            shutil.copy(file, f"files/input/{new_name}")


def load_input(input_directory):
    """Funcion load_input"""
    result = []
    for filepath in glob.glob(f"{input_directory}/*.txt"):
        with open(filepath, encoding="utf-8") as f:
            filename = os.path.basename(filepath)
            for line in f:
                result.append((filename, line.strip()))
    return result


def line_preprocessing(sequence):
    """Line Preprocessing"""
    result = []
    for filename, line in sequence:
        cleaned = re.sub(r"[^\w\s]", "", line).lower()
        words = cleaned.split()
        result.extend([(word, 1) for word in words])
    return result


def mapper(sequence):
    """Mapper"""
    return sequence


def shuffle_and_sort(sequence):
    """Shuffle and Sort"""
    return sorted(sequence, key=lambda x: x[0])


def reducer(sequence):
    """Reducer"""
    result = []
    for key, group in groupby(sequence, key=lambda x: x[0]):
        total = sum(count for _, count in group)
        result.append((key, total))
    return result


def create_ouptput_directory(output_directory):
    """Create Output Directory"""
    if os.path.exists(output_directory):
        shutil.rmtree(output_directory)
    os.makedirs(output_directory)


def save_output(output_directory, sequence):
    """Save Output"""
    with open(os.path.join(output_directory, "part-00000"), "w", encoding="utf-8") as f:
        for key, value in sequence:
            f.write(f"{key}\t{value}\n")


def create_marker(output_directory):
    """Create Marker"""
    open(os.path.join(output_directory, "_SUCCESS"), "w").close()


def run_job(input_directory, output_directory):
    """Job"""
    create_ouptput_directory(output_directory)
    data = load_input(input_directory)
    preprocessed = line_preprocessing(data)
    mapped = mapper(preprocessed)
    sorted_data = shuffle_and_sort(mapped)
    reduced = reducer(sorted_data)
    save_output(output_directory, reduced)
    create_marker(output_directory)


if __name__ == "__main__":
    copy_raw_files_to_input_folder(n=1000)

    start_time = time.time()

    run_job(
        "files/input",
        "files/output",
    )

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")
