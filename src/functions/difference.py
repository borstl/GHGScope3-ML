"""
Collection of functions to clean up the data frames
"""


def remove_empty_columns(input_file, output_file, columns_file):
    """Removes all columns listed in a file from the input and saves it in a new file"""
    empty_columns = set()
    every_columns = set()

    with open(columns_file, encoding="utf-8") as f:
        for line in f:
            empty_columns.add(line.strip())

    with open(input_file, encoding="utf-8") as f:
        for line in f:
            every_columns.add(line.strip())

    diff = empty_columns.difference(every_columns)

    with open(output_file, 'w', encoding="utf-8") as f:
        f.write("\n".join(diff))
