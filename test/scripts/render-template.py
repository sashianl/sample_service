import os
import sys
from pathlib import Path

import jinja2


def process_template(template_file, output_file):
    path = Path(template_file)
    directory = path.parent.absolute()
    filename = path.name
    loader = jinja2.FileSystemLoader(directory)
    template = jinja2.Environment(loader=loader).get_template(filename)
    with open(output_file, 'w') as fout:
        fout.write(template.render(env=os.environ))


def main():
    # read in the file
    if len(sys.argv) < 3:
        usage(f"invalid number of parameters; {len(sys.argv) -1} provided, 2 expected")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    process_template(input_file, output_file)


def usage(message):
    print(message)
    print("render-template.py input-file output-file")


if __name__ == "__main__":
    main()
