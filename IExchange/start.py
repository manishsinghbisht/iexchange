from root.__main__ import main
from version import __version__
import argparse

# main
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    version =__version__
    main(args, version)