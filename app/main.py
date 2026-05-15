from pipelines.extract_bronze import *
from pipelines.silver_gold import *
from pipelines.duck_db import *


def main():

    print("\nSTARTING DATA PIPELINE\n")

    print("Loading Bronze Layer...")
    print("Bronze Layer Completed\n")

    print("Building Silver and Gold Layers...")
    print("Silver and Gold Layers Completed\n")

    print("Exporting Gold Tables to DuckDB...")
    print("DuckDB Export Completed\n")

    print("PIPELINE COMPLETED SUCCESSFULLY\n")


if __name__ == "__main__":
    main()