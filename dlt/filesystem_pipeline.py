import dlt
from dlt.sources.filesystem import filesystem

def load_csv_file():

    # Define the source as a local CSV file
    source = filesystem().with_resources("file:///path/to/your/file.csv")

    # Create a pipeline that loads data into DuckDB
    pipeline = dlt.pipeline(
        pipeline_name="csv_to_duckdb_pipeline",
        destination="duckdb",
        dataset_name="mydata"
    )

    # Run the pipeline
    load_info = pipeline.run(source, table_name="motherduck")

    print(load_info)

if __name__ == '__main__':
    load_csv_file()