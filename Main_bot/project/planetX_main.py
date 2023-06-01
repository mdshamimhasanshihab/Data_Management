import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from cleaner import data_clean
from snowflake_data_pull import PlanetSnowflake
schema = pa.schema([
    pa.field('id', pa.string()),
    pa.field('product_des', pa.list_(pa.struct([
        pa.field('unit', pa.string()),
        pa.field('value', pa.string()),
        pa.field('dimentionality', pa.int64())
        ])))
])


# dataset_path = r"C:\Users\User\PycharmProjects\pythonProject3\Planet_X\Phase_1\Final\project\output.parquet"
dataset_path=r"output.parquet"
existing_dataset = pq.ParquetDataset(dataset_path).read().to_pandas()
if __name__ == "__main__":
    for i in range(2000000000000):
        with PlanetSnowflake() as ps:
            start_offset = i  # Start offset for the range
            fetch_size = 250  # Number of rows to fetch

            sql = f"SELECT * FROM analytics.amazon.amazon_attributes OFFSET {start_offset*fetch_size} ROWS FETCH NEXT {fetch_size} ROWS ONLY"
            df = ps.fetch_table_as_df(sql)
            for index, row in df.head(1).iterrows():
                x,y=data_clean(index,row)
                new_row = {'id': x, 'product_des': y}
                new_row_df = pd.DataFrame([new_row])
                existing_dataset = pd.concat([existing_dataset, new_row_df], ignore_index=True)
                # if i==5:
                #     deduplicated_data = existing_dataset.drop_duplicates(subset='id')
                # else:
                    # deduplicated_data=existing_dataset
                deduplicated_data = existing_dataset.drop_duplicates(subset='id')
                table = pa.Table.from_pandas(deduplicated_data, schema=schema)
                # Specify the output file path
                output_file = r"output.parquet"
                # Save the PyArrow table to Parquet format
                pq.write_table(table, output_file)
print(deduplicated_data)