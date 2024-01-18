import pyarrow.parquet as pq
import msg_pb2
import sys

# Check the number of arguments
if len(sys.argv) != 3:
    print("Usage: python script.py <argument>")
    sys.exit(1)


def print_column_row_by_row(parquet_file_path, output_filename):
    # Open the Parquet file for reading
    parquet_file = pq.ParquetFile(parquet_file_path)

    # Get the specified column
    colls = parquet_file.read(columns=["id", "title", "langs", "emb"])
    df = colls.to_pandas()

    # Iterate through each row and print the value of the column
    with open(output_filename, 'wb', buffering=4096) as output_file:
        for _, row in df.iterrows():
            msg = msg_pb2.Msg()
            msg.id = row['id']
            msg.title = row['title']
            msg.lang = row['langs']
            msg.emb.extend(row['emb'])
            data = msg.SerializeToString()
            output_file.write(len(data).to_bytes(4, byteorder='big'))
            output_file.write(data)
    # Close the Parquet file
    parquet_file.close()

print_column_row_by_row(sys.argv[1], sys.argv[2])