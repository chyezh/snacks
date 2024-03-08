from datasets import load_dataset
import msg_pb2
import sys

# Check the number of arguments
if len(sys.argv) != 3:
    print("Usage: python script.py <argument>")
    sys.exit(1)

file = sys.argv[1]
output_filename = sys.argv[2]
docs = load_dataset(file, split="train", streaming=True)

with open(output_filename, 'wb', buffering=4096) as output_file:
    for doc in docs:
        msg = msg_pb2.Msg()
        msg.id = doc["id"]
        msg.title = doc["title"]
        msg.emb.extend(doc["emb"])
        msg.lang = doc["langs"]
        data = msg.SerializeToString()
        output_file.write(len(data).to_bytes(4, byteorder='big'))
        output_file.write(data)
