from datasets import load_dataset
import msg_pb2

docs = load_dataset("./wikipedia-22-12-en-embeddings", split="train", streaming=True)

file_counter = 1
lines_counter = 0
output_filename = f"output_part_{file_counter}.bin"
threshold=1000000

with open(output_filename, 'wb', buffering=4096) as output_file:
    for doc in docs:
        if lines_counter == threshold:
            file_counter += 1
            lines_counter = 0
            output_filename = f"output_part_{file_counter}.bin"
            output_file.close()
            output_file = open(output_filename, 'wb', buffering=4096)

        msg = msg_pb2.Msg()
        msg.id = doc["id"]
        msg.title = doc["title"]
        msg.emb.extend(doc["emb"])
        msg.lang = doc["langs"]
        data = msg.SerializeToString()
        output_file.write(len(data).to_bytes(4, byteorder='big'))
        output_file.write(data)
        lines_counter += 1
