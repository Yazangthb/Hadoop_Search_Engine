# app/prepare_data.py

import os
import sys

def convert_file(input_path, output_path):
    filename = os.path.basename(input_path)
    doc_id = filename.replace('.txt', '')
    title = doc_id.replace("_", " ")
    
    with open(input_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read().replace('\n', ' ')
    
    with open(output_path, 'w', encoding='utf-8') as out:
        out.write(f"{doc_id}\t{title}\t{content}\n")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python prepare_data.py <input.txt> <output.txt>")
        sys.exit(1)
    
    convert_file(sys.argv[1], sys.argv[2])
