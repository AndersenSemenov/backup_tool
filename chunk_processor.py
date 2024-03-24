import xxhash

from jump_based_chunking import get_chunks_boarders


class ChunkedFile:
    def __init__(self, file_path, file_name):
        with open(file_path) as f:
            self.file_name = file_name

            content = f.read()

            self.file_chunk_list = get_chunks_boarders(content)
            print(f"Len of chunk list is {len(self.file_chunk_list)}")

            # calculate checksum for each chunk
            self.checksum_list = list(xxhash.xxh32(chunk).hexdigest() for chunk in self.file_chunk_list)
            print(self.checksum_list)
