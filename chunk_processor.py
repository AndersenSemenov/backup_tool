import xxhash

from jump_based_chunking import gear_consume, get_chunks_boarders

chunk_fix_size = 10


class ChunkedFile:
    def __init__(self, file_path, file_name):
        with open(file_path) as f:
            self.file_name = file_name

            content = f.read()

            self.file_chunk_list = get_chunks_boarders(content)
            print(self.file_chunk_list)

            # calculate checksum for each chunk
            self.checksum_list = list(xxhash.xxh32(chunk).hexdigest() for chunk in self.file_chunk_list)
            print(self.checksum_list)


def split_string_into_chunks(string, length):
    return (string[0 + i:length + i] for i in range(0, len(string), length))
