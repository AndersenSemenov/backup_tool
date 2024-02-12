import xxhash

chunk_fix_size = 5


class ChunkedFile:
    def __init__(self, file_path):
        with open(file_path) as f:
            contents = f.read()

            self.chunk_list = list(split_string_into_chunks(contents, chunk_fix_size))
            print(self.chunk_list)

            self.chunk_name_list = list(file_path + '_' + i for i in range(0, len(self.chunk_list)))

            # calculate checksum for each chunk
            self.checksum_list = list(xxhash.xxh32(chunk).digest() for chunk in self.chunk_list)
            print(self.checksum_list)


def split_string_into_chunks(string, length):
    return (string[0 + i:length + i] for i in range(0, len(string), length))
