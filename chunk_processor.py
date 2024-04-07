from zip_jump_based_chunking import init_chunks_and_checksums


class ChunkedFile:
    def __init__(self, file_path, file_name, tmp_dir):
        with open(file_path) as f:
            self.file_name = file_name

            content = f.read()

            self.checksum_list = init_chunks_and_checksums(content, file_name, tmp_dir)
