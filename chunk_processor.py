from zip_jump_based_chunking import get_chunks_boarders


class ChunkedFile:
    def __init__(self, file_path, file_name, tmp_dir):
        with open(file_path) as f:
            self.file_name = file_name

            content = f.read()

            # self.checksum_list =\
            get_chunks_boarders(content, file_name, tmp_dir)
