import re

__all__ = ['transform_fs_safe']

# transform file names to be safe to be used when creating a unix directory / file
def transform_fs_safe(file_name: str) -> str:
    # replace all non alphanumeric characters with _
    file_name = re.sub(r'[^a-zA-Z0-9\-_() ]', '_', file_name)

    # replace multiple _ with single _
    file_name = re.sub(r'[_]+', '_', file_name)

    # replace leading and trailing _
    file_name = re.sub(r'^[_]+|[_]+$', '', file_name)

    # check length
    if len(file_name) > 255:
        file_name = file_name[:255]

    return file_name