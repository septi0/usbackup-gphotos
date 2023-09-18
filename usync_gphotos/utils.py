import re
from datetime import timedelta

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

def gen_batch_stats(t_start: float, t_end: float, processed: int, total: int) -> tuple:
    # calc percentage completed, add % sign
    percentage = round(processed / total * 100, 2) if total > 0 else 0
    # format percentage
    percentage_str = f'{percentage}%'

    # calc estimated time left
    elapsed = (t_end - t_start).total_seconds()
    eta = int(round((elapsed / processed) * (total - processed), 2))
    # format eta
    eta_str = str(timedelta(seconds=eta))

    return (percentage_str, eta_str)