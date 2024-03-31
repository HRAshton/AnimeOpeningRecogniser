import zipfile
from typing import Dict, List, Tuple

import numpy as np

from config import RATE


def main():
    archive_path = r'D:\AOR\artifacts\correlations.zip'
    archive = zipfile.ZipFile(archive_path, 'r')
    correlations = load_archive(archive)
    results = []
    i = 0
    for series_id, data in correlations.items():
        i += 1
        print(f'Processing series {series_id} ({i}/{len(correlations)})')
        offsets = find_and_group_offsets_by_series(data)
        true_offsets = find_true_offsets(offsets)
        fixed_offsets = fix_offsets(true_offsets)
        for file, offsets in fixed_offsets.items():
            results.append((series_id, file, offsets[0], offsets[1]))
    csv = '\n'.join([f'{series_id},{file.replace(".wav", "")},{offset1:.1f},{offset2:.1f}'
                     for series_id, file, offset1, offset2 in results])
    with open(r'D:\AOR\artifacts\offsets.csv', 'w') as f:
        f.write(csv)


def load_archive(archive) -> Dict[int, List[Tuple[str, str, float, float, np.ndarray]]]:
    results = {}
    i = 0
    for file in archive.namelist():
        if not file.endswith('.csv'):
            continue
        i += 1
        if i % 1000 == 0:
            print(f'Loading file {i} ({file})')
        _, series_id, filename = file.split('/')
        file1, file2, offset1, offset2 = filename.replace('.csv', '').split('_')
        series_id = int(series_id)
        offset1 = float(offset1) / 1000
        offset2 = float(offset2) / 1000
        corr = np.loadtxt(archive.open(file), delimiter=',')
        results.setdefault(series_id, []).append((file1, file2, offset1, offset2, corr))
    return results


def find_and_group_offsets_by_series(data):
    offsets = {}
    for file1, file2, offset1, offset2, corr in data:
        # print(file1, file2, offset1, offset2, corr.shape)

        corr_values = corr[:, 1]
        max_limit = np.mean(corr_values) + 3 * np.std(corr_values)
        filtered = corr_values[corr_values < max_limit]

        if np.mean(filtered) < np.median(filtered) * 2:
            # print(f'Not enough correlation: {file1}, {file2}. An opening not found. Skipping.')
            continue

        if filtered.shape[0] == 0:
            print(f'Fragments are the same: {file1}, {file2}. An opening not found. Skipping.')
            continue

        filtered_max = np.max(filtered)
        threshold = filtered_max / 2
        begin_idx = np.argmax(corr_values > threshold)
        begin_idx = begin_idx if begin_idx > 3 else 0

        end_idx = begin_idx
        bad_count = 0
        for i in range(begin_idx, len(corr_values)):
            if corr_values[i] > threshold:
                end_idx = i
            else:
                bad_count += 1
                if bad_count > 30:
                    break

        offset_begin = corr[begin_idx, 0] / RATE
        offset_end = corr[end_idx, 0] / RATE

        offsets.setdefault(file1, []).append((offset1 + offset_begin, offset1 + offset_end))
        offsets.setdefault(file2, []).append((offset2 + offset_begin, offset2 + offset_end))

    # for file, file_offsets in offsets.items():
    #     print(file)
    #     print(file_offsets)

    return offsets


def find_true_offsets(offsets_by_audio):
    true_offsets_by_audio = {}
    for file, offsets in offsets_by_audio.items():
        # if file != '5.wav':
        #     continue
        start_offsets = [offset[0] for offset in offsets if offset[1] != 0]
        end_offsets = [offset[1] for offset in offsets if offset[1] != 0]

        start_median = np.median(start_offsets)
        end_median = np.median(end_offsets)

        true_offsets_by_audio[file] = (start_median, end_median)
        # print(f'{file}: {start_median:.1f}, {end_median:.1f} ({end_median - start_median:.1f}s)')

    return true_offsets_by_audio


def fix_offsets(true_offsets):
    fixed_offsets = {}
    average_length = np.mean([end_offset - start_offset for start_offset, end_offset in true_offsets.values()])
    for file, (start_offset, end_offset) in true_offsets.items():
        is_on_edge = end_offset > 5 * 60 + 30
        if is_on_edge:
            print(f'On edge: {file}. Original: {start_offset:.1f}, {end_offset:.1f}, fixed: {start_offset:.1f}, {start_offset + average_length:.1f}')
            end_offset = start_offset + average_length
        fixed_offsets[file] = (start_offset, end_offset)
    return fixed_offsets


if __name__ == '__main__':
    main()
