import os
import time
from typing import Tuple

import cupy as cp

from config import RATE, SERIES_WINDOW
from services.audio_loader import load_folder
from services.correlator import correlation_with_async_moving_window, correlation_with_sync_moving_window
from services.fragments_normalizer import normalize_fragments
from services.offset_searcher import find_true_offsets


def find_longest_same_fragment(corr_by_secs) -> Tuple[float, float, bool]:
    # Вычисление среднего значения по оси y
    mean_y = cp.mean(corr_by_secs[:, 1])
    median_y = cp.median(corr_by_secs[:, 1])

    # Создание булевого массива, где True - значения выше среднего
    above_mean = corr_by_secs[:, 1] > mean_y

    bools = cp.where(above_mean)[0]
    if len(bools) == 0:
        return 0, 0, False

    start_index = bools[0]
    end_index = bools[-1]

    start_secs = corr_by_secs[start_index][0] / RATE
    end_secs = corr_by_secs[end_index][0] / RATE

    is_average_bigger = mean_y > median_y * 2

    return start_secs, end_secs, is_average_bigger


def find_offsets_by_window(audio1, audio2):
    offsets_by_windows = correlation_with_async_moving_window(audio1, audio2)
    best_offset1, best_offset2, max_corr = offsets_by_windows[cp.argmax(offsets_by_windows[:, 2])]

    truncated_audio1, truncated_audio2, offset1_secs, offset2_secs = \
        normalize_fragments(best_offset1, best_offset2, audio1, audio2)

    corr_by_secs = correlation_with_sync_moving_window(truncated_audio1, truncated_audio2)
    print(corr_by_secs.get())

    start_secs, end_secs, is_correlate = find_longest_same_fragment(corr_by_secs)
    if not is_correlate:
        return cp.array([0, 0, 0, 0])

    file1_start_secs = offset1_secs + start_secs
    file1_end_secs = offset1_secs + end_secs

    file2_start_secs = offset2_secs + start_secs
    file2_end_secs = offset2_secs + end_secs

    return [file1_start_secs, file1_end_secs, file2_start_secs, file2_end_secs]


@cp.fuse()
def load_to_gpu_if_needed(filename, audio, registry):
    if filename in registry:
        return
    registry[filename] = cp.asarray(audio, dtype=cp.float32)
    registry[filename] = registry[filename] / cp.max(cp.abs(registry[filename]))
    registry[filename] = registry[filename] - cp.mean(registry[filename])


def generate_pairs(files):
    files_tmp = files.copy()
    files_tmp.sort(key=lambda x: int(x[0].split('.')[0]))

    gpu_audios = {}

    for i in range(len(files_tmp) - 1):
        filename1, audio1 = files_tmp[i]
        load_to_gpu_if_needed(filename1, audio1, gpu_audios)

        for j in range(i + 1, min(len(files_tmp), i + SERIES_WINDOW)):
            load_to_gpu_if_needed(files_tmp[j][0], files_tmp[j][1], gpu_audios)
            yield (filename1, gpu_audios[filename1]), (files_tmp[j][0], gpu_audios[files_tmp[j][0]])

        del gpu_audios[filename1]


def find_all_offsets(files):
    offsets_by_audio: dict[str, list[tuple[float, float]]] = {file: [] for file, _ in files}

    for pair1, pair2 in generate_pairs(files):
        file1, audio1 = pair1
        file2, audio2 = pair2
        if (file1, file2) not in [('11.wav', '12.wav')]:
            continue

        file1_start_secs, file1_end_secs, file2_start_secs, file2_end_secs = \
            find_offsets_by_window(audio1, audio2)

        offsets_by_audio[file1].append((file1_start_secs.get(), file1_end_secs.get()))
        offsets_by_audio[file2].append((file2_start_secs.get(), file2_end_secs.get()))

        print(f'{file1},{file2},'
              f'{file1_start_secs:.3f},{file1_end_secs:.3f},'
              f'{file2_start_secs:.3f},{file2_end_secs:.3f},'
              f'{file1_end_secs - file1_start_secs:.3f}')

    return offsets_by_audio


def analyze_season(series_id):
    print(rf'Loading files for season {series_id}...')
    t = time.time()
    files = load_folder(rf'D:\AOR\artifacts\audio\{series_id}')

    all_offsets_by_audio = find_all_offsets(files)
    filtered_offsets = find_true_offsets(all_offsets_by_audio)

    csv_content = 'File,Start,End,Length\n'
    for file, (start, end) in filtered_offsets.items():
        print(f'{file},{start:.3f},{end:.3f},{end - start:.3f}')
        csv_content += f'{file},{start:.3f},{end:.3f},{end - start:.3f}\n'

    with open(fr'D:\AOR\artifacts\offsets\{series_id}.csv', 'w') as f:
        f.write(csv_content)

    print(time.time() - t)


def main():
    all_series_folders = os.listdir(r'D:\AOR\artifacts\audio')
    all_series_ids = [int(folder)
                      for folder in all_series_folders
                      if folder.isdigit()]
    all_series_ids.sort()

    processed_series = [int(file.split('.')[0])
                        for file in os.listdir(r'D:\AOR\artifacts\offsets')]

    series_to_process = [series_id
                         for series_id in all_series_ids
                         if series_id not in processed_series]
    for series_id in series_to_process:
        analyze_season(series_id)


if __name__ == '__main__':
    main()
