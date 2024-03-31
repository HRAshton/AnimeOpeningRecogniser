# Loads audio files, correlates them and saves results to db.
import os
import time

import cupy as cp

from config import RATE, SERIES_WINDOW
from services.audio_loader import load_folder
from services.correlator import correlation_with_async_moving_window, correlation_with_sync_moving_window
from services.fragments_normalizer import normalize_fragments


@cp.fuse()
def load_to_gpu_if_needed(filename, audio, registry):
    if filename in registry:
        return
    registry[filename] = cp.asarray(audio, dtype=cp.float32)
    registry[filename] = registry[filename] - cp.mean(registry[filename])
    registry[filename] = registry[filename] / cp.max(cp.abs(registry[filename]))


def generate_pairs(files):
    files_tmp = files.copy()
    files_tmp.sort(key=lambda x: int(x[0].split('.')[0]))

    gpu_audios = {}

    for i in range(len(files_tmp) - 1):
        filename1, audio1 = files_tmp[i]
        load_to_gpu_if_needed(filename1, audio1, gpu_audios)

        for j in range(i + 1, min(len(files_tmp), i + SERIES_WINDOW)):
            load_to_gpu_if_needed(files_tmp[j][0], files_tmp[j][1], gpu_audios)
            yield ((filename1, gpu_audios[filename1]),
                   (files_tmp[j][0], gpu_audios[files_tmp[j][0]]))

        del gpu_audios[filename1]


def analyze_files(files):
    results = []
    for pair1, pair2 in generate_pairs(files):
        file1, audio1 = pair1
        file2, audio2 = pair2
        print(f'Correlating {file1} and {file2}...')
        # if (file1, file2) not in [('14.wav', '15.wav')]:
        #     continue
        if audio1.shape[0] < 30 * RATE or audio2.shape[0] < 30 * RATE:
            print(f'One of the audios is shorter than 30 seconds: {file1}, {file2}. Skipping.')
            continue

        offsets_by_windows = correlation_with_async_moving_window(audio1, audio2)
        best_offset1, best_offset2, max_corr = offsets_by_windows[cp.argmax(offsets_by_windows[:, 2])]

        truncated_audio1, truncated_audio2, offset1_secs, offset2_secs = \
            normalize_fragments(best_offset1, best_offset2, audio1, audio2)
        if (truncated_audio1.shape[0] == 0
                or truncated_audio2.shape[0] == 0
                or truncated_audio1.shape[0] != truncated_audio2.shape[0]):
            print(f'One of the audios is shorter than the other: {file1}, {file2}. Skipping.')
            continue

        corr_by_beats = correlation_with_sync_moving_window(truncated_audio1, truncated_audio2)

        results.append((file1, file2, offset1_secs, offset2_secs, corr_by_beats.get()))

    return results


def analyze_season(series_id):
    print(rf'Loading files for season {series_id}...')
    t = time.time()

    files = load_folder(rf'D:\AOR\artifacts\audio\{series_id}')
    correlations = analyze_files(files)
    del files

    print('Saving results...')
    corr_dir = rf'D:\AOR\artifacts\correlations\{series_id}'
    os.makedirs(corr_dir, exist_ok=True)
    for file1, file2, offset1, offset2, corr in correlations:
        filename = f'{file1}_{file2}_{int(offset1 * 1000)}_{int(offset2 * 1000)}.csv'
        filepath = os.path.join(corr_dir, filename)
        cp.savetxt(filepath, corr, delimiter=',', fmt='%1.3e')

    print(time.time() - t)


def main():
    all_series_folders = os.listdir(r'D:\AOR\artifacts\audio')
    all_series_ids = [int(folder)
                      for folder in all_series_folders
                      if folder.isdigit()]
    all_series_ids.sort()

    processed_series = [int(file.split('.')[0])
                        for file in os.listdir(r'D:\AOR\artifacts\correlations')]

    series_to_process = [series_id
                         for series_id in all_series_ids
                         if series_id not in processed_series]

    for series_id in series_to_process:
        analyze_season(series_id)


if __name__ == '__main__':
    main()
