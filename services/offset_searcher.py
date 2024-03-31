import numpy as np
from scipy.stats import gaussian_kde


def find_true_offset(offsets: list[float]) -> float:
    # Добавление небольшого случайного смещения к данным для предотвращения сингулярности
    random_noise = np.random.normal(0, 0.01, len(offsets))
    data_with_noise = [d + noise for d, noise in zip(offsets, random_noise)]

    kde = gaussian_kde(data_with_noise)

    grid = np.linspace(min(data_with_noise) - 1, max(data_with_noise) + 1, 1000)
    density = kde(grid)

    most_likely_value = grid[np.argmax(density)]

    return float(most_likely_value)


def find_true_offsets_kde(offsets_by_audio: dict[str, list[tuple[float, float]]]) -> dict[str, tuple[float, float]]:
    """
    Находит наиболее вероятные смещения для каждого файла.
    """
    true_offsets_by_audio: dict[str, tuple[float, float]] = {}
    for file, offsets in offsets_by_audio.items():
        start_offsets = [offset[0] for offset in offsets if offset[1] != 0]
        end_offsets = [offset[1] for offset in offsets if offset[1] != 0]

        start_offset = find_true_offset(start_offsets)
        end_offset = find_true_offset(end_offsets)

        true_offsets_by_audio[file] = (start_offset, end_offset)

    return true_offsets_by_audio


def find_true_offsets(offsets_by_audio: dict[str, list[tuple[float, float]]]) -> dict[str, tuple[float, float]]:
    """
    Находит наиболее вероятные смещения для каждого файла.
    """
    true_offsets_by_audio: dict[str, tuple[float, float]] = {}
    for file, offsets in offsets_by_audio.items():
        start_offsets = [offset[0] for offset in offsets if offset[1] != 0]
        end_offsets = [offset[1] for offset in offsets if offset[1] != 0]

        start_median = np.median(start_offsets)
        end_median = np.median(end_offsets)

        true_offsets_by_audio[file] = (start_median, end_median)

    return true_offsets_by_audio
