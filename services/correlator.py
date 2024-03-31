import logging
import cupy as cp

from config import WINDOW_BEAT, RATE

logger = logging.getLogger(__name__)


def correlation_with_async_moving_window(audio1: cp.ndarray,
                                         audio2: cp.ndarray) -> cp.stack:
    """
    Разбивает файл audio1 на фрагменты размером WINDOW_BEAT и рассчитывает корреляцию с audio2.
    Возвращает список кортежей (offset1, offset2, corr), содержащий данные о смещении наиболее похожего фрагмента
    audio2 на каждый фрагмент audio1.
    :param audio1: ndarray с аудио
    :param audio2: ndarray с аудио
    :return: список кортежей (offset1, offset2, corr),
      где offset1 - смещение в audio1,
          offset2 - смещение в audio2,
          corr - коэффициент корреляции
    """
    num_fragments = (len(audio1) + WINDOW_BEAT - 1) // WINDOW_BEAT
    fragments = cp.stack([audio1[i * WINDOW_BEAT: i * WINDOW_BEAT + WINDOW_BEAT]
                          for i in range(num_fragments - 1)])

    corr_per_fragment = cp.array([cp.correlate(audio2, fragment, mode='valid')
                                  for fragment in fragments])

    audio2_offsets = cp.argmax(corr_per_fragment, axis=1)
    corr_peaks_per_fragment = cp.max(corr_per_fragment, axis=1)

    offsets = cp.stack((cp.arange(num_fragments - 1) * WINDOW_BEAT,
                        audio2_offsets,
                        corr_peaks_per_fragment), axis=-1)

    return offsets


def correlation_with_sync_moving_window(audio1: cp.ndarray, audio2: cp.ndarray) -> cp.ndarray:
    if audio1.shape[0] > audio2.shape[0]:
        raise ValueError("audio2 должен быть не короче, чем audio1")

    num_seconds = audio1.shape[0] // RATE
    offsets = cp.arange(num_seconds) * RATE

    # Создаем массивы для фрагментов
    fragments1 = cp.array([audio1[i * RATE:(i + 1) * RATE] for i in range(num_seconds)])
    fragments2 = cp.array([audio2[i * RATE:(i + 1) * RATE] for i in range(num_seconds)])

    # Нормализация фрагментов
    mean1 = fragments1.mean(axis=1, keepdims=True)
    std1 = fragments1.std(axis=1, keepdims=True)
    normalized_fragments1 = (fragments1 - mean1) / std1

    mean2 = fragments2.mean(axis=1, keepdims=True)
    std2 = fragments2.std(axis=1, keepdims=True)
    normalized_fragments2 = (fragments2 - mean2) / std2

    # Вычисление корреляций и нахождение максимальных значений
    max_correlations = cp.array([cp.max(cp.correlate(norm_frag1, norm_frag2, mode='full'))
                                 for norm_frag1, norm_frag2 in zip(normalized_fragments1, normalized_fragments2)])

    # Объединение отступов и максимальных значений корреляции
    results = cp.stack((offsets, max_correlations), axis=-1)

    return results
