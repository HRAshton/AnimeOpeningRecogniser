import os
import time
from typing import List, Tuple

import librosa
import cupy as cp
import numpy as np

from config import RATE
import soundfile as sf


def _load_audio(file: str) -> cp.ndarray:
    t0 = time.time()
    audio, rate = sf.read(file)
    if rate != RATE:
        raise ValueError(f'Wrong rate: {rate} != {RATE}')

    print(f'{file} loaded ({round(time.time() - t0, 3)}s)')

    return audio


def load_folder(folder: str) -> List[Tuple[str, cp.ndarray]]:
    files = os.listdir(folder)

    audios = []
    for file in files:
        audio = _load_audio(fr'{folder}\{file}')
        audios.append((file, audio))

    return audios
