import cupy as cp

from config import RATE


@cp.fuse()
def compute_offsets_and_indices(offsets_diff, length):
    offset1_secs = cp.maximum(0.0, offsets_diff / RATE)
    offset2_secs = cp.maximum(0.0, -offsets_diff / RATE)

    start_idx_audio1 = cp.maximum(0, offsets_diff)
    end_idx_audio1 = start_idx_audio1 + length
    start_idx_audio2 = cp.maximum(0, -offsets_diff)
    end_idx_audio2 = start_idx_audio2 + length

    return offset1_secs, offset2_secs, start_idx_audio1, end_idx_audio1, start_idx_audio2, end_idx_audio2


def normalize_fragments(best_offset1, best_offset2, audio1, audio2):
    offsets_diff = best_offset1 - best_offset2
    length = min(len(audio1), len(audio2)) - abs(offsets_diff)

    (offset1_secs, offset2_secs,
     start_idx_audio1, end_idx_audio1,
     start_idx_audio2, end_idx_audio2) = compute_offsets_and_indices(offsets_diff, length)

    truncated_audio1 = audio1[start_idx_audio1:end_idx_audio1]
    truncated_audio2 = audio2[start_idx_audio2:end_idx_audio2]

    return truncated_audio1, truncated_audio2, offset1_secs, offset2_secs
