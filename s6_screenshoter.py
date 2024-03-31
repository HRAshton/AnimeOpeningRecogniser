import asyncio
import csv
import os
import cv2
import zipfile
from concurrent.futures import ThreadPoolExecutor


def extract_screenshots(title_id, episode, op_begin_secs, op_end_secs):
    video_path = os.path.join(fr'D:\AOR\artifacts\video\{title_id}\{episode}.mp4')
    offsets = [
        op_begin_secs - 2,
        op_begin_secs + 2,
        op_end_secs - 2,
        op_end_secs + 2,
    ]

    cap = cv2.VideoCapture(video_path)
    frames = []

    for offset in offsets:
        cap.set(cv2.CAP_PROP_POS_MSEC, offset * 1000)
        success, frame = cap.read()
        if success:
            _, buffer = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 10])
            frames.append(buffer.tobytes())

    for i, frame in enumerate(frames):
        with open(os.path.join(fr'D:\AOR\artifacts\screenshots\{title_id}\{episode}_{i}.png'), 'wb') as f:
            f.write(frame)
    print(f'Extracted screenshots for {title_id}, {episode}')

    cap.release()
    return frames


async def process_row(executor, row):
    title_id, title_name, episode, status, op_begin_secs, op_end_secs = row
    op_begin_secs = float(op_begin_secs)
    op_end_secs = float(op_end_secs)
    os.makedirs(os.path.join(fr'D:\AOR\artifacts\screenshots\{title_id}'), exist_ok=True)
    frames = await asyncio.get_event_loop().run_in_executor(
        executor, extract_screenshots, title_id, episode, op_begin_secs, op_end_secs
    )
    return title_id, episode, frames


async def main():
    with open(r'D:\AOR\artifacts\AOR - export.csv', 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        data = list(reader)

    existing = set(os.listdir(r'D:\AOR\artifacts\screenshots'))
    data = [row for row in data if row[0] not in existing]
    print(f'Found {len(data)} rows to process')

    with ThreadPoolExecutor(max_workers=8) as executor:
        tasks = [asyncio.ensure_future(process_row(executor, row)) for row in data]
        for i, future in enumerate(asyncio.as_completed(tasks)):
            await future
            print(f'Processed {i + 1}/{len(data)}')


if __name__ == '__main__':
    asyncio.run(main())
