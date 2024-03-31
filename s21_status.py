# Нужно получить минимальное и максимальное значение названия файла.
# Все названия числовые как 1.wav

import os


dirs = os.listdir(r'D:\AOR\artifacts\audio')
dir_ids = [int(dir) for dir in dirs if dir.isdigit()]

csv_data = 'Dir,Min,Max\n'
for dir_id in dir_ids:
    print(dir_id)
    files = os.listdir(rf'D:\AOR\artifacts\audio\{dir_id}')
    files_ids = [int(file.split('.')[0]) for file in files]
    if len(files_ids) == 0:
        print('Empty dir')
        continue
    csv_data += f'{dir_id},{min(files_ids)},{max(files_ids)}\n'

with open(r'D:\AOR\artifacts\audio\min_max.csv', 'w') as f:
    f.write(csv_data)
