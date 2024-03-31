import io
import sqlite3

import numpy as np
from numpy import ndarray

from constants import episode_statuses, series_statuses


class AnilibriaRepository:
    database: sqlite3

    def __init__(self):
        sqlite3.register_adapter(np.ndarray, AnilibriaRepository.adapt_array)
        sqlite3.register_converter("array", AnilibriaRepository.convert_array)
        self.database = sqlite3.connect('./anilibria.sqlite3', detect_types=sqlite3.PARSE_DECLTYPES)
        self.create()

    # <editor-fold desc="Downloading">

    def get_next_series_to_download(self) -> tuple[int | None, str | None]:
        cursor = self.database.cursor()
        cursor.execute('''
            SELECT id, name
            FROM series
            WHERE downloading_status = ?
            ORDER BY id DESC
            LIMIT 1
            ''', (series_statuses.initialized,))
        return cursor.fetchone() or (None, None)

    def set_series_already_has_timestamps(self, series_id: int):
        cursor = self.database.cursor()
        cursor.execute('UPDATE series SET downloading_status = ? WHERE id = ?',
                       (series_statuses.already_has_timestamps, series_id))
        self.database.commit()

    def set_series_status_downloaded(self, series_id: int):
        cursor = self.database.cursor()
        cursor.execute('UPDATE series SET downloading_status = ? WHERE id = ?',
                       (series_statuses.downloaded, series_id))
        self.database.commit()

    def set_series_status_downloading_error(self, series_id: int):
        cursor = self.database.cursor()
        cursor.execute('UPDATE series SET downloading_status = ? WHERE id = ?',
                       (series_statuses.downloading_error, series_id))
        self.database.commit()
    
    def set_series_status_few_episodes(self, series_id: int):
        cursor = self.database.cursor()
        cursor.execute('UPDATE series SET downloading_status = ? WHERE id = ?',
                       (series_statuses.few_episodes, series_id))
        self.database.commit()

    def register_episode(self, series_id: int, episode: int):
        cursor = self.database.cursor()
        cursor.execute('INSERT INTO episodes (series_id, episode, status) VALUES (?, ?, ?)',
                       (series_id, episode, episode_statuses.downloaded))
        self.database.commit()

    def is_episode_downloaded(self, series_id: int, episode: int) -> bool:
        cursor = self.database.cursor()
        cursor.execute('SELECT 1 FROM episodes WHERE series_id = ? AND episode = ?',
                       (series_id, episode))
        return cursor.fetchone() is not None

    # </editor-fold>

    # <editor-fold desc="Scenes">

    def get_next_episode_to_hash_and_lock(self) -> tuple[int | None, int | None]:
        cursor = self.database.cursor()
        cursor.execute('SELECT series_id, episode '
                       'FROM episodes '
                       'WHERE status = ? '
                       'ORDER BY series_id, episode desc '
                       'LIMIT 1', (episode_statuses.downloaded,))
        pair = cursor.fetchone()
        if pair is None:
            return None, None

        cursor.execute('UPDATE episodes SET status = ? WHERE series_id = ? AND episode = ?',
                       (episode_statuses.hashing, pair[0], pair[1]))
        self.database.commit()
        return pair

    def register_episode_scenes(self, series_id: int, episode: int, scenes: list[tuple[float, float, ndarray]]):
        cursor = self.database.cursor()
        cursor.execute('DELETE FROM scenes WHERE series_id = ? AND episode = ?', (series_id, episode))
        cursor.executemany('INSERT INTO scenes (series_id, episode, scene_begin_secs, scene_end_secs, hash) '
                           'VALUES (?, ?, ?, ?, ?)',
                           ((series_id, episode, scene[0], scene[1], scene[2]) for scene in scenes))
        cursor.execute('UPDATE episodes SET status = ? WHERE series_id = ? AND episode = ?',
                       (episode_statuses.hashed, series_id, episode))
        self.database.commit()

    def set_episode_status_hashing_error(self, series_id: int, episode: int):
        cursor = self.database.cursor()
        cursor.execute('UPDATE episodes SET status = ? WHERE series_id = ? AND episode = ?',
                       (episode_statuses.hashing_error, series_id, episode))
        self.database.commit()

    # </editor-fold>

    # <editor-fold desc="Cross-correlation">

    def get_next_scenes_to_cross_correlate(self) -> list[tuple[int, int, float, float, ndarray]]:
        """
        Берем все сцены всех эпизодов, если сериал в статусе downloaded и все эпизоды в статусе hashed.
        Иначе ничего не берем
        """
        cursor = self.database.cursor()
        cursor.execute('SELECT series_id, episode, scene_begin_secs, scene_end_secs, hash '
                       'FROM scenes '
                       'WHERE series_id IN ( '
                       '    SELECT id '
                       '    FROM series '
                       '    WHERE downloading_status = ? '
                       '        AND cross_correlation_status IS NULL '
                       '        AND NOT EXISTS ( '
                       '            SELECT 1 '
                       '            FROM episodes '
                       '            WHERE series_id = series.id '
                       '                AND status != ? '
                       '        ) '
                       ') ',
                       (series_statuses.downloaded, episode_statuses.hashed))
        return cursor.fetchall()

    def register_episodes_openings(self, series_id: int, openings: list[tuple[int, float, float]]):
        cursor = self.database.cursor()
        cursor.executemany('UPDATE episodes SET opening_begin_secs = ?, opening_end_secs = ? '
                           'WHERE series_id = ? AND episode = ?',
                           ((opening[1], opening[2], series_id, opening[0]) for opening in openings))
        cursor.execute('UPDATE series SET cross_correlation_status = ? WHERE id = ?',
                       (series_statuses.cross_correlated, series_id))
        self.database.commit()

    def set_series_status_cross_correlation_error(self, series_id: int):
        cursor = self.database.cursor()
        cursor.execute('UPDATE series SET cross_correlation_status = ? WHERE id = ?',
                       (series_statuses.cross_correlation_error, series_id))
        self.database.commit()

    # </editor-fold>

    # <editor-fold desc="Finalization">

    def get_next_episode_to_finalize(self) -> tuple[int, int, float, float]:
        cursor = self.database.cursor()
        cursor.execute('SELECT series_id, episode, opening_begin_secs, opening_end_secs '
                       'FROM episodes '
                       'JOIN series ON series.id = episodes.series_id '
                       'WHERE series.cross_correlation_status is not null '
                       '    AND episodes.status NOT IN (?, ?) '
                       'LIMIT 1', (episode_statuses.finalized, episode_statuses.finalizing_error))
        return cursor.fetchone()

    def get_scenes_for_episode(self, series_id: int, episode: int) -> list[tuple[float, float]]:
        cursor = self.database.cursor()
        cursor.execute('SELECT scene_begin_secs, scene_end_secs '
                       'FROM scenes '
                       'WHERE series_id = ? AND episode = ?',
                       (series_id, episode))
        return cursor.fetchall()

    def set_episode_finalized(self, series_id: int, episode: int, screenshot_timecodes: set[int]) -> None:
        cursor = self.database.cursor()
        cursor.execute('UPDATE episodes SET status = ? WHERE series_id = ? AND episode = ?',
                       (episode_statuses.finalized, series_id, episode))
        cursor.executemany('INSERT INTO screenshots (series_id, episode, timecode) VALUES (?, ?, ?)',
                           ((series_id, episode, timecode) for timecode in screenshot_timecodes))
        self.database.commit()

    def set_episode_finalizing_error(self, series_id: int, episode: int):
        cursor = self.database.cursor()
        cursor.execute('UPDATE episodes SET status = ? WHERE series_id = ? AND episode = ?',
                       (episode_statuses.finalizing_error, series_id, episode))
        self.database.commit()

    # </editor-fold>

    def create(self):
        cursor = self.database.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS series (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                downloading_status TEXT NOT NULL,
                cross_correlation_status TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS episodes (
                series_id INTEGER NOT NULL,
                episode INTEGER NOT NULL,
                status TEXT NOT NULL,
                opening_begin_secs REAL,
                opening_end_secs REAL,
                FOREIGN KEY (series_id) REFERENCES series (id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scenes (
                series_id INTEGER NOT NULL,
                episode INTEGER NOT NULL,
                scene_begin_secs REAL NOT NULL,
                scene_end_secs REAL NOT NULL,
                hash array NOT NULL,
                FOREIGN KEY (series_id) REFERENCES series (id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS screenshots (
                series_id INTEGER NOT NULL,
                episode INTEGER NOT NULL,
                timecode INTEGER NOT NULL,
                FOREIGN KEY (series_id, episode) REFERENCES episodes (series_id, episode)
                CONSTRAINT unique_screenshot UNIQUE (series_id, episode, timecode)
            )
        ''')
        self.database.commit()

    @staticmethod
    def adapt_array(arr):
        """
        http://stackoverflow.com/a/31312102/190597 (SoulNibbler)
        """
        out = io.BytesIO()
        np.save(out, arr)
        out.seek(0)
        return sqlite3.Binary(out.read())

    @staticmethod
    def convert_array(text):
        out = io.BytesIO(text)
        out.seek(0)
        return np.load(out)
