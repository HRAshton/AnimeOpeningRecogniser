def get_base_artifacts_path() -> str:
    return r"D:\AOR\artifacts"


def get_series_path(series_id: int) -> str:
    return f"{get_base_artifacts_path()}/video/{series_id}"


def get_episode_path(series_id: int, episode: int) -> str:
    return f"{get_series_path(series_id)}/{episode}.mp4"


def get_screenshot_path(series_id: int, episode: int, timestamp: int) -> str:
    return f"{get_base_artifacts_path()}/screenshots/{series_id}/{episode}_{timestamp}.jpg"
