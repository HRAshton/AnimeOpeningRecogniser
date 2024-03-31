function doit() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const all_series = read_values(ss, "series", "id,name,status");
  const all_episodes = read_values(ss, "episodes", "series_id,episode");
  const all_extracting_errors = read_values(ss, "errors", "_,series_id,episode,code");
  const all_offsets = read_values(ss, "offsets", "_,series_id,episode,begin,end");
  const all_overrides = read_values(ss, "overrides", "series_id,title_status,episode,ep_status,begin,end");

  all_series.sort((a, b) => a.id - b.id);

  const result = [[
    'title_id', 'title_name', 'episode', 'status', 'op_begin_secs', 'op_end_secs',
    'in_mins', 'found', 'length', 'is_length_in_window',
    'median_length', 'diff_from_median', 'is_diff_large'
  ]];
  for (const title of all_series) {
    const title_overrides = all_overrides.filter(row => row.series_id === title.id);
    if (title_overrides.length === 1 && !!title.title_status) {
      title.status = title_overrides[0].title_status;
    }

    const is_already_timestamped = title.status === 'sr_already_has_timestamps';
    if (is_already_timestamped) {
      result.push([title.id, title.name, "", "title_already_has_timestamps", "", "", "", "", "", "", "", "", ""]);
      continue;
    }

    const download_error = title.status === 'sr_downloading_error';
    if (download_error) {
      result.push([title.id, title.name, "", "title_has_not_been_provided_by_bot", "", "", "", "", "", "", "", "", ""]);
      continue;
    }

    const few_episodes = title.status === 'sr_few_episodes';
    if (few_episodes) {
      result.push([title.id, title.name, "", "title_has_less_than_4_episodes", "", "", "", "", "", "", "", "", ""]);
      continue;
    }

    const initialized = title.status === 'sr_initialized';
    if (initialized) {
      continue;
    }

    const successful = title.status === 'sr_downloaded';
    if (!successful) {
      throw `Strange status: ${title.status}`
    }

    const title_errors = all_extracting_errors.filter(err => err.series_id === title.id);
    const title_offsets = all_offsets.filter(offset => offset.series_id === title.id);
    const title_episodes = all_episodes.filter(ep => ep.series_id === title.id).map(ep => ep.episode);
    if (!title_episodes.length) {
      throw "Title has no episodes";
    }

    const title_result_rows = [];
    const min_episode = Math.min(...title_episodes);
    const max_episode = Math.max(...title_episodes);
    for (let episode = min_episode; episode <= max_episode; episode++) {
      const offsets = title_offsets.find(offset => offset.episode === episode);
      const override = title_overrides.find(row => row.episode === episode);

      const marked_manually = Number.isInteger(override?.begin);

      const ep_status = override?.ep_status
        ? override.ep_status
        : marked_manually
          ? 'episode_marked_manually'
          : !title_episodes.includes(episode)
            ? 'episode_download_error'
            : title_errors.find(err => err.episode === episode && err.code === 'TooShort')
              ? 'episode_is_too_short'
              : title_errors.find(err => err.episode === episode && err.code === 'Errors')
                ? 'ffmpeg_thrown_errors'
                : offsets
                  ? 'opening_found'
                  : 'opening_not_found';

      const op_begin = override?.begin === -1 ? undefined : marked_manually ? override.begin : offsets?.begin;
      const op_end = override?.end === -1 ? undefined : marked_manually ? override.end : offsets?.end;

      const found = !!op_begin || !!op_end;
      const in_mins = found ?
        `${Math.trunc(op_begin / 60)}:${Math.trunc(op_begin % 60)} - ${Math.trunc(op_end / 60)}:${Math.trunc(op_end % 60)}`
        : undefined;
      const length = found
        ? op_end - op_begin
        : undefined;
      const is_length_in_window = found
        ? length >= 80 && length <= 110
        : undefined;
      const median_length = undefined;
      const diff_from_median = undefined;
      const is_diff_large = undefined;

      title_result_rows.push([
        title.id, title.name, episode, ep_status, op_begin, op_end,
        in_mins, found, length, is_length_in_window,
        median_length, diff_from_median, is_diff_large,
      ]);
    }

    const all_episodes_are_short = title_result_rows.every(row => row[3] === 'episode_is_too_short');
    if (all_episodes_are_short) {
      result.push([title.id, title.name, "", "all_title_episodes_are_too_short", "", "", "", "", "", "", "", "", ""]);
      continue;
    }

    const median_length = median(title_result_rows.map(row => row[8]).filter(val => val !== undefined));
    for (const row of title_result_rows) {
      if (!row[7]) {
        continue;
      }

      row[10] = median_length;
      row[11] = Math.abs(row[10] - row[8]);
      row[12] = row[11] > 10;
    }

    result.push(...title_result_rows);
  }

  const result_sheet = ss.getSheetByName("results");
  const result_range = result_sheet.getRange(1, 1, result.length, result[0].length);
  result_sheet.clear();
  result_range.setValues(result);
}

function read_values(ss, table_name, columns_str) {
  const columns = columns_str.split(",");
  const sheet = ss.getSheetByName(table_name);
  const values = sheet.getRange(1, 1, sheet.getLastRow(), columns.length).getValues();
  const result = values.map(row => Object.fromEntries(row.map((val, i) => [columns[i], val])));

  return result;
}

function median(values) {
  if (values.length === 0) {
    return undefined;
  }

  // Sorting values, preventing original array
  // from being mutated.
  values = [...values].sort((a, b) => a - b);

  const half = Math.floor(values.length / 2);

  return (values.length % 2
    ? values[half]
    : (values[half - 1] + values[half]) / 2
  );
}
