from telethon.tl.types import Message, KeyboardButtonCallback


def get_all_buttons(message: Message) -> list[KeyboardButtonCallback]:
    return [button for row in message.reply_markup.rows for button in row.buttons]


def get_prev_episode_button(message: Message) -> KeyboardButtonCallback | None:
    episode_num = get_current_episode(message)
    return next((button
                 for button in get_all_buttons(message)
                 if button.text == str(episode_num - 1)),
                None)


def get_prev_page_button(message: Message) -> KeyboardButtonCallback | None:
    return next((button
                 for button in get_all_buttons(message)
                 if b'\xe2\x97' in button.text.encode()),
                None)


def get_current_episode(message: Message) -> int:
    for row in message.reply_markup.rows:
        for button in row.buttons:
            if button.text.startswith('['):
                episode = int(button.text[1:-1])
                return episode

    raise ValueError('No current episode')


def are_more_then_3_episodes_in_series(message: Message) -> bool:
    if get_prev_page_button(message):
        return True

    # assume that the current episode always has a non-numeric button
    count = len([1 for button in get_all_buttons(message) if button.text.isnumeric()])
    return count >= 3


def are_more_then_13_episodes_in_series(message: Message) -> bool:
    # assume that the current episode always has a non-numeric button
    count = max([int(button.text) for button in get_all_buttons(message) if button.text.isnumeric()])
    return count >= 13
