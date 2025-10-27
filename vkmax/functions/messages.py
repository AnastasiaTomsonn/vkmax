from io import BytesIO
from random import randint
from vkmax.client import MaxClient


async def send_message(
        client: MaxClient,
        chat_id: int,
        text: str,
        notify: bool = True
):
    """Sends message to specified chat"""

    return await client.invoke_method(
        opcode=64,
        payload={
            "chatId": chat_id,
            "message": {
                "text": text,
                "cid": randint(1750000000000, 2000000000000),
                "elements": [],
                "attaches": []
            },
            "notify": notify
        }
    )


async def edit_message(
        client: MaxClient,
        chat_id: int,
        message_id: int,
        text: str
):
    """Edits the specified message"""

    return await client.invoke_method(
        opcode=67,
        payload={
            "chatId": chat_id,
            "messageId": str(message_id),
            "text": text,
            "elements": [],
            "attachments": []
        }
    )


async def delete_message(
        client: MaxClient,
        chat_id: int,
        message_ids: list,
        delete_for_me: bool = False
):
    """ Deletes the specified message """

    return await client.invoke_method(
        opcode=66,
        payload={
            "chatId": chat_id,
            "messageIds": message_ids,
            "forMe": delete_for_me
        }
    )


async def pin_message(
        client: MaxClient,
        chat_id: int,
        message_id: int,
        notify=False
):
    """Pins message in the chat"""

    return await client.invoke_method(
        opcode=55,
        payload={
            "chatId": chat_id,
            "notifyPin": notify,
            "pinMessageId": str(message_id)
        }
    )


async def reply_message(
        client: MaxClient,
        chat_id: int,
        text: str,
        reply_to_message_id: int,
        notify=True
):
    """Replies to message in the chat"""

    return await client.invoke_method(
        opcode=64,
        payload={
            "chatId": chat_id,
            "message": {
                "text": text,
                "cid": randint(1750000000000, 2000000000000),
                "elements": [],
                "link": {
                    "type": "REPLY",
                    "messageId": str(reply_to_message_id)
                },
                "attaches": []
            },
            "notify": notify
        }
    )


async def send_photo(
        client: MaxClient,
        chat_id: int,
        image_path: str,
        caption: str,
        notify: bool = True
):
    """ Sends photo to specified chat """

    import requests

    photo_token = await client.invoke_method(
        opcode=80,
        payload={
            "count": 1
        }
    )

    url = photo_token["payload"]["url"]

    params = {
        'apiToken': url.split('apiToken=')[1]
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Origin': 'https://web.max.ru',
        'Referer': 'https://web.max.ru/',
        'Sec-Fetch-Site': 'cross-site',
        'Sec-Fetch-Mode': 'cors',
        'Accept-Language': 'ru-RU,ru;q=0.9',
    }

    try:
        resp = requests.get(image_path, timeout=15)
        resp.raise_for_status()

        files = {
            'file': ('image.jpg', BytesIO(resp.content), 'image/jpeg')  # that's not matter don't think about that lol
        }

        uploaded_photo = requests.post(url, params=params, headers=headers, files=files)
        uploaded_photo.raise_for_status()
        uploaded_photo = uploaded_photo.json()
    except Exception as e:
        print(f"Image upload failed: {e}")
        return

    try:
        photo_token_value = list(uploaded_photo['photos'].values())[0]['token']
    except Exception as e:
        print(f"Invalid upload response: {uploaded_photo} {e}")
        return

    await client.invoke_method(
        opcode=64,
        payload={
            "chatId": chat_id,
            "message": {
                "text": caption,
                "cid": randint(1750000000000, 2000000000000),
                "elements": [],
                "attaches": [
                    {
                        "_type": "PHOTO",
                        "photoToken": photo_token_value
                    }
                ]
            },
            "notify": notify
        }
    )


async def reaction_message(
        client: MaxClient,
        chat_id: int,
        message_id: str,
        reaction: str = ""
):
    if not reaction:
        """Remove the reaction"""
        return await client.invoke_method(
            opcode=179,
            payload={
                "chatId": chat_id,
                "messageId": str(message_id)
            }
        )

    """Add react to a message"""
    return await client.invoke_method(
        opcode=178,
        payload={
            "chatId": chat_id,
            "messageId": str(message_id),
            "reaction": {
                "reactionType": "EMOJI",
                "id": reaction,
            }
        }
    )
