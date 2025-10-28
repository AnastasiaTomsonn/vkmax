import aiohttp
import mimetypes
import os
import asyncio
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
    """ Sends photo to specified chat (async, with prepare_file) """

    photo_token = await client.invoke_method(
        opcode=80,
        payload={
            "count": 1
        }
    )

    upload_url = photo_token["payload"]["url"]
    api_token = upload_url.split("apiToken=")[1]

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Origin": "https://web.max.ru",
        "Referer": "https://web.max.ru/",
    }
    params = {"apiToken": api_token}

    try:
        prep_file = await prepare_file(image_path)
        file_name = prep_file["filename"]
        mime_type = prep_file["mime_type"]
        content = prep_file["content"]

        data = aiohttp.FormData()
        data.add_field("file", BytesIO(content), filename=file_name, content_type=mime_type)

        async with aiohttp.ClientSession() as session:
            async with session.post(upload_url, headers=headers, params=params, data=data) as resp:
                if resp.status != 200:
                    raise ValueError(f"Ошибка загрузки фото: HTTP {resp.status}")
                uploaded_photo = await resp.json()
                print("Photo uploaded:", uploaded_photo)
    except Exception as e:
        print(f"Image upload failed: {e}")
        return

    try:
        photo_token_value = list(uploaded_photo['photos'].values())[0]['token']
    except Exception as e:
        print(f"Invalid upload response: {uploaded_photo} {e}")
        return

    return await client.invoke_method(
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


async def send_file(
        client: MaxClient,
        chat_id: int,
        file_url: str,
        caption: str,
        notify: bool = True,
        max_attempts: int = 5,
        wait_seconds: float = 2.0
):
    """ Sends a file from a URL to the chat with waiting for file processing """
    file_token = await client.invoke_method(
        opcode=87,
        payload={"count": 1}
    )

    info = file_token["payload"]["info"][0]
    upload_url = info["url"]
    fileId = info["fileId"]
    api_token = info["token"]

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Origin": "https://web.max.ru",
        "Referer": "https://web.max.ru/",
    }
    params = {"apiToken": api_token}

    try:
        prep_file = await prepare_file(file_url)
        file_name = prep_file["filename"]
        mime_type = prep_file["mime_type"]
        content = prep_file["content"]

        data = aiohttp.FormData()
        data.add_field("file", BytesIO(content), filename=file_name, content_type=mime_type)

        async with aiohttp.ClientSession() as session:
            async with session.post(upload_url, headers=headers, params=params, data=data) as resp:
                if resp.status != 200:
                    raise ValueError(f"File upload error: HTTP {resp.status}")
                print("Upload successful")

    except Exception as e:
        print(f"File upload failed: {e}")
        return

    response = {}
    for attempt in range(max_attempts):
        response = await client.invoke_method(
            opcode=64,
            payload={
                "chatId": chat_id,
                "message": {
                    "text": caption,
                    "cid": randint(1750000000000, 2000000000000),
                    "elements": [],
                    "attaches": [{"_type": "FILE", "fileId": fileId}]
                },
                "notify": notify
            }
        )

        error = response.get("payload", {}).get("error")
        if not error:
            print("Message sent successfully ✅")
            return response

        if error == "attachment.not.ready":
            print(f"The file is not ready yet, we are waiting {wait_seconds} s... (attempt {attempt + 1})")
            await asyncio.sleep(wait_seconds)
        else:
            print("Unexpected error:", response)
            return response

    print("The file was never ready after all the attempts.")
    return response


async def send_video(
        client: MaxClient,
        chat_id: int,
        file_url: str,
        notify: bool = True,
        max_attempts: int = 5,
        wait_seconds: float = 2.0
):
    """ Sends a file from a URL to the chat with waiting for file processing """
    file_token = await client.invoke_method(
        opcode=82,
        payload={"count": 1}
    )

    info = file_token["payload"]["info"][0]
    upload_url = info["url"]
    video_id = info["videoId"]
    api_token = info["token"]

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Origin": "https://web.max.ru",
        "Referer": "https://web.max.ru/",
    }
    params = {"apiToken": api_token}

    try:
        prep_file = await prepare_file(file_url)
        file_name = prep_file["filename"]
        mime_type = prep_file["mime_type"]
        content = prep_file["content"]

        data = aiohttp.FormData()
        data.add_field("file", BytesIO(content), filename=file_name, content_type=mime_type)

        async with aiohttp.ClientSession() as session:
            async with session.post(upload_url, headers=headers, params=params, data=data) as resp:
                if resp.status != 200:
                    raise ValueError(f"File upload error: HTTP {resp.status}")
                print("Upload successful")

    except Exception as e:
        print(f"File upload failed: {e}")
        return

    response = {}
    for attempt in range(max_attempts):
        response = await client.invoke_method(
            opcode=64,
            payload={
                "chatId": chat_id,
                "message": {
                    "cid": randint(1750000000000, 2000000000000),
                    "elements": [],
                    "attaches": [{"_type": "VIDEO", "videoId": video_id, "token": api_token}],
                },
                "notify": notify
            }
        )

        error = response.get("payload", {}).get("error")
        if not error:
            print("Message sent successfully ✅")
            return response

        if error == "attachment.not.ready":
            print(f"The file is not ready yet, we are waiting {wait_seconds} s... (attempt {attempt + 1})")
            await asyncio.sleep(wait_seconds)
        else:
            print("Unexpected error:", response)
            return response

    print("The file was never ready after all the attempts.")
    return response


async def prepare_file(file_url: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(file_url) as resp:
            if resp.status != 200:
                raise ValueError(f"Failed to download file: HTTP {resp.status}")

            cd = resp.headers.get("Content-Disposition", "")

            if "filename=" in cd:
                filename = cd.split("filename=")[-1].strip().strip('"')
            else:
                filename = os.path.basename(file_url.split("?")[0]) or "file.bin"

            mime_type = resp.headers.get("Content-Type")
            if not mime_type:
                mime_type, _ = mimetypes.guess_type(filename)
            if not mime_type:
                mime_type = "application/octet-stream"

            content = await resp.read()
            content_length = len(content)

            return {
                "filename": filename,
                "mime_type": mime_type,
                "content": content,
                "content_length": content_length
            }


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
