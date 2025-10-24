from vkmax.client import MaxClient


async def get_contacts(client: MaxClient):
    cached_response = client.get_cached_contacts()
    if cached_response is None:
        raise Exception(
            "No chats cached. Please call login_by_token() or sign_in() first. "
            "Chats are automatically loaded during login."
        )
    return cached_response


async def search_user_by_phone(client: MaxClient, phone: str):
    """Search users by phone number."""

    return await client.invoke_method(
        opcode=46,
        payload={
            "phone": phone
        }
    )


async def resolve_users(client: MaxClient, user_id: list):
    """Resolving users via userid"""

    return await client.invoke_method(
        opcode=32,
        payload={
            "contactIds": user_id
        }
    )


async def add_to_contacts(client: MaxClient, user_id: int):
    """Adding user to contacts via userid"""

    return await client.invoke_method(
        opcode=34,
        payload={
            "contactId": user_id,
            "action": "ADD"
        }
    )


async def ban(client: MaxClient, user_id: int):
    """Banhammer to user's head"""

    return await client.invoke_method(
        opcode=34,
        payload={
            "contactId": user_id,
            "action": "BLOCK"
        }
    )
