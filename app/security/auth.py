from typing import Union

from fastapi import HTTPException
from pydantic import BaseModel
from app.database.users import fake_users_db


def fake_hash_password(password: str):
    return "fakehashed" + password


class User(BaseModel):
    username: str
    email: Union[str, None] = None
    full_name: Union[str, None] = None
    disabled: Union[bool, None] = None


class UserInDB(User):
    hashed_password: str


def get_user(db, username: str):
    if username in db:
        user_dict = db[username]
        return UserInDB(**user_dict)


async def authenticate(form_data):
    user_dict = fake_users_db.get(form_data.username)
    if not user_dict:
        raise HTTPException(
            status_code=400, detail="Incorrect username or password")

    user = UserInDB(**user_dict)

    hashed_password = fake_hash_password(form_data.password)
    if not hashed_password == fake_hash_password(user.hashed_password):
        raise HTTPException(
            status_code=400, detail="Incorrect username or password")

