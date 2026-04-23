# -*- coding: utf-8 -*-
"""
看板用户：注册与校验、个人资料（昵称/邮箱/头像）、修改密码。
使用本地 JSON 持久化。
"""

import json
import os
import re
import tempfile
import threading
from datetime import datetime

from werkzeug.security import check_password_hash, generate_password_hash

from src import config

_LOCK = threading.Lock()


def _users_json_path():
    config.ensure_dirs()
    return config.DASHBOARD_USERS_JSON


def _default_store():
    return {"next_user_id": 1, "users": [], "profiles": {}}


def _load_store():
    path = _users_json_path()
    if not os.path.isfile(path):
        return _default_store()
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        return _default_store()
    data.setdefault("next_user_id", 1)
    data.setdefault("users", [])
    data.setdefault("profiles", {})
    if not isinstance(data["users"], list):
        data["users"] = []
    if not isinstance(data["profiles"], dict):
        data["profiles"] = {}
    return data


def _save_store(data):
    path = _users_json_path()
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="dashboard_users_", suffix=".json", dir=parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _ensure_profile(data, username):
    if username in data["profiles"]:
        return False
    data["profiles"][username] = {"display_name": "", "email": "", "avatar_path": None}
    return True


def _find_user(data, username):
    u = (username or "").strip()
    for item in data["users"]:
        if item.get("username") == u:
            return item
    return None

def init_user_table():
    """初始化用户 JSON 文件，并补全历史字段。"""
    with _LOCK:
        data = _load_store()
        max_id = 0
        for u in data["users"]:
            uid = int(u.get("id") or 0)
            max_id = max(max_id, uid)
            if not u.get("created_at"):
                u["created_at"] = datetime.utcnow().isoformat() + "Z"
            _ensure_profile(data, u.get("username", ""))
        data["next_user_id"] = max(int(data.get("next_user_id") or 1), max_id + 1)
        _save_store(data)


def validate_username(username):
    if not username or len(username) < 2 or len(username) > 24:
        return "用户名长度为 2～24 个字符"
    if not re.match(r"^[\w\u4e00-\u9fff]+$", username):
        return "用户名仅支持字母、数字、下划线与中文"
    return None


def validate_password(password):
    if not password or len(password) < 6:
        return "密码至少 6 位"
    if len(password) > 128:
        return "密码过长"
    return None


def _validate_email(email):
    if not email or not email.strip():
        return None
    email = email.strip()
    if len(email) > 120:
        return "邮箱过长"
    if not re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", email):
        return "邮箱格式不正确"
    return None


def register_user(username, password, reserved_names=None):
    """
    注册新用户。
    返回 (success: bool, message: str)
    """
    username = (username or "").strip()
    reserved_names = reserved_names or []
    if username.lower() in {n.lower() for n in reserved_names}:
        return False, "该用户名为系统保留，请更换"
    err = validate_username(username)
    if err:
        return False, err
    err = validate_password(password)
    if err:
        return False, err
    pwd_hash = generate_password_hash(password)
    now = datetime.utcnow().isoformat() + "Z"
    with _LOCK:
        data = _load_store()
        if _find_user(data, username):
            return False, "该用户名已被注册"
        next_id = int(data.get("next_user_id") or 1)
        data["users"].append(
            {
                "id": next_id,
                "username": username,
                "password_hash": pwd_hash,
                "created_at": now,
            }
        )
        data["next_user_id"] = next_id + 1
        _ensure_profile(data, username)
        _save_store(data)
        return True, "注册成功，请登录"


def verify_db_user(username, password):
    """校验数据库用户，成功返回 True。"""
    username = (username or "").strip()
    if not username or not password:
        return False
    with _LOCK:
        data = _load_store()
        user = _find_user(data, username)
    if not user:
        return False
    return check_password_hash(user.get("password_hash", ""), password)


def is_registered_user(username):
    u = (username or "").strip()
    if not u:
        return False
    with _LOCK:
        data = _load_store()
        return _find_user(data, u) is not None


def get_user_id(username):
    u = (username or "").strip()
    with _LOCK:
        data = _load_store()
        user = _find_user(data, u)
    return user.get("id") if user else None


def get_profile(username):
    """
    返回 dict: username, display_name, email, avatar_path, is_registered, user_id
    avatar_path 为相对 static 的路径，如 uploads/avatars/u1.png；无则为 None
    """
    u = (username or "").strip()
    with _LOCK:
        data = _load_store()
        user = _find_user(data, u)
        created = _ensure_profile(data, u)
        p = data["profiles"].get(u, {})
        if created:
            _save_store(data)
    return {
        "username": u,
        "display_name": p.get("display_name") or "",
        "email": p.get("email") or "",
        "avatar_path": p.get("avatar_path"),
        "is_registered": user is not None,
        "user_id": user.get("id") if user else None,
    }


def update_profile(username, display_name=None, email=None):
    """更新昵称、邮箱（可只传其一）。返回 (ok, err_msg)。"""
    u = (username or "").strip()
    if not u:
        return False, "无效用户"
    prof = get_profile(u)
    dn = prof["display_name"] if display_name is None else (display_name or "").strip()[:64]
    em = prof["email"] if email is None else (email or "").strip()
    if email is not None and em:
        err = _validate_email(em)
        if err:
            return False, err
    with _LOCK:
        data = _load_store()
        _ensure_profile(data, u)
        data["profiles"][u]["display_name"] = dn
        data["profiles"][u]["email"] = em
        _save_store(data)
    return True, None


def update_password(username, old_password, new_password):
    """仅注册用户可改密。返回 (ok, err_msg)。"""
    u = (username or "").strip()
    err = validate_password(new_password)
    if err:
        return False, err
    if not verify_db_user(u, old_password):
        return False, "原密码不正确"
    with _LOCK:
        data = _load_store()
        user = _find_user(data, u)
        if not user:
            return False, "用户不存在"
        user["password_hash"] = generate_password_hash(new_password)
        _save_store(data)
    return True, None


def set_avatar_path(username, relative_path):
    """relative_path 如 uploads/avatars/u1.jpg"""
    u = (username or "").strip()
    with _LOCK:
        data = _load_store()
        _ensure_profile(data, u)
        data["profiles"][u]["avatar_path"] = relative_path
        _save_store(data)

