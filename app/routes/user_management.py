import os
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from dotenv import load_dotenv

from app.config import load_environment
from app.agents import rag_agent
from app.models.request_models import AIChatRequest, ConnectionProfileRequest, CreateUserRequest, LoginRequest, UpdateUserRequest
from app.services.email_service import send_user_invite_email
from app.services.app_storage import delete_connection_profile, initialize_app_storage, is_enabled, list_connection_profiles, save_connection_profile
from app.services.user_store import authenticate_user, create_user, delete_user, list_users, update_user


load_environment()
load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env.rag", override=False)
initialize_app_storage()

router = APIRouter()


def _ensure_admin(actor_role: str) -> None:
    if str(actor_role or "").strip().lower() != "admin":
        raise HTTPException(status_code=403, detail="Only admins can access this option.")


def _ensure_authenticated_role(actor_role: str) -> str:
    normalized = str(actor_role or "").strip().lower()
    if normalized not in {"admin", "operator", "viewer"}:
        raise HTTPException(status_code=401, detail="Invalid session role.")
    return normalized


@router.post("/auth/login")
def login_user(payload: LoginRequest):
    if not is_enabled():
        raise HTTPException(status_code=503, detail="Application MySQL storage is not configured.")
    try:
        user = authenticate_user(payload.username, payload.password)
    except RuntimeError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password.")
    return {
        "status": "success",
        "item": user,
    }


@router.get("/users")
def get_users(actor_role: str = Query(...)):
    _ensure_admin(actor_role)
    try:
        return {
            "status": "success",
            "items": list_users(),
        }
    except RuntimeError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@router.post("/users")
def create_user_account(payload: CreateUserRequest):
    _ensure_admin(payload.actor_role)
    try:
        created_user = create_user(payload.username, payload.password, payload.email, payload.role)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except RuntimeError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    login_url = f"{os.getenv('FRONTEND_BASE_URL', 'http://localhost:3000').rstrip('/')}/login"
    email_sent, message = send_user_invite_email(
        recipient_email=created_user["email"],
        username=created_user["username"],
        role_label=created_user["role_label"],
        login_url=login_url,
    )
    return {
        "status": "success",
        "message": message,
        "item": created_user,
        "email_sent": email_sent,
        "login_url": login_url,
    }


@router.put("/users/{username}")
def update_user_account(username: str, payload: UpdateUserRequest):
    _ensure_admin(payload.actor_role)
    try:
        updated_user = update_user(username, payload.email, payload.role)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except RuntimeError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    return {
        "status": "success",
        "message": "User updated successfully.",
        "item": updated_user,
    }


@router.delete("/users/{username}")
def delete_user_account(username: str, actor_role: str = Query(...)):
    _ensure_admin(actor_role)
    try:
        delete_user(username)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except RuntimeError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    return {
        "status": "success",
        "message": "User deleted successfully.",
    }


def _call_ai_chat(payload: AIChatRequest):
    if not str(payload.message or "").strip():
        raise HTTPException(status_code=400, detail="Message is required.")
    api_key = (
        os.getenv("GROK_API_KEY")
        or os.getenv("RAG_AGENT_API_KEY")
        or os.getenv("OPENAI_API_KEY")
    )
    if not api_key:
        raise HTTPException(status_code=500, detail="API key missing")

    print("API KEY:", api_key[:5])
    print("User Input:", payload.message)

    try:
        import requests

        response = requests.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": os.getenv("RAG_AGENT_MODEL") or "grok-beta",
                "messages": [
                    {"role": "user", "content": str(payload.message or "").strip()}
                ],
            },
            timeout=60,
        )
    except Exception as error:
        raise HTTPException(status_code=502, detail=f"AI chat request failed: {error}") from error

    print("Response:", response.text)

    try:
        body = response.json()
    except ValueError as error:
        raise HTTPException(status_code=502, detail="AI chat returned invalid JSON.") from error

    if not response.ok:
        raise HTTPException(status_code=response.status_code, detail=body)
    return body


@router.post("/ai/chat")
def ai_chat(payload: AIChatRequest):
    return _call_ai_chat(payload)


@router.post("/ai-chat")
def ai_chat_compat(payload: AIChatRequest):
    return _call_ai_chat(payload)


@router.get("/connections")
def get_connections(actor_role: str = Query(...)):
    _ensure_authenticated_role(actor_role)
    try:
        return {
            "status": "success",
            "items": list_connection_profiles(),
        }
    except RuntimeError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@router.post("/connections")
def save_connection(payload: ConnectionProfileRequest):
    _ensure_admin(payload.actor_role)
    try:
        item = save_connection_profile(
            profile_id=payload.id,
            name=payload.name,
            engine=payload.engine,
            fields=payload.fields,
            actor_username=payload.actor_username,
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except RuntimeError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    return {
        "status": "success",
        "message": "Connection profile saved successfully.",
        "item": item,
    }


@router.delete("/connections/{profile_id}")
def remove_connection(profile_id: str, actor_role: str = Query(...)):
    _ensure_admin(actor_role)
    try:
        delete_connection_profile(profile_id)
    except RuntimeError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    return {
        "status": "success",
        "message": "Connection profile deleted successfully.",
    }
