from __future__ import annotations
import json
from typing import Dict, List, Optional, Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request
from starlette.responses import Response

app: FastAPI = FastAPI()
templates: Jinja2Templates = Jinja2Templates(directory="templates")


class ChatRoomManager:
    def __init__(self) -> None:
        self.active_connections: Dict[WebSocket, str] = {}

    async def connect(self, websocket: WebSocket, username: str) -> None:
        await websocket.accept()
        self.active_connections[websocket] = username
        await self.broadcast({
            "type": "system",
            "target": "all",
            "sender": "System",
            "text": f"{username} joined the server.",
            "event": "join"
        })
        await self.broadcast_user_list()

    def disconnect(self, websocket: WebSocket) -> str:
        username: str = self.active_connections.pop(websocket, "Unknown User")
        return username

    def get_socket_by_username(self, username: str) -> Optional[WebSocket]:
        for ws, user in self.active_connections.items():
            if user == username:
                return ws
        return None

    async def send_personal_message(self, message: Dict[str, Any], websocket: WebSocket) -> None:
        try:
            await websocket.send_text(json.dumps(message))
        except Exception:
            pass

    async def broadcast(self, message: Dict[str, Any]) -> None:
        payload: str = json.dumps(message)
        dead_connections: List[WebSocket] = []
        for connection in list(self.active_connections.keys()):
            try:
                await connection.send_text(payload)
            except Exception:
                dead_connections.append(connection)

        for dead in dead_connections:
            self.disconnect(dead)

    async def broadcast_user_list(self) -> None:
        users: List[str] = list(self.active_connections.values())
        payload: str = json.dumps({"type": "userlist", "users": users})
        for connection in list(self.active_connections.keys()):
            try:
                await connection.send_text(payload)
            except Exception:
                pass


manager: ChatRoomManager = ChatRoomManager()


@app.get("/", response_class=HTMLResponse)
async def get(request: Request) -> Response:
    return templates.TemplateResponse(request=request, name="index.html")


@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str) -> None:
    await manager.connect(websocket, username)
    try:
        while True:
            raw_data: str = await websocket.receive_text()

            try:
                data: Dict[str, Any] = json.loads(raw_data)
            except (json.JSONDecodeError, TypeError):
                data = {
                    "type": "message",
                    "target": "all",
                    "content": raw_data,
                    "kind": "text"
                }

            event_type: Optional[str] = data.get("type")

            if event_type == "typing":
                target: str = str(data.get("target", "all"))
                typing_payload: Dict[str, Any] = {
                    "type": "typing",
                    "sender": username,
                    "target": target,
                }
                if target == "all":
                    for ws in manager.active_connections:
                        if ws != websocket:
                            await manager.send_personal_message(typing_payload, ws)
                else:
                    target_ws: Optional[WebSocket] = manager.get_socket_by_username(target)
                    if target_ws and target_ws != websocket:
                        await manager.send_personal_message(typing_payload, target_ws)

            elif event_type == "message":
                target_chat: str = str(data.get("target", "all"))
                content: str = str(data.get("content", ""))
                kind: str = str(data.get("kind", "text"))

                msg_payload: Dict[str, Any] = {
                    "type": "message",
                    "sender": username,
                    "target": target_chat,
                    "kind": kind,
                    "content": content,
                }

                if target_chat == "all":
                    await manager.broadcast(msg_payload)
                else:
                    target_sock: Optional[WebSocket] = manager.get_socket_by_username(target_chat)
                    if target_sock:
                        await manager.send_personal_message(msg_payload, target_sock)
                    if target_sock != websocket:
                        await manager.send_personal_message(msg_payload, websocket)

    except WebSocketDisconnect:
        left_user: str = manager.disconnect(websocket)
        await manager.broadcast({
            "type": "system",
            "target": "all",
            "sender": "System",
            "text": f"{left_user} left the server.",
            "event": "leave"
        })
        await manager.broadcast_user_list()