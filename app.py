import json
from typing import Dict, List, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

app = FastAPI()
templates = Jinja2Templates(directory="templates")


class ChatRoomManager:
    def __init__(self) -> None:
        self.active_connections: Dict[WebSocket, str] = {}

    async def connect(self, websocket: WebSocket, username: str) -> None:
        await websocket.accept()
        self.active_connections[websocket] = username
        await self.broadcast({
            "type": "system",
            "sender": "System",
            "text": f"{username} joined the chat.",
        })
        await self.broadcast_user_list()

    def disconnect(self, websocket: WebSocket) -> str:
        username = self.active_connections.pop(websocket, "Unknown User")
        return username

    def get_socket_by_username(self, username: str) -> Optional[WebSocket]:
        for ws, user in self.active_connections.items():
            if user == username:
                return ws
        return None

    async def send_personal_message(self, message: dict, websocket: WebSocket) -> None:
        try:
            await websocket.send_text(json.dumps(message))
        except Exception:
            pass

    async def broadcast(self, message: dict) -> None:
        payload = json.dumps(message)
        dead_connections: List[WebSocket] = []
        for connection in list(self.active_connections.keys()):
            try:
                await connection.send_text(payload)
            except Exception:
                dead_connections.append(connection)

        for dead in dead_connections:
            self.disconnect(dead)

    async def broadcast_user_list(self) -> None:
        users = list(self.active_connections.values())
        await self.broadcast({"type": "userlist", "users": users})


manager = ChatRoomManager()


@app.get("/", response_class=HTMLResponse)
async def get(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str) -> None:
    await manager.connect(websocket, username)
    try:
        while True:
            raw_data = await websocket.receive_text()

            try:
                data = json.loads(raw_data)
            except (json.JSONDecodeError, TypeError):
                data = {
                    "type": "message",
                    "target": "all",
                    "content": raw_data,
                    "kind": "text"
                }

            event_type = data.get("type")

            if event_type == "typing":
                target = data.get("target", "all")
                typing_payload = {
                    "type": "typing",
                    "sender": username,
                    "target": target,
                }
                if target == "all":
                    for ws in manager.active_connections:
                        if ws != websocket:
                            await manager.send_personal_message(typing_payload, ws)
                else:
                    target_ws = manager.get_socket_by_username(target)
                    if target_ws and target_ws != websocket:
                        await manager.send_personal_message(typing_payload, target_ws)

            elif event_type == "message":
                target = data.get("target", "all")
                content = data.get("content", "")
                kind = data.get("kind", "text")

                msg_payload = {
                    "type": "message",
                    "sender": username,
                    "target": target,
                    "kind": kind,
                    "content": content,
                }

                if target == "all":
                    await manager.broadcast(msg_payload)
                else:
                    target_ws = manager.get_socket_by_username(target)
                    if target_ws:
                        await manager.send_personal_message(msg_payload, target_ws)
                    if target_ws != websocket:
                        await manager.send_personal_message(msg_payload, websocket)

    except WebSocketDisconnect:
        left_user = manager.disconnect(websocket)
        await manager.broadcast({
            "type": "system",
            "sender": "System",
            "text": f"{left_user} left the room.",
        })
        await manager.broadcast_user_list()