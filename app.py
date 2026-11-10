import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

app = FastAPI()
templates = Jinja2Templates(directory="templates")


class ChatRoomManager:

  def __init__(self):
    # Active sockets mapped to username: {websocket: username}
    self.active_connections: dict[WebSocket, str] = {}

  async def connect(self, websocket: WebSocket, username: str):
    await websocket.accept()
    self.active_connections[websocket] = username
    # Broadcast arrival to all clients over TCP
    await self.broadcast({
        "type": "system",
        "sender": "System",
        "text": f"{username} joined the chat.",
    })
    await self.broadcast_user_list()

  def disconnect(self, websocket: WebSocket) -> str:
    username = self.active_connections.pop(websocket, "Unknown User")
    return username

  def get_socket_by_username(self, username: str) -> WebSocket | None:
    for ws, user in self.active_connections.items():
      if user == username:
        return ws
    return None

  async def send_personal_message(self, message: dict, websocket: WebSocket):
    try:
      await websocket.send_text(json.dumps(message))
    except Exception:
      pass

  async def broadcast(self, message: dict):
    payload = json.dumps(message)
    dead_connections = []
    for connection in list(self.active_connections.keys()):
      try:
        await connection.send_text(payload)
      except Exception:
        dead_connections.append(connection)

    for dead in dead_connections:
      self.disconnect(dead)

  async def broadcast_user_list(self):
    users = list(self.active_connections.values())
    await self.broadcast({"type": "userlist", "users": users})


manager = ChatRoomManager()


@app.get("/", response_class=HTMLResponse)
async def get(request: Request):
  return templates.TemplateResponse(request=request, name="index.html")


@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
  await manager.connect(websocket, username)
  try:
    while True:
      raw_data = await websocket.receive_text()
      data = json.loads(raw_data)
      event_type = data.get("type")

      # 1. Typing Indicator Event
      if event_type == "typing":
        target = data.get("target", "all")
        typing_payload = {
            "type": "typing",
            "sender": username,
            "target": target,
        }
        if target == "all":
          # Broadcast typing to everyone except sender
          for ws, user in manager.active_connections.items():
            if ws != websocket:
              await manager.send_personal_message(typing_payload, ws)
        else:
          # Send typing indicator solely to the private recipient
          target_ws = manager.get_socket_by_username(target)
          if target_ws:
            await manager.send_personal_message(typing_payload, target_ws)

      # 2. Chat Message Event (Group or One-to-One DM)
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
          # Group message: broadcast to all connected clients
          await manager.broadcast(msg_payload)
        else:
          # One-to-one private DM: deliver only to recipient and sender
          target_ws = manager.get_socket_by_username(target)
          if target_ws:
            await manager.send_personal_message(msg_payload, target_ws)
          # Echo back to sender so it displays in their active DM thread
          await manager.send_personal_message(msg_payload, websocket)

  except WebSocketDisconnect:
    left_user = manager.disconnect(websocket)
    await manager.broadcast({
        "type": "system",
        "sender": "System",
        "text": f"{left_user} left the room.",
    })
    await manager.broadcast_user_list()