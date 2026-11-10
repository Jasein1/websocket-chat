import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

app = FastAPI()
templates = Jinja2Templates(directory="templates")


class ChatRoomManager:

  def __init__(self):
    # Map active WebSocket connections to usernames
    self.active_connections: dict[WebSocket, str] = {}

  async def connect(self, websocket: WebSocket, username: str):
    await websocket.accept()
    self.active_connections[websocket] = username
    # Broadcast join event to all connected users
    await self.broadcast({
        "sender": "System",
        "text": f"{username} joined the chat.",
        "type": "system",
    })
    await self.broadcast_user_list()

  def disconnect(self, websocket: WebSocket) -> str:
    username = self.active_connections.pop(websocket, "Unknown User")
    return username

  async def broadcast(self, message: dict):
    payload = json.dumps(message)
    dead_connections = []
    for connection in self.active_connections:
      try:
        await connection.send_text(payload)
      except Exception:
        dead_connections.append(connection)

    for dead in dead_connections:
      self.disconnect(dead)

  async def broadcast_user_list(self):
    users = list(self.active_connections.values())
    payload = json.dumps({"type": "userlist", "users": users})
    for connection in self.active_connections:
      try:
        await connection.send_text(payload)
      except Exception:
        pass


manager = ChatRoomManager()


@app.get("/", response_class=HTMLResponse)
async def get(request: Request):
  # Correct signature: pass request and template name explicitly
  return templates.TemplateResponse(request=request, name="index.html")


@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
  await manager.connect(websocket, username)
  try:
    while True:
      data = await websocket.receive_text()
      await manager.broadcast(
          {"sender": username, "text": data, "type": "user"}
      )
  except WebSocketDisconnect:
    left_user = manager.disconnect(websocket)
    await manager.broadcast({
        "sender": "System",
        "text": f"{left_user} left the room.",
        "type": "system",
    })
    await manager.broadcast_user_list()