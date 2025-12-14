
import time
import asyncio
import json
import httpx
from fastapi import BackgroundTasks, FastAPI, Request, HTTPException, Depends, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import Boolean, Column, Integer, String, select, update, delete
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlmodel import SQLModel, Field, Session
from datetime import datetime

engine = create_async_engine(
    "sqlite+aiosqlite:///./tasks.db",
    echo=True
)

async_session = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    
    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)
    
    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

class TaskModel(SQLModel, table=True):
    __tablename__ = "tasks"
    id: Optional[int] = Field(default=None, primary_key=True)
    title: str
    description: str
    done: bool = False
    created_at: datetime = Field(default_factory=datetime.utcnow)

async def get_db():
    async with async_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

app = FastAPI(title="TODO API", version="1.0")

# Фоновая задача для получения данных извне
async def fetch_external_tasks():
    """Фоновая задача для получения задач из внешнего API"""
    try:
        async with httpx.AsyncClient() as client:
            # Используем jsonplaceholder для демонстрации
            response = await client.get('https://jsonplaceholder.typicode.com/todos?_limit=5')
            if response.status_code == 200:
                external_tasks = response.json()
                
                async with async_session() as session:
                    for task_data in external_tasks:
                        # Проверяем, существует ли уже такая задача
                        stmt = select(TaskModel).where(TaskModel.title == task_data['title'])
                        result = await session.execute(stmt)
                        existing_task = result.scalar_one_or_none()
                        
                        if not existing_task:
                            new_task = TaskModel(
                                title=task_data['title'][:50],  # Ограничиваем длину
                                description=f"External task: {task_data.get('title', 'No title')}",
                                done=task_data['completed']
                            )
                            session.add(new_task)
                    
                    await session.commit()
                    
                    # Уведомляем WebSocket клиентов
                    await manager.broadcast(
                        json.dumps({
                            "type": "background_task_completed",
                            "message": f"Added {len(external_tasks)} external tasks",
                            "timestamp": datetime.utcnow().isoformat()
                        })
                    )
                    
    except Exception as e:
        print(f"Error in background task: {e}")
        await manager.broadcast(
            json.dumps({
                "type": "background_task_error",
                "message": str(e),
                "timestamp": datetime.utcnow().isoformat()
            })
        )

# Периодическая фоновая задача
async def periodic_background_task():
    """Периодически запускает фоновую задачу"""
    while True:
        await asyncio.sleep(600)  # Каждые 10 минут
        await fetch_external_tasks()

@app.on_event("startup")
async def on_startup():
    # Создаем таблицы
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    
    # Запускаем периодическую фоновую задачу
    asyncio.create_task(periodic_background_task())
    
    print("Application started")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    print(f"Request to {request.url.path} processed in {process_time:.4f} seconds")
    return response

# Схемы Pydantic
class TaskCreate(BaseModel):
    title: str
    description: str

class TaskUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    done: Optional[bool] = None

class TaskResponse(BaseModel):
    id: int
    title: str
    description: str
    done: bool
    created_at: datetime
    
    class Config:
        from_attributes = True

# REST API endpoints
@app.get("/tasks", response_model=List[TaskResponse])
async def get_tasks(session: AsyncSession = Depends(get_db)):
    """Получить список всех задач"""
    result = await session.execute(select(TaskModel).order_by(TaskModel.created_at.desc()))
    tasks = result.scalars().all()
    return tasks

@app.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(task_id: int, session: AsyncSession = Depends(get_db)):
    """Получить задачу по ID"""
    result = await session.execute(select(TaskModel).where(TaskModel.id == task_id))
    task = result.scalar_one_or_none()
    
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return task

@app.post("/tasks", response_model=TaskResponse, status_code=201)
async def create_task(task: TaskCreate, session: AsyncSession = Depends(get_db)):
    """Создать новую задачу"""
    new_task = TaskModel(
        title=task.title,
        description=task.description,
        done=False
    )
    session.add(new_task)
    await session.commit()
    await session.refresh(new_task)
    
    # Уведомляем WebSocket клиентов
    await manager.broadcast(
        json.dumps({
            "type": "task_created",
            "task_id": new_task.id,
            "title": new_task.title,
            "timestamp": datetime.utcnow().isoformat()
        })
    )
    
    return new_task

@app.patch("/tasks/{task_id}", response_model=TaskResponse)
async def update_task(task_id: int, task_update: TaskUpdate, session: AsyncSession = Depends(get_db)):
    """Частично обновить задачу"""
    # Получаем задачу
    result = await session.execute(select(TaskModel).where(TaskModel.id == task_id))
    task = result.scalar_one_or_none()
    
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    # Обновляем поля
    update_data = task_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(task, field, value)
    
    await session.commit()
    await session.refresh(task)
    
    # Уведомляем WebSocket клиентов
    await manager.broadcast(
        json.dumps({
            "type": "task_updated",
            "task_id": task.id,
            "title": task.title,
            "timestamp": datetime.utcnow().isoformat()
        })
    )
    
    return task

@app.delete("/tasks/{task_id}", status_code=204)
async def delete_task(task_id: int, session: AsyncSession = Depends(get_db)):
    """Удалить задачу"""
    result = await session.execute(select(TaskModel).where(TaskModel.id == task_id))
    task = result.scalar_one_or_none()
    
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    await session.delete(task)
    await session.commit()
    
    # Уведомляем WebSocket клиентов
    await manager.broadcast(
        json.dumps({
            "type": "task_deleted",
            "task_id": task_id,
            "timestamp": datetime.utcnow().isoformat()
        })
    )

# Принудительный запуск фоновой задачи
@app.post("/task-generator/run")
async def run_background_task(background_tasks: BackgroundTasks):
    """Принудительно запустить фоновую задачу"""
    background_tasks.add_task(fetch_external_tasks)
    return {
        "message": "Background task started",
        "timestamp": datetime.utcnow().isoformat()
    }

# WebSocket endpoint для уведомлений о задачах
@app.websocket("/ws/tasks")
async def websocket_tasks(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Отправляем начальное состояние
        await websocket.send_text(
            json.dumps({
                "type": "connected",
                "message": "Connected to tasks WebSocket",
                "timestamp": datetime.utcnow().isoformat()
            })
        )
        
        # Периодически отправляем heartbeat
        async def heartbeat():
            while True:
                await asyncio.sleep(30)
                try:
                    await websocket.send_text(
                        json.dumps({
                            "type": "heartbeat",
                            "timestamp": datetime.utcnow().isoformat()
                        })
                    )
                except:
                    break
        
        heartbeat_task = asyncio.create_task(heartbeat())
        
        # Обрабатываем входящие сообщения
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            if message.get("type") == "ping":
                await websocket.send_text(
                    json.dumps({
                        "type": "pong",
                        "timestamp": datetime.utcnow().isoformat()
                    })
                )
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        heartbeat_task.cancel()
    except Exception as e:
        print(f"WebSocket error: {e}")
        manager.disconnect(websocket)

def blocking_io_task():
    time.sleep(2)
    return "Blocking IO task completed"


def heavy_func(n: int):
    result = 0
    for i in range(n):
        result += i * i
    return result

# Health check
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "websocket_connections": len(manager.active_connections)
    }

# Запуск приложения
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)