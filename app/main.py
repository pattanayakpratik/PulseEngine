from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings
from app.database import init_db
from app.redis_client import get_redis_client, close_redis_client
from app.api.v1.notify import router as notify_router
from app.api.v1.segments import router as segments_router
from app.api.v1.campaigns import router as campaigns_router
from app.api.v1.metrics import router as metrics_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: ensure tables exist and Redis is reachable
    print(f"⚡ Starting PulseEngine in [{settings.ENVIRONMENT}] environment...")
    try:
        await init_db()
        print("✅ PostgreSQL database models synchronized.")
    except Exception as e:
        print(f"⚠️ Warning: DB connection during startup deferred: {e}")

    try:
        redis = get_redis_client()
        await redis.ping()
        print("✅ Redis broker connection established.")
    except Exception as e:
        print(f"⚠️ Warning: Redis connection during startup deferred: {e}")

    yield

    # Shutdown
    print("🛑 Shutting down PulseEngine connections...")
    await close_redis_client()


app = FastAPI(
    title="PulseEngine",
    description="Distributed Multi-Channel Notification & Dynamic Segmentation Infrastructure",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS Middleware for Next.js 16 Dashboard & Portfolio Integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register API Routers
app.include_router(notify_router)
app.include_router(segments_router)
app.include_router(campaigns_router)
app.include_router(metrics_router)


@app.get("/", tags=["Health"])
async def root():
    return {
        "service": "PulseEngine",
        "status": "healthy",
        "version": "1.0.0",
        "docs": "/docs",
    }


@app.get("/health", tags=["Health"])
async def health():
    return {"status": "ok", "environment": settings.ENVIRONMENT}
