"""
FastAPI application entry point
"""
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from contextlib import asynccontextmanager
from datetime import timedelta
from redis.asyncio import Redis
from fastapi_cache import FastAPICache

from fastapi_cache.backends.redis import RedisBackend
from app.api.endpoints import metadata, dbt, models_enhanced
from app.knowledge_graph.api import endpoints as knowledge_graph_endpoints
from app.knowledge_graph.api import visualizer_endpoints as knowledge_graph_visualizer
from app.core.config import settings
from app.core.security import authenticate_user, create_access_token, fake_users_db
from app.api.error_handlers import setup_exception_handlers
from app.models.config import Token, LoginRequest, User

from app.core.logging import logger
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize Redis cache
    try:
        redis = Redis.from_url(
            settings.REDIS_URL, 
            decode_responses=True
        )
        FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache:")
        logger.info("Redis cache initialized")
    except Exception as e:
        logger.warning(f"Failed to initialize Redis cache: {str(e)}. Continuing without cache.")
    
    yield
    
    # Shutdown event handler
    # Close Redis connection
    if hasattr(FastAPICache, "_backend") and hasattr(FastAPICache._backend, "_redis"):
        await FastAPICache._backend._redis.close()
        logger.info("Redis connection closed")

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="API for automated data modeling and DBT operations",
    version="0.1.0",
    lifespan=lifespan
)

# Set up CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(metadata.router, prefix="/api/metadata", tags=["metadata"])
app.include_router(models_enhanced.router, prefix="/api/models", tags=["models"])
app.include_router(dbt.router, prefix="/api/dbt", tags=["dbt"])
app.include_router(knowledge_graph_endpoints.router, prefix="/api/knowledge-graph", tags=["knowledge-graph"])
app.include_router(knowledge_graph_visualizer.router, prefix="/api/visualize", tags=["visualize"])

# Setup exception handlers
setup_exception_handlers(app)

# Authentication endpoint
@app.post("/api/auth/login", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(fake_users_db, form_data.username, form_data.password)
    if not user:
        logger.warning(f"Failed login attempt for user: {form_data.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Create access token with expiration
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        subject=user.username, expires_delta=access_token_expires
    )
    
    logger.info(f"User {user.username} logged in successfully")
    return {
        "access_token": access_token, 
        "token_type": "bearer",
        "expires_in": settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        "user": User(
            username=user.username,
            email=user.email,
            full_name=getattr(user, 'full_name', None),
            disabled=user.disabled,
            role=user.role
        )
    }

@app.post("/api/auth/token", response_model=Token)
async def login_with_json(login_data: LoginRequest):
    """Login endpoint that accepts JSON body instead of form data"""
    user = authenticate_user(fake_users_db, login_data.username, login_data.password)
    if not user:
        logger.warning(f"Failed login attempt for user: {login_data.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Create access token with expiration
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        subject=user.username, expires_delta=access_token_expires
    )
    
    logger.info(f"User {user.username} logged in successfully")
    return {
        "access_token": access_token, 
        "token_type": "bearer",
        "expires_in": settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        "user": User(
            username=user.username,
            email=user.email,
            full_name=getattr(user, 'full_name', None),
            disabled=user.disabled,
            role=user.role
        )
    }

@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "ok"}


# Debug endpoint (temporary)
@app.post("/api/debug/login", response_model=Token)
async def debug_login(login_data: LoginRequest):
    """Simplified login for debugging only - REMOVE in production"""
    logger.warning(f"Debug login attempt for user: {login_data.username}")
    
    # Hard-coded debug credentials
    if login_data.username == "debugadmin" and login_data.password == "debug123":
        # Create access token with expiration
        access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(
            subject=login_data.username, expires_delta=access_token_expires
        )
        
        logger.info(f"Debug login successful for {login_data.username}")
        return {
            "access_token": access_token, 
            "token_type": "bearer",
            "expires_in": settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            "user": User(
                username=login_data.username,
                email="debug@example.com",
                full_name="Debug Admin",
                disabled=False,
                role="admin"
            )
        }
    else:
        logger.warning(f"Failed debug login attempt for user: {login_data.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect debug credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


# Swagger UI helper endpoint
@app.get("/api/swagger-token/{token}")
async def set_swagger_token(token: str):
    """Helper endpoint to display token for Swagger UI"""
    return {
        "token_type": "Bearer",
        "access_token": token,
        "instructions": "Copy the access_token value and use it in the 'Authorization' field with format: Bearer YOUR_TOKEN"
    }


if __name__ == "__main__":
    import uvicorn
    logger.info("Starting server...")
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
