from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import agent_migration
from app.routes import user_management
from app.services.app_storage import initialize_app_storage
from app.services.job_scheduler import start_scheduler, stop_scheduler
from app.services.mysql_job_store import initialize_mysql_job_store

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(agent_migration.router, prefix="/api")
app.include_router(user_management.router, prefix="/api")


@app.on_event("startup")
def startup_scheduler():
    initialize_app_storage()
    initialize_mysql_job_store()
    start_scheduler(agent_migration.rerun_saved_job)


@app.on_event("shutdown")
def shutdown_scheduler():
    stop_scheduler()
