from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from research import process_research, get_job_status, stop_job
import threading
import logging
from logging.handlers import RotatingFileHandler
import os

# Configure logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
file_handler = RotatingFileHandler("logs/api.log", maxBytes=10*1024*1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

app = FastAPI()

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"An error occurred: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": f"An error occurred: {str(exc)}"}
    )

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

class ResearchRequest(BaseModel):
    user_input: str

@app.post("/trigger_research")
async def trigger_research(request: ResearchRequest, background_tasks: BackgroundTasks):
    try:
        logger.info(f"Received research request: {request.user_input}")
        job_id = process_research(request.user_input)
        thread = threading.Thread(target=process_research, args=(request.user_input,))
        thread.start()
        logger.info(f"Research job started with ID: {job_id}")
        return {"job_id": job_id}
    except Exception as e:
        logger.error(f"Error in trigger_research: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.get("/poll_status/{job_id}")
async def poll_status(job_id: str):
    try:
        logger.info(f"Polling status for job: {job_id}")
        status = get_job_status(job_id)
        logger.info(f"Status for job {job_id}: {status}")
        return status
    except Exception as e:
        logger.error(f"Error in poll_status for job {job_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.post("/stop_job/{job_id}")
async def stop_research_job(job_id: str):
    try:
        logger.info(f"Received request to stop job: {job_id}")
        if stop_job(job_id):
            logger.info(f"Job {job_id} is being stopped.")
            return {"message": f"Job {job_id} is being stopped."}
        else:
            logger.warning(f"Job {job_id} not found or not running.")
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found or not running.")
    except Exception as e:
        logger.error(f"Error in stop_research_job for job {job_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
