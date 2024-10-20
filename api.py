from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from research import process_research, get_job_status, stop_job
import threading
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        return get_job_status(job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.post("/stop_job/{job_id}")
async def stop_research_job(job_id: str):
    try:
        if stop_job(job_id):
            return {"message": f"Job {job_id} is being stopped."}
        else:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found or not running.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
