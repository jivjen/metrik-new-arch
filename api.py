from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from research import process_research, get_job_status, stop_job
import threading

app = FastAPI()

class ResearchRequest(BaseModel):
    user_input: str

@app.post("/trigger_research")
async def trigger_research(request: ResearchRequest, background_tasks: BackgroundTasks):
    try:
        job_id = process_research(request.user_input)
        thread = threading.Thread(target=process_research, args=(request.user_input,))
        thread.start()
        return {"job_id": job_id}
    except Exception as e:
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
