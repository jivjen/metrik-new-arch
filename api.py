from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from research import process_research, get_job_status

app = FastAPI()

class ResearchRequest(BaseModel):
    user_input: str

@app.post("/trigger_research")
async def trigger_research(request: ResearchRequest, background_tasks: BackgroundTasks):
    job_id = process_research(request.user_input)
    background_tasks.add_task(process_research, request.user_input)
    return {"job_id": job_id}

@app.get("/poll_status/{job_id}")
async def poll_status(job_id: str):
    return get_job_status(job_id)
