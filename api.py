from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from research import process_research, get_job_status

app = FastAPI()

class ResearchRequest(BaseModel):
    user_input: str

@app.post("/trigger_research")
async def trigger_research(request: ResearchRequest, background_tasks: BackgroundTasks):
    try:
        job_id = process_research(request.user_input)
        background_tasks.add_task(process_research, request.user_input)
        return {"job_id": job_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.get("/poll_status/{job_id}")
async def poll_status(job_id: str):
    try:
        return get_job_status(job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
