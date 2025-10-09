from fastapi import APIRouter
import httpx
import asyncio

router = APIRouter()

task_urls = []

async def call_task(url):
    async with httpx.AsyncClient() as client:
        response = await client.post(url)
        return {url: response.json()}

@router.post("/trigger-task/")
async def trigger_task():
    tasks = [call_task(url) for url in task_urls]
    results = await asyncio.gather(*tasks)
    return {"results": results}