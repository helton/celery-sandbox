import asyncio
from fastapi import FastAPI

app = FastAPI()
sleep_in_secs = 1

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/add")
async def add(x: float, y: float):
    await asyncio.sleep(sleep_in_secs)
    return { "result": x + y }

@app.get("/subtract")
async def subtract(x: float, y: float):
    await asyncio.sleep(sleep_in_secs)
    return { "result": x - y }

@app.get("/multiply")
async def multiply(x: float, y: float):
    await asyncio.sleep(sleep_in_secs)
    return { "result": x * y }

@app.get("/divide")
async def divide(x: float, y: float):
    await asyncio.sleep(sleep_in_secs)
    return { "result": x / y if y != 0 else 0 }

@app.get("/double")
async def add(x: float):
    await asyncio.sleep(sleep_in_secs)
    return { "result": x * 2 }

@app.get("/square")
async def add(x: float):
    await asyncio.sleep(sleep_in_secs)
    return { "result": x * x }
