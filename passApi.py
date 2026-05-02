from fastapi import FastAPI, Request, HTTPException
import os

app = FastAPI()

@app.post("/api/password")
async def validate_password(request: Request):
    body = await request.json()
    password = body.get("password")

    # Validate input
    if not password or not isinstance(password, str):
        raise HTTPException(status_code=400, detail="Password is required")

    # Get password from environment variable
    correct_password = os.getenv("SITE_PASSWORD")

    if not correct_password:
        print("SITE_PASSWORD not set")
        raise HTTPException(status_code=500, detail="Server configuration error")

    # Check password
    if password == correct_password:
        return {"success": True, "message": "Access granted"}
    else:
        raise HTTPException(status_code=401, detail="Invalid password")
