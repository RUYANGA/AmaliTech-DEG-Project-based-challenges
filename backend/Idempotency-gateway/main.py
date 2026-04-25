import os
import time
import json
import hashlib
import asyncio
from uuid import uuid4
from datetime import datetime, timezone, timedelta


from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


from sqlalchemy import (
   create_engine, Column, Integer, Text, String, JSON, TIMESTAMP, func
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker, declarative_base


DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./dev.db")


engine = create_engine(DATABASE_URL, connect_args={
                      "check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {})
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
Base = declarative_base()




class IdempotencyRecord(Base):
   __tablename__ = "idempotency_records"
   id = Column(Integer, primary_key=True, index=True)
   idempotency_key = Column(Text, unique=True, nullable=False, index=True)
   payload_hash = Column(Text, nullable=False)
   status = Column(String(32), nullable=False)
   response_body = Column(JSON, nullable=True)
   response_status = Column(Integer, nullable=True)
   created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
   updated_at = Column(TIMESTAMP(timezone=True),
                       server_default=func.now(), onupdate=func.now())
   expires_at = Column(TIMESTAMP(timezone=True), nullable=True)




def canonical_json(obj: dict) -> str:
   return json.dumps(obj, sort_keys=True, separators=(',', ':'))




def payload_hash_of(obj: dict) -> str:
   canonical = canonical_json(obj)
   return hashlib.sha256(canonical.encode('utf-8')).hexdigest()




app = FastAPI(title="Idempotency Gateway",
             version="0.1.0", openapi_url="/openapi.json")




class PaymentRequest(BaseModel):
   amount: int = Field(..., example=100)
   currency: str = Field("GHS", example="GHS")
   metadata: dict | None = Field(None, example={"order_id": "abc-123"})




class PaymentResponse(BaseModel):
   status: str
   id: str




@app.on_event("startup")
async def on_startup():
    Base.metadata.create_all(bind=engine)
    # start background cleanup task
    asyncio.create_task(_cleanup_expired_task())


IDEMPOTENCY_TTL_DAYS = int(os.getenv("IDEMPOTENCY_TTL_DAYS", "30"))
CLEANUP_INTERVAL_SECONDS = int(os.getenv("CLEANUP_INTERVAL_SECONDS", "86400"))


async def _cleanup_expired_task():
    """Background task that periodically deletes expired idempotency records."""
    while True:
        try:
            session = SessionLocal()
            now = datetime.now(timezone.utc)
            session.query(IdempotencyRecord).filter(IdempotencyRecord.expires_at != None).filter(IdempotencyRecord.expires_at < now).delete(synchronize_session=False)
            session.commit()
        except Exception:
            pass
        finally:
            try:
                session.close()
            except Exception:
                pass
        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)




@app.post(
   "/process-payment",
   response_model=PaymentResponse,
   status_code=201,
   summary="Process Payment",
)
async def process_payment(
   payload: PaymentRequest, idempotency_key: str = Header(..., alias="Idempotency-Key")
):
   if not idempotency_key:
       raise HTTPException(
           status_code=400, detail="Missing Idempotency-Key header")


   phash = payload_hash_of(payload.dict())


   session = SessionLocal()
   try:
       # Try to create the idempotency record as processing
       rec = IdempotencyRecord(
           idempotency_key=idempotency_key,
           payload_hash=phash,
           status='processing',
           expires_at=(datetime.now(timezone.utc) + timedelta(days=IDEMPOTENCY_TTL_DAYS)),
       )
       session.add(rec)
       session.commit()


       # Simulate payment processing
       time.sleep(2)
       txn_id = str(uuid4())
       response_body = {
           "status": f"Charged {payload.amount} {payload.currency}", "id": txn_id}
       response_status = 201


       # Persist response
       rec.response_body = response_body
       rec.response_status = response_status
       rec.status = 'completed'
       rec.updated_at = datetime.now(timezone.utc)
       session.add(rec)
       session.commit()


       return JSONResponse(status_code=response_status, content=response_body)


   except IntegrityError:
       session.rollback()
       existing = session.query(IdempotencyRecord).filter_by(
           idempotency_key=idempotency_key).one_or_none()
       if not existing:
           raise HTTPException(
               status_code=500, detail="Unexpected idempotency error")


       if existing.payload_hash != phash:
           raise HTTPException(
               status_code=409, detail="Idempotency key already used for a different request body.")


       # If still processing, wait for completion (in-flight check)
       wait_seconds = 10
       poll_interval = 0.1
       waited = 0.0
       while existing.status == 'processing' and waited < wait_seconds:
           time.sleep(poll_interval)
           waited += poll_interval
           session.refresh(existing)


       if existing.status == 'processing':
           raise HTTPException(
               status_code=500, detail="Timeout waiting for in-flight request to complete")


       # Return stored response
       headers = {"X-Cache-Hit": "true"}
       return JSONResponse(status_code=existing.response_status or 200, content=existing.response_body or {}, headers=headers)
   finally:
       session.close()



