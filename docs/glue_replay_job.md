# 🔁 Glue Replay Job

The glue job (`taxi_trip_glue_replay.py`) implements:

### ✔ Decimal-safe JSON parsing  
DynamoDB requires numeric precision, so JSON numbers are parsed as `Decimal`.

### ✔ Validation  
Every replayed record must include `trip_id`.

### ✔ Dynamic UpdateExpression generation  
The job builds DynamoDB update expressions dynamically based on fields present.

### ✔ Idempotent updates  
If the same event is replayed multiple times, it simply overwrites the same trip record safely.

### ✔ Safe delete-on-success behavior  
Messages are removed from SQS *only after* DynamoDB write success.

---
