from src.storage.athena_client import AthenaClient
client = AthenaClient()
print(client.query("SELECT completed_laps, status, count(1) as cnt FROM telemetry_raw WHERE session_id = 'acc_20260418_165038_spa_gt3' GROUP BY completed_laps, status"))
