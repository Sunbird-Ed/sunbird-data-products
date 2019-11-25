from datetime import datetime, date, timedelta
import json, time, pytz
import sys, os

util_path = os.path.abspath(os.path.join(__file__, '..', '..', '..', 'util'))
sys.path.append(util_path)

from utils import push_metric_event

start_time = int(round(time.time()))
end_time = int(round(time.time()))
time_taken = end_time - start_time
metrics = {
    "system": "AdhocJob",
    "subsystem": "CMO Dashboard",
    "metrics": [
        {
            "metric": "timeTakenSecs",
            "value": time_taken
        },
        {
            "metric": "date",
            "value": date.today().strftime("%Y-%m-%d")
        },
        {
            "metric": "noOfFilesUploaded",
            "value": 0
        }
    ]
}
push_metric_event(metrics, "CMO Dashboard")
