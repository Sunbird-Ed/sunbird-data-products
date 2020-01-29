def init():
    return {
        "ingest": {
            "outputKafkaTopic": "telemetry.raw",
            "dependentSinkSources": [
                {
                    "type": "azure",
                    "prefix": "raw"
                },
                {
                    "type": "azure",
                    "prefix": "unique"
                },
                {
                    "type": "azure",
                    "prefix": "channel"
                },
                {
                    "type": "azure",
                    "prefix": "telemetry-denormalized"
                },
                {
                    "type": "druid",
                    "prefix": "telemetry-events"
                },
                {
                    "type": "druid",
                    "prefix": "telemetry-log-events"
                },
                {
                    "type": "druid",
                    "prefix": "telemetry-error-events"
                },
                {
                    "type": "druid",
                    "prefix": "telemetry-feedback-events"
                }
            ]
        },
        "raw": {
            "outputKafkaTopic": "telemetry.valid",
            "dependentSinkSources": [
                {
                    "type": "azure",
                    "prefix": "unique"
                },
                {
                    "type": "azure",
                    "prefix": "channel"
                },
                {
                    "type": "azure",
                    "prefix": "telemetry-denormalized"
                },
                {
                    "type": "druid",
                    "prefix": "telemetry-events"
                },
                {
                    "type": "druid",
                    "prefix": "telemetry-log-events"
                },
                {
                    "type": "druid",
                    "prefix": "telemetry-error-events"
                },
                {
                    "type": "druid",
                    "prefix": "telemetry-feedback-events"
                }
            ]
        },
        "unique": {
            "outputKafkaTopic": "telemetry.sink",
            "dependentSinkSources": [
                {
                    "type": "azure",
                    "prefix": "channel"
                },
                {
                    "type": "azure",
                    "prefix": "telemetry-denormalized"
                },
                {
                    "type": "druid",
                    "prefix": "telemetry-events"
                },
                {
                    "type": "druid",
                    "prefix": "telemetry-log-events"
                },
                {
                    "type": "druid",
                    "prefix": "telemetry-error-events"
                },
                {
                    "type": "druid",
                    "prefix": "telemetry-feedback-events"
                }
            ]
        },
        "failed": {
            "outputKafkaTopic": "telemetry.raw",
            "dependentSinkSources": [
                
            ]
        },
        "wfs": {
            "outputKafkaTopic": "telemetry.sink",
            "dependentSinkSources": [
                {
                    "type": "azure",
                    "prefix": "channel"
                },
                {
                    "type": "azure",
                    "prefix": "telemetry-denormalized"
                },
                {
                    "type": "druid",
                    "prefix": "summary-events"
                }
            ]
        }    
    }