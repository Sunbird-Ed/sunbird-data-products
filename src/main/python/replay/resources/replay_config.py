def init():
    return {
        "ingest": {
            "outputKafkaTopic": "telemetry.ingest",
            "inputPrefix": "ingest",
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
                    "prefix": "telemetry-denormalized/raw"
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
            "outputKafkaTopic": "telemetry.raw",
            "inputPrefix": "raw",
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
                    "prefix": "telemetry-denormalized/raw"
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
            "outputKafkaTopic": "telemetry.unique",
            "inputPrefix": "unique",
            "dependentSinkSources": [
                {
                    "type": "azure",
                    "prefix": "channel"
                },
                {
                    "type": "azure",
                    "prefix": "telemetry-denormalized/raw"
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
        "telemetry-denorm": {
            "outputKafkaTopic": "telemetry.denorm",
            "inputPrefix": "telemetry-denormalized/raw",
            "dependentSinkSources": [
                {
                    "type": "druid",
                    "prefix": "telemetry-events"
                },
                {
                    "type": "druid",
                    "prefix": "telemetry-feedback-events"
                }
            ]
        },
        "summary-denorm": {
            "outputKafkaTopic": "telemetry.denorm",
            "inputPrefix": "telemetry-denormalized/summary",
            "dependentSinkSources": [
                {
                    "type": "druid",
                    "prefix": "summary-events"
                }
            ]
        },
        "failed": {
            "outputKafkaTopic": "telemetry.raw",
            "inputPrefix": "failed",
            "dependentSinkSources": [
                
            ]
        },
        "wfs": {
            "outputKafkaTopic": "telemetry.derived",
            "inputPrefix": "derived/wfs",
            "dependentSinkSources": [
                {
                    "type": "azure",
                    "prefix": "channel"
                },
                {
                    "type": "azure",
                    "prefix": "telemetry-denormalized/summary"
                },
                {
                    "type": "druid",
                    "prefix": "summary-events"
                }
            ]
        }    
    }