def init():
    return """
    {
        "queryType": "groupBy",
        "dataSource": "telemetry-events",
        "dimensions": [
            "object_id"
        ],
        "aggregations": [
            {
                "type": "count",
                "name": "count"
            }
        ],
        "granularity": "all",
        "postAggregations": [],
        "intervals": "$start_date/$end_date",
        "filter": {
            "type": "and",
            "fields": [
                {
                    "type": "not",
                    "field": {
                        "type": "selector",
                        "dimension": "object_id",
                        "value": null
                    }
                },
                {
                    "type": "selector",
                    "dimension": "eid",
                    "value": "INTERACT"
                },
                {
                    "type": "selector",
                    "dimension": "edata_subtype",
                    "value": "ContentDownload-Success"
                },
                {
                    "type": "selector",
                    "dimension": "context_pdata_id",
                    "value": "$app"
                }
            ]
        },
        "pagingSpec": {
            "pagingIdentifiers": {},
            "threshold": 10000
        }
    }
    """