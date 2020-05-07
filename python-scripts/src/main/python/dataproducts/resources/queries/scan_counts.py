def init():
    return """
    {
        "queryType": "groupBy",
        "dataSource": "telemetry-rollup-syncts",
        "dimensions": [
            "edata_filters_dialcodes"
        ],
        "aggregations": [
            {
                "type": "longSum",
                "fieldName": "total_count",
                "name": "Total Scans"
            }
        ],
        "granularity": "all",
        "postAggregations": [],
        "intervals": "start_date/end_date",
        "filter": {
            "type": "and",
            "fields": [
            {
                "type": "or",
                "fields": [
                {
                    "type": "selector",
                    "dimension": "object_type",
                    "value": "DialCode"
                },
                {
                    "type": "selector",
                    "dimension": "object_type",
                    "value": "dialcode"
                },
                {
                    "type": "selector",
                    "dimension": "object_type",
                    "value": "qr"
                },
                {
                    "type": "selector",
                    "dimension": "object_type",
                    "value": "Qr"
                }
                ]
            },
            {
                "type": "selector",
                "dimension": "eid",
                "value": "SEARCH"
            }
            ]
        }
    }
    """