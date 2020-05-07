def init():
    return """
    {
        "queryType": "groupBy",
        "dataSource": "telemetry-rollup-syncts",
        "dimensions": [
            "dialcode_channel",
            "object_id",
            "edata_size"
        ],
        "aggregations": [
            {
                "type": "longSum",
                "fieldName": "total_count",
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
                },
                {
                    "type": "in",
                    "dimension": "context_pdata_id",
                    "values": [
                        "$app",
                        "$portal"
                    ],
                    "extractionFn": null
                }
            ]
        },
        "pagingSpec": {
            "pagingIdentifiers": {},
            "threshold": 10000
        }
    }
    """