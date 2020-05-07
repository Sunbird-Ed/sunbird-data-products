def init():
    return """
    {
        "queryType": "groupBy",
        "dataSource": {
            "type": "table",
            "name": "telemetry-rollup-syncts"
        },
        "intervals": {
            "type": "intervals",
            "intervals": [
                "$start_date/$end_date"
            ]
        },
        "filter": {
            "type": "and",
            "fields": [
                {
                    "type": "selector",
                    "dimension": "eid",
                    "value": "SEARCH",
                    "extractionFn": null
                },
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
                    "type": "in",
                    "dimension": "context_pdata_id",
                    "values": [
                        "$app",
                        "$portal"
                    ],
                    "extractionFn": null
                },
                {
                    "type": "selector",
                    "dimension": "derived_loc_state",
                    "value": "$state",
                    "extractionFn": null
                }
            ]
        },
        "granularity": {
            "type": "all"
        },
        "dimensions": [
            {
                "type": "default",
                "dimension": "derived_loc_district",
                "outputName": "District",
                "outputType": "STRING"
            },
            {
                "type": "default",
                "dimension": "context_pdata_id",
                "outputName": "Platform",
                "outputType": "STRING"
            }
        ],
        "aggregations": [
            {
                "type": "longSum",
                "fieldName": "total_count",
                "name": "Number of QR Scans"
            }
        ],
        "postAggregations": [],
        "having": null,
        "limitSpec": {
            "type": "NoopLimitSpec"
        },
        "context": {},
        "descending": false
    }
    """