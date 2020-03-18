def init():
    return """
    {
        "queryType": "groupBy",
        "dataSource": "summary-events",
        "granularity": "day",
        "intervals": "$start_date/$end_date",
        "dimensions": [
            "dimensions_pdata_id",
            "object_id"
        ],
        "aggregations": [
            {
                "fieldName": "dimensions_sid",
                "fieldNames": [
                    "dimensions_sid"
                ],
                "type": "count",
                "name": "Number of plays"
            },
            {
                "fieldName": "edata_time_spent",
                "fieldNames": [
                    "edata_time_spent"
                ],
                "type": "doubleSum",
                "name": "Total time spent"
            }
        ],
        "postAggregations": [],
        "filter": {
            "type": "and",
            "fields": [
                {
                    "type": "selector",
                    "dimension": "dimensions_type",
                    "value": "content"
                },
                {
                    "type": "selector",
                    "dimension": "dimensions_mode",
                    "value": "play"
                },
                {
                    "type": "or",
                    "fields": [
                        {
                            "type": "selector",
                            "dimension": "dimensions_pdata_id",
                            "value": "$app"
                        },
                        {
                            "type": "selector",
                            "dimension": "dimensions_pdata_id",
                            "value": "$portal"
                        }
                    ]
                }
            ]
        }
    }
    """