def init():
    return """
    {
        "queryType": "groupBy",
        "dataSource": "telemetry-events-syncts",
        "dimensions": [
            "dialcode_channel",
            "edata_filters_dialcodes",
            "edata_size"
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
                        "dimension": "edata_filters_dialcodes",
                        "value": null
                    }
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