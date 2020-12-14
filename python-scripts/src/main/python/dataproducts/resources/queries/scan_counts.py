def init():
    return """
    {
        "queryType": "groupBy",
        "dataSource": "telemetry-events-syncts",
        "dimensions": [
            "edata_filters_dialcodes"
        ],
        "aggregations": [
            {
                "type": "count",
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
                }
            ]
        }
    }
    """