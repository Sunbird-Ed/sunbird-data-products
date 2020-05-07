def init():
    return """
    {
        "queryType": "groupBy",
        "dataSource": "summary-rollup-syncts",
        "aggregations": [
            {
                "type": "longSum",
                "name": "Total App Sessions",
                "fieldName": "total_count"
            },
            {
                "fieldName": "dimensions_did",
                "fieldNames": [
                    "dimensions_did"
                ],
                "type": "cardinality",
                "name": "Total Devices on App"
            },
            {
                "fieldName": "edata_time_spent",
                "fieldNames": [
                    "edata_time_spent"
                ],
                "type": "doubleSum",
                "name": "Total Time on App"
            }
        ],
        "granularity": "all",
        "postAggregations": [],
        "intervals": [
            "$start_date/$end_date"
        ],
        "filter": {
            "type": "and",
            "fields": [
                {
                    "type": "selector",
                    "dimension": "dimensions_type",
                    "value": "app"
                },
                {
                    "type": "selector",
                    "dimension": "dimensions_pdata_id",
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