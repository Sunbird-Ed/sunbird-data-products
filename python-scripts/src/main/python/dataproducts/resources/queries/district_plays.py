def init():
    return """
    {
        "queryType": "groupBy",
        "dataSource": {
            "type": "table",
            "name": "summary-events"
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
                    "type": "in",
                    "dimension": "dimensions_pdata_id",
                    "values": [
                        "$app",
                        "$portal"
                    ],
                    "extractionFn": null
                },
                {
                    "type": "selector",
                    "dimension": "dimensions_mode",
                    "value": "play"
                },
                {
                    "type": "selector",
                    "dimension": "dimensions_type",
                    "value": "content"
                },
                {
                    "type": "selector",
                    "dimension": "derived_loc_state",
                    "value": "$state"
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
                "dimension": "dimensions_pdata_id",
                "outputName": "Platform",
                "outputType": "STRING"
            }
        ],
        "aggregations": [
            {
                "type": "count",
                "name": "Number of Content Plays"
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