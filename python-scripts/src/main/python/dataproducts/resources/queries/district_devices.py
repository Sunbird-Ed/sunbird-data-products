def init():
    return """
    {
        "queryType": "groupBy",
        "dataSource": {
            "type": "table",
            "name": "summary-distinct-counts"
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
                "dimension": "dimensions_pdata_id",
                "outputName": "Platform",
                "outputType": "STRING"
            }
        ],
        "aggregations": [
            {
            "type": "HLLSketchMerge",
            "name": "Unique Devices",
            "fieldName": "unique_devices",
            "lgK": "12",
            "tgtHllType": "HLL_4"
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