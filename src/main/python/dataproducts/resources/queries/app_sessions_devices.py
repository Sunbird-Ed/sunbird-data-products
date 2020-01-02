def init():
    return """
    {
        "queryType": "groupBy",
        "dataSource": "summary-events",
        "aggregations": [
            {
                "fieldName": "dimensions_sid",
                "fieldNames": [
                    "dimensions_sid"
                ],
                "type": "count",
                "name": "Total App Sessions"
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
            "2019-12-08T00:00:00+00:00/2019-12-09T00:00:00+00:00",
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
                },
                {
                    "type": "bound",
                    "dimension": "context_date_range_to",
                    "lower": null,
                    "lowerStrict": false,
                    "upper": $epoch_to,
                    "upperStrict": true,
                    "alphaNumeric": true
                },
                {
                    "type": "bound",
                    "dimension": "context_date_range_to",
                    "lower": $epoch_from,
                    "lowerStrict": true,
                    "upper": null,
                    "upperStrict": false,
                    "alphaNumeric": true
                }
            ]
        },
        "pagingSpec": {
            "pagingIdentifiers": {},
            "threshold": 10000
        }
    }
    """