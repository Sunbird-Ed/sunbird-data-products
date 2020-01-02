def init():
    return """
    {
        "queryType": "groupBy",
        "dataSource": "summary-events",
        "dimensions": [
            "dimensions_pdata_id",
            "dimensions_did",
            "object_rollup_l1"
        ],
        "aggregations": [
            {
                "fieldName": "dimensions_sid",
                "fieldNames": [
                    "dimensions_sid"
                ],
                "type": "count",
                "name": "Total Content Plays"
            },
            {
                "fieldName": "edata_time_spent",
                "fieldNames": [
                    "edata_time_spent"
                ],
                "type": "doubleSum",
                "name": "Content Play Time"
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
                    "type": "not",
                    "field": {
                        "type": "selector",
                        "dimension": "object_rollup_l1",
                        "value": null
                    }
                },
                {
                    "type": "and",
                    "fields": [
                        {
                            "type": "selector",
                            "dimension": "dimensions_mode",
                            "value": "play"
                        },
                        {
                            "type": "and",
                            "fields": [
                                {
                                    "type": "or",
                                    "fields": [
                                        {
                                            "type": "selector",
                                            "dimension": "dimensions_type",
                                            "value": "content"
                                        },
                                        {
                                            "type": "selector",
                                            "dimension": "dimensions_type",
                                            "value": "app"
                                        }
                                    ]
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
                }
            ]
        },
        "pagingSpec": {
            "pagingIdentifiers": {},
            "threshold": 10000
        }
    }
    """