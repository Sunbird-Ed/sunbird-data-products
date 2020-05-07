def init():
    return """
    {
        "queryType": "groupBy",
        "dataSource": "summary-rollup-syncts",
        "dimensions": [
            "dimensions_pdata_id",
            "dimensions_did",
            "object_rollup_l1"
        ],
        "aggregations": [
            {
                "type": "longSum",
                "name": "Total Content Plays",
                "fieldName": "total_count"
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