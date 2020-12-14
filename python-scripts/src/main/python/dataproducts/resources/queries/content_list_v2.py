def init():
    return """
    {
        "queryType": "scan",
        "dataSource": "content-model-snapshot",
        "intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00",
        "filter": {
            "type": "and",
            "fields": [
                {
                    "type": "selector",
                    "dimension": "objectType",
                    "value": "Content"
                },
                {
                    "type": "in",
                    "dimension": "mimeType",
                    "values": []
                },
                {
                    "type": "in",
                    "dimension": "status",
                    "values": ["Live"],
                    "extractionFn": null
                }
            ]
        },
        "columns": [
            "identifier",
            "board",
            "medium",
            "gradeLevel",
            "subject",
            "name",
            "channel",
            "contentType",
            "mimeType",
            "status",
            "creator",
            "createdOn",
            "lastPublishedOn",
            "me_averageRating",
            "me_totalRatings"
        ]
    }
    """
