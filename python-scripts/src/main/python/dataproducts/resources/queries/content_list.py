def init():
    return """
    {
        "queryType": "select",
        "dataSource": "content-model-snapshot",
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
                    "dimension": "contentType",
                    "values": ["Resource", "ExplanationResource", "FocusSpot", "PracticeQuestionSet", "eTextBook", "LearningOutcomeDefinition", "PracticeResource", "ExperientialResource", "SelfAssess", "CuriosityQuestionSet"]
                },
                {
                    "type": "in",
                    "dimension": "status",
                    "values": ["Live"],
                    "extractionFn": null
                }
            ]
        },
        "aggregations": [],
        "granularity": "all",
        "postAggregations": [],
        "intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00",
        "dimensions": [
            "identifier",
            "board",
            "medium",
            "gradeLevel",
            "subject",
            "name",
            "channel",
            "contentType",
            "mediaType",
            "mimeType",
            "objectType",
            "resourceType",
            "status",
            "author",
            "creator",
            "createdOn",
            "lastPublishedOn",
            "lastSubmittedOn",
            "lastUpdatedOn",
            "me_averageRating",
            "me_totalRatings",
            "me_totalDownloads",
            "me_totalTimeSpentInApp",
            "me_totalTimeSpentInPortal",
            "me_totalTimeSpentInDesktop",
            "me_totalPlaySessionCountInApp",
            "me_totalPlaySessionCountInPortal",
            "me_totalPlaySessionCountInDesktop"
        ],
        "metrics": [
            ""
        ],
        "pagingSpec": {
            "pagingIdentifiers": {},
            "threshold": 5000
        }
    }
    """
