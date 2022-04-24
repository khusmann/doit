import pytest

from doit.common import (
    Some,
    Omitted,
)

from doit.unsanitizedtable.impl.qualtrics import (
    load_unsanitizedtable_qualtrics,
)

from doit.unsanitizedtable.model import (
    UnsanitizedColumnId,
    UnsanitizedTable,
)

def test_qualtrics_load(qualtrics_table: UnsanitizedTable):

    test_column = [row.get(UnsanitizedColumnId('Q4')) for row in qualtrics_table.data.rows]

    expected_column = [
        Some('First response'),
        Some('Submission 2'),
        Some('Submission 3'),
        Some('Test Omitted values'),
        Some('Test Omitted Values 2'),
        Some('Some items not displayed?'),
        Some('Some items not displayed2'),
    ]

    assert test_column == expected_column

def test_qualtrics_load2(qualtrics_table: UnsanitizedTable):

    test_column = [row.get(UnsanitizedColumnId('Q5')) for row in qualtrics_table.data.rows]

    expected_column = [
        Some('10.5'),
        Some('24'),
        Omitted(),
        Omitted(),
        Omitted(),
        Some('10'),
        Omitted(),
    ]

    assert test_column == expected_column

@pytest.fixture
def qualtrics_table():
    return load_unsanitizedtable_qualtrics(SCHEMA_JSON, DATA_JSON)

SCHEMA_JSON = '{"$schema": "https://json-schema.org/draft/2019-09/schema", "$id": "http://qualtrics.com/API/v3/surveys/SV_0ojG9qk3wyQw1ro/response-schema", "title": "Test Survey (SV_0ojG9qk3wyQw1ro) - Survey Response", "description": "This is a schema for survey responses from SV_0ojG9qk3wyQw1ro", "type": "object", "required": ["values", "displayedFields", "displayedValues", "labels", "responseId"], "properties": {"displayedFields": {"type": "array", "description": "A list of questions or question rows that were displayed to the survey respondent", "items": {"type": "string"}}, "displayedValues": {"type": "object", "description": "A list of the possible answers shown to the survey respondent for each question or question row", "additionalProperties": {"type": "array"}}, "labels": {"type": "object", "description": "The labels of the answers given by the survey respondent for each question or question row", "additionalProperties": true}, "responseId": {"type": "string"}, "values": {"type": "object", "description": "The answers given by the survey respondent", "properties": {"QID3_DO": {"description": "Multiselect Prompt - Display Order", "dataType": "question", "type": "array", "items": {"type": "string", "oneOf": [{"label": "Multiselect Option 1", "const": "1"}, {"label": "Multiselect Option 2", "const": "2"}, {"label": "Multiselect Option 3", "const": "3"}]}, "exportTag": "Q3_DO", "questionId": "QID3"}, "QID5_DO": {"description": "Number Entry Prompt - Display Order", "dataType": "question", "type": "array", "items": {"type": "string"}, "exportTag": "Q5_DO", "questionId": "QID5"}, "endDate": {"description": "End Date", "dataType": "metadata", "type": "string", "format": "date-time", "exportTag": "EndDate"}, "QID1_DO": {"description": "Multiple Choice Prompt - Display Order", "dataType": "question", "type": "array", "items": {"type": "string", "oneOf": [{"label": "Multiple Choice Option 1", "const": "1"}, {"label": "Multiple Choice Option 2", "const": "2"}, {"label": "Multiple Choice Option 3", "const": "3"}, {"label": "Multiple Choice Option 4", "const": "4"}]}, "exportTag": "Q1_DO", "questionId": "QID1"}, "userLanguage": {"description": "User Language", "dataType": "metadata", "type": "string", "exportTag": "UserLanguage"}, "duration": {"description": "Duration (in seconds)", "dataType": "metadata", "type": "number", "exportTag": "Duration (in seconds)"}, "locationLongitude": {"description": "Location Longitude", "dataType": "metadata", "type": "string", "exportTag": "LocationLongitude"}, "distributionChannel": {"description": "Distribution Channel", "dataType": "metadata", "type": "string", "exportTag": "DistributionChannel"}, "recipientEmail": {"description": "Recipient Email", "dataType": "metadata", "type": "string", "exportTag": "RecipientEmail"}, "QID1": {"description": "Multiple Choice Prompt", "dataType": "question", "type": "number", "oneOf": [{"label": "Multiple Choice Option 1", "const": 1}, {"label": "Multiple Choice Option 2", "const": 2}, {"label": "Multiple Choice Option 3", "const": 3}, {"label": "Multiple Choice Option 4", "const": 4}], "exportTag": "Q1", "questionId": "QID1"}, "QID6_1": {"description": "Likert Grid Prompt - Statement 1", "dataType": "question", "type": "number", "oneOf": [{"label": "Dislike a great deal", "const": 1}, {"label": "Dislike somewhat", "const": 2}, {"label": "Neither like nor dislike", "const": 3}, {"label": "Like somewhat", "const": 4}, {"label": "Like a great deal", "const": 5}], "exportTag": "Q6_1", "questionId": "QID6"}, "QID6_2": {"description": "Likert Grid Prompt - Statement 2", "dataType": "question", "type": "number", "oneOf": [{"label": "Dislike a great deal", "const": 1}, {"label": "Dislike somewhat", "const": 2}, {"label": "Neither like nor dislike", "const": 3}, {"label": "Like somewhat", "const": 4}, {"label": "Like a great deal", "const": 5}], "exportTag": "Q6_2", "questionId": "QID6"}, "recipientLastName": {"description": "Recipient Last Name", "dataType": "metadata", "type": "string", "exportTag": "RecipientLastName"}, "QID3": {"description": "Multiselect Prompt", "dataType": "question", "type": "array", "items": {"type": "string", "oneOf": [{"label": "Multiselect Option 1", "const": "1"}, {"label": "Multiselect Option 2", "const": "2"}, {"label": "Multiselect Option 3", "const": "3"}]}, "exportTag": "Q3", "questionId": "QID3"}, "QID6_3": {"description": "Likert Grid Prompt - Statement 3", "dataType": "question", "type": "number", "oneOf": [{"label": "Dislike a great deal", "const": 1}, {"label": "Dislike somewhat", "const": 2}, {"label": "Neither like nor dislike", "const": 3}, {"label": "Like somewhat", "const": 4}, {"label": "Like a great deal", "const": 5}], "exportTag": "Q6_3", "questionId": "QID6"}, "QID6_DO": {"description": "Likert Grid Prompt - Display Order", "dataType": "question", "type": "array", "items": {"type": "string", "oneOf": [{"label": "Statement 1", "const": "1"}, {"label": "Statement 2", "const": "2"}, {"label": "Statement 3", "const": "3"}]}, "exportTag": "Q6_DO", "questionId": "QID6"}, "QID2": {"description": "Multiple Choice Prompt2", "dataType": "question", "type": "number", "oneOf": [{"label": "custom_name_1", "const": 1}, {"label": "custom_name_2", "const": 2}, {"label": "custom_name_3", "const": 3}, {"label": "custom_name_4", "const": 4}], "exportTag": "Q2", "questionId": "QID2"}, "QID4_DO": {"description": "Text Entry Prompt - Display Order", "dataType": "question", "type": "array", "items": {"type": "string"}, "exportTag": "Q4_DO", "questionId": "QID4"}, "QID2_DO": {"description": "Multiple Choice Prompt2 - Display Order", "dataType": "question", "type": "array", "items": {"type": "string", "oneOf": [{"label": "custom_name_1", "const": "1"}, {"label": "custom_name_2", "const": "2"}, {"label": "custom_name_3", "const": "3"}, {"label": "custom_name_4", "const": "4"}]}, "exportTag": "Q2_DO", "questionId": "QID2"}, "recipientFirstName": {"description": "Recipient First Name", "dataType": "metadata", "type": "string", "exportTag": "RecipientFirstName"}, "QID4_TEXT": {"description": "Text Entry Prompt", "dataType": "question", "type": "string", "exportTag": "Q4", "questionId": "QID4"}, "recordedDate": {"description": "Recorded Date", "dataType": "metadata", "type": "string", "format": "date-time", "exportTag": "RecordedDate"}, "ipAddress": {"description": "IP Address", "dataType": "metadata", "type": "string", "exportTag": "IPAddress"}, "locationLatitude": {"description": "Location Latitude", "dataType": "metadata", "type": "string", "exportTag": "LocationLatitude"}, "finished": {"description": "Finished", "dataType": "metadata", "type": "number", "oneOf": [{"label": "False", "const": 0}, {"label": "True", "const": 1}], "exportTag": "Finished"}, "externalDataReference": {"description": "External Data Reference", "dataType": "metadata", "type": "string", "exportTag": "ExternalReference"}, "QID5_TEXT": {"description": "Number Entry Prompt", "dataType": "question", "type": "number", "exportTag": "Q5", "questionId": "QID5"}, "progress": {"description": "Progress", "dataType": "metadata", "type": "number", "exportTag": "Progress"}, "startDate": {"description": "Start Date", "dataType": "metadata", "type": "string", "format": "date-time", "exportTag": "StartDate"}, "status": {"description": "Response Type", "dataType": "metadata", "type": "number", "oneOf": [{"label": "IP Address", "const": 0}, {"label": "Survey Preview", "const": 1}, {"label": "Survey Test", "const": 2}, {"label": "Imported", "const": 4}, {"label": "Spam", "const": 8}, {"label": "Survey Preview Spam", "const": 9}, {"label": "Imported Spam", "const": 12}, {"label": "Offline", "const": 16}, {"label": "Offline Survey Preview", "const": 17}, {"label": "EX", "const": 32}, {"label": "EX Spam", "const": 40.0}, {"label": "EX Offline", "const": 48}], "exportTag": "Status"}}}}}'
DATA_JSON = '{"responses":[{"responseId":"R_3fZ6aDWkf6E9Tjq","values":{"startDate":"2022-04-22T23:54:04Z","endDate":"2022-04-22T23:54:33Z","status":0,"ipAddress":"71.58.88.221","progress":100,"duration":29,"finished":1,"recordedDate":"2022-04-22T23:54:34.201Z","_recordId":"R_3fZ6aDWkf6E9Tjq","locationLatitude":"40.8103","locationLongitude":"-77.888","distributionChannel":"anonymous","userLanguage":"EN","QID2":2,"QID3":["1","3"],"QID1":1,"QID4_TEXT":"First response","QID5_TEXT":10.5,"QID6_1":1,"QID6_2":2,"QID6_3":3},"labels":{"status":"IP Address","finished":"True","QID2":"custom_name_2","QID3":["Multiselect Option 1","Multiselect Option 3"],"QID1":"Multiple Choice Option 1","QID6_1":"Dislike a great deal","QID6_2":"Dislike somewhat","QID6_3":"Neither like nor dislike"},"displayedFields":["QID1","QID6_1","QID6_2","QID3","QID6_3","QID2","QID5_TEXT","QID4_TEXT"],"displayedValues":{"QID1":[1,2,3,4],"QID6_1":[1,2,3,4,5],"QID6_2":[1,2,3,4,5],"QID3":["1","2","3"],"QID6_3":[1,2,3,4,5],"QID2":[1,2,3,4]}},{"responseId":"R_Q5lg7vKnugYxXZn","values":{"startDate":"2022-04-22T23:54:37Z","endDate":"2022-04-22T23:54:57Z","status":0,"ipAddress":"71.58.88.221","progress":100,"duration":20,"finished":1,"recordedDate":"2022-04-22T23:54:57.996Z","_recordId":"R_Q5lg7vKnugYxXZn","locationLatitude":"40.8103","locationLongitude":"-77.888","distributionChannel":"anonymous","userLanguage":"EN","QID2":3,"QID3":["2"],"QID1":2,"QID4_TEXT":"Submission 2","QID5_TEXT":24,"QID6_1":5,"QID6_2":4,"QID6_3":3},"labels":{"status":"IP Address","finished":"True","QID2":"custom_name_3","QID3":["Multiselect Option 2"],"QID1":"Multiple Choice Option 2","QID6_1":"Like a great deal","QID6_2":"Like somewhat","QID6_3":"Neither like nor dislike"},"displayedFields":["QID1","QID6_1","QID6_2","QID3","QID6_3","QID2","QID5_TEXT","QID4_TEXT"],"displayedValues":{"QID1":[1,2,3,4],"QID6_1":[1,2,3,4,5],"QID6_2":[1,2,3,4,5],"QID3":["1","2","3"],"QID6_3":[1,2,3,4,5],"QID2":[1,2,3,4]}},{"responseId":"R_wRIWvK0aSgPwqCR","values":{"startDate":"2022-04-22T23:55:00Z","endDate":"2022-04-22T23:55:14Z","status":0,"ipAddress":"71.58.88.221","progress":100,"duration":14,"finished":1,"recordedDate":"2022-04-22T23:55:15.154Z","_recordId":"R_wRIWvK0aSgPwqCR","locationLatitude":"40.8103","locationLongitude":"-77.888","distributionChannel":"anonymous","userLanguage":"EN","QID2":4,"QID3":["3"],"QID1":4,"QID4_TEXT":"Submission 3","QID6_1":5,"QID6_2":1,"QID6_3":5},"labels":{"status":"IP Address","finished":"True","QID2":"custom_name_4","QID3":["Multiselect Option 3"],"QID1":"Multiple Choice Option 4","QID6_1":"Like a great deal","QID6_2":"Dislike a great deal","QID6_3":"Like a great deal"},"displayedFields":["QID1","QID6_1","QID6_2","QID3","QID6_3","QID2","QID5_TEXT","QID4_TEXT"],"displayedValues":{"QID1":[1,2,3,4],"QID6_1":[1,2,3,4,5],"QID6_2":[1,2,3,4,5],"QID3":["1","2","3"],"QID6_3":[1,2,3,4,5],"QID2":[1,2,3,4]}},{"responseId":"R_1hR1C30bD3fA90l","values":{"startDate":"2022-04-23T00:06:57Z","endDate":"2022-04-23T00:07:18Z","status":0,"ipAddress":"71.58.88.221","progress":100,"duration":21,"finished":1,"recordedDate":"2022-04-23T00:07:19.230Z","_recordId":"R_1hR1C30bD3fA90l","locationLatitude":"40.8103","locationLongitude":"-77.888","distributionChannel":"anonymous","userLanguage":"EN","QID2":2,"QID3":[],"QID4_TEXT":"Test Omitted values","QID6_2":3},"labels":{"status":"IP Address","finished":"True","QID2":"custom_name_2","QID3":[],"QID6_2":"Neither like nor dislike"},"displayedFields":["QID1","QID6_1","QID6_2","QID3","QID6_3","QID2","QID5_TEXT","QID4_TEXT"],"displayedValues":{"QID1":[1,2,3,4],"QID6_1":[1,2,3,4,5],"QID6_2":[1,2,3,4,5],"QID3":["1","2","3"],"QID6_3":[1,2,3,4,5],"QID2":[1,2,3,4]}},{"responseId":"R_2YXKu5duPDuKw3k","values":{"startDate":"2022-04-23T00:07:21Z","endDate":"2022-04-23T00:07:29Z","status":0,"ipAddress":"71.58.88.221","progress":100,"duration":7,"finished":1,"recordedDate":"2022-04-23T00:07:29.374Z","_recordId":"R_2YXKu5duPDuKw3k","locationLatitude":"40.8103","locationLongitude":"-77.888","distributionChannel":"anonymous","userLanguage":"EN","QID3":[],"QID4_TEXT":"Test Omitted Values 2"},"labels":{"status":"IP Address","finished":"True","QID3":[]},"displayedFields":["QID1","QID6_1","QID6_2","QID3","QID6_3","QID2","QID5_TEXT","QID4_TEXT"],"displayedValues":{"QID1":[1,2,3,4],"QID6_1":[1,2,3,4,5],"QID6_2":[1,2,3,4,5],"QID3":["1","2","3"],"QID6_3":[1,2,3,4,5],"QID2":[1,2,3,4]}},{"responseId":"R_22FbSD9Pm0rpHS7","values":{"startDate":"2022-04-23T00:16:18Z","endDate":"2022-04-23T00:16:35Z","status":0,"ipAddress":"71.58.88.221","progress":100,"duration":16,"finished":1,"recordedDate":"2022-04-23T00:16:35.371Z","_recordId":"R_22FbSD9Pm0rpHS7","locationLatitude":"40.8103","locationLongitude":"-77.888","distributionChannel":"anonymous","userLanguage":"EN","QID3":["2"],"QID1":3,"QID4_TEXT":"Some items not displayed?","QID5_TEXT":10,"QID6_1":1},"labels":{"status":"IP Address","finished":"True","QID3":["Multiselect Option 2"],"QID1":"Multiple Choice Option 3","QID6_1":"Dislike a great deal"},"displayedFields":["QID1","QID6_1","QID6_2","QID3","QID6_3","QID5_TEXT","QID4_TEXT"],"displayedValues":{"QID1":[1,2,3,4],"QID6_1":[1,2,3,4,5],"QID6_2":[1,2,3,4,5],"QID3":["1","2","3"],"QID6_3":[1,2,3,4,5]}},{"responseId":"R_78Oo2ZWncZtyVVL","values":{"startDate":"2022-04-23T00:17:06Z","endDate":"2022-04-23T00:17:20Z","status":0,"ipAddress":"71.58.88.221","progress":100,"duration":13,"finished":1,"recordedDate":"2022-04-23T00:17:20.350Z","_recordId":"R_78Oo2ZWncZtyVVL","locationLatitude":"40.8103","locationLongitude":"-77.888","distributionChannel":"anonymous","userLanguage":"EN","QID1":1,"QID4_TEXT":"Some items not displayed2","QID6_1":3,"QID6_3":4},"labels":{"status":"IP Address","finished":"True","QID1":"Multiple Choice Option 1","QID6_1":"Neither like nor dislike","QID6_3":"Like somewhat"},"displayedFields":["QID1","QID6_1","QID6_2","QID6_3","QID5_TEXT","QID4_TEXT"],"displayedValues":{"QID1":[1,2,3,4],"QID6_1":[1,2,3,4,5],"QID6_2":[1,2,3,4,5],"QID6_3":[1,2,3,4,5]}}]}'