# JSON Serde for Hive

## Features

* Full support for arrays, maps and structures
* Automatic column to field mapping using table DDL
* Map keys are case-insensitive for convenience
* Optional ignoring of bad records

## Setup

Compile using `mvn clean package`, or download the release JAR:

    curl -L http://bit.ly/mRYaNB > hive-serde-1.0.jar

Register the JAR with Hive:

    add jar hive-serde-1.0.jar;

## Examples

### Simple Table

Create the table:

    CREATE EXTERNAL TABLE message (
      messageid string,
      messagesize int
    )
    ROW FORMAT SERDE 'com.proofpoint.hive.serde.JsonSerde'
    LOCATION '/tmp/json';

Corresponding JSON record:

    {
      "messageId": "34dd0d3c-f53b-11e0-ac12-d3e782dff199",
      "messageSize": 12345
    }

Notice that the JSON field names can contain upper case characters.

### Ignoring Errors

Create a table and set the `errors.ignore` serde property:

    CREATE EXTERNAL TABLE message (
      messageid string,
      messagesize int
    )
    ROW FORMAT SERDE 'com.proofpoint.hive.serde.JsonSerde'
    WITH SERDEPROPERTIES ('errors.ignore' = 'true')
    LOCATION '/tmp/json';

With the default `errors.ignore` value of `false`, an error in any record
will cause the entire query to fail.

When set to `true`, if a record has errors, then every column for that
record will be `NULL`. This is a limitation of the Hive serde API.
Unfortunately, it is not possible for the serde to cause Hive to skip the
record entirely. However, if you have a column that is never `NULL`, such
as the primary key, you can use this column to filter out bad records:

    SELECT * FROM message WHERE messageid IS NOT NULL;

This logic can be encapsulated into a view:

    CREATE VIEW v_message AS
    SELECT * FROM message WHERE messageid IS NOT NULL;

### Nested Structures

Create the table:

    CREATE EXTERNAL TABLE message (
      messageid string,
      messagesize int,
      sender string,
      recipients array<string>,
      messageparts array<struct<
        extension: string,
        size: int
      >>,
      headers map<string,string>
    )
    ROW FORMAT SERDE 'com.proofpoint.hive.serde.JsonSerde'
    LOCATION '/tmp/json';

Corresponding JSON record:

    {
      "messageId": "34dd0d3c-f53b-11e0-ac12-d3e782dff199",
      "messageSize": 12345,
      "sender": "alice@example.com",
      "recipients": ["joe@example.com", "bob@example.com"],
      "messageParts": [
        {
          "extension": "pdf",
          "size": 4567
        },
        {
          "extension": "jpg",
          "size": 9451
        }
      ],
      "headers": {
        "Received-SPF": "pass",
        "X-Broadcast-Id": "9876"
      }
    }

Query the table:

    SELECT
      messageid,
      recipients[0],
      SIZE(recipients) AS recipient_count,
      messageParts[0].extension,
      headers['received-spf']
    FROM message;
