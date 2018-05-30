# robopoint - gateway to robocloud for simple devices

As the headline suggests the purpose of this web app is to provide easier access to robocloud
for devices for which it would be too difficult to interact directly. Here are a few concrete
examples:
 * A simple unit based on an ESP chip is sending meteorology measurement data
   to robocloud for further processing.
 * Robotic mouse that is following a line is seeking a buddy to get help to find 
   the lost line.
 * Two maze solvers want to share info on the shortest track to get through a maze.
 
## Installation

* Make sure you read the robocloud README.md file first
(https://github.com/martinhej/robocloud/blob/master/README.md) to learn
about the basics.

* In your symfony project run:
```
composer require martinhej/robopoint
```
* Copy the `robopoint.yaml.dist` to `robopoint.yaml`. You can also move
a copy of the file into desired `packages` directory to handle environment
specific configuration.

* After addressing Kinesis specific settings (and DynamoDB in case you are going
  to store messages in DynamoDB table) make sure the `message_schema_dir` 
  and `recovery_file` path settings are correct.
  
* Deploy to a server of your choice or use the Symfony server bundle
  `bin/console server:run`.

## Push messages to stream

At this moment the only reference point for available endpoints is the 
`robopoint\Controller\Api\V1\MessageController` class. The REST API is far from what 
it should be and its initial structure is in progress.

POST at the `/messages/` endpoint your first message into the stream. I.e. try
```json
{
  "messages":
  	[
      {
          "roboId":"robogarden",
          "purpose":"garden.water_pump.start",
          "data":{
              "pumpId":"demo",
              "runTime":"3200"
          }
      }
	]
}
```
If you get 200 response it should contain the message id and time:
```json
{
"messages": 
    [
        {
            "messageId": "92089bf40a4ed3a623ba6140fa2a520ab77fcb6d",
            "messageTime": "2018-05-30T15:13:21+0000"
        }
    ]
}
```

## Read messages

Visit `api/v1/messages/robo1/read-by-purpose/garden.water_pump.start` to read
the message you just sent.

The response contains `messages` and `lag`. The `lag` is the time in microseconds
behind the latest message in the stream.
