# s3-play-plugin

A play plugin for uploading to Amazon S3 - gives you a BodyParser for streaming data to S3


There are to sections:
* example - a play 2.1.1 application
* plugin - an sbt 12 plugin

## Running the example

To set it up you'll need to add the following conf file: `example/conf/amazon-s3.conf` in which you specify `amazonKey` and `amazonSecret`. This will allow it to upload to a bucket.

You'll also need to publish-local in the plugin folder, so that the dependency is available to the play app (not on a internet repo yet).

## Running the tests

As above you'll need to configure `amazonKey`, `amazonSecret` and `testBucket`.
