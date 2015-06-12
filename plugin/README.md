### s3-play-plugin/plugin

This is the main project to run for the plugin.

#### Run

    sbt
    
#### Test 
Configure amazonKey, amazonSecret and testBucket in src/test/resources/application.conf

This file should not committed to the repo, therefore the .gitignore file
     
     sbt test