# dynamodb-quartz
[Quartz](http://quartz-scheduler.org/) [JobStore](http://quartz-scheduler.org/api/2.2.1/index.html?org/quartz/spi/JobStore.html) implementation on DynamoDB
* Uses [DynamoDB v2 Low-level API](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/AboutJava.html) for a good-enough SDK backward compatibility
* Supports [DynamoDB Local](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html)
