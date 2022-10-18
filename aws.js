// snippet-start:[sqs.JavaScript.queues.createQueueV3]
// const required AWS SDK clients and commands for Node.js
const {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  SetQueueAttributesCommand,
  DeleteQueueCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand,
} = require('@aws-sdk/client-sqs');
const {
  CreateTopicCommand,
  SubscribeCommand,
  DeleteTopicCommand,
} = require('@aws-sdk/client-sns');
const { SQSClient } = require('@aws-sdk/client-sqs');
const { SNSClient } = require('@aws-sdk/client-sns');
const {
  TextractClient,
  StartDocumentTextDetectionCommand,
  StartDocumentAnalysisCommand,
  GetDocumentAnalysisCommand,
  GetDocumentTextDetectionCommand,
} = require('@aws-sdk/client-textract');
const { stdout } = require('process');

// Set the AWS Region.
const REGION = 'us-east-1'; //e.g. "us-east-1"
// Create SNS service object.
const sqsClient = new SQSClient({ region: REGION });
const snsClient = new SNSClient({ region: REGION });
const textractClient = new TextractClient({ region: REGION });

// Set bucket and video variables
const bucket = 'teste123434534534';
const documentName = 'new.pdf';
const roleArn = 'role-arn';
const processType = 'DETECTION';
var startJobId = '';

var ts = Date.now();
const snsTopicName = 'AmazonTextractExample' + ts;
const snsTopicParams = { Name: snsTopicName };
const sqsQueueName = 'AmazonTextractQueue-' + ts;

// Set the parameters
const sqsParams = {
  QueueName: sqsQueueName, //SQS_QUEUE_URL
  Attributes: {
    DelaySeconds: '60', // Number of seconds delay.
    MessageRetentionPeriod: '86400', // Number of seconds delay.
  },
};

// Process a document based on operation type
const processDocumment = async (
  type,
  bucket,
  videoName,
  roleArn,
  sqsQueueUrl,
  snsTopicArn
) => {
  try {
    // Set job found and success status to false initially
    var jobFound = false;
    var succeeded = false;
    var dotLine = 0;
    var processType = type;
    var validType = false;

    if (processType == 'DETECTION') {
      var response = await textractClient.send(
        new StartDocumentTextDetectionCommand({
          DocumentLocation: { S3Object: { Bucket: bucket, Name: videoName } }
        })
      );
      console.log('Processing type: Detection');
      validType = true;
    }

    if (processType == 'ANALYSIS') {
      var response = await textractClient.send(
        new StartDocumentAnalysisCommand({
          DocumentLocation: { S3Object: { Bucket: bucket, Name: videoName } }
        })
      );
      console.log('Processing type: Analysis');
      validType = true;
    }

    if (validType == false) {
      console.log('Invalid processing type. Choose Detection or Analysis.');
      return;
    }
    // while not found, continue to poll for response
    console.log(`Start Job ID: ${response.JobId}`);


    return await GetResults(
      processType,
      response.JobId
    );
  } catch (err) {
    console.log('Error', err);
  }
};

// Create the SNS topic and SQS Queue
const createTopicandQueue = async () => {
  try {
    // Create SNS topic
    const topicResponse = await snsClient.send(
      new CreateTopicCommand(snsTopicParams)
    );
    const topicArn = topicResponse.TopicArn;
    console.log('Success', topicResponse);
    // Create SQS Queue
    const sqsResponse = await sqsClient.send(new CreateQueueCommand(sqsParams));
    console.log('Success', sqsResponse);
    const sqsQueueCommand = await sqsClient.send(
      new GetQueueUrlCommand({ QueueName: sqsQueueName })
    );
    const sqsQueueUrl = sqsQueueCommand.QueueUrl;
    const attribsResponse = await sqsClient.send(
      new GetQueueAttributesCommand({
        QueueUrl: sqsQueueUrl,
        AttributeNames: ['QueueArn'],
      })
    );
    const attribs = attribsResponse.Attributes;
    console.log(attribs);
    const queueArn = attribs.QueueArn;

    const policy = {
      Version: '2012-10-17',
      Statement: [
        {
          Sid: 'MyPolicy',
          Effect: 'Allow',
          Principal: { AWS: '*' },
          Action: 'SQS:SendMessage',
          Resource: queueArn,
          Condition: {
            ArnEquals: {
              'aws:SourceArn': topicArn,
            },
          },
        },
      ],
    };

    const response = await sqsClient.send(
      new SetQueueAttributesCommand({
        QueueUrl: sqsQueueUrl,
        Attributes: { Policy: JSON.stringify(policy) },
      })
    );
    console.log(response);
    console.log(sqsQueueUrl, topicArn);
    return [sqsQueueUrl, topicArn];
  } catch (err) {
    console.log('Error', err);
  }
};

const deleteTopicAndQueue = async (sqsQueueUrlArg, snsTopicArnArg) => {
  const deleteQueue = await sqsClient.send(
    new DeleteQueueCommand({ QueueUrl: sqsQueueUrlArg })
  );
  const deleteTopic = await snsClient.send(
    new DeleteTopicCommand({ TopicArn: snsTopicArnArg })
  );
  console.log('Successfully deleted.');
};

const displayBlockInfo = async (block) => {
  console.log(`Block ID: ${block.Id}`);
  console.log(`Block Type: ${block.BlockType}`);
  if (String(block).includes(String('EntityTypes'))) {
    console.log(`EntityTypes: ${block.EntityTypes}`);
  }
  if (String(block).includes(String('Text'))) {
    console.log(`EntityTypes: ${block.Text}`);
  }
  if (!String(block.BlockType).includes('PAGE')) {
    console.log(`Confidence: ${block.Confidence}`);
  }
  console.log(`Page: ${block.Page}`);
  if (String(block.BlockType).includes('CELL')) {
    console.log('Cell Information');
    console.log(`Column: ${block.ColumnIndex}`);
    console.log(`Row: ${block.RowIndex}`);
    console.log(`Column Span: ${block.ColumnSpan}`);
    console.log(`Row Span: ${block.RowSpan}`);
    if (String(block).includes('Relationships')) {
      console.log(`Relationships: ${block.Relationships}`);
    }
  }

  console.log('Geometry');
  console.log(`Bounding Box: ${JSON.stringify(block.Geometry.BoundingBox)}`);
  console.log(`Polygon: ${JSON.stringify(block.Geometry.Polygon)}`);

  if (String(block.BlockType).includes('SELECTION_ELEMENT')) {
    console.log('Selection Element detected:');
    if (String(block.SelectionStatus).includes('SELECTED')) {
      console.log('Selected');
    } else {
      console.log('Not Selected');
    }
  }
};

const GetResults = async (processType, JobID) => {
  var maxResults = 1000;
  var paginationToken = null;
  var finished = false;

  var JobStatus = 'IN_PROGRESS'

  while (finished == false && JobStatus === 'IN_PROGRESS') {
    var response = null;
    if (processType == 'ANALYSIS') {
      if (paginationToken == null) {
        response = await textractClient.send(
          new GetDocumentAnalysisCommand({
            JobId: JobID,
            MaxResults: maxResults,
          })
        );
      } else {
        response = textractClient.send(
          new GetDocumentAnalysisCommand({
            JobId: JobID,
            MaxResults: maxResults,
            NextToken: paginationToken,
          })
        );
      }
    }

    if (processType == 'DETECTION') {
      if (paginationToken == null) {
        response = await textractClient.send(
          new GetDocumentTextDetectionCommand({
            JobId: JobID,
            MaxResults: maxResults,
          })
        );
      } else {
        response = textractClient.send(
          new GetDocumentTextDetectionCommand({
            JobId: JobID,
            MaxResults: maxResults,
            NextToken: paginationToken,
          })
        );
      }
      JobStatus = response.JobStatus;
    }

    await new Promise((resolve) => setTimeout(resolve, 5000));
    console.log('Detected Documented Text');
    console.log(response);
    //console.log(Object.keys(response))
    console.log(typeof response);
    var blocks = (await response).Blocks;
    console.log(blocks);
    console.log(typeof blocks);
    var docMetadata = (await response).DocumentMetadata;
    var blockString = JSON.stringify(blocks);
    var parsed = JSON.parse(JSON.stringify(blocks));
    console.log(Object.keys(blocks));
    console.log(`Pages: ${docMetadata.Pages}`);
    blocks.forEach((block) => {
      displayBlockInfo(block);
      console.log();
      console.log();
    });

    //console.log(blocks[0].BlockType)
    //console.log(blocks[1].BlockType)

    if (String(response).includes('NextToken')) {
      paginationToken = response.NextToken;
    } else {
      finished = true;
    }
  }
};

// DELETE TOPIC AND QUEUE
const main = async () => {
  var sqsAndTopic = await createTopicandQueue();
  var process = await processDocumment(
    processType,
    bucket,
    documentName,
    roleArn,
    sqsAndTopic[0],
    sqsAndTopic[1]
  );
  var deleteResults = await deleteTopicAndQueue(sqsAndTopic[0], sqsAndTopic[1]);
};

main();
