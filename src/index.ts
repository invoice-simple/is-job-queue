import * as AWS from 'aws-sdk';
import uuid = require('uuid/v1');

interface SqsMessage {
  MessageId?: string; //'7155df36-53f9-4202-9ec4-1d7635f3b185',
  ReceiptHandle?: string;
  //'AQEBvooHDpYxP5thACOQLM+z2NDNHdKm1SDCjwPWtpsf8QxeaeIgrxnEPSW6eMGIqQhZyp6XSmXNsUWlQLplT6hqd9jBaEYP9hBKnEFdu0B89yOziaKh2banVxXhCO7MMw2Cvn2TbLSJ1XyUrnUSqRi0SWvl9d0ni2fwQ0cISDA5YCSLs5CB+3d541ETpKn7dm1Vvc6mMrYGN6ui53tloei35fYfC0X9/VZRdKrWm0zN1xjohQR2AbQU1N5CFsSWcEX64dJfokX/kltEHXr6xw=',
  MD5OfBody?: string; // 'c4b6f8113e3452ae8cd052bdb3850b46',
  Body?: string; // '{"Test": "Go!!!!"}',
  Attributes?: {
    SenderId?: string; // 'AIDAAV2KPFNQQLNWMSIVW',
    ApproximateFirstReceiveTimestamp?: string; // '1511232241395',
    ApproximateReceiveCount?: string; //'1',
    SentTimestamp?: string; //'1544132219967',
    SequenceNumber?: string; //'18842061522021113616',
    MessageDeduplicationId?: string; //'fgdfg111dghdfhdfgh',
    MessageGroupId?: string; // 'vxcvb11vxbxb',
  };
}

interface SqsMessageResponse {
  // ResponseMetadata: {
  //   RequestId: string; //'01df7aaa-bbe4-5177-b52c-24f6a495d394'
  // };
  Messages?: SqsMessage[];
}

interface ISQueueManagerOptions {
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
}

export class ISQueueManager {
  awsSqs: AWS.SQS;

  constructor(private queueUrl: string, options?: ISQueueManagerOptions) {
    const config: AWS.SQS.ClientConfiguration = {};

    if (options) {
      for (let k of ['region', 'accessKeyId', 'secretAccessKey']) {
        if (k in options) {
          //@ts-ignore
          config[k] = options[k];
        }
      }
    }

    this.awsSqs = new AWS.SQS(config);
  }

  public receiveMessages(nMessagesMax?: number, timeWait?: number) {
    return new Promise<SqsMessage[]>((resolve, reject) => {
      const receiveParams: AWS.SQS.ReceiveMessageRequest = {
        QueueUrl: this.queueUrl /* required */,
        AttributeNames: ['All'],
        MaxNumberOfMessages: nMessagesMax || 10,
        MessageAttributeNames: ['All'], //ReceiveRequestAttemptId: 'STRING_VALUE', // | Policy | VisibilityTimeout | MaximumMessageSize | MessageRetentionPeriod | ApproximateNumberOfMessages | ApproximateNumberOfMessagesNotVisible | CreatedTimestamp | LastModifiedTimestamp | QueueArn | ApproximateNumberOfMessagesDelayed | DelaySeconds | ReceiveMessageWaitTimeSeconds | RedrivePolicy | FifoQueue | ContentBasedDeduplication | KmsMasterKeyId | KmsDataKeyReusePeriodSeconds,
        /* more items */
        /* more items */
        VisibilityTimeout: 30,
        WaitTimeSeconds: timeWait || 10,
      };

      this.awsSqs.receiveMessage(receiveParams, (err, data: SqsMessageResponse) => {
        if (err) {
          reject(err);
        } else {
          resolve(data.Messages || []);
        }
      });
    });
  }

  public sendMessage(body: string) {
    var params = {
      MessageBody: body /* required */,
      QueueUrl: this.queueUrl,
      DelaySeconds: 0,
      //MessageAttributes: { Title: { DataType: 'String' /* required */, StringValue: 'STRING_VALUE' } },
      MessageDeduplicationId: uuid(), // Don't use deduplication features
      MessageGroupId: uuid(), // Process the messages in any order
    };

    return new Promise((resolve, reject) => {
      this.awsSqs.sendMessage(params, function(err, data) {
        if (err) {
          reject(err);
        } else {
          resolve(data);
        }
      });
    });
  }

  /**
   * Send up to 10 messages to SQS
   */
  public sendMessages(messages: Array<{ id: string; body: string }>) {
    var params: AWS.SQS.SendMessageBatchRequest = {
      QueueUrl: this.queueUrl,

      Entries: messages.map(body => {
        const ret: AWS.SQS.SendMessageBatchRequestEntry = {
          Id: body.id,
          MessageBody: body.body /* required */,
          DelaySeconds: 0,
          //MessageAttributes: { Title: { DataType: 'String' /* required */, StringValue: 'STRING_VALUE' } },
          MessageDeduplicationId: uuid(), // Don't use deduplication features
          MessageGroupId: body.id, // Process the messages in any order
        };
        return ret;
      }),
    };

    return new Promise<AWS.SQS.SendMessageBatchResult>((resolve, reject) => {
      this.awsSqs.sendMessageBatch(params, function(err, data) {
        if (err) {
          reject(err);
        } else {
          resolve(data);
        }
      });
    });
  }

  public deleteMessage(messageHandle: string) {
    return new Promise<any>((resolve, reject) => {
      this.awsSqs.deleteMessage(
        {
          QueueUrl: this.queueUrl,
          ReceiptHandle: messageHandle,
        },
        (err, data) => {
          if (err) {
            reject(err);
          } else {
            resolve(data);
          }
        },
      );
    });
  }
}
