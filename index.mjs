import AWS from 'aws-sdk'
console.log('config aws')
import { DynamoDBClient, QueryCommand } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  ScanCommand,
  PutCommand,
  GetCommand,
  DeleteCommand,
} from "@aws-sdk/lib-dynamodb";

// @ts-ignore 
AWS.config.update({region: 'us-east-1'})

const client = new DynamoDBClient({});

const dynamo = DynamoDBDocumentClient.from(client);
// const dynamoDB = new AWS.DynamoDB();

const tableName = "metaStore";


// Change this value to adjust the signed URL's expiration
const URL_EXPIRATION_SECONDS = 300

export const handler = async (event) => {
   return await getUploadURL(event);
};



const getUploadURL = async function (event) {
  const { searchParams } = new URL(`http://localhost/?${event.rawQueryString}`)
  const type = searchParams.get('type')
  const name = searchParams.get('name')
  if (!type || !name) {
    throw new Error('Missing name or type query parameter: ' + event.rawQueryString)
  }
  const result= await metaUploadParams(searchParams, event);
  return result;
}

async function metaUploadParams(searchParams, event) {
  const name = searchParams.get('name')
  const branch = searchParams.get('branch')
  if (!name || !branch) {
    throw new Error('Missing name or branch query parameter: ' + event.rawQueryString)
  }

  // const httpMethod = event.httpMethod ? event.httpMethod : "default";
  const httpMethod=event.requestContext.http.method;
  // console.log("This is the event object",event);
  // console.log(httpMethod);
  if(httpMethod=="PUT")
  {
    const requestBody = JSON.parse(event.body)
    if(requestBody)
    {
       const { data, cid, parents }=requestBody
       if(!data || !cid )
       {
        throw new Error('Missing data or from the metadata:' + event.rawQueryString);
       }

       //name is the partition key and cid is the sort key for the DynamoDB table
       try{
       await dynamo.send(
          new PutCommand({
            TableName: tableName,
            Item: {
              name:name,
              cid:cid,
              data:data
            },
          })
        );
       for (const p of parents) {
         await dynamo.send(
          new DeleteCommand({
            TableName: tableName,
            Key: {
              name:name,
              cid:p
            },
          })
        );
       }
       
       //Now we return the result after the data has been added
       return {
        status: 201,
        body: JSON.stringify({ message: 'Metadata has been added' })
      };
    }
    catch(error)
    {
      console.error('Error inserting items:', error);
      return {
        status: 500,
        body: JSON.stringify({ message: 'Internal Server Error' })
      };
    }
       
    }
    else{
       return {
        status: 400,
        body: JSON.stringify({ message: 'JSON Payload data not found!' })
      };
    }
  }
  else if(httpMethod==="GET")
  {

    const input = {
      "ExpressionAttributeValues": {
        ":v1": {
          "S": name
        }
      },
      "ExpressionAttributeNames": {
        "#nameAttr": "name",
        "#dataAttr": "data"
      },
      "KeyConditionExpression": "#nameAttr = :v1",
      "ProjectionExpression": "cid, #dataAttr",
      "TableName": tableName
    };

    try {
      const command = new QueryCommand(input);
      const data = await dynamo.send(command);
      let items=[];
      console.log("This is the name",name);
      console.log("This is the returned data",data);
      // const data = await dynamoDB.scan(params).promise();
      if (data.Items && data.Items.length > 0) {
        items = data.Items.map((item) => AWS.DynamoDB.Converter.unmarshall(item));
        console.log('Payload metadata items are:', items);
        return {
          status: 200,
          body: JSON.stringify({ items })
        };
      } else {
        return {
          status: 404,
          body: JSON.stringify({ message: 'No items found' })
        };
      }
    } catch (error) {
      console.error('Error fetching items:', error);
      return {
        status: 500,
        body: JSON.stringify({ message: 'Internal Server Error' })
      };
    }
  }
  else{
    return {
      status: 400,
      body: JSON.stringify({ message: 'Invalid HTTP method' })
    };
  }
}