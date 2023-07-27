import json
import base64
from json import loads
import boto3
import datetime
from json import JSONEncoder



#revice kinesis stream data
def lambda_handler(event, context):
    for record in event['Records']:
        
        # kinesis_data = record["kinesis"]["data"]
        # print(kinesis_data)
        # print(type(kinesis_data))
        
        #Kinesis data is base64 encoded so decode here
        
        payload=base64.b64decode(record["kinesis"]["data"])
        print("start to print log here")
        print("-----------------1--------------\n")
        print("message Decoded payload: " + str(payload) + "\n")
        print("type of record data: " + str(type(payload)))
        
        #convert byte to dict directly
        json_data = loads(payload)
        # print(json_data)
        # print(str(type(json_data)))
        print(json_data.keys())
        
       
        
        # if json_data['type'] == 'transaction':
        #     json_data["lock_time"] = datetime.datetime.fromtimestamp(json_data["lock_time"])
        # if json_data['type'] == 'block':    
        #     json_data["timestamp"] = datetime.datetime.fromtimestamp(json_data["timestamp"])
        
        #     print(str(type(json_data["hash"])))
        #     print(str(type(json_data["size"])))
            # print(str(type(json_data["timestamp"])))
            
            
        
        # convert bytes to string
        # print("-----------------2-----------------------\n")
        # json_data = payload.decode('utf-8')
        # print("decoded payload using utf-8: " + json_data + "\n")
        # print("type of record data: " + str(type(json_data)))
        
        
        # convert string to dict
        # print("-----------------3-----------------------\n")
        # jsonData = json.loads(json_data)
        # # jsonData = json.dumps(json_data)
        # print("message Json data: " + str(jsonData) + "\n")
        # print("type of json data: " + str(type(jsonData)))
        
        # print('----------------4-----------------------\n')
        # fin_jsonData = json.dumps(jsonData)
        # print(fin_jsonData)
        
        
        # print("-------------------5---------------------\n")
        # col_type = jsonData['type']
        # print("value of column type: " + col_type)
        # col_type = jsonData["type"]
        # print(col_type)
        # b_value = loads(payload)
        # print("0.---type--- " + b_value['type'])
       
        # if b_value['type'] == 'block':
            
            # TODO: write code...
            # print(type(b_value)) 
            # del b_value['input']
    
            # del b_value['hash']
            # del b_value['nonce']
            # del b_value['transaction_index']
            # del b_value['from_address']
            # del b_value['to_address']
            ###############
            # del b_value['value']
            # del b_value['gas']
            # del b_value['gas_price']
            # del b_value['block_timestamp']
    
            # del b_value['block_number']
            # del b_value['block_hash']
            # del b_value['max_fee_per_gas']
            # del b_value['receipt_gas_used']
            # del b_value['max_priority_fee_per_gas']
            # del b_value['transaction_type']
            # del b_value['receipt_cumulative_gas_used']
            # del b_value['receipt_contract_address']
            # del b_value['receipt_root']
            # del b_value['receipt_status']
            # del b_value['receipt_effective_gas_price']
            # del b_value['item_id']
            # del b_value['item_timestamp']
            # print(type(b_value)) 
        
            # tinydict = {}
            # tinydict['type'] = b_value['type']
        pk = str(datetime.datetime.now().timestamp())
        # print ("pk is " + str(pk))
        result_event = json.dumps(json_data).encode('utf8')
        print(result_event)
        kinesis_client = boto3.client('kinesis')
        response = kinesis_client.put_record(
            StreamName='processed_bitcoin_stream',
            PartitionKey=pk,
            Data=result_event
        )
        print("---response--- " +str(response))

  
  
        
        # payload_type=base64.b64decode(record["kinesis"]["data"]["type"])
        # print("1.---type--- " + str(payload_type))
        # transactions_root =base64.b64decode(record["kinesis"]["data"]["transactions_root"])
        # print("2.---transactions_root---" + str(transactions_root))
        # print("over")
        #respons_format = json.loads(payload)
        #print(" respons_format is: " + str(respons_format))

