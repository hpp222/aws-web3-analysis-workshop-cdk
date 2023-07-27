# Welcome to AWS Web3 serverless analysis workshop!

## Getting Started
For a detailed walkthrough of this workshop, see [here](https://github.com/tsaol/Web3-serverless-analytics-on-aws)

## Prerequisites
To perform this workshop, you’ll need the following:
   * [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html)
   * [Maven](https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth-connect-prerq.html)
   * [Java](https://aws.amazon.com/corretto/?filtered-posts.sort-by=item.additionalFields.createdDate&filtered-posts.sort-order=desc)
   * 
   
## Useful commands

 * `mvn package`     compile and run tests
 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

## Deploy this project
1. deploy all chains:
    cdk deploy --all --parameters EC2Stack:ImageID="ami-0123456789"
    ```
    cdk deploy --all --parameters EthereumStreamingStack:EthereumImageID="ami-0123456789" \
    --parameters BitcoinStreamingStack:BitcoinImageID="ami-0123456789" \
    --parameters IoTexStreamingStack:IoTexImageID="ami-0123456789" \
    --parameters PolygonStreamingStack:PolygonImageID="ami-0123456789"
    ```
2. deploy specifc chain:
    ```
    cdk deploy VpcStack RedshiftServerlessStack EthereumStreamingStack --parameters EthereumStreamingStack:EthereumImageID="ami-0123456789"
    ```

## Write data to Kinesis Data Streams
1. From Ethereum full node
    ```
    export AWS_DEFAULT_REGION=<region>
    ethereumetl stream -e block --provider-uri file:///home/ubuntu/.ethereum/geth.ipc --output=kinesis://raw_ethereum_stream
    ```
2. From IoTex
    ```
    export AWS_DEFAULT_REGION=<region>
    iotexetl stream -e block,action,log --provider-uri grpcs://api.mainnet.iotex.one:443 --output kinesis://raw_iotex_stream
    ```
3. From Bitcoin full node
    ```
    bitcoind -testnet -daemon
    export AWS_DEFAULT_REGION=<region>
    bitcoinetl stream --provider-uri http://mybitcoin:123mybitcoin@localhost:18332 --output kinesis://raw_bitcoin_stream
    ```

4. From Polygon full node
    ```
    export AWS_DEFAULT_REGION=<region>
    polygonetl stream -e block --provider-uri file:///var/lib/bor/bor.ipc --output=kinesis://raw_polygon_stream
    ```




