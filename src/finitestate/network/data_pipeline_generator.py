import copy
import yaml
import os.path
import finitestate.network.data_pipelines
import inflection
import logging

ENTITY_IDENTIFIER      = "entity_identifier"
AGGREGATE_ID           = "aggregate_id"
SNAPSHOT_VERSION       = "snapshot_version"
DATA_WINDOW_START_TIME = "data_window_start_time"

NETWORK = "network"
MAC     = "mac"

SNS = "sns"
SQS = "sqs"
DDB = "ddb"

TOPIC = "topic"
QUEUE = "queue"
TABLE = "table"

REGION  = "#{AWS::Region}"
ACCOUNT = "#{AWS::AccountId}"
STAGE   = "${opt:stage}"

AGGREGATION  = "aggregation"
ACCUMULATION = "accumulation"

RESOURCE_LAYER_SEPARATORS = {
    SNS : "-",
    SQS : "-",
    DDB : "-",
}

class DataPipelineGenerator(object):
    def __init__(self):
        self.__load_pipelines()

    def __load_pipelines(self):
        self.pipelines = {}

        for dirent in finitestate.network.data_pipelines.__loader__.contents():
            config_path = os.path.join(os.path.dirname(__file__), "data_pipelines", dirent, "config.yml")

            try:
                config_payload = finitestate.network.data_pipelines.__loader__.get_data(config_path)
            except:
                continue

            try:
                self.pipelines[dirent] = yaml.safe_load(config_payload)
            except:
                logging.getLogger(__name__).error(f"failed to parse config.yaml for {dirent} pipeline")
                raise

    def _generate_outputs(self, pipeline_name, processing_element_kind):
        return [
            [
                f"{self._generate_table_logical_name(pipeline_name, processing_element_kind)}Name",
                {
                    "Value"  : { "Ref"  : self._generate_table_logical_name(pipeline_name, processing_element_kind) },
                    "Export" : { "Name" : f"{self._generate_table_logical_name(pipeline_name, processing_element_kind)}Arn-{STAGE}" }
                }
            ],
            [
                f"{self._generate_topic_logical_name(pipeline_name, processing_element_kind)}Arn",
                {
                    "Value"  : { "Ref"  : self._generate_topic_logical_name(pipeline_name, processing_element_kind) },
                    "Export" : { "Name" : f"{self._generate_topic_logical_name(pipeline_name, processing_element_kind)}Arn-{STAGE}" }
                }
            ]
        ]

    def enrich_file(self, input_path, output_path, aggregators, accumulators):
        with open(input_path, "r") as input_stream:
            document = yaml.safe_load(input_stream)

        enriched_document = self.enrich_document(document, aggregators, accumulators)

        with open(output_path, "w") as output_stream:
            yaml.dump(enriched_document, output_stream, default_flow_style = False)

    def enrich_document(self, document, aggregators, accumulators):
        enriched_document = copy.deepcopy(document)

        if not "functions" in enriched_document:
            enriched_document["functions"] = {}

        if not "resources" in enriched_document:
            enriched_document["resources"] = {}

        if not "Resources" in enriched_document["resources"]:
            enriched_document["resources"]["Resources"] = {}

        if not "Outputs" in enriched_document["resources"]:
            enriched_document["resources"]["Outputs"] = {}

        for agg in aggregators:
            for resource in self.generate_aggregator_resources(agg):
                enriched_document["resources"]["Resources"][resource[0]] = resource[1]

            for output in self.generate_aggregator_outputs(agg):
                enriched_document["resources"]["Outputs"][output[0]] = output[1]

        for acc in accumulators:
            for resource in self.generate_accumulator_resources(acc, True if acc in aggregators else False):
                enriched_document["resources"]["Resources"][resource[0]] = resource[1]

            for output in self.generate_accumulator_outputs(acc):
                enriched_document["resources"]["Outputs"][output[0]] = output[1]

            fn = self.generate_accumulator_function(acc)
            enriched_document["functions"][fn[0]] = fn[1]

        return enriched_document

    def generate_aggregator_outputs(self, pipeline_name):
        return self._generate_outputs(pipeline_name, AGGREGATION)

    def generate_accumulator_outputs(self, pipeline_name):
        return self._generate_outputs(pipeline_name, ACCUMULATION)

    def generate_aggregator_resources(self, pipeline_name):
        return [
            self._generate_topic(pipeline_name, AGGREGATION),
            self._generate_topic_policy(pipeline_name, AGGREGATION),
            self._generate_table(pipeline_name, AGGREGATION)
        ]

    def generate_accumulator_resources(self, pipeline_name, collocated_with_aggregator = True):
        return [
            self._generate_queue(pipeline_name),
            self._generate_queue_policy(pipeline_name, collocated_with_aggregator),
            self._generate_subscription(pipeline_name, collocated_with_aggregator),
            self._generate_topic(pipeline_name, ACCUMULATION),
            self._generate_topic_policy(pipeline_name, ACCUMULATION),
            self._generate_table(pipeline_name, ACCUMULATION)
        ]

    def generate_accumulator_function(self, pipeline_name):
        return [
            f"{inflection.camelize(MAC)}{inflection.camelize(pipeline_name)}Accumulator",
            {
                "handler"          : f"{MAC}_{pipeline_name}_accumulator.handle_event",
                "versionFunctions" : False,
                "role"             : f"arn:aws:iam::{ACCOUNT}:role/network-data-pipeline-lambda",
                "memorySize"       : 1024,
                "environment"      : {
                    "stage"                     : STAGE,
                    "account"                   : ACCOUNT,
                    "region"                    : REGION,
                    "snapshot_s3_bucket_name"   : { "Fn::ImportValue" : f"NetworkWarehouseBucketRef-{STAGE}" },
                    "snapshot_s3_key_prefix"    : f"accumulated/{MAC}/{pipeline_name}/",
                    "snapshot_ddb_table_name"   : { "Ref" : self._generate_table_logical_name(pipeline_name, ACCUMULATION) },
                    "snapshot_ddb_table_region" : REGION,
                    "snapshot_sns_topic_arn"    : { "Ref" : self._generate_topic_logical_name(pipeline_name, ACCUMULATION) }
                },
                "events" : [
                    {
                        "sqs" : {
                            "batchSize" : 1,
                            "arn"       : { "Fn::GetAtt" : [self._generate_queue_logical_name(pipeline_name), "Arn"] }
                        }
                    }
                ],
                "package" : {
                    "exclude" : [
                        "./**"
                    ],
                    "include" : [
                        f"{MAC}_{pipeline_name}_accumulator.py",
                        "finitestate/**/*.py",
                        "finitestate/**/*.json",
                    ]
                }
            }
        ]

    def _generate_topic(self, pipeline_name, processing_element_kind):
        return [
            self._generate_topic_logical_name(pipeline_name, processing_element_kind),
            {
                "Type"       : "AWS::SNS::Topic",
                "Properties" : {
                    "TopicName" : self._generate_topic_physical_name(pipeline_name, processing_element_kind)
                }
            }
        ]

    def _generate_topic_arn(self, pipeline_name, processing_element_kind):
        return ":".join([
            'arn', 'aws', SNS, REGION, ACCOUNT, self._generate_topic_physical_name(pipeline_name, processing_element_kind)
        ])

    def _generate_logical_name(self, pipeline_name, processing_element_kind, resource_kind):
        return f"{inflection.camelize(pipeline_name)}{inflection.camelize(MAC)}{inflection.camelize(processing_element_kind)}{inflection.camelize(resource_kind)}"

    def _generate_topic_logical_name(self, pipeline_name, processing_element_kind):
        return self._generate_logical_name(pipeline_name, processing_element_kind, TOPIC)

    def _generate_topic_physical_name(self, pipeline_name, processing_element_kind):
        return self._generate_physical_name(SNS, pipeline_name, processing_element_kind)

    def _generate_physical_name(self, resource_type, pipeline_name, processing_element_kind):
        return RESOURCE_LAYER_SEPARATORS[resource_type].join([
            NETWORK, STAGE, processing_element_kind, MAC, pipeline_name
        ])

    def _generate_topic_policy(self, pipeline_name, processing_element_kind):
        return [
            f"{self._generate_topic_logical_name(pipeline_name, processing_element_kind)}Policy",
            {
                "Type"       : "AWS::SNS::TopicPolicy",
                "DependsOn"  : self._generate_topic_logical_name(pipeline_name, processing_element_kind),
                "Properties" : {
                    "PolicyDocument" : {
                        "Version"   : "2012-10-17",
                        "Statement" : [
                            {
                                "Effect"    : "Allow",
                                "Principal" : { "AWS" : [ ACCOUNT ] },
                                "Action"    : [ "sns:Publish", "sns:Subscribe", "sns:Receive"],
                                "Resource"  : { "Ref" : self._generate_topic_logical_name(pipeline_name, processing_element_kind) }
                            }
                        ]
                    },
                    "Topics" : [
                        { "Ref" : self._generate_topic_logical_name(pipeline_name, processing_element_kind) }
                    ]
                }
            }
        ]

    def _generate_queue(self, pipeline_name):
        return [
            self._generate_queue_logical_name(pipeline_name),
            {
                "Type"       : "AWS::SQS::Queue",
                "Properties" : {
                    "QueueName"         : self._generate_queue_physical_name(pipeline_name),
                    "VisibilityTimeout" : 900
                }
            }
        ]

    def _generate_queue_logical_name(self, pipeline_name):
        return self._generate_logical_name(pipeline_name, ACCUMULATION, QUEUE)

    def _generate_queue_physical_name(self, pipeline_name):
        return self._generate_physical_name(SQS, pipeline_name, ACCUMULATION)

    def _generate_queue_policy(self, pipeline_name, collocated_with_aggregator):
        return [
            f"{self._generate_queue_logical_name(pipeline_name)}Policy",
            {
                "Type"       : "AWS::SQS::QueuePolicy",
                "DependsOn"  : list(filter(None, [
                    self._generate_topic_logical_name(pipeline_name, AGGREGATION) if collocated_with_aggregator else None,
                    self._generate_queue_logical_name(pipeline_name)
                ])),
                "Properties" : {
                    "PolicyDocument" : {
                        "Version"   : "2012-10-17",
                        "Statement" : [
                            {
                                "Effect"    : "Allow",
                                "Principal" : { "Service": "sns.amazonaws.com" },
                                "Resource"  : { "Fn::GetAtt" : [ self._generate_queue_logical_name(pipeline_name), "Arn" ] },
                                "Action"    : "sqs:SendMessage",
                                "Condition" : {
                                    "ArnEquals" : {
                                        "aws:SourceArn": (
                                            { "Ref" : self._generate_topic_logical_name(pipeline_name, AGGREGATION) }
                                            if collocated_with_aggregator
                                            else
                                            self._generate_topic_arn(pipeline_name, AGGREGATION)
                                        )
                                    }
                                }
                            }
                        ]
                    },
                    "Queues" : [
                        { "Ref" : self._generate_queue_logical_name(pipeline_name) }
                    ]
                }
            }
        ]

    def _generate_subscription(self, pipeline_name, collocated_with_aggregator):
        return [
            f"{self._generate_queue_logical_name(pipeline_name)}Subscription",
            {
                'Type'       : 'AWS::SNS::Subscription',
                'DependsOn'  : list(filter(None, [
                    f"{self._generate_topic_logical_name(pipeline_name, AGGREGATION)}Policy" if collocated_with_aggregator else None,
                    f"{self._generate_queue_logical_name(pipeline_name)}Policy"
                ])),
                'Properties' : {
                    'TopicArn'           : (
                        { "Ref" : self._generate_topic_logical_name(pipeline_name, AGGREGATION) }
                        if collocated_with_aggregator
                        else
                        self._generate_topic_arn(pipeline_name, AGGREGATION)
                    ),
                    'Endpoint'           : { "Fn::GetAtt" : [ self._generate_queue_logical_name(pipeline_name), "Arn"] },
                    'Protocol'           : 'sqs',
                    'RawMessageDelivery' : 'true'
                }
            }
        ]

    def _generate_table_logical_name(self, pipeline_name, processing_element_kind):
        return self._generate_logical_name(pipeline_name, processing_element_kind, TABLE)

    def _generate_table_physical_name(self, pipeline_name, processing_element_kind):
        return self._generate_physical_name(DDB, pipeline_name, processing_element_kind)

    def _generate_table(self, pipeline_name, processing_element_kind):
        hash_key_name  = ENTITY_IDENTIFIER if processing_element_kind == ACCUMULATION else AGGREGATE_ID
        range_key_name = SNAPSHOT_VERSION  if processing_element_kind == ACCUMULATION else DATA_WINDOW_START_TIME

        return [
            self._generate_table_logical_name(pipeline_name, processing_element_kind),
            {
                "Type"       : "AWS::DynamoDB::Table",
                "Properties" : {
                    "TableName"            : self._generate_table_physical_name(pipeline_name, processing_element_kind),
                    "BillingMode"          : "PAY_PER_REQUEST",
                    "AttributeDefinitions" : [
                        {
                            "AttributeName" : hash_key_name,
                            "AttributeType" : "S"
                        },
                        {
                            "AttributeName" : range_key_name,
                            "AttributeType" : "N"
                        }
                    ],
                    "KeySchema"             : [
                        {
                            "KeyType"       : "HASH",
                            "AttributeName" : hash_key_name
                        },
                        {
                            "KeyType"       : "RANGE",
                            "AttributeName" : range_key_name
                        }
                    ]
                }
            }
        ]
