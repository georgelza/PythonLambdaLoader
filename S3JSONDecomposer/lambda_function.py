#######################################################################################################################
#
#
#  	Project     	: 	TFM 2.0, *.JSON loader -> Post to Kafka Topic
#
#   File            :   aws_loader.py
#
#   Description     :   This will run on AWS Lambda, It consumes a JSON file, decomposes the array of records into
#                   :   individual messages and post them onto a Confluent Cloud hosted Kafka cluster/topic
#
#	By              :   George Leonard ( georgelza@gmail.com )
#
#   Created     	:   22 Jun 2023
#
#   Changelog       :   See bottom
#
#   starting  S3    :   https://www.youtube.com/watch?v=sFvYUE8M7zY
#
#   https://stackoverflow.com/questions/59055302/how-can-i-decode-a-gz-file-from-s3-using-an-aws-lambda-function
#
########################################################################################################################

__author__ = "George Leonard"
__email__ = "georgelza@gmail.com"
__version__ = "0.0.1"
__copyright__ = "Copyright 2020, George Leonard"


import  json
from datetime import datetime
from time import perf_counter
import urllib
import boto3
import io
import gzip
import os
import logging


from confluent_kafka import Producer, KafkaError, KafkaException

# Logging Handler
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create formatter and add it to the handlers
#formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)

config_params = {}
config_params["Bootstrap_servers"] = os.environ["kafka_bootstrap_servers"]
config_params["Bootstrap_port"] = os.environ["kafka_bootstrap_port"]       
config_params["Topicname"] = os.environ["kafka_topic_name"]
config_params["Security_protocol"] =  os.environ["kafka_security_protocol"]
config_params["Sasl_mechanisms"] =  os.environ["kafka_sasl_mechanisms"]
config_params["Sasl_username"] = os.environ["kafka_sasl_username"]
config_params["Sasl_password"] = os.environ["kafka_sasl_password"]


def error_cb(err):
    """ The error callback is used for generic client errors. These
        errors are generally to be considered informational as the client will
        automatically try to recover from all errors, and no extra action
        is typically required by the application.
        For this example however, we terminate the application if the client
        is unable to connect to any broker (_ALL_BROKERS_DOWN) and on
        authentication errors (_AUTHENTICATION). """

    logger.error("Client error: {}".format(err))
    if err.code() == KafkaError._ALL_BROKERS_DOWN or err.code() == KafkaError._AUTHENTICATION:
        # Any exception raised from this callback will be re-raised from the
        # triggering flush() or poll() call.
        raise KafkaException(err)

# end error_cb


def create_kafka_producer(config_params):
    # Create producer
    p = Producer({
        'bootstrap.servers': config_params["Bootstrap_servers"],
        'sasl.mechanism': config_params["Sasl_mechanisms"],
        'security.protocol': config_params["Security_protocol"],
        'sasl.username': config_params["Sasl_username"],
        'sasl.password': config_params["Sasl_password"],
        'error_cb': error_cb,
    })

    logger.info("Kafka Producer instantiated: {}".format(p))
    logger.info("")

    return p

#end create_kafka_producer


def acked(err, msg):
    """Delivery report callback called (from flush()) on successful or failed delivery of the message."""
    if err is not None:
       logger.error('Failed to deliver message: {}'.format(err.str()))
    else:
        logger.info('Produced to: {} [{}] @ {}'.format(msg.topic(), msg.partition(), msg.offset()))

#end acked

def lambda_handler(event, context):
    
    return main(event, context)

#end lambda_handler

def main(event, context):


    logger.info("JSONDecomposer - Start")
    step0starttime = datetime.now()
    step0start = perf_counter()

    logger.info("AppName:" + os.environ["AppName"])
    logger.info("Bootstrap_servers "+ config_params["Bootstrap_servers"])
    logger.info("Bootstrap_port    "+ config_params["Bootstrap_port"])
    logger.info("Topicname         "+ config_params["Topicname"])
    logger.info("Security_protocol "+ config_params["Security_protocol"])
    logger.info("Sasl_mechanisms   "+ config_params["Sasl_mechanisms"])
    
    # Step 1
    step1starttime = datetime.now()
    step1start = perf_counter()

    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    logger.info("bucket    :"+ bucket)

    key = urllib.parse.unquote_plus(event["Records"][0]["s3"]["object"]["key"], encoding="utf-8")
    logger.info("key       :"+ key)

    json_file = "s3://" + bucket + "/" + key
    logger.info("json_file :"+ json_file)

    topicname = key.split("/")[1]
    logger.info("topicname :"+ topicname)

    filename = json_file.split("/")[9]
    logger.info("filename  :"+ filename)
    logger.info("")

    step1endtime = datetime.now()
    step1end = perf_counter()
    step1time = step1end - step1start


    try:
        
        
        p = create_kafka_producer(config_params)
        
        
        # Step 2
        step2starttime = datetime.now()
        step2start = perf_counter()


        step2endtime = datetime.now()
        step2end = perf_counter()
        step2time = step2end - step2start

        # Step 3
        step3starttime = datetime.now()
        step3start = perf_counter()

        s3_client = boto3.client("s3")

        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        with gzip.GzipFile(fileobj=io.BytesIO(content), mode='rb') as fh:
            records = json.load(fh)

        step3endtime = datetime.now()
        step3end = perf_counter()
        step3time = step3end - step3start
        

        # Step 4
        step4starttime = datetime.now()
        step4start = perf_counter()

        rec = 0
        for record in records:
            record["Topic"] = topicname
            record["Outbound_Source_File"] = json_file

            msg = json.dumps(record)
            p.produce(config_params["Topicname"],
                      value=msg,
                      callback=acked)

            p.poll(0)
            #p.flush()

            print("REC :", rec)
            #pp_json(record)

            # Just during testing, this is so that we consume/decompress the entire Large zip file but for testing now
            # only post 1000 records.
            rec += 1
            if rec == 1000:
                break

        step4endtime = datetime.now()
        step4end = perf_counter()
        step4time = step4end - step4start

        # DONE, lets wrap up
        step0endtime = datetime.now()
        step0end = perf_counter()
        step0time = step0end - step0start

        return "Success!"

    except Exception as e:
        logger.error("############ Generic Eerror", e)
        raise e

    except kafka.Exception as e:
        logger.error("############ KAFKA Error", e)
        raise e

    finally:
        logger.info("############ Summary ############ ")

        logger.info("Topic:{topicname}, File:{json_file}".format(topicname=topicname, json_file=json_file))

        logger.info("Step 1, St:{start} Et:{end} Rt:{runtime}".format(
            start=str(step1starttime.strftime("%Y-%m-%d %H:%M:%S.%f")),
            end=str(step1endtime.strftime("%Y-%m-%d %H:%M:%S.%f")),
            runtime=str(step1time)))
        logger.info("Step 2, St:{start} Et:{end} Rt:{runtime}".format(
            start=str(step2starttime.strftime("%Y-%m-%d %H:%M:%S.%f")),
            end=str(step2endtime.strftime("%Y-%m-%d %H:%M:%S.%f")),
            runtime=str(step2time)))
        logger.info("Step 3, St:{start} Et:{end} Rt:{runtime}".format(
            start=str(step3starttime.strftime("%Y-%m-%d %H:%M:%S.%f")),
            end=str(step3endtime.strftime("%Y-%m-%d %H:%M:%S.%f")),
            runtime=str(step3time)))
        logger.info("Step 4, St:{start} Et:{end} Rt:{runtime}".format(
            start=str(step4starttime.strftime("%Y-%m-%d %H:%M:%S.%f")),
            end=str(step4endtime.strftime("%Y-%m-%d %H:%M:%S.%f")),
            runtime=str(step4time)))

        logger.info("Step 0, St:{start} Et:{end} Rt:{runtime}".format(
            start=str(step0starttime.strftime("%Y-%m-%d %H:%M:%S.%f")),
            end=str(step0endtime.strftime("%Y-%m-%d %H:%M:%S.%f")),
            runtime=str(step0time)))

        logger.info("JSONDecomposer - End")
        logger.info("############ Completed ############ ")

    pass

#end lambda_handler



def pp_json(json_thing, sort=True, indents=4):
    if type(json_thing) is str:
        print(json.dumps(json.loads(json_thing), sort_keys=sort, indent=indents))
    else:
        print(json.dumps(json_thing, sort_keys=sort, indent=indents))
    return None

# end pp_json
