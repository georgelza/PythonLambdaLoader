#######################################################################################################################
#
#
#  	Project     	: 	TFM 2.0, *.JSON loader -> Post to Kafka Topic
#
#   File            :   aws_conf_loader.py
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
#   JSON Viewer     :   https://jsonviewer.stack.hu
#
#   https://stackoverflow.com/questions/59055302/how-can-i-decode-a-gz-file-from-s3-using-an-aws-lambda-function
#   https://docs.aws.amazon.com/lambda/latest/dg/python-package.html
#   https://github.com/awsdocs/aws-lambda-developer-guide/tree/master/sample-apps/blank-python
#   https://www.linkedin.com/pulse/add-external-python-libraries-aws-lambda-using-layers-gabe-olokun/
#
########################################################################################################################

__author__ = "George Leonard"
__email__ = "georgelza@gmail.com"
__version__ = "0.0.1"
__copyright__ = "Copyright 2020, George Leonard"


import json
from datetime import datetime
from time import perf_counter
import os
import logging
import gzip
import io
import urllib
import boto3

from confluent_kafka import Producer, KafkaError, KafkaException

# Logging Handler
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)

# create file handler which logs even debug messages
fh = logging.FileHandler('local_conf_readline_loader.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

config_params = {}
config_params["Bootstrap_servers"] = os.environ["kafka_bootstrap_servers"]
config_params["Bootstrap_port"] = os.environ["kafka_bootstrap_port"]
config_params["Topicname"] = os.environ["kafka_topic_name"]
config_params["Security_protocol"] = os.environ["kafka_security_protocol"]
config_params["Sasl_mechanisms"] = os.environ["kafka_sasl_mechanisms"]        
config_params["Sasl_username"] = os.environ["kafka_sasl_username"]
config_params["Sasl_password"] = os.environ["kafka_sasl_password"]
config_params["flushcap"] = int(os.environ["flushcap"])
config_params["reccap"] = int(os.environ["reccap"])


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
        pass
        #logger.info('Produced to: {} [{}] @ {}'.format(msg.topic(), msg.partition(), msg.offset()))

#end acked


def pp_json(json_thing, sort=True, indents=4):
    if type(json_thing) is str:
        logger.info(json.dumps(json.loads(json_thing), sort_keys=sort, indent=indents))
    else:
        logger.info(json.dumps(json_thing, sort_keys=sort, indent=indents))
    return None

# end pp_json


def lambda_handler(event, context):

    return main(event, context)

#end lambda_handler


def main(event, context):

    step0starttime = datetime.now()
    step0start = perf_counter()
    
    logger.info("Python JSONDecomposer - Start")

    logger.debug("Bootstrap_servers :"+ config_params["Bootstrap_servers"])
    logger.debug("Bootstrap_port    :"+ config_params["Bootstrap_port"])
    logger.debug("Topicname         :"+ config_params["Topicname"])
    logger.debug("Security_protocol :"+ config_params["Security_protocol"])
    logger.debug("Sasl_mechanisms   :"+ config_params["Sasl_mechanisms"])
    logger.debug("flushcap          :"+ str(config_params["flushcap"]))
    logger.debug("reccap            :"+ str(config_params["reccap"]))

    # Step 1
    step1starttime = datetime.now()
    step1start = perf_counter()

    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    logger.debug("bucket            :"+ bucket)

    key = urllib.parse.unquote_plus(event["Records"][0]["s3"]["object"]["key"], encoding="utf-8")
    logger.debug("key               :"+ key)

    json_file = "s3://" + bucket + "/" + key
    logger.debug("json_file         :"+ json_file)

    topicname = key.split("/")[1]
    logger.debug("topicname         :"+ topicname)

    filename = json_file.split("/")[9]
    logger.debug("filename          :"+ filename)
    logger.debug("")

    step1endtime = datetime.now()
    step1end = perf_counter()
    step1time = step1end - step1start


    try:
        # Step 2
        step2starttime = datetime.now()
        step2start = perf_counter()

        p = create_kafka_producer(config_params)

        step2endtime = datetime.now()
        step2end = perf_counter()
        step2time = step2end - step2start

        # Step 3
        step3starttime = datetime.now()
        step3start = perf_counter()


        s3_client = boto3.client("s3")
        logger.debug("s3 client created")

        response = s3_client.get_object(Bucket=bucket, Key=key)
        logger.debug("s3 client.get_object created")

        content = response['Body'].read()
        
        logger.debug("s3 body read")

        with gzip.GzipFile(fileobj=io.BytesIO(content), mode='rb') as fh:

            logger.debug("file handle created")

            step3endtime = datetime.now()
            step3end = perf_counter()
            step3time = step3end - step3start

            # Step 4
            step4starttime = datetime.now()
            step4start = perf_counter()
        
            flushcap = config_params["flushcap"]
            flush = 0
            reccap = config_params["reccap"]
            rec = 0
            logger.debug("starting enumerater")
            for line_no, line in enumerate(fh):
                line = line.decode().replace("\n", "")

                json_msg = {}
                json_msg["fs_payload"] = json.loads(line)
                json_msg["json_file"] = json_file

                msg_txt = json.dumps(json_msg)


                p.produce(config_params["Topicname"],
                        value=msg_txt,
                        callback=acked)
                
                #pp_json(msg_txt)
                p.poll(0)

                flush +=1 
                if flush==flushcap:
                    p.flush()
                    flush = 0
                    logger.info("Flushing @ {flush}".format(flush=rec))

                rec += 1
                if reccap > 0 and rec == reccap:
                    break 

            # end for
            logger.debug("completed enumerater")

            step4endtime = datetime.now()
            step4end = perf_counter()
            step4time = step4end - step4start

        # DONE, lets wrap up
        step0endtime = datetime.now()
        step0end = perf_counter()
        step0time = step0end - step0start

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
        
        logger.info("Rate:{rate} docs/s".format(
            rate=str(rec/step4time)))
        
        return "Success!"

    except Exception as e:
        logger.error("Generic error :{error}".format(error=e))
        raise e

    except KafkaError.Exception as e:
        logger.error("Kafka error :{error}".format(error=e))
        raise e
    
    finally:
        p.flush()
        fh.close()
        logger.debug("File handle closed")

        logger.info("JSONDecomposer - End")

    pass

#end main


# For local testing
if __name__ == '__main__':

    x = "a"
    event = {
        "Records" : [{
            "s3":{
                "bucket":
                    {"name": "applab-epay-sandbox-filedrop"},
                "object":
                    {"key": "Kafka-connect/AsyncOut/year=2023/month=06/day=20/hour=16/largecomplexline.json.gz"}
                }
        }]
    }


    lambda_handler(event, x)

#end main