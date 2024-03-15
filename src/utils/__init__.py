import time
import logging


def real_sleep(
    millisecs: int,
    start_time: float,
):
    """Sleep function to take into account the elapsed time in between messages generated/published"""
    total_secs = millisecs / 1000 - (time.time() - start_time)
    if total_secs > 0:
        time.sleep(total_secs)


def delivery_report(err, msg):
    if err is not None:
        logging.error("Delivery failed for User record {}: {}".format(msg.key(), err))
    else:
        logging.debug(
            "Record successfully produced to {} [{}] at offset {}: {}".format(
                msg.topic(),
                msg.partition(),
                msg.offset(),
                msg.value(),
            )
        )