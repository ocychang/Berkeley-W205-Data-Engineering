#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf


@udf('boolean')
def is_purchase(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'purchase_sword':
        return True
    return False


@udf('boolean')
def is_play_hrs(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'play_hrs':
        return True
    return False

@udf('boolean')
def is_ref_count(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'ref_count':
        return True
    return False

@udf('boolean')
def is_sub(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'sub_count':
        return True
    return False

@udf('boolean')
def is_money(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'money_paid':
        return True
    return False

@udf('boolean')
def is_account_open(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'account_open':
        return True
    return False

@udf('boolean')
def is_join_guild(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'join_guild':
        return True
    return False

@udf('boolean')
def is_sw_a_g(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type'] == 'sw_a_g':
        return True
    return False


def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .getOrCreate()

    raw_events = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    play_hrs_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_play_hrs('raw'))

    extracted_play_hrs_events = play_hrs_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_play_hrs_events.printSchema()
    extracted_play_hrs_events.show()

    extracted_play_hrs_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/play_hrs')

    ####
    
    purchase_a_sword_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_purchase('raw'))

    extracted_purchase_a_sword_events = purchase_a_sword_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_purchase_a_sword_events.printSchema()
    extracted_purchase_a_sword_events.show()

    extracted_purchase_a_sword_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/purchase_sword')
    
    ####
    
    ref_count_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_ref_count('raw'))

    extracted_ref_count_events = ref_count_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_ref_count_events.printSchema()
    extracted_ref_count_events.show()

    extracted_ref_count_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/ref_count')
    
    ####
    
    sub_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_sub('raw'))

    extracted_sub_events = sub_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_sub_events.printSchema()
    extracted_sub_events.show()

    extracted_sub_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/sub_count')
    
    ####
    
    money_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_money('raw'))

    extracted_money_events = money_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_money_events.printSchema()
    extracted_money_events.show()

    extracted_money_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/money_paid')

    ####
    
    account_open_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_account_open('raw'))

    extracted_account_open_events = account_open_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_account_open_events.printSchema()
    extracted_account_open_events.show()

    extracted_account_open_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/account_open')

    ####
    
    join_guild_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_join_guild('raw'))

    extracted_join_guild_events = join_guild_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_join_guild_events.printSchema()
    extracted_join_guild_events.show()

    extracted_join_guild_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/join_guild')
    
    ####
    
    sw_a_g_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_sw_a_g('raw'))

    extracted_sw_a_g_events = sw_a_g_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_sw_a_g_events.printSchema()
    extracted_sw_a_g_events.show()

    extracted_sw_a_g_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/sw_a_g')
    
if __name__ == "__main__":
    main()
