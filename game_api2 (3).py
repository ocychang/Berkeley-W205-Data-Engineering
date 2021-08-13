#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())


@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"


@app.route("/purchase_sword/<sword>")
def purchase_a_sword(sword):
    purchase_sword_event = {'event_type': 'purchase_sword', 'sword': sword}
    log_to_kafka('events', purchase_sword_event)
    return "Sword Purchased!\n"

@app.route("/play_hrs/<hrs>")
def play_hrs(hrs):
    play_hrs_event = {'event_type': 'play_hrs', 'total_hrs': hrs}
    log_to_kafka('events', play_hrs_event)
    return "Hours logged!\n"

@app.route("/ref_count/<ref>")
def ref_count(ref):
    ref_count_event = {'event_type': 'ref_count', 'total_ref_count': ref}
    log_to_kafka('events', ref_count_event)
    return "ref_count logged!\n"

@app.route("/sub_count/<sub_sub>")
def sub_sub(sub_sub):
    sub_event = {'event_type': 'sub_count', 'total_sub': sub_sub}
    log_to_kafka('events', sub_event)
    return "sub_event logged!\n"

@app.route("/money_paid/<money>")
def money(money):
    money_paid_event = {'event_type': 'money_paid', 'money_paid_amount': money}
    log_to_kafka('events', money_paid_event)
    return "money_paid_event logged!\n"

@app.route("/account_open/<acctopen>")
def account_open(acctopen):
    account_open_event = {'event_type': 'account_open', 'account_open': acctopen}
    log_to_kafka('events', account_open_event)
    return "account_open_event logged!\n"

@app.route("/join_guild/<guild>")
def join_guild(guild):
    join_guild_event = {'event_type': 'join_guild', 'join_guild': guild}
    log_to_kafka('events', join_guild_event)
    return "join_guild_event logged!\n"

@app.route("/sw_a_g/<swag>")
def sw_a_g(swag):
    swag_event = {'event_type': 'sw_a_g', 'swag': swag}
    log_to_kafka('events', swag_event)
    return "swag_event logged!\n"