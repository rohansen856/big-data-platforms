#!/usr/bin/env python3

"""
Storm topology for processing streaming events
"""

from streamparse import Topology
from bolts.parser_bolt import ParserBolt
from bolts.aggregator_bolt import AggregatorBolt
from bolts.writer_bolt import WriterBolt
from spouts.kafka_spout import KafkaSpout

class EventProcessingTopology(Topology):
    """Main event processing topology"""
    
    kafka_spout = KafkaSpout.spec(
        name="kafka-spout",
        par=1,
        outputs=["event"]
    )
    
    parser_bolt = ParserBolt.spec(
        name="parser-bolt",
        par=2,
        inputs=[kafka_spout],
        outputs=["parsed_event"]
    )
    
    aggregator_bolt = AggregatorBolt.spec(
        name="aggregator-bolt",
        par=2,
        inputs=[parser_bolt],
        outputs=["aggregated_data"]
    )
    
    writer_bolt = WriterBolt.spec(
        name="writer-bolt",
        par=1,
        inputs=[aggregator_bolt]
    )