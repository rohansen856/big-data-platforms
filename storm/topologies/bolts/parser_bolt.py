#!/usr/bin/env python3

from streamparse import Bolt
import json
import logging

logger = logging.getLogger(__name__)

class ParserBolt(Bolt):
    """Parse incoming JSON messages"""
    
    def initialize(self, stormconf, context):
        """Initialize the bolt"""
        self.processed_count = 0
        
    def process(self, tup):
        """Process each tuple"""
        try:
            # Get the raw event data
            raw_event = tup.values[0]
            
            # Parse JSON
            event = json.loads(raw_event)
            
            # Extract and validate fields
            parsed_event = {
                'timestamp': event.get('timestamp', ''),
                'event_type': event.get('event_type', 'unknown'),
                'user_id': event.get('user_id', ''),
                'value': event.get('value', 0)
            }
            
            # Emit the parsed event
            self.emit([parsed_event])
            
            # Acknowledge the tuple
            self.ack(tup)
            
            self.processed_count += 1
            if self.processed_count % 1000 == 0:
                logger.info(f"Processed {self.processed_count} events")
                
        except Exception as e:
            logger.error(f"Error parsing event: {e}")
            self.fail(tup)