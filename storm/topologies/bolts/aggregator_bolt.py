#!/usr/bin/env python3

from streamparse import Bolt
import json
import logging
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)

class AggregatorBolt(Bolt):
    """Aggregate events by type in sliding windows"""
    
    def initialize(self, stormconf, context):
        """Initialize the bolt"""
        self.event_counts = defaultdict(int)
        self.event_values = defaultdict(list)
        self.window_size = 60  # 60 second window
        self.last_emit = datetime.now()
        
    def process(self, tup):
        """Process each tuple"""
        try:
            # Get the parsed event
            event = tup.values[0]
            event_type = event.get('event_type', 'unknown')
            value = event.get('value', 0)
            
            # Aggregate data
            self.event_counts[event_type] += 1
            self.event_values[event_type].append(value)
            
            # Check if we should emit aggregated data
            now = datetime.now()
            if (now - self.last_emit).seconds >= self.window_size:
                self.emit_aggregated_data()
                self.last_emit = now
            
            # Acknowledge the tuple
            self.ack(tup)
                
        except Exception as e:
            logger.error(f"Error aggregating event: {e}")
            self.fail(tup)
    
    def emit_aggregated_data(self):
        """Emit aggregated data for the current window"""
        for event_type, count in self.event_counts.items():
            values = self.event_values[event_type]
            
            aggregated_data = {
                'event_type': event_type,
                'count': count,
                'total_value': sum(values),
                'avg_value': sum(values) / len(values) if values else 0,
                'min_value': min(values) if values else 0,
                'max_value': max(values) if values else 0,
                'window_end': datetime.now().isoformat()
            }
            
            self.emit([aggregated_data])
            logger.info(f"Emitted aggregation for {event_type}: {count} events")
        
        # Reset counters for next window
        self.event_counts.clear()
        self.event_values.clear()