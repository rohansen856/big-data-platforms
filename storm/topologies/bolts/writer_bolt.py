#!/usr/bin/env python3

from streamparse import Bolt
import json
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class WriterBolt(Bolt):
    """Write aggregated data to files"""
    
    def initialize(self, stormconf, context):
        """Initialize the bolt"""
        self.output_dir = "/shared-data/storm_output"
        os.makedirs(self.output_dir, exist_ok=True)
        self.written_count = 0
        
    def process(self, tup):
        """Process each tuple"""
        try:
            # Get the aggregated data
            aggregated_data = tup.values[0]
            
            # Write to file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.output_dir}/aggregated_{timestamp}.json"
            
            with open(filename, 'a') as f:
                f.write(json.dumps(aggregated_data) + '\n')
            
            self.written_count += 1
            logger.info(f"Written aggregated data to {filename} (total: {self.written_count})")
            
            # Acknowledge the tuple
            self.ack(tup)
                
        except Exception as e:
            logger.error(f"Error writing aggregated data: {e}")
            self.fail(tup)