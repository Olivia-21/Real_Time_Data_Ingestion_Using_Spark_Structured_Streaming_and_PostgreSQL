"""
Data Generator for E-commerce Events.
Continuously generates fake user activity events as CSV files for streaming ingestion.

Features:
- Deterministic event IDs for reproducibility
- Configurable batch size and interval
- Realistic product catalog with categories
- Nullable price for view events
"""

import csv
import hashlib     # for deterministic event ID generation
import os
import random
import signal
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

import yaml
from faker import Faker

# Set seeds for reproducibility
Faker.seed(42)
random.seed(42)


class DataGenerator:
    """Generates fake e-commerce events and writes them to CSV files."""
    
    def __init__(self, config_path: str = "/app/config/settings.yaml"):
        """
        Initialize the data generator with configuration.
        
        Args:
            config_path: Path to settings.yaml configuration file
        """
        self.config = self._load_config(config_path)
        self.output_path = Path(self.config["generator"]["output_path"])
        self.event_size = self.config["generator"]["event_size"]
        self.interval = self.config["generator"]["interval_seconds"]
        self.event_types = self.config["generator"]["event_types"]
        self.purchase_probability = self.config["generator"]["purchase_probability"]
        self.categories = self.config["products"]["categories"]
        
        # Product catalog for realistic data
        self.product_catalog = self._generate_product_catalog()
        
        # User pool for consistent user IDs
        self.user_pool = [f"USER_{i:06d}" for i in range(1, 1001)]
        
        # Running flag for graceful shutdown
        self.running = True
        
        # Ensure output directory exists
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        self._log(f"Data Generator initialized. Output: {self.output_path}")
    
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, "r") as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            self._log(f"Config file not found at {config_path}, using defaults")
            return self._default_config()
    
    def _default_config(self) -> Dict:
        """Return default configuration if file not found."""
        return {
            "generator": {
                "output_path": "/data/incoming",
                "event_size": 10,
                "interval_seconds": 5,
                "event_types": ["view", "purchase"],
                "purchase_probability": 0.3
            },
            "products": {
                "categories": [
                    "Electronics", "Clothing", "Home & Garden",
                    "Sports", "Books", "Toys", "Beauty", "Food & Grocery"
                ]
            }
        }
    
    def _generate_product_catalog(self) -> List[Dict]:
        """Generate a catalog of products with names, IDs, and categories."""
        catalog = []
        product_templates = {
            "Electronics": ["Wireless Headphones", "Smart Watch", "Bluetooth Speaker", 
                           "Laptop Stand", "USB-C Hub", "Power Bank", "Webcam", "Mouse"],
            "Clothing": ["Cotton T-Shirt", "Denim Jeans", "Running Shoes", 
                        "Winter Jacket", "Casual Dress", "Wool Sweater", "Hat"],
            "Home & Garden": ["Table Lamp", "Plant Pot", "Throw Pillow", 
                             "Wall Clock", "Kitchen Mat", "Garden Tools Set"],
            "Sports": ["Yoga Mat", "Resistance Bands", "Water Bottle", 
                      "Running Belt", "Dumbbell Set", "Jump Rope"],
            "Books": ["Fiction Bestseller", "Cookbook", "Self-Help Guide", 
                     "Science Book", "History Novel", "Biography"],
            "Toys": ["Board Game", "Building Blocks", "Action Figure", 
                    "Puzzle Set", "Remote Car", "Stuffed Animal"],
            "Beauty": ["Face Cream", "Lipstick Set", "Perfume", 
                      "Hair Serum", "Nail Polish Kit", "Makeup Brush"],
            "Food & Grocery": ["Organic Coffee", "Protein Bars", "Olive Oil", 
                              "Spice Set", "Tea Collection", "Snack Mix"]
        }
        
        product_id = 1
        for category in self.categories:
            products = product_templates.get(category, ["Generic Product"])
            for product_name in products:
                catalog.append({
                    "product_id": f"PROD_{product_id:06d}",
                    "product_name": product_name,
                    "product_category": category,
                    "base_price": round(random.uniform(9.99, 299.99), 2)
                })
                product_id += 1
        
        return catalog
    
    def _generate_event_id(self, user_id: str, product_id: str, timestamp: str) -> str:
        """
        Generate a deterministic event ID based on event components.
        This ensures the same event always produces the same ID, enabling deduplication.
        
        Args:
            user_id: User identifier
            product_id: Product identifier
            timestamp: Event timestamp string
        
        Returns:
            UUID-format deterministic event ID
        """
        # Create a unique string from event components
        unique_string = f"{user_id}_{product_id}_{timestamp}"
        
        # Generate a deterministic UUID using MD5 hash
        hash_bytes = hashlib.md5(unique_string.encode()).digest()
        return str(uuid.UUID(bytes=hash_bytes))
    
    def _generate_event(self) -> Dict:
        """Generate a single fake e-commerce event."""
        # Select random user and product
        user_id = random.choice(self.user_pool)
        product = random.choice(self.product_catalog)
        
        # Determine event type based on probability
        event_type = "purchase" if random.random() < self.purchase_probability else "view"
        
        # Generate timestamp (within last few minutes for realism)
        timestamp = datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 60))
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        
        # Price is set for purchases, NULL for views
        price = None
        if event_type == "purchase":
            # Add some variation to the base price
            price = round(product["base_price"] * random.uniform(0.9, 1.1), 2)
        
        # Generate deterministic event ID
        event_id = self._generate_event_id(
            user_id, product["product_id"], timestamp_str
        )
        
        return {
            "event_id": event_id,
            "user_id": user_id,
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "product_category": product["product_category"],
            "event_type": event_type,
            "price": price,
            "event_timestamp": timestamp_str
        }
    
    def _write_batch(self, events: List[Dict], batch_num: int) -> str:
        """
        Write a batch of events to a CSV file.
        
        Args:
            events: List of event dictionaries
            batch_num: Batch number for filename
        
        Returns:
            Path to the written file
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"events_{timestamp}_{batch_num:06d}.csv"
        filepath = self.output_path / filename
        
        # Write to a temporary file first, then rename (atomic operation)
        temp_filepath = self.output_path / f".tmp_{filename}"
        
        fieldnames = [
            "event_id", "user_id", "product_id", "product_name",
            "product_category", "event_type", "price", "event_timestamp"
        ]
        
        with open(temp_filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(events)
        
        # Atomic rename
        os.rename(temp_filepath, filepath)
        
        return str(filepath)
    
    def _log(self, message: str):
        """Simple logging to stdout (captured by Docker)."""
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        print(f"{timestamp} - DataGenerator - INFO - {message}", flush=True)
    
    def stop(self):
        """Stop the generator gracefully."""
        self._log("Received stop signal, shutting down...")
        self.running = False
    
    def run(self):
        """
        Main loop: continuously generate batches of events.
        Runs until stop() is called or interrupted.
        """
        self._log(f"Starting event generation. Event size: {self.event_size}, Interval: {self.interval}s")
        
        batch_num = 0
        total_events = 0
        
        while self.running:
            try:
                # Generate a batch of events
                events = [self._generate_event() for _ in range(self.event_size)]
                
                # Write to CSV
                filepath = self._write_batch(events, batch_num)
                
                batch_num += 1
                total_events += len(events)
                
                # Log progress
                purchase_count = sum(1 for e in events if e["event_type"] == "purchase")
                view_count = len(events) - purchase_count
                
                self._log(
                    f"Batch {batch_num}: Generated {len(events)} events "
                    f"({purchase_count} purchases, {view_count} views) -> {os.path.basename(filepath)}"
                )
                
                # Wait for next interval
                time.sleep(self.interval)
                
            except KeyboardInterrupt:
                self._log("Interrupted by user")
                break
            except Exception as e:
                self._log(f"Error generating events: {e}")
                time.sleep(1)  # Brief pause before retry
        
        self._log(f"Data generator stopped. Total batches: {batch_num}, Total events: {total_events}")


def main():
    """Entry point for the data generator."""
    generator = DataGenerator()
    
    # Handle graceful shutdown signals
    def signal_handler(signum, frame):
        generator.stop()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Run the generator
    generator.run()


if __name__ == "__main__":
    main()
