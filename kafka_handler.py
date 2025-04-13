from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
from typing import Dict, Any
from database import ReminderDatabase
import logging

logger = logging.getLogger(__name__)

class KafkaNotificationHandler:
    def __init__(self, bootstrap_servers: str = 'localhost:9092', topic: str = 'notifications'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3,
                retry_backoff_ms=1000
            )
            logger.info(f"Successfully connected to Kafka at {bootstrap_servers}")
        except NoBrokersAvailable as e:
            logger.error(f"Could not connect to Kafka broker at {bootstrap_servers}: {e}")
            raise
        
    def send_notification(self, notification_data: Dict[str, Any]):
        """Send notification to Kafka topic"""
        try:
            self.producer.send(self.topic, notification_data)
            self.producer.flush()
            logger.info(f"Successfully sent notification to topic {self.topic}")
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")
            raise
        
    @staticmethod
    def process_notification(notification_data: Dict[str, Any]):
        """Process notification and add to database"""
        db = ReminderDatabase()
        db.connect()
        try:
            db.add_reminder(
                chat_id=notification_data['chat_id'],
                remind_id=notification_data['remind_id'],
                text=notification_data['text'],
                time=notification_data['time']
            )
            logger.info(f"Successfully processed notification for chat_id {notification_data['chat_id']}")
        except Exception as e:
            logger.error(f"Failed to process notification: {e}")
            raise
        finally:
            db.close()

def start_kafka_consumer(bootstrap_servers: str = 'localhost:9092', topic: str = 'notifications'):
    """Start Kafka consumer to process notifications"""
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='reminder_group'
        )
        logger.info(f"Successfully started Kafka consumer for topic {topic}")
        
        for message in consumer:
            try:
                KafkaNotificationHandler.process_notification(message.value)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    except NoBrokersAvailable as e:
        logger.error(f"Could not connect to Kafka broker at {bootstrap_servers}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error in Kafka consumer: {e}")
        raise 