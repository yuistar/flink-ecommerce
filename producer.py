import json
from datetime import datetime, timezone
import random
import time

from faker import Faker
from confluent_kafka import SerializingProducer

fake = Faker()


def generate_sales_transactions():
    user = fake.simple_profile()
    return {
        "transactionId": fake.uuid4(),
        "productId": random.choice(['product1', 'product2', 'product3', 'product4', 'product5', 'product6']),
        "productName": random.choice(['laptop', 'mobile', 'tablet', 'watch', 'headphone', 'speaker']),
        'productCategory': random.choice(['electronic', 'fashion', 'grocery', 'home', 'beauty', 'sports']),
        'productPrice': round(random.uniform(10, 1000), 2),
        'productQuantity': random.randint(1, 10),
        'productBrand': random.choice(['apple', 'samsung', 'oneplus', 'mi', 'boat', 'sony']),
        'currency': random.choice(['USD', 'GBP']),
        'customerId': user['username'],
        'transactionDate': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "paymentMethod": random.choice(['credit_card', 'debit_card', 'online_transfer'])
    }


class TransactionGen:
    def __init__(self):
        self.producer = SerializingProducer({
            "bootstrap.servers": "localhost:9092"
        })
        self.message_count = 0

    def delivery_report(self, err, msg):
        self.message_count += 1
        if err is not None:
            print(f"Message deliver failed: {err}")
        else:
            print(f"[{self.message_count}] Message delivered to: {msg.topic} [{msg.partition()}]")


    def produce_messages(self, topic = 'product_transactions'):

        curr_time = datetime.now(timezone.utc)

        while (datetime.now(timezone.utc) - curr_time).seconds < 120:
            try:
                transaction = generate_sales_transactions()
                transaction['totalAmount'] = transaction['productPrice'] * transaction['productQuantity']

                print(transaction)

                self.producer.produce(topic,
                                 key=transaction['transactionId'],
                                 value=json.dumps(transaction),
                                 on_delivery=self.delivery_report
                                 )
                self.producer.poll(0)

                #wait for 5 seconds before sending the next transaction
                time.sleep(5)
            except BufferError:
                print("Buffer full! Waiting...")
                time.sleep(1)
            except Exception as e:
                print(e)

if __name__ == "__main__":
    TransactionGen().produce_messages()