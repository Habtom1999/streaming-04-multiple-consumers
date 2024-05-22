"""
This program listens for work messages continuously.
Start multiple versions to add more workers.
Author: Habtom Woldu
Date: 21 May 2024
"""

import pika
import sys
import time

# Define a callback function to be called when a message is received
def callback(ch, method, properties, body):
    """Define behavior on getting a message."""
    # Decode the binary message body to a string
    print(f"[X] Received {body.decode()}")
    # Simulate work by sleeping for the number of dots in the message
    time.sleep(body.count(b"."))
    # When done with task, tell the user
    print(" [X] Done.")
    # Acknowledge the message was received and processed
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Define a main function to run the program
def main(hn: str = "localhost", qn: str = "task_queue3"):
    """ Continuously listen for task messages on a named queue."""
    # When a statement can go wrong, use a try-except block
    try:
        # Create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))
    except Exception as e:
        print()
        print("ERROR: Connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # Use the connection to create a communication channel
        channel = connection.channel()

        # Use the channel to declare a durable queue
        # A durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # Messages will not be deleted until the consumer acknowledges 
        channel.queue_declare(queue=qn, durable=True)

        # The QoS level controls the number of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance
        # prefetch_count = Per consumer limit of unacknowledged messages
        channel.basic_qos(prefetch_count=1)

        # Configure the channel to listen on a specific queue,
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume(queue=qn, on_message_callback=callback)

        # Print a message to the console for the user
        print("[*] Ready for work. To exit press CTRL+C")

        # Start consuming messages via the communication channel
        channel.start_consuming()
    except Exception as e:
        print()
        print("ERROR: Something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print("User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # Call the main function with the information needed
    main("localhost", "task_queue3")
