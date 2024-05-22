"""
This program sends a message to a queue on the RabbitMQ server.
Make tasks harder/longer-running by adding dots at the end of the message.
Author: Habtom Woldu
Date: 21 May 2024

Approach
---------
Work Queues - one task producer / many workers sharing work.
"""

import pika
import sys
import webbrowser
import csv
import time

# Define Global variables

# Decide if you want to show the offer to open RabbitMQ admin site 
# Input "True" or "False"
show_offer = "False"

def offer_rabbitmq_admin_site():
    """ Offer to open the RabbitMQ Admin website """
    ans = input("Would you like to monitor RabbitMQ queues? (y/n): ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.
    
    Parameters:
    host (str): The host name or IP address of the RabbitMQ server
    queue_name (str): The name of the queue
    message (str): The message to be sent to the queue
    """
    try:
        # Create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # Use the connection to create a communication channel
        ch = conn.channel()
        # Use the channel to declare a durable queue
        # A durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # Messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # Use the channel to publish a message to the queue
        # Every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        print(f"[x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit()
    finally:
        # Close the connection to the server
        conn.close()

def send_csv(input_file):
    """
    Creates a message for each row of a CSV file and sends it to the queue.
    This process runs and finishes.
    
    Parameters:
    input_file (str): The CSV file you want to send
    """
    # Show the offer to open Admin Site if show_offer is set to True, else open automatically
    if show_offer == "True":
        offer_rabbitmq_admin_site()
    else:
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()
    
    # Open the input file
    with open(input_file, "r") as open_file:
        # Read the input file
        reader = csv.reader(open_file, delimiter=",")
        # Get message from each row of file
        for row in reader:
            # Get each row of file as a string for the message 
            message = ",".join(row)
            # Send the message to the queue
            send_message("localhost", "task_queue3", message)
            # Wait for some time (e.g., 1 second)
            time.sleep(1)

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    send_csv('tasks.csv')
