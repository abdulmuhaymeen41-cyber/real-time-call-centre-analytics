
import pyodbc
import random
import time
import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer
# ---------------------------------
# KAFKA CONFIGURATION
# ---------------------------------

load_dotenv(".env")

KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET")
}

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "call-events")
# ---------------------------------
# KAFKA PRODUCER
# ---------------------------------

def create_kafka_producer():
    return Producer(KAFKA_CONFIG)


def publish_call_event(producer, call_id, event_type, description):

    event = {
        "call_id": call_id,
        "event_type": event_type,
        "event_description": description,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "source": "call-centre-simulator"
    }

    producer.produce(
        topic=KAFKA_TOPIC,
        key=str(call_id),
        value=json.dumps(event).encode("utf-8")
    )

    producer.poll(0)

    print(f"Kafka event published: {event_type} | Call ID: {call_id}")# ---------------------------------
# DATABASE CONFIGURATION
# ---------------------------------

SERVER = os.getenv("SQL_SERVER")
DATABASE = os.getenv("SQL_DATABASE")
USERNAME = os.getenv("SQL_USERNAME")
PASSWORD = os.getenv("SQL_PASSWORD")


# ---------------------------------
# DATABASE CONNECTION
# ---------------------------------

def connect_to_database():

    password = PASSWORD

    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )

    return pyodbc.connect(connection_string)


# ---------------------------------
# RETRIEVE EXISTING DATA
# ---------------------------------

def get_call_centre_data(connection):

    cursor = connection.cursor()

    cursor.execute("SELECT CustomerID FROM dbo.Customers")
    customers = cursor.fetchall()

    cursor.execute("""
        SELECT AgentID
        FROM dbo.Agents
        WHERE AgentStatus = 'Available'
    """)
    agents = cursor.fetchall()

    cursor.execute("SELECT QueueID FROM dbo.Queues")
    queues = cursor.fetchall()

    cursor.close()

    return customers, agents, queues


# ---------------------------------
# INSERT CALL EVENT
# ---------------------------------

def insert_call_event(connection, call_id, event_type, description):

    cursor = connection.cursor()

    cursor.execute("""
        INSERT INTO dbo.CallEvents
        (CallID, EventType, EventTime, EventDescription)
        VALUES (?, ?, ?, ?)
    """, call_id, event_type, datetime.now(), description)

    connection.commit()
    cursor.close()


# ---------------------------------
# SIMULATE ONE CALL
# ---------------------------------

def simulate_call(connection, customers, agents, queues, producer):

    customer_id = random.choice(customers)[0]
    agent_id = random.choice(agents)[0]
    queue_id = random.choice(queues)[0]

    start_time = datetime.now()

    print("\n----------------------------------")
    print("NEW INCOMING CALL")
    print("----------------------------------")

    print(f"Customer ID: {customer_id}")
    print(f"Queue ID: {queue_id}")

    cursor = connection.cursor()

    # Create initial call record

    cursor.execute("""
        INSERT INTO dbo.Calls
        (CustomerID, AgentID, QueueID, CallStatus,
         StartTime, AnswerTime, EndTime, ResolutionNotes)
        OUTPUT INSERTED.CallID
        VALUES (?, NULL, ?, ?, ?, NULL, NULL, NULL)
    """, customer_id, queue_id, "Queued", start_time)

    call_id = cursor.fetchone()[0]

    connection.commit()
    cursor.close()

    print(f"Call ID: {call_id}")
    print("Status: INCOMING")

    # Record incoming event

    insert_call_event(
        connection,
        call_id,
        "Incoming",
        "Incoming customer call received"
    )
    publish_call_event(
        producer,
        call_id,
        "Incoming",
        "Incoming customer call received"
    )

    # Record queued event

    insert_call_event(
        connection,
        call_id,
        "Queued",
        f"Customer waiting in queue ID {queue_id}"
    )
    publish_call_event(
        producer,
        call_id,
        "Queued",
        f"Customer waiting in queue ID {queue_id}"
    )
    print("Status: QUEUED")

    # Simulate queue waiting time

    time.sleep(random.randint(2, 5))
    # Simulate customer abandoning the queue

    if random.random() < 0.20:

        end_time = datetime.now()

        cursor = connection.cursor()

        cursor.execute("""
            UPDATE dbo.Calls
            SET CallStatus = ?,
                EndTime = ?
            WHERE CallID = ?
        """, "Abandoned", end_time, call_id)

        connection.commit()
        cursor.close()

        insert_call_event(
            connection,
            call_id,
            "Abandoned",
            "Customer abandoned the call while waiting in queue"
        )

        publish_call_event(
            producer,
            call_id,
            "Abandoned",
            "Customer abandoned the call while waiting in queue"
        )

        print("Status: ABANDONED")
        print("Customer disconnected while waiting in queue")
        print("----------------------------------")

        return
      # Assign agent and answer call

    answer_time = datetime.now()

    cursor = connection.cursor()

    cursor.execute("""
        UPDATE dbo.Calls
        SET AgentID = ?,
            CallStatus = ?,
            AnswerTime = ?
        WHERE CallID = ?
    """, agent_id, "In Progress", answer_time, call_id)

    connection.commit()
    cursor.close()

    print(f"Agent ID: {agent_id}")
    print("Status: ASSIGNED")

    insert_call_event(
        connection,
        call_id,
        "Assigned",
        f"Call assigned to agent ID {agent_id}"
    )
    publish_call_event(
        producer,
        call_id,
        "Assigned",
        f"Call assigned to agent ID {agent_id}"
    )
    insert_call_event(
        connection,
        call_id,
        "In Progress",
        "Agent is handling the customer call"
    )
    publish_call_event(
        producer,
        call_id,
        "In Progress",
        "Agent is handling the customer call"
    )
    # Simulate conversation duration

    time.sleep(random.randint(5, 12))

    # Complete the call

    end_time = datetime.now()

    resolution = random.choice([
        "Customer enquiry successfully resolved",
        "Technical issue resolved",
        "Billing enquiry completed",
        "Customer complaint addressed",
        "Product information provided"
    ])

    cursor = connection.cursor()

    cursor.execute("""
        UPDATE dbo.Calls
        SET CallStatus = ?,
            EndTime = ?,
            ResolutionNotes = ?
        WHERE CallID = ?
    """, "Resolved", end_time, resolution, call_id)

    connection.commit()
    cursor.close()

    insert_call_event(
        connection,
        call_id,
        "Resolved",
        resolution
    )
    publish_call_event(
        producer,
        call_id,
        "Resolved",
        resolution
    )
    duration = int((end_time - start_time).total_seconds())

    print("Status: RESOLVED")
    print(f"Total call duration: {duration} seconds")
    print(f"Resolution: {resolution}")
    print("----------------------------------")


# ---------------------------------
# MAIN PROGRAM
# ---------------------------------

def main():

    print("Starting Call Centre Simulator...")

    producer = create_kafka_producer()
    print("Successfully connected to Kafka producer!")

    connection = connect_to_database()

    print("Successfully connected to SQL Server!")

    customers, agents, queues = get_call_centre_data(connection)

    print("\nExisting call centre data retrieved!")

    print(f"Customers available: {len(customers)}")
    print(f"Agents available: {len(agents)}")
    print(f"Queues available: {len(queues)}")

    if not customers or not agents or not queues:

        print("ERROR: Missing customers, agents or queues.")

        connection.close()
        return

    print("\nCall simulator is now running!")
    print("Press CTRL+C to stop.\n")

    try:

        while True:

            simulate_call(connection, customers, agents, queues, producer)

            print("Waiting before next incoming call...")
            time.sleep(2)

    except KeyboardInterrupt:

        print("\nSimulator stopped by user.")

    finally:

        producer.flush(15)

        connection.close()

        print("Kafka producer flushed.")
        print("Database connection closed.")


# ---------------------------------
# RUN PROGRAM
# ---------------------------------

if __name__ == "__main__":
    main()
