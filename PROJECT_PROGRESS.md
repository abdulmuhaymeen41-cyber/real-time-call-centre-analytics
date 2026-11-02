
# Call Centre Real-Time Analytics Project

## Project objective
Build an end-to-end real-time call centre analytics pipeline.

SQL Server → Debezium → Kafka → Snowflake → Power BI

## Environment
- Windows 11
- Ubuntu 26.04.1 LTS (WSL 2)
- Python 3.14.4
- VS Code
- Docker 29.9.2
- SQL Server 2025 Developer Edition
- SSMS 22
- Database: CallCentreDB

## Completed
- Installed Python and VS Code.
- Installed WSL 2 and Ubuntu.
- Installed Git.
- Installed Docker and successfully ran hello-world.
- Installed SQL Server Developer Edition.
- Created CallCentreDB.
- Created Customers, Agents, Queues, Calls and CallEvents tables.
- Established foreign key relationships.
- Created sample call centre data and events.
- Created SQL login: callcentre_app.
- Granted db_datareader and db_datawriter.
- Configured SQL Server TCP/IP.
- Configured SQL Server to listen on port 1433.
- Created Windows Firewall inbound rule:
  Allow SQL Server from WSL.
- Verified Ubuntu can reach Windows SQL Server on port 1433.

## Important network configuration
Windows WSL gateway: 172.31.240.1
Ubuntu IP at last check: 172.31.254.248
SQL Server instance: SQLEXPRESS
SQL Server port: 1433

Firewall rule:
Allow SQL Server from WSL

## Python project
Location: ~/call-centre-project

Virtual environment:
venv

Activate using:
source venv/bin/activate

Installed libraries:
pyodbc

## Current position
Phase 6: Python-to-SQL Server connection.

Next task:
Create test_connection.py and test whether Python can connect to CallCentreDB using pyodbc.

SQL login:
callcentre_app

Password: Stored separately. Never put it in this document.

## Important learning notes
1. SQL authentication and database permissions are different from firewall permissions.
2. SQL Server must listen on a TCP port for network connections.
3. WSL has its own network interface and communicates with Windows through a virtual network.
4. Windows Firewall can block connections even when SQL Server is running.
5. ODBC Driver 18 provides the database connectivity layer.
6. pyodbc allows Python to communicate with SQL Server through ODBC.

## Future project stages
1. Complete Python-to-SQL Server connection.
2. Build automated call centre simulator.
3. Generate incoming calls and call events.
4. Configure SQL Server CDC.
5. Deploy Debezium and Kafka Connect.
6. Connect Confluent Cloud.
7. Stream data into Snowflake.
8. Build Power BI dashboards.

