import logging
import sys

from neo4j import GraphDatabase, exceptions

logger = logging.getLogger(__name__)


def verify_connection(uri: str = "bolt://localhost:7687", user: str = "", password: str = ""):
    """Verify connection to Memgraph instance."""
    try:
        # Auth is disabled by default in our docker setup if env vars are empty
        auth = (user, password) if user and password else None

        with GraphDatabase.driver(uri, auth=auth) as driver:
            driver.verify_connectivity()
            records, summary, keys = driver.execute_query(
                'RETURN "Connection Successful" AS status;'
            )

            if records:
                status = records[0]["status"]
                print(f"✓ {status}")
            else:
                print("⚠ Connected but received no result.")

    except exceptions.ServiceUnavailable as e:
        logger.error(f"Failed to connect to Memgraph at {uri}. Is the container running?")
        print(f"ConnectionError: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        print(f"ConnectionError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    verify_connection()
