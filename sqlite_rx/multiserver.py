import logging
import os
import signal
import socket
import time
import uuid
from typing import Dict, List, Union, Optional

import billiard as multiprocessing  # Using billiard for better process management
import msgpack
import zmq
import zlib
from sqlite_rx import get_version
from sqlite_rx.auth import KeyMonkey
from sqlite_rx.exception import SQLiteRxZAPSetupError
from sqlite_rx.server import SQLiteServer

LOG = logging.getLogger(__name__)

class DatabaseProcess:
    """Represents a database process with its configuration and control info."""
    
    def __init__(self, 
                 database_name: str,
                 database_path: Union[bytes, str],
                 bind_port: int,
                 auth_config: dict = None,
                 use_encryption: bool = False,
                 use_zap_auth: bool = False,
                 curve_dir: str = None,
                 server_curve_id: str = None,
                 backup_database: str = None,
                 backup_interval: int = 600):
        """
        Initialize a database process configuration.
        
        Args:
            database_name: Name of the database for routing
            database_path: Path to the database file
            bind_port: Port on which the database server will listen
            auth_config: Authorization configuration
            use_encryption: Whether to use CurveZMQ encryption
            use_zap_auth: Whether to use ZAP authentication
            curve_dir: Directory containing curve keys
            server_curve_id: Server's curve key ID
            backup_database: Path to backup database
            backup_interval: Backup interval in seconds
        """
        self.database_name = database_name
        self.database_path = database_path
        self.bind_port = bind_port
        self.bind_address = f"tcp://127.0.0.1:{bind_port}"
        self.process_id = None
        self.process = None
        self.auth_config = auth_config
        self.use_encryption = use_encryption
        self.use_zap_auth = use_zap_auth
        self.curve_dir = curve_dir
        self.server_curve_id = server_curve_id
        self.backup_database = backup_database
        self.backup_interval = backup_interval
        

    def start(self):
        """Start the database process."""
        if self.process and self.process.is_alive():
            LOG.warning(f"Process for database '{self.database_name}' is already running")
            return
            
        # Create a server but DON'T pass 'name' in the constructor
        server = SQLiteServer(
            bind_address=self.bind_address,
            database=self.database_path,
            auth_config=self.auth_config,
            use_encryption=self.use_encryption,
            use_zap_auth=self.use_zap_auth,
            curve_dir=self.curve_dir,
            server_curve_id=self.server_curve_id,
            backup_database=self.backup_database,
            backup_interval=self.backup_interval
        )
        
        # Set the name directly instead
        server.name = f"DB-{self.database_name}-{uuid.uuid4().hex[:8]}"
        
        server.start()
        self.process = server
        self.process_id = server.pid
        LOG.info(f"Started database process for '{self.database_name}' on {self.bind_address} with PID {self.process_id}")
        
    def stop(self):
        """Stop the database process."""
        if not self.process:
            LOG.warning(f"No process found for database '{self.database_name}'")
            return
            
        if not self.process.is_alive():
            LOG.warning(f"Process for database '{self.database_name}' is not running")
            return
            
        try:
            LOG.info(f"Stopping database process for '{self.database_name}' (PID {self.process_id})")
            os.kill(self.process_id, signal.SIGTERM)
            # Give it some time to clean up
            self.process.join(timeout=5)
            
            # Force kill if still alive
            if self.process.is_alive():
                LOG.warning(f"Process for database '{self.database_name}' did not terminate, forcing...")
                os.kill(self.process_id, signal.SIGKILL)
                self.process.join(timeout=1)
        except Exception as e:
            LOG.error(f"Error stopping database process for '{self.database_name}': {e}")
        
        self.process = None
        self.process_id = None
        
    def is_running(self):
        """Check if the database process is running."""
        return self.process is not None and self.process.is_alive()


class SQLiteMultiServer(multiprocessing.Process):
    """A ZeroMQ ROUTER socket server that routes SQLite requests to separate database processes."""

    def __init__(self,
                 bind_address: str,
                 default_database: Union[bytes, str],
                 database_map: Dict[str, Union[bytes, str]] = None,
                 auth_config: dict = None,
                 curve_dir: str = None,
                 server_curve_id: str = None,
                 use_encryption: bool = False,
                 use_zap_auth: bool = False,
                 backup_dir: str = None,
                 backup_interval: int = 600,
                 base_port: int = 6000,
                 *args, **kwargs):
        """
        SQLiteMultiServer starts separate processes for each database.

        Args:
            bind_address: The address and port for the router socket
            default_database: Path to the default database
            database_map: Dictionary mapping database names to paths
            auth_config: Authorization configuration
            curve_dir: Directory for curve keys
            server_curve_id: Server's curve ID
            use_encryption: Whether to use encryption
            use_zap_auth: Whether to use ZAP authentication
            backup_dir: Directory for database backups
            backup_interval: Backup interval in seconds
            base_port: Starting port for database processes
        """
        super(SQLiteMultiServer, self).__init__(*args, **kwargs)
        self._bind_address = bind_address
        self._default_database = default_database
        self._database_map = database_map or {}
        self._auth_config = auth_config
        self._encrypt = use_encryption
        self._zap_auth = use_zap_auth
        self.server_curve_id = server_curve_id
        self.curve_dir = curve_dir
        self.backup_dir = backup_dir
        self.backup_interval = backup_interval
        self.base_port = base_port
        
        # Will be initialized in setup()
        self.context = None
        self.router_socket = None
        self.database_processes = {}
        self.identity_db_map = {}
        self.poller = None
        self.running = False
        
    def setup_database_processes(self):
        """Set up all database processes."""
        # Start with the default database
        default_backup = None
        if self.backup_dir:
            default_backup = os.path.join(self.backup_dir, "default_backup.db")
            
        default_process = DatabaseProcess(
            database_name="",
            database_path=self._default_database,
            bind_port=self.base_port,
            auth_config=self._auth_config,
            use_encryption=self._encrypt,
            use_zap_auth=self._zap_auth,
            curve_dir=self.curve_dir,
            server_curve_id=self.server_curve_id,
            backup_database=default_backup,
            backup_interval=self.backup_interval
        )
        self.database_processes[""] = default_process
        
        # Now set up each named database
        port = self.base_port + 1
        for db_name, db_path in self._database_map.items():
            backup_path = None
            if self.backup_dir:
                backup_path = os.path.join(self.backup_dir, f"{db_name}_backup.db")
                
            db_process = DatabaseProcess(
                database_name=db_name,
                database_path=db_path,
                bind_port=port,
                auth_config=self._auth_config,
                use_encryption=self._encrypt,
                use_zap_auth=self._zap_auth,
                curve_dir=self.curve_dir,
                server_curve_id=self.server_curve_id,
                backup_database=backup_path,
                backup_interval=self.backup_interval
            )
            self.database_processes[db_name] = db_process
            port += 1
            
    def start_database_processes(self):
        """Start all database processes."""
        for db_name, db_process in self.database_processes.items():
            db_process.start()
            # Give a short delay between process starts to avoid port conflicts
            time.sleep(0.2)
            
    def stop_database_processes(self):
        """Stop all database processes."""
        for db_name, db_process in self.database_processes.items():
            db_process.stop()
            
    def check_database_processes(self):
        """Check if all database processes are running and restart any that died."""
        for db_name, db_process in self.database_processes.items():
            if not db_process.is_running():
                LOG.warning(f"Database process for '{db_name or 'default'}' is not running, restarting...")
                db_process.start()
                # Give it a moment to start up
                time.sleep(0.5)
    
    def setup(self):
        """Set up the ROUTER socket and database connections."""
        LOG.info("Python Platform %s", multiprocessing.current_process().name)
        LOG.info("libzmq version %s", zmq.zmq_version())
        LOG.info("pyzmq version %s", zmq.__version__)
        
        # Create ZeroMQ context
        self.context = zmq.Context()
        
        # Initialize ROUTER socket for client connections
        self.router_socket = self.context.socket(zmq.ROUTER)
        
        if self._encrypt or self._zap_auth:
            server_curve_id = self.server_curve_id if self.server_curve_id else f"id_server_{socket.gethostname()}_curve"
            keymonkey = KeyMonkey(key_id=server_curve_id, destination_dir=self.curve_dir)

            if self._encrypt:
                LOG.info("Setting up encryption using CurveCP")
                self.router_socket = keymonkey.setup_secure_server(self.router_socket, self._bind_address)

            if self._zap_auth:
                if not self._encrypt:
                    raise SQLiteRxZAPSetupError("ZAP requires CurveZMQ(use_encryption = True) to be enabled.")

                LOG.info("ZAP enabled. Authorizing clients in %s.", keymonkey.authorized_clients_dir)
                # Note: ZAP auth would need to be properly implemented here

        self.router_socket.bind(self._bind_address)
        LOG.info(f"ROUTER socket bound to {self._bind_address}")
        
        # Set up polling for incoming messages
        self.poller = zmq.Poller()
        self.poller.register(self.router_socket, zmq.POLLIN)
        
        # Set up dealer sockets to each database process
        self.setup_database_processes()
        self.start_database_processes()

    def handle_client_request(self, message_parts):
        """Process a client request received on the ROUTER socket."""
        # The first part is the client identity
        identity = message_parts[0]
        
        # In the REQ/REP pattern, there's an empty delimiter frame after the identity
        empty_delimiter = message_parts[1]
        
        # The actual message content is in the last part
        message_data = message_parts[-1]
        
        try:
            # Decompress and unpack the message
            unpacked_message = msgpack.loads(zlib.decompress(message_data), raw=False)
            
            # Extract database name from the message
            database_name = unpacked_message.get("database_name", "")
            
            # Store the mapping between identity and database name for responses
            self.identity_db_map[identity] = database_name
            
            # Check if we have a process for this database
            if database_name in self.database_processes:
                db_process = self.database_processes[database_name]
                
                # Check for db process health with more detailed logging
                if not db_process.is_running():
                    LOG.warning(f"Database process for '{database_name or 'default'}' is not running, restarting...")
                    
                    # Try to determine why the process isn't running
                    exit_code = db_process.process.exitcode if hasattr(db_process.process, 'exitcode') else None
                    LOG.info(f"Previous process exit code: {exit_code}")
                    
                    # Start a new process
                    db_process.start()
                    LOG.info(f"Started new process for database '{database_name or 'default'}'")
                    
                    # Give it time to initialize
                    time.sleep(1.0)  # Increased delay to ensure the process is ready
                    
                    # Verify the process is now running
                    if not db_process.is_running():
                        LOG.error(f"Failed to restart database process for '{database_name or 'default'}'")
                        self._send_error_response(identity, empty_delimiter, 
                                                f"Unable to restart database process for '{database_name}'")
                        return
                
                # Forward the request to the database process
                try:
                    # Create a temporary dealer socket to forward the request
                    dealer_socket = self.context.socket(zmq.DEALER)
                    dealer_socket.setsockopt(zmq.LINGER, 0)  # Don't wait for unsent messages
                    dealer_socket.connect(db_process.bind_address)
                    
                    # Forward the message
                    dealer_socket.send_multipart(message_parts[1:])  # Skip the identity
                    
                    # Wait for a response with timeout
                    poller = zmq.Poller()
                    poller.register(dealer_socket, zmq.POLLIN)
                    
                    timeout_ms = 5000  # 5 seconds timeout
                    socks = dict(poller.poll(timeout_ms))
                    
                    if dealer_socket in socks and socks[dealer_socket] == zmq.POLLIN:
                        # Get the response
                        response_parts = dealer_socket.recv_multipart()
                        
                        # Send the response back to the client with the original identity
                        response = [identity] + response_parts
                        self.router_socket.send_multipart(response)
                    else:
                        # Handle timeout
                        LOG.warning(f"Timeout waiting for response from database '{database_name or 'default'}'")
                        self._send_error_response(identity, empty_delimiter, 
                                                f"Timeout waiting for response from database '{database_name}'")
                    
                    # Clean up the temporary socket
                    poller.unregister(dealer_socket)
                    dealer_socket.close()
                    
                except Exception as e:
                    LOG.exception(f"Error forwarding request to database '{database_name}': {e}")
                    self._send_error_response(identity, empty_delimiter, 
                                            f"Error communicating with database '{database_name}': {str(e)}")
                    
            else:
                # We don't have a process for this database
                LOG.warning(f"Database '{database_name}' not found")
                self._send_error_response(identity, empty_delimiter, f"Database '{database_name}' not found")
                
        except Exception as e:
            LOG.exception(f"Error processing client request: {e}")
            self._send_error_response(identity, empty_delimiter, f"Error processing request: {str(e)}")

    def _send_error_response(self, identity, empty_delimiter, error_message):
        """Helper method to send an error response to the client."""
        error = {
            "type": "sqlite_rx.exception.SQLiteRxError",
            "message": error_message
        }
        result = {"items": [], "error": error}
        
        try:
            compressed_result = zlib.compress(msgpack.dumps(result))
            self.router_socket.send_multipart([identity, empty_delimiter, compressed_result])
            LOG.debug(f"Error response sent to client {identity}: {error_message}")
        except Exception as send_error:
            LOG.error(f"Failed to send error response to client: {send_error}")

    def handle_signal(self, signum, frame):
        """Handle termination signals."""
        LOG.info(f"SQLiteMultiServer {self} PID {self.pid} received {signum}")
        LOG.info("SQLiteMultiServer Shutting down")
        
        # Stop all database processes
        self.stop_database_processes()
        
        # Close sockets
        if self.router_socket:
            self.router_socket.close()
        
        self.running = False
        
        raise SystemExit()

    def check_database_processes(self):
        """Check all database processes and restart any that have died."""
        for db_name, db_process in self.database_processes.items():
            if not db_process.is_running():
                LOG.warning(f"Database process for '{db_name or 'default'}' is not running, restarting...")
                
                # Try to determine why the process isn't running
                exit_code = db_process.process.exitcode if hasattr(db_process.process, 'exitcode') else None
                LOG.info(f"Process exit code: {exit_code}")
                
                # Start a new process
                db_process.start()
                LOG.info(f"Started new process for database '{db_name or 'default'}'")
                
                # Give it time to initialize
                time.sleep(0.5)

    def run(self):
        """Run the multi-database server."""
        # Set up signal handlers
        LOG.info("Setting up signal handlers")
        signal.signal(signal.SIGTERM, self.handle_signal)
        signal.signal(signal.SIGINT, self.handle_signal)
        
        # Set up sockets and connections
        self.setup()
        
        LOG.info(f"SQLiteMultiServer version {get_version()}")
        LOG.info(f"Ready to accept client connections on {self._bind_address}")

        self.running = True
        process_check_time = time.time()
        
        # Start the main loop
        while self.running:
            try:
                # Poll for events on the router socket
                socks = dict(self.poller.poll(1000))  # 1000ms timeout
                
                # Check for messages on the ROUTER socket (from clients)
                if self.router_socket in socks and socks[self.router_socket] == zmq.POLLIN:
                    message = self.router_socket.recv_multipart()
                    self.handle_client_request(message)
                
                # Periodically check if all database processes are alive (every 30 seconds)
                current_time = time.time()
                if current_time - process_check_time > 30:
                    self.check_database_processes()
                    process_check_time = current_time
                    
            except KeyboardInterrupt:
                LOG.info("Keyboard interrupt received, shutting down")
                break
            except Exception as e:
                LOG.exception(f"Error in main loop: {e}")
                
        # Clean up
        self.handle_signal(signal.SIGTERM, None)

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Set up database paths
    database_map = {
        "users": "users.db",
        "products": "products.db",
        "orders": "orders.db"
    }
    
    # Create and start the multi-process server
    server = SQLiteMultiServer(
        bind_address="tcp://0.0.0.0:5000",
        default_database="default.db",
        database_map=database_map,
        backup_dir="./backups",
        backup_interval=300  # 5 minutes
    )
    
    server.start()
    server.join()