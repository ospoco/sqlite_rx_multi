# Create a new file: sqlite_rx/monitor.py

import logging
import multiprocessing
import os
import signal
import time
from typing import Dict, List, Callable, Optional

LOG = logging.getLogger(__name__)

class ProcessMonitor:
    """
    Monitors and manages SQLite database processes.
    
    Features:
    - Regular health checks
    - Automatic process restart
    - Graceful shutdown
    - Statistics collection
    """
    
    def __init__(self, check_interval: int = 10):
        """
        Initialize the process monitor.
        
        Args:
            check_interval: Interval in seconds between process checks
        """
        self.processes = {}  # Maps process names to (process, start_time, restart_count) tuples
        self.check_interval = check_interval
        self.monitor_thread = None
        self.running = False
        self.stats = {
            "total_restarts": 0,
            "uptime": 0,
            "start_time": time.time()
        }
        
    def register_process(self, name: str, process: multiprocessing.Process):
        """
        Register a process to be monitored.
        
        Args:
            name: Unique name for the process
            process: The multiprocessing.Process instance
        """
        if process.is_alive():
            self.processes[name] = {
                "process": process,
                "start_time": time.time(),
                "restart_count": 0,
                "pid": process.pid
            }
            LOG.info(f"Registered process '{name}' (PID {process.pid}) for monitoring")
        else:
            LOG.warning(f"Cannot register non-running process '{name}'")
    
    def unregister_process(self, name: str):
        """
        Stop monitoring a process.
        
        Args:
            name: The name of the process to unregister
        """
        if name in self.processes:
            LOG.info(f"Unregistered process '{name}' from monitoring")
            del self.processes[name]
    
    def start_monitoring(self):
        """Start the monitoring thread."""
        if self.monitor_thread and self.monitor_thread.is_alive():
            LOG.warning("Monitoring thread is already running")
            return
            
        self.running = True
        self.monitor_thread = multiprocessing.Process(
            target=self._monitor_loop,
            name="SQLiteRx-ProcessMonitor"
        )
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        LOG.info(f"Process monitor started (checking every {self.check_interval} seconds)")
    
    def stop_monitoring(self):
        """Stop the monitoring thread."""
        self.running = False
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)
            if self.monitor_thread.is_alive():
                os.kill(self.monitor_thread.pid, signal.SIGTERM)
                self.monitor_thread.join(timeout=1)
        LOG.info("Process monitor stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop that runs in a separate process."""
        signal.signal(signal.SIGTERM, lambda signum, frame: setattr(self, 'running', False))
        
        while self.running:
            try:
                self._check_processes()
                time.sleep(self.check_interval)
            except Exception as e:
                LOG.exception(f"Error in process monitor: {e}")
    
    def _check_processes(self):
        """Check all registered processes and restart if needed."""
        for name, info in list(self.processes.items()):
            process = info["process"]
            
            if not process.is_alive():
                LOG.warning(f"Process '{name}' (PID {info['pid']}) is not running, restarting...")
                
                # Create a new process with the same target and args
                target = process._target
                args = process._args if process._args else ()
                kwargs = process._kwargs if process._kwargs else {}
                
                new_process = multiprocessing.Process(
                    target=target,
                    args=args,
                    kwargs=kwargs,
                    name=process.name
                )
                new_process.daemon = process.daemon
                
                # Start the new process
                new_process.start()
                
                # Update process info
                self.processes[name] = {
                    "process": new_process,
                    "start_time": time.time(),
                    "restart_count": info["restart_count"] + 1,
                    "pid": new_process.pid
                }
                
                # Update statistics
                self.stats["total_restarts"] += 1
                
                LOG.info(f"Process '{name}' restarted with new PID {new_process.pid}")
    
    def get_process_info(self, name: str = None):
        """
        Get information about monitored processes.
        
        Args:
            name: Optional name of a specific process
            
        Returns:
            Dict with process information
        """
        if name:
            if name in self.processes:
                info = self.processes[name].copy()
                info["uptime"] = time.time() - info["start_time"]
                return info
            return None
        
        # Return info for all processes
        result = {}
        for name, info in self.processes.items():
            process_info = info.copy()
            process_info["uptime"] = time.time() - info["start_time"]
            result[name] = process_info
        
        # Add overall stats
        self.stats["uptime"] = time.time() - self.stats["start_time"]
        result["__stats__"] = self.stats
        
        return result
    
    def restart_process(self, name: str):
        """
        Manually restart a process.
        
        Args:
            name: Name of the process to restart
            
        Returns:
            bool: True if successful, False otherwise
        """
        if name not in self.processes:
            LOG.warning(f"Process '{name}' not found")
            return False
            
        info = self.processes[name]
        process = info["process"]
        
        # Terminate the process
        try:
            os.kill(process.pid, signal.SIGTERM)
            process.join(timeout=5)
            
            if process.is_alive():
                os.kill(process.pid, signal.SIGKILL)
                process.join(timeout=1)
        except Exception as e:
            LOG.error(f"Error terminating process '{name}': {e}")
        
        # Create and start a new process
        target = process._target
        args = process._args if process._args else ()
        kwargs = process._kwargs if process._kwargs else {}
        
        new_process = multiprocessing.Process(
            target=target,
            args=args,
            kwargs=kwargs,
            name=process.name
        )
        new_process.daemon = process.daemon
        
        new_process.start()
        
        # Update process info
        self.processes[name] = {
            "process": new_process,
            "start_time": time.time(),
            "restart_count": info["restart_count"] + 1,
            "pid": new_process.pid
        }
        
        # Update statistics
        self.stats["total_restarts"] += 1
        
        LOG.info(f"Process '{name}' manually restarted with new PID {new_process.pid}")
        return True
