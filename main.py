import subprocess
import sys
import os
import time
import signal
import threading

SCRIPTS = [
    "backend/basket_producer.py",
    "backend/preprocessor.py",
    "backend/classifier.py",
    "backend/expiration_service.py"
]

class ProcessManager:
    def __init__(self):
        self.processes = []

    def start_docker_compose(self):
        """Start Kafka and Zookeeper using Docker Compose."""
        try:
            print("Starting Kafka using Docker Compose...")
            proc = subprocess.Popen(
                ['docker-compose', 'up', '-d'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            stdout, stderr = proc.communicate(timeout=10)
            if proc.returncode != 0:
                print(f"Error starting Docker Compose:\n{stderr.decode()}")
                sys.exit(1)
            print("Kafka started successfully.")
        except subprocess.TimeoutExpired:
            print("Docker Compose is taking too long to start.")
            proc.kill()
            sys.exit(1)
        except FileNotFoundError:
            print("Docker Compose is not installed or not found in PATH.")
            sys.exit(1)

    def run_script(self, script_path):
        """Run a Python script as a subprocess."""
        if not os.path.exists(script_path):
            print(f"Script not found: {script_path}")
            return

        print(f"Starting script: {script_path}")
        proc = subprocess.Popen(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid  # To allow killing the whole process group
        )
        self.processes.append(proc)

        # Start a thread to monitor the script's output
        threading.Thread(target=self.monitor_process, args=(proc, script_path), daemon=True).start()

    def monitor_process(self, proc, script_path):
        """Monitor the stdout and stderr of a subprocess."""
        try:
            for line in iter(proc.stdout.readline, b''):
                print(f"[{script_path} STDOUT]: {line.decode().strip()}")
            for line in iter(proc.stderr.readline, b''):
                print(f"[{script_path} STDERR]: {line.decode().strip()}")
        except Exception as e:
            print(f"Error monitoring {script_path}: {e}")

    def run_all_scripts(self):
        """Run all producer and consumer scripts."""
        for script in SCRIPTS:
            self.run_script(script)
            time.sleep(1)

    def run_frontend(self):
        """Run the Streamlit frontend and send a newline to dismiss pop-up."""
        try:
            print("Starting Streamlit frontend...")
            proc = subprocess.Popen(
                [sys.executable, "-m", "streamlit", "run", "frontend/app.py", "--server.port", "8501"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE,  # Enable sending input
                preexec_fn=os.setsid
            )
            self.processes.append(proc)

            # A fix for when Streamlit asks for an email

            # Start a thread to monitor the script's output
            threading.Thread(target=self.monitor_process, args=(proc, "Streamlit frontend"), daemon=True).start()

            # Give Streamlit some time to start and then send a newline
            time.sleep(2)
            proc.stdin.write(b'\n')
            proc.stdin.flush()
        except subprocess.CalledProcessError as e:
            print(f"Error starting Streamlit frontend:\n{e}")

    def shutdown(self):
        """Terminate all running subprocesses and Docker Compose services."""
        print("\nShutting down all processes...")
        for proc in self.processes:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                proc.wait(timeout=5)
                print(f"Terminated process with PID {proc.pid}")
            except Exception as e:
                print(f"Failed to terminate process {proc.pid}: {e}")

        try:
            print("Stopping Docker Compose services...")
            subprocess.run(['docker-compose', 'down'], check=True)
            print("Docker Compose services stopped.")
        except subprocess.CalledProcessError as e:
            print(f"Error stopping Docker Compose:\n{e}")

def main():
    manager = ProcessManager()

    try:
        manager.start_docker_compose()

        print("Waiting for Kafka to initialize...")
        time.sleep(5)

        manager.run_all_scripts()

        print("Backend scripts are running.")

        manager.run_frontend()

        print("All producer and consumer scripts are running.")
        print("Press Ctrl+C to stop.")

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        manager.shutdown()
        print("All processes have been terminated.")
        sys.exit(0)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        manager.shutdown()
        sys.exit(1)


if __name__ == "__main__":
    main()
