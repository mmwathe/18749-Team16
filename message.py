from datetime import datetime
import uuid

# Define color functions for printing
def prGreen(skk): print("\033[92m{}\033[00m".format(skk))
def prRed(skk): print("\033[91m{}\033[00m".format(skk))
def prYellow(skk): print("\033[93m{}\033[00m".format(skk))
def prLightPurple(skk): print("\033[94m{}\033[00m".format(skk))
def prPurple(skk): print("\033[95m{}\033[00m".format(skk))
def prCyan(skk): print("\033[96m{}\033[00m".format(skk))

class Message:
    def __init__(self, client_id, message):
        self.timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.client_id = client_id
        self.message = message
        self.message_id = str(uuid.uuid4())  # Generate a unique message ID

    def __str__(self):
        prPurple("=" * 80)
        prYellow(f"{self.timestamp:<20} {self.client_id} -> S1")
        prLightPurple(f"{'':<20} {'Message ID:':<15} {self.message_id}")
        prLightPurple(f"{'':<20} {'Message:':<15} {self.message}")
        return ""