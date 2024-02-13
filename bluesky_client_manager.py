import os
from atproto import Client


class BlueskyClientManager:
    def __init__(self):
        self.session_string = ""

    def get(self):
        client = Client()
        try:
            client.login(session_string=self.session_string)
            return client
        except Exception as e:
            print(e)

        for retry in range(3):
            try:
                client.login(os.environ.get("USERNAME"), os.environ.get("PASSWORD"))
                self.session_string = client.export_session_string()
                return client
            except Exception as e:
                print(e)
                pass


