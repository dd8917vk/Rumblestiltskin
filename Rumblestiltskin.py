import json
import grpc
import time
import yaml
import pandas as pd
import pyvelociraptor
from pyvelociraptor import api_pb2
from pyvelociraptor import api_pb2_grpc
import io
import subprocess
import sys
import re
import requests

class Rumblestiltskin:
    def __init__(self):

        # Will use sysargv 1 or 2 below to replace redacted with the client short name
        self.raptor_api_key = self.set_raptor_api_key()
        self.rumble_api_key = self.get_creds()
        self.raptor_count = 0
        self.rumble_count = 0
    
    def get_creds(self):
        try:
            return sys.argv[2]
        except IndexError:
            with open('./creds.json') as credentials:
                creds = json.load(credentials)
            return creds["rumble"]

    def set_raptor_api_key(self):
        try:
            return f'/etc/velociraptor/{sys.argv[1]}.dfir.com.api.config.yaml'
        except IndexError:
            return f'blur.dfir.com.api.config.yaml'


    def get_rumble_data(self):
        url = "https://console.rumble.run/api/v1.0/export/org/assets.csv"

        payload={}
        headers = {
            'Authorization': f'Bearer {self.rumble_api_key}',
            'Cookie': 'redacted'
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        content = response.content
        data = pd.read_csv(io.StringIO(content.decode('utf-8'))) 
        new_data = data.filter(["names", "os", "type", "address"], axis=1)
        final_data = new_data[new_data["names"].notna()]
        final_data = final_data[final_data["type"].str.contains("^Desktop|Laptop|^Server", na=False)]
        pd.set_option('display.max_rows', None)
        final_data = final_data.reset_index(drop=True)
        return final_data

    
    def get_raptor_data(self):
        config_file = self.raptor_api_key
        config = pyvelociraptor.LoadConfigFile(config_file)
        query = "select os_info from clients()"
        creds = grpc.ssl_channel_credentials(
            root_certificates=config["ca_certificate"].encode("utf8"),
            private_key=config["client_private_key"].encode("utf8"),
            certificate_chain=config["client_cert"].encode("utf8"))
        options = (('grpc.ssl_target_name_override', "VelociraptorServer",),)

        # Empty list needed for api call
        env = []

        with grpc.secure_channel(config["api_connection_string"],
                                creds, options) as channel:
            stub = api_pb2_grpc.APIStub(channel)
            request = api_pb2.VQLCollectorArgs(
                max_wait=1,
                max_row=100,
                Query=[api_pb2.VQLRequest(
                    Name="Test",
                    VQL=query,
                )],
                env=env,
            )

            json_response_list = []
            for response in stub.Query(request):
                if response.Response:
                    package = json.loads(response.Response)
                    json_response_list.append(package)

            nested_dict = []
            for item in json_response_list:
                for d in item:
                    nested_dict.append(d)
            hostnames = [d["os_info"]["hostname"].lower() for d in nested_dict]
            return hostnames
        

    def compare_data(self):
        rumble_dataframe = self.get_rumble_data()
        raptor_hostnames = self.get_raptor_data()
        self.raptor_count = len(raptor_hostnames)
        rumble_names_list = []

        for n in rumble_dataframe["names"]:
            # print(n.split("."))
            hostname = re.split('[\s\.]', n)[0]
            rumble_names_list.append(hostname.lower())

        self.rumble_count = len(rumble_names_list)

        bools = []
        for n in rumble_names_list:
            if n not in raptor_hostnames:
                bools.append(False)
            else:
                bools.append(True)
        new_series = pd.Series(bools)
        rumble_dataframe["RaptorDeployed"] = new_series
        rumble_dataframe = rumble_dataframe[rumble_dataframe.RaptorDeployed != True]
        rumble_dataframe["RaptorCount"] = 'NaN'
        rumble_dataframe = rumble_dataframe.reset_index(drop=True)
        rumble_dataframe.loc[0, 'RaptorCount'] = self.raptor_count
        rumble_dataframe["RumbleCount"] = 'NaN'
        rumble_dataframe = rumble_dataframe.reset_index(drop=True)
        rumble_dataframe.loc[0, 'RumbleCount'] = self.rumble_count
        names_list = rumble_dataframe["names"].str.split(".")
        short_names = [name[0] for name in names_list]
        rumble_dataframe["names"] = short_names
        rumble_dataframe.to_csv(f'{sys.argv[1]}-rumble.csv', sep=",", index=False)
        #print(rumble_dataframe)
        

rumble_data = Rumblestiltskin().compare_data()
