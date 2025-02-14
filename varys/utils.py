from collections import namedtuple
import sys
import json
import os


varys_message = namedtuple("varys_message", "basic_deliver properties body")


class configurator:
    def __init__(self, profile, config_path):
        try:
            with open(config_path, "rt") as config_fh:
                config_obj = json.load(config_fh)
        except Exception as e:
            print(
                f"Varys Configuration JSON parsing failed with exception:\n{e}",
                file=sys.stderr,
            )
            sys.exit(11)

        if config_obj["version"] != "0.1":
            print(
                "Version number in the ROZ configuration file does not appear to be current, ensure configuration format is correct if you experience errors",
                file=sys.stderr,
            )

        profile_dict = config_obj["profiles"].get(profile)
        if profile_dict:
            try:
                self.profile = profile
                self.username = profile_dict["username"]
                self.password = profile_dict["password"]
                self.ampq_url = profile_dict["amqp_url"]
                self.port = int(profile_dict["port"])

                self.use_tls = profile_dict["use_tls"]

                if self.use_tls:
                    if os.environ.get("VARYS_CA_CERTIFICATE"):
                        self.ca_certificate = os.environ.get("VARYS_CA_CERTIFICATE")

                    if profile_dict.get("ca_certificate"):
                        self.ca_certificate = profile_dict.get("ca_certificate", None)

                    if os.environ.get("VARYS_CLIENT_CERTIFICATE"):
                        self.client_certificate = os.environ.get("VARYS_CLIENT_CERTIFICATE")

                    if profile_dict.get("client_certificate"):
                        self.client_certificate = profile_dict["client_certificate"]

                    if os.environ.get("VARYS_CLIENT_KEY"):
                        self.client_key = os.environ.get("VARYS_CLIENT_KEY")

                    if profile_dict.get("client_key"):
                        self.client_key = profile_dict["client_key"]
                
                    if not self.ca_certificate or not self.client_certificate or not self.client_key:
                        print(
                            "Varys configuration JSON does not appear to contain the necessary fields for TLS configuration",
                            file=sys.stderr,
                        )
                        sys.exit(11)
                    
                    if not os.path.exists(self.ca_certificate) or not os.path.exists(self.client_certificate) or not os.path.exists(self.client_key):
                        print(
                            "Varys configuration JSON does not appear to contain the necessary files for TLS configuration",
                            file=sys.stderr,
                        )
                        sys.exit(11)

                        

            except KeyError:
                print(
                    f"Varys configuration JSON does not appear to contain the necessary fields for profile: {profile}",
                    file=sys.stderr,
                )
                sys.exit(11)
        else:
            print(
                f"Varys configuration JSON does not appear to contain the specified profile {profile}",
                file=sys.stderr,
            )
            sys.exit(2)
