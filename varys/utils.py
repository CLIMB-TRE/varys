from collections import namedtuple
import sys
import json


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
                self.ca_certificate = profile_dict.get("ca_certificate", None)

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
