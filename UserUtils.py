# Copyright 2020-2022 Google, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
from typing import List, Optional

import googleapiclient.discovery
from google.auth import iam
from google.auth.transport import requests
from google.oauth2 import service_account

SCOPES = [
    "https://www.googleapis.com/auth/cloud-identity.groups.readonly",
    "https://www.googleapis.com/auth/cloud-identity",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/admin.directory.group.readonly",
    "https://www.googleapis.com/auth/admin.directory.user.readonly",
    "https://www.googleapis.com/auth/admin.directory.group.member.readonly",
]
TOKEN_URI = 'https://accounts.google.com/o/oauth2/token'



class UserUtils:
    def __init__(self, data_domain: list, admin_email: str, domain: str):
        self.domain = domain
        self.admin_email = admin_email
        self.service = self.__create_service__(admin_email)
        if not data_domain:
            data_domain = []
        else:
            try:
                list_response = self.service.groups().list(domain=domain).execute()
            except Exception as e:
                logging.error(f"Unknown error while calling groups list method. {e}")
                raise
            data_domain = [(group["name"], group["email"], group["id"]) for group in list_response.get("groups", [])
                           if group["email"] in data_domain]
            # save 2 dicts of data domains to pair and groups to pair.
        self.groups = {_email: dict(data_domain=name, group_email=_email, group_key=_id)
                       for (name, _email, _id) in data_domain}

    def __create_service__(self, delegated_email: str):
        service_name = 'admin'
        api_version = 'directory_v1'
        try:
            delegated_credentials = self.get_credentials(delegated_email)
        except Exception as e:
            logging.error(f"Error getting the credentials; {e}")
            raise e
        service = googleapiclient.discovery.build(service_name, api_version, credentials=delegated_credentials)
        logging.info(f"Constructed service {service_name} {api_version}")
        return service

    def get_groups_for_user(self, user_id_or_email: str) -> List[dict]:
        logging.info(f"Fetching groups for user {user_id_or_email}")
        try:

            # for group in list_response
            groups = []
            for group_email, group in self.groups.items():
                is_member = self.service.members() \
                    .hasMember(groupKey=group_email, memberKey=user_id_or_email.replace("accounts.google.com:", "")) \
                    .execute()
                if is_member.get("isMember") is True:
                    groups.append(group)
            return groups
        except Exception as e:
            import sys
            logging.error("Error---")
            logging.exception(e)
            exc_type, exc_value, tb = sys.exc_info()
            tb.print_stack()
            tb.print_tb()
            tb.print_exception()
            return []

    @staticmethod
    def get_credentials(delegated_email: Optional[str]):
        # if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        #     credentials = service_account.Credentials.from_service_account_file(
        #         os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"), scopes=SCOPES)
        #     logging.info(f"Got credentials from env variable")
        # else:
        import google.auth
        credentials, project_id = google.auth.default()
        if delegated_email:
            subject = delegated_email.replace("accounts.google.com:", "")
            try:
                admin_creds = credentials.with_subject(subject).with_scopes(SCOPES)
                logging.info(
                    f"Created credentials direct; email {credentials.service_account_email}; subject {subject}")
            except AttributeError:  # Looks like a compute creds object
                # Refresh the boostrap credentials. This ensures that the information
                # about this account, notably the email, is populated.
                request = requests.Request()
                credentials.refresh(request)

                # Create an IAM signer using the bootstrap credentials.
                signer = iam.Signer(request, credentials,
                                    credentials.service_account_email)
                # Create OAuth 2.0 Service Account credentials using the IAM-based
                # signer and the bootstrap_credential's service account email.
                admin_creds = service_account.Credentials(
                    signer, credentials.service_account_email, TOKEN_URI,
                    scopes=SCOPES, subject=subject, project_id=project_id)
                logging.info(f"Created credentials using workaround; signer {signer}; email {credentials.service_account_email}; subject {subject}")
            except Exception as e:
                logging.exception(e)
                raise

            return admin_creds
        return credentials


if __name__ == "__main__":
    uu = UserUtils(['marketing@eyalbenivri-playground.net',
                    "sales@eyalbenivri-playground.net"],
                   "eyalbenivri@eyalbenivri-playground.net", "eyalbenivri-playground.net")

    """
    ('X-Goog-Authenticated-User-Id', 'accounts.google.com:<LONG>')
    ('X-Goog-Authenticated-User-Email', 'accounts.google.com:eyalbenivri@eyalbenivri-playground.net')
    ('X-Goog-Iap-Jwt-Assertion', '<VERY LONG HASH>')
    """
    email = "eyalbenivri@eyalbenivri-playground.net"
    groups = uu.get_groups_for_user(email)
    print(email, groups)

    email = "david-sales@eyalbenivri-playground.net"
    groups = uu.get_groups_for_user(email)
    print(email, groups)

    email = "shirley-marketing@eyalbenivri-playground.net"
    groups = uu.get_groups_for_user(email)
    print(email, groups)

    email = "bob-just-bob@eyalbenivri-playground.net"
    groups = uu.get_groups_for_user(email)
    print(email, groups)
    # groups = uu.get_groups_for_user(email, user_id)
    # print(groups)
