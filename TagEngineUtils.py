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

import configparser
import hashlib
import uuid
from datetime import datetime
from datetime import timedelta
from typing import List, Callable, Tuple

import google.api_core.exceptions
from google.cloud import bigquery
from google.cloud import firestore

import DataCatalogUtils as dc

from enum import Enum


class SchedulingStatus(str, Enum):
    PENDING = "PENDING"
    READY = "READY"


class VerificationStatus(str, Enum):
    NOT_STARTED = "NOT_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    FAILED = "FAILED"
    SUCCEEDED = "SUCCEEDED"


class ConfigStatus(str, Enum):
    UNVERIFIED = "UNVERIFIED"
    PENDING = 'PENDING'
    ACTIVE = "ACTIVE"
    RUNNING = 'RUNNING'
    INACTIVE = 'INACTIVE'
    ERROR = "ERROR"
    PROCESSING_FORMAT = "PROCESSING: {}% complete"


class TagEngineUtils:

    def __init__(self):

        self.db = firestore.Client()

        # TODO: Do we need this? Doesn't seem this has usage in the code.
        config = configparser.ConfigParser()
        config.read("tagengine.ini")

    # region per-user entities

    def read_default_settings(self, user_id: str):

        settings = {}
        exists = False

        doc_ref = self.db.collection('settings').document(f'default_tag_template_{user_id}')
        doc = doc_ref.get()

        if doc.exists:
            settings = doc.to_dict()
            exists = True

        return exists, settings

    def write_default_settings(self, user_id: str, template_id: str, project_id: str, region: str, group_key: str):
        report_settings = self.db.collection('settings')
        doc_ref = report_settings.document(f'default_tag_template_{user_id}')
        doc_ref.set({
            'template_id': template_id,
            'project_id': project_id,
            'region': region,
            'user_id': user_id,
            'group_key': group_key,
        })
        print('Saved default settings.')

    # endregion

    # region supporting per-group settings

    @staticmethod
    def __get_key_for_group__(perf: str, group_key: str):
        return f'{perf}_{group_key}'

    @staticmethod
    def __map_groups_to_per_group_settings__(method: Callable[[dict], Tuple[bool, dict]], groups: List[dict]) -> \
            List[Tuple[bool, dict, dict]]:
        return [method(group) + (group,) for group in groups]

    def __read_settings_by_group__(self, pref: str, group: dict, enabled_on_exists: bool = False):
        settings = {}
        enabled = False

        doc_ref = self.db.collection('settings').document(self.__get_key_for_group__(pref, group['group_key']))
        doc = doc_ref.get()

        if doc.exists:
            settings = doc.to_dict()
            if settings['group_key']:
                assert settings['group_key'] == group['group_key']
                settings['group'] = group

            # Some entities need an enabled flag - and some entities just want to know if the document exists.
            # This trick will read either the property or the True value passed for that behaviour.
            if settings.get('enabled', enabled_on_exists):
                enabled = True

        return enabled, settings

    # endregion

    # region per-group settings
    def read_tag_history_settings_for_groups(self, groups: List[dict]) -> List[Tuple[bool, dict, dict]]:
        return self.__map_groups_to_per_group_settings__(self.read_tag_history_settings, groups)

    def write_tag_history_settings(self, group_key, user_id, enabled, project_id, region, dataset):
        history_settings = self.db.collection('settings')
        doc_ref = history_settings.document(self.__get_key_for_group__('tag_history', group_key))
        doc_ref.set({
            'enabled': bool(enabled),
            'project_id': project_id,
            'region': region,
            'dataset': dataset,
            'group_key': group_key,
            'user_id': user_id,
        })

        print('Saved tag history settings.')

        # assume that the BQ dataset exists
        # bqu = bq.BigQueryUtils()
        # bqu.create_dataset(project_id, region, dataset)

    def read_tag_history_settings(self, group: dict):
        return self.__read_settings_by_group__('tag_history', group)

    def read_tag_stream_settings_for_groups(self, groups: List[dict]) -> List[Tuple[bool, dict, dict]]:
        return [self.read_tag_stream_settings(group) + (group,) for group in groups]

    def read_tag_stream_settings(self, group: dict):
        return self.__read_settings_by_group__('tag_stream', group)

    def write_tag_stream_settings(self, group_key: str, user_id, enabled, project_id, topic):
        history_settings = self.db.collection('settings')
        doc_ref = history_settings.document(self.__get_key_for_group__('tag_stream', group_key))
        doc_ref.set({
            'enabled': bool(enabled),
            'project_id': project_id,
            'topic': topic,
            'group_key': group_key,
            'user_id': user_id,
        })
        print('Saved tag stream settings.')

    def read_coverage_settings_for_groups(self, groups: List[dict]):
        return self.__map_groups_to_per_group_settings__(self.read_coverage_settings, groups)

    def read_coverage_settings(self, group: dict):
        return self.__read_settings_by_group__('coverage', group, enabled_on_exists=True)

    def write_coverage_settings(self, group_key: str, user_id, project_ids, datasets, tables):
        report_settings = self.db.collection('settings')
        doc_ref = report_settings.document(self.__get_key_for_group__('coverage', group_key))
        doc_ref.set({
            'project_ids': project_ids,
            'excluded_datasets': datasets,
            'excluded_tables': tables,
            'group_key': group_key,
            'user_id': user_id,
        })

        print('Saved coverage settings.')

    def generate_coverage_report(self, user_email: str, groups: List[dict]):
        report = []
        coverage_settings_per_group = self.read_coverage_settings_for_groups(groups)
        for exists, settings, group in coverage_settings_per_group:
            group_report_entry = {"exists": exists, "settings": settings, "group": group}
            if exists:
                group_report_entry["project_reports"] = {}
                project_ids = settings['project_ids']
                excluded_datasets = settings['excluded_datasets'] or []
                excluded_tables = settings['excluded_tables'] or []

                print('project_ids: ' + str(project_ids))
                print('excluded_datasets: ' + str(excluded_datasets))
                print('excluded_tables: ' + str(excluded_tables))

                # Note: What is this used for?
                log_ref = self.db.collection('logs')

                # list datasets and tables for chosen projects
                for project in project_ids.split(','):
                    project_id = project.strip()
                    group_report_entry["project_reports"][project_id] = {"project_id": project_id, "datasets": {}}
                    bq_client = bigquery.Client(project=project_id)
                    try:
                        datasets = list(bq_client.list_datasets())
                    except google.api_core.exceptions.BadRequest as br:
                        group_report_entry["project_reports"][project_id]['error'] = br.errors[0]['message']
                        continue
                    except Exception as e:
                        group_report_entry["project_reports"][project_id]['error'] = str(e)
                        continue

                    # Note: What is this used for?
                    total_tags = 0

                    for dataset in datasets:
                        dataset_id = dataset.dataset_id
                        if project_id + "." + dataset_id in excluded_datasets:
                            # print('skipping ' + project_id + "." + dataset_id)
                            continue

                        print("dataset: " + dataset_id)
                        qualified_dataset = project_id + "." + dataset_id
                        group_report_entry["project_reports"][project_id]["datasets"][dataset_id] = {
                            "qualified_dataset": qualified_dataset,
                            "overall_sum": 0,
                            "tables": {}
                        }
                        tables = list(bq_client.list_tables(dataset_id))

                        dcu = dc.DataCatalogUtils(user_email)
                        linked_resources = dcu.search_catalog(project_id, dataset_id)

                        print('linked_resources: ' + str(linked_resources))

                        for table in tables:
                            print("full_table_id: " + str(table.full_table_id))
                            table_path_full = table.full_table_id.replace(':', '/datasets/').replace('.', '/tables/')
                            table_path_short = table.full_table_id.replace(':', '.')
                            table_name = table_path_full.split('/')[4]

                            print('table_path_full: ' + table_path_full)
                            print('table_path_short: ' + table_path_short)
                            print('table_name: ' + table_name)

                            # NOTE: I changed this, as this felt like a bug.
                            if f'{table_path_short}.{project_id}' in excluded_tables:
                                print('skipping ' + table_path_short)
                                continue
                            group_report_entry["project_reports"][project_id]["datasets"][dataset_id]["tables"][
                                table.full_table_id] = {
                                "full_table_id": table.full_table_id,
                                "table_path_full": table_path_full,
                                "table_path_short": table_path_short,
                                "table_name": table_name,
                                "tag_count": 0,
                            }

                            if table_name in linked_resources:
                                tag_count = linked_resources[table_name]
                                group_report_entry["project_reports"][project_id]["datasets"][
                                    dataset_id]["overall_sum"] += tag_count
                                print("tag_count = " + str(tag_count))
                                print("overall_sum = " + str(
                                    group_report_entry["project_reports"][project_id]["datasets"][dataset_id][
                                        "overall_sum"]))

                                # add the table name and tag count to a list
                                group_report_entry["project_reports"][project_id]["datasets"][dataset_id]["tables"][
                                    table.full_table_id]["tag_count"] = tag_count

            report.append(group_report_entry)
        return report

    def update_verification_status(self, config_uuid, config_type, status: VerificationStatus):

        coll_name = self.get_config_collection(config_type)
        self.db.collection(coll_name).document(config_uuid).update({
            'verification_status': status
        })

    def update_config_status(self, config_uuid, config_type, status: ConfigStatus):

        coll_name = self.get_config_collection(config_type)
        self.db.collection(coll_name).document(config_uuid).update({
            'config_status': status
        })

    def update_scheduling_status(self, config_uuid, config_type, status: SchedulingStatus):

        coll_name = self.get_config_collection(config_type)
        config_ref = self.db.collection(coll_name).document(config_uuid)
        doc = config_ref.get()

        if doc.exists:
            config = doc.to_dict()

            if 'scheduling_status' in config:
                config_ref.update({'scheduling_status': status})

    def increment_version_next_run(self, config_uuid, config_type):

        config = self.read_config(config_uuid, config_type)

        version = config.get('version', 0) + 1
        delta = config.get('refresh_frequency', 24)
        unit = config.get('refresh_unit', 'hours')

        if unit == 'minutes':
            next_run = datetime.utcnow() + timedelta(minutes=delta)
        elif unit == 'hours':
            next_run = datetime.utcnow() + timedelta(hours=delta)
        elif unit == 'days':
            next_run = datetime.utcnow() + timedelta(days=delta)
        else:
            raise Exception(f"Invalid unit detected {unit}")

        coll_name = self.get_config_collection(config_type)
        self.db.collection(coll_name).document(config_uuid).update({
            'version': version,
            'next_run': next_run
        })

    def update_overwrite_flag(self, config_uuid, config_type):

        coll_name = self.get_config_collection(config_type)
        self.db.collection(coll_name).document(config_uuid).update({
            'overwrite': False
        })

    def read_template_config(self, template_uuid):

        template_ref = self.db.collection('tag_templates').document(template_uuid)
        doc = template_ref.get()

        if doc.exists:
            template_config = doc.to_dict()
            # print(str(config))

        return template_config

    def read_tag_template(self, template_id, project_id, region, group_key):

        template_exists = False
        template_uuid = ""

        # check to see if this template already exists
        template_ref = self.db.collection('tag_templates')
        query = (
            template_ref.where('template_id', '==', template_id)
            .where('project_id', '==', project_id)
            .where('region', '==', region)
            .where('group_key', '==', group_key)
        )

        matches = query.get()

        # should either be a single matching template or no matching templates
        if len(matches) == 1:
            if matches[0].exists:
                print('Tag Template exists. Template uuid: ' + str(matches[0].id))
                template_uuid = matches[0].id
                template_exists = True

        return (template_exists, template_uuid)

    def write_tag_template(self, group_key, template_id, project_id, region):
        template_exists, template_uuid = self.read_tag_template(template_id, project_id, region, group_key)
        if not template_exists:
            print('tag template {} doesn\'t exist. Creating new template'.format(template_id))
            template_uuid = uuid.uuid1().hex

            doc_ref = self.db.collection('tag_templates').document(template_uuid)
            doc_ref.set({
                'template_uuid': template_uuid,
                'template_id': template_id,
                'project_id': project_id,
                'region': region,
                'group_key': group_key,
            })

        return template_uuid

    def __write_config__(self, config_type, group_key, owner_user_id, fields, included_uris,
                         excluded_uris, template_uuid, refresh_mode, refresh_frequency, refresh_unit, tag_history,
                         tag_stream, **kwargs):
        included_uris_hash = hashlib.md5(included_uris.encode()).hexdigest()
        configs_ref = self.db.collection(config_type)
        config_type_short = config_type.replace('_configs', '').upper()
        # check to see if this config already exists
        query = (
            configs_ref.where('template_uuid', '==', template_uuid)
            .where('included_uris_hash', '==', included_uris_hash)
            .where('config_type', '==', config_type_short)  # TODO: do we need this
            .where('config_status', '!=', ConfigStatus.INACTIVE)
            .where('group_key', '==', group_key)
        )
        matches = query.get()
        for match in matches:
            if match.exists:
                config_uuid_match = match.id
                # print('Config already exists. Config_uuid: ' + str(config_uuid_match))

                # update status to INACTIVE
                self.db.collection(config_type).document(config_uuid_match).update({
                    'config_status': ConfigStatus.INACTIVE
                })
                print('Updated status to INACTIVE.')

        config_uuid = uuid.uuid1().hex
        config = self.db.collection(config_type)
        doc_ref = config.document(config_uuid)
        doc = {
            'config_uuid': config_uuid,
            'config_type': config_type_short,
            'config_status': ConfigStatus.UNVERIFIED,
            'verification_status': VerificationStatus.NOT_STARTED,
            'creation_time': datetime.utcnow(),
            'fields': fields,
            'included_uris': included_uris,
            'included_uris_hash': included_uris_hash,
            'excluded_uris': excluded_uris,
            'template_uuid': template_uuid,
            'refresh_mode': refresh_mode,
            'refresh_frequency': 0,
            'tag_history': tag_history,
            'tag_stream': tag_stream,
            'group_key': group_key,
            'owner_user_id': owner_user_id,
            'version': 1,
        }
        doc.update(kwargs)
        if refresh_mode == 'AUTO':
            delta, next_run = self.validate_auto_refresh(refresh_frequency, refresh_unit)
            doc.update({
                'refresh_frequency': delta,
                'refresh_unit': refresh_unit,
                'next_run': next_run,
                'scheduling_status': SchedulingStatus.PENDING,
            })
        doc_ref.set(doc)

        print(f'Created new {config_type_short} config.')

        return config_uuid, included_uris_hash

    @staticmethod
    def validate_auto_refresh(refresh_frequency, refresh_unit):
        delta = 24
        if type(refresh_frequency) is str and refresh_frequency.isdigit():
            delta = int(refresh_frequency)

        if type(refresh_frequency) is int:
            if refresh_frequency > 0:
                delta = refresh_frequency

        if refresh_unit == 'minutes':
            next_run = datetime.utcnow() + timedelta(minutes=delta)
        elif refresh_unit == 'hours':
            next_run = datetime.utcnow() + timedelta(hours=delta)
        elif refresh_unit == 'days':
            next_run = datetime.utcnow() + timedelta(days=delta)
        else:
            next_run = datetime.utcnow() + timedelta(days=delta)  # default to days

        return delta, next_run

    def write_static_config(self, group_key, owner_user_id, fields, included_uris, excluded_uris,
                            template_uuid, refresh_mode, refresh_frequency, refresh_unit, tag_history, tag_stream,
                            overwrite=False, old_config_uuid=None):
        return self.__write_config__('static_configs',
                                     group_key=group_key,
                                     owner_user_id=owner_user_id,
                                     fields=fields,
                                     included_uris=included_uris,
                                     excluded_uris=excluded_uris,
                                     template_uuid=template_uuid,
                                     refresh_mode=refresh_mode,
                                     refresh_frequency=refresh_frequency,
                                     refresh_unit=refresh_unit,
                                     tag_history=tag_history,
                                     tag_stream=tag_stream,
                                     overwrite=overwrite,
                                     old_config_uuid=old_config_uuid)

    def write_dynamic_config(self, group_key, owner_user_id, fields, included_uris, excluded_uris,
                             template_uuid, refresh_mode, refresh_frequency, refresh_unit, tag_history, tag_stream,
                             old_config_uuid=None):
        return self.__write_config__('dynamic_configs',
                                     group_key=group_key,
                                     owner_user_id=owner_user_id,
                                     fields=fields,
                                     included_uris=included_uris,
                                     excluded_uris=excluded_uris,
                                     template_uuid=template_uuid,
                                     refresh_mode=refresh_mode,
                                     refresh_frequency=refresh_frequency,
                                     refresh_unit=refresh_unit,
                                     tag_history=tag_history,
                                     tag_stream=tag_stream,
                                     old_config_uuid=old_config_uuid)

    def write_entry_config(self, group_key, owner_user_id, fields, included_uris, excluded_uris,
                           template_uuid, refresh_mode, refresh_frequency, refresh_unit, tag_history, tag_stream,
                           old_config_uuid=None):
        print('** enter write_entry_config **')
        print('refresh_mode: ', refresh_mode)
        print('refresh_frequency: ', refresh_frequency)
        print('refresh_unit: ', refresh_unit)
        return self.__write_config__('entry_configs',
                                     group_key=group_key,
                                     owner_user_id=owner_user_id,
                                     fields=fields,
                                     included_uris=included_uris,
                                     excluded_uris=excluded_uris,
                                     template_uuid=template_uuid,
                                     refresh_mode=refresh_mode,
                                     refresh_frequency=refresh_frequency,
                                     refresh_unit=refresh_unit,
                                     tag_history=tag_history,
                                     tag_stream=tag_stream,
                                     old_config_uuid=old_config_uuid)

    def write_glossary_config(self, group_key, owner_user_id, fields, mapping_table, included_uris,
                              excluded_uris, template_uuid, refresh_mode, refresh_frequency, refresh_unit, tag_history,
                              tag_stream, overwrite=False, old_config_uuid=None):

        print('** enter write_glossary_config **')
        return self.__write_config__('glossary_config',
                                     group_key=group_key,
                                     owner_user_id=owner_user_id,
                                     fields=fields,
                                     included_uris=included_uris,
                                     excluded_uris=excluded_uris,
                                     template_uuid=template_uuid,
                                     refresh_mode=refresh_mode,
                                     refresh_frequency=refresh_frequency,
                                     refresh_unit=refresh_unit,
                                     tag_history=tag_history,
                                     tag_stream=tag_stream,
                                     overwrite=overwrite,
                                     mapping_table=mapping_table,
                                     old_config_uuid=old_config_uuid)

    def write_sensitive_config(self, group_key, owner_user_id, fields, dlp_dataset, mapping_table,
                               included_uris, excluded_uris, create_policy_tags, taxonomy_id, template_uuid,
                               refresh_mode, refresh_frequency, refresh_unit, tag_history, tag_stream, overwrite=False,
                               old_config_uuid=None):

        print('** enter write_sensitive_config **')
        return self.__write_config__('sensitive_configs',
                                     group_key=group_key,
                                     owner_user_id=owner_user_id,
                                     fields=fields,
                                     included_uris=included_uris,
                                     excluded_uris=excluded_uris,
                                     template_uuid=template_uuid,
                                     refresh_mode=refresh_mode,
                                     refresh_frequency=refresh_frequency,
                                     refresh_unit=refresh_unit,
                                     tag_history=tag_history,
                                     tag_stream=tag_stream,
                                     overwrite=overwrite,
                                     mapping_table=mapping_table,
                                     dlp_dataset=dlp_dataset,
                                     create_policy_tags=create_policy_tags,
                                     taxonomy_id=taxonomy_id,
                                     old_config_uuid=old_config_uuid)

    def write_restore_config(self, group_key, owner_user_id, config_status, source_template_uuid, source_template_id,
                             source_template_project, source_template_region, target_template_uuid, target_template_id,
                             target_template_project, target_template_region, metadata_export_location, tag_history,
                             tag_stream, overwrite=False):

        print('** write_restore_config **')

        # This is actually a bit different than other config types - the query is different from the generic query,
        # and logic as well (no refresh, different properties)

        # check to see if this config already exists
        configs_ref = self.db.collection('restore_configs')
        query = (
            configs_ref.where('source_template_uuid', '==', source_template_uuid)
            .where('target_template_uuid', '==', target_template_uuid)
            .where('config_status', '!=', ConfigStatus.INACTIVE)
            .where('group_key', '==', group_key)
        )

        matches = query.get()

        for match in matches:
            if match.exists:
                config_uuid_match = match.id
                print('config already exists. Found config_uuid: ' + str(config_uuid_match))

                # update status to INACTIVE
                self.db.collection('restore_configs').document(config_uuid_match).update({
                    'config_status': ConfigStatus.INACTIVE
                })
                print('Updated status to INACTIVE.')

        config_uuid = uuid.uuid1().hex
        configs = self.db.collection('restore_configs')
        doc_ref = configs.document(config_uuid)

        doc_ref.set({
            'config_uuid': config_uuid,
            'config_type': 'RESTORE',
            'config_status': config_status,
            'creation_time': datetime.utcnow(),
            'source_template_uuid': source_template_uuid,
            'source_template_id': source_template_id,
            'source_template_project': source_template_project,
            'source_template_region': source_template_region,
            'target_template_uuid': target_template_uuid,
            'target_template_id': target_template_id,
            'target_template_project': target_template_project,
            'target_template_region': target_template_region,
            'metadata_export_location': metadata_export_location,
            'tag_history': tag_history,
            'tag_stream': tag_stream,
            'group_key': group_key,
            'owner_user_id': owner_user_id,
            'overwrite': overwrite
        })

        return config_uuid

    def write_import_config(self, group_key, owner_user_id, config_status, template_uuid, template_id, template_project,
                            template_region, metadata_import_location, tag_history, tag_stream, overwrite=False):

        print('** write_import_config **')

        # This is actually a bit different than other config types - the query is different from the generic query,
        # and logic as well (no refresh, different properties)

        # check to see if this config already exists
        configs_ref = self.db.collection('import_configs')
        query = (
            configs_ref.where('template_uuid', '==', template_uuid)
            .where('metadata_import_location', '==', metadata_import_location)
            .where('config_status', '!=', ConfigStatus.INACTIVE)
            .where('group_key', '==', group_key)
        )

        matches = query.get()

        for match in matches:
            if match.exists:
                config_uuid_match = match.id
                print('config already exists. Found config_uuid: ' + str(config_uuid_match))

                # update status to INACTIVE
                self.db.collection('import_configs').document(config_uuid_match).update({
                    'config_status': ConfigStatus.INACTIVE
                })
                print('Updated status to INACTIVE.')

        config_uuid = uuid.uuid1().hex
        configs = self.db.collection('import_configs')
        doc_ref = configs.document(config_uuid)

        doc_ref.set({
            'config_uuid': config_uuid,
            'config_type': 'IMPORT',
            'config_status': config_status,
            'creation_time': datetime.utcnow(),
            'template_uuid': template_uuid,
            'template_id': template_id,
            'template_project': template_project,
            'template_region': template_region,
            'metadata_import_location': metadata_import_location,
            'tag_history': tag_history,
            'tag_stream': tag_stream,
            'group_key': group_key,
            'owner_user_id': owner_user_id,
            'overwrite': overwrite
        })

        return config_uuid

    def write_log_entry(self, dc_op, resource_type, resource, column, config_type, config_uuid, tag_id, template_uuid):

        log_entry = {}
        log_entry['ts'] = datetime.utcnow()
        log_entry['dc_op'] = dc_op
        log_entry['res_type'] = resource_type
        log_entry['config_type'] = 'MANUAL'
        log_entry['res'] = resource

        if len(column) > 0:
            log_entry['col'] = column

        log_entry['config_type'] = config_type
        log_entry['config_uuid'] = config_uuid
        log_entry['dc_tag_id'] = tag_id
        log_entry['template_uuid'] = template_uuid

        self.db.collection('logs').add(log_entry)
        # print('Wrote log entry.')

    def write_tag_op_error(self, op, config_uuid, config_type, msg):

        error = {}
        error['ts'] = datetime.utcnow()
        error['op'] = op
        error['config_uuid'] = config_uuid
        error['config_type'] = config_type
        error['msg'] = msg

        self.db.collection('tag_op_error').add(error)
        # print('Wrote error entry.')

    def write_tag_value_error(self, msg):

        error = {}
        error['ts'] = datetime.utcnow()
        error['error'] = msg

        self.db.collection('tag_value_error').add(error)
        # print('Wrote error entry.')

    @staticmethod
    def get_config_collection(config_type):
        coll = None
        if config_type == 'STATIC':
            coll = 'static_configs'
        if config_type == 'DYNAMIC':
            coll = 'dynamic_configs'
        if config_type == 'ENTRY':
            coll = 'entry_configs'
        if config_type == 'GLOSSARY':
            coll = 'glossary_configs'
        if config_type == 'SENSITIVE':
            coll = 'sensitive_configs'
        if config_type == 'RESTORE':
            coll = 'restore_configs'
        if config_type == 'IMPORT':
            coll = 'import_configs'
        if not coll:
            raise Exception(f"Unknown config type {config_type}")
        return coll

    def read_configs(self, group_key, template_id, project_id, region, config_type='ALL'):

        colls = []
        config_results = []

        if config_type == 'ALL':
            colls = ['dynamic_configs', 'static_configs', 'entry_configs', 'glossary_configs', 'sensitive_configs',
                     'restore_configs', 'import_configs']
        else:
            colls.append(self.get_config_collection(config_type))

        template_exists, template_uuid = self.read_tag_template(template_id, project_id, region, group_key)

        for coll_name in colls:
            configs_ref = self.db.collection(coll_name)

            if coll_name == 'restore_configs':
                docs = (
                    configs_ref
                    .where('target_template_uuid', '==', template_uuid)
                    .where('config_status', '!=', ConfigStatus.INACTIVE)
                    .where('group_key', '==', group_key)
                    .stream()
                )
            else:
                docs = (
                    configs_ref
                    .where('template_uuid', '==', template_uuid)
                    .where('config_status', '!=', ConfigStatus.INACTIVE)
                    .where('group_key', '==', group_key)
                    .stream()
                )

            for doc in docs:
                config = doc.to_dict()
                config_results.append(config)

        # print(str(configs))
        return config_results

    def read_config(self, config_uuid, config_type):

        config_result = {}
        coll_name = self.get_config_collection(config_type)

        config_ref = self.db.collection(coll_name).document(config_uuid)
        doc = config_ref.get()

        if doc.exists:
            config_result = doc.to_dict()

        return config_result

    def read_ready_configs(self):
        # This method is called internally by a scheduler, so we don't need to filter for a specific group
        ready_configs = []
        colls = ['static_configs', 'dynamic_configs', 'entry_configs', 'glossary_configs']

        for coll_name in colls:
            config_ref = self.db.collection(coll_name)
            config_ref = config_ref.where("refresh_mode", "==", "AUTO")
            config_ref = config_ref.where("scheduling_status", "==", SchedulingStatus.READY)
            config_ref = config_ref.where("config_status", "==", ConfigStatus.ACTIVE)
            config_ref = config_ref.where("next_run", "<=", datetime.utcnow())

            config_stream = list(config_ref.stream())

            for ready_config in config_stream:
                config_dict = ready_config.to_dict()
                ready_configs.append((config_dict['config_uuid'], config_dict['config_type']))

        return ready_configs

    def lookup_config_by_included_uris(self, group_key, template_uuid, included_uris, included_uris_hash):

        success = False
        config_result = {}

        colls = ['dynamic_configs', 'static_configs', 'entry_configs', 'glossary_configs', 'sensitive_configs']

        for coll_name in colls:

            config_ref = self.db.collection(coll_name)

            if included_uris is not None:
                docs = (
                    config_ref
                    .where('template_uuid', '==', template_uuid)
                    .where('config_status', '==', ConfigStatus.ACTIVE)
                    .where('included_uris', '==', included_uris)
                    .where('group_key', '==', group_key)
                    .stream()
                )
            if included_uris_hash is not None:
                docs = (
                    config_ref
                    .where('template_uuid', '==', template_uuid)
                    .where('config_status', '==', ConfigStatus.ACTIVE)
                    .where('included_uris_hash', '==', included_uris_hash)
                    .where('group_key', '==', group_key)
                    .stream()
                )

            for doc in docs:
                config_result = doc.to_dict()
                success = True
                return success, config_result

        return success, config_result

    def update_config(self, group_key, owner_user_id, old_config_uuid, config_type, fields,
                      included_uris, excluded_uris, template_uuid, refresh_mode, refresh_frequency, refresh_unit,
                      tag_history, tag_stream, overwrite=False, mapping_table=None):

        coll_name = self.get_config_collection(config_type)
        self.db.collection(coll_name).document(old_config_uuid).update({
            'config_status': ConfigStatus.INACTIVE
        })

        if config_type == 'STATIC':
            new_config_uuid, included_uris_hash = self.write_static_config(group_key=group_key,
                                                                           owner_user_id=owner_user_id,
                                                                           fields=fields,
                                                                           included_uris=included_uris,
                                                                           excluded_uris=excluded_uris,
                                                                           template_uuid=template_uuid,
                                                                           refresh_mode=refresh_mode,
                                                                           refresh_frequency=refresh_frequency,
                                                                           refresh_unit=refresh_unit,
                                                                           tag_history=tag_history,
                                                                           tag_stream=tag_stream,
                                                                           overwrite=overwrite,
                                                                           old_config_uuid=old_config_uuid)

        if config_type == 'DYNAMIC':
            new_config_uuid, included_uris_hash = self.write_dynamic_config(group_key=group_key,
                                                                            owner_user_id=owner_user_id,
                                                                            fields=fields,
                                                                            included_uris=included_uris,
                                                                            excluded_uris=excluded_uris,
                                                                            template_uuid=template_uuid,
                                                                            refresh_mode=refresh_mode,
                                                                            refresh_frequency=refresh_frequency,
                                                                            refresh_unit=refresh_unit,
                                                                            tag_history=tag_history,
                                                                            tag_stream=tag_stream,
                                                                            old_config_uuid=old_config_uuid)
        if config_type == 'ENTRY':
            new_config_uuid, included_uris_hash = self.write_entry_config(group_key=group_key,
                                                                          owner_user_id=owner_user_id,
                                                                          fields=fields,
                                                                          included_uris=included_uris,
                                                                          excluded_uris=excluded_uris,
                                                                          template_uuid=template_uuid,
                                                                          refresh_mode=refresh_mode,
                                                                          refresh_frequency=refresh_frequency,
                                                                          refresh_unit=refresh_unit,
                                                                          tag_history=tag_history,
                                                                          tag_stream=tag_stream,
                                                                          old_config_uuid=old_config_uuid)

        if config_type == 'GLOSSARY':
            new_config_uuid, included_uris_hash = self.write_glossary_config(group_key=group_key,
                                                                             owner_user_id=owner_user_id,
                                                                             fields=fields,
                                                                             mapping_table=mapping_table,
                                                                             included_uris=included_uris,
                                                                             excluded_uris=excluded_uris,
                                                                             template_uuid=template_uuid,
                                                                             refresh_mode=refresh_mode,
                                                                             refresh_frequency=refresh_frequency,
                                                                             refresh_unit=refresh_unit,
                                                                             tag_history=tag_history,
                                                                             tag_stream=tag_stream,
                                                                             overwrite=overwrite,
                                                                             old_config_uuid=old_config_uuid)
        # note: no need to return the included_uris_hash

        return new_config_uuid

    def update_sensitive_config(self, group_key, owner_user_id, old_config_uuid, dlp_dataset,
                                mapping_table, included_uris, excluded_uris, create_policy_tags, taxonomy_id,
                                template_uuid, refresh_mode, refresh_frequency, refresh_unit, tag_history, tag_stream,
                                overwrite):

        self.db.collection('sensitive_configs').document(old_config_uuid).update({
            'config_status': ConfigStatus.INACTIVE
        })

        config = self.read_config(old_config_uuid, 'SENSITIVE')

        new_config_uuid, included_uris_hash = self.write_sensitive_config(group_key=group_key,
                                                                          owner_user_id=owner_user_id,
                                                                          fields=config['fields'],
                                                                          dlp_dataset=dlp_dataset,
                                                                          mapping_table=mapping_table,
                                                                          included_uris=included_uris,
                                                                          excluded_uris=excluded_uris,
                                                                          create_policy_tags=create_policy_tags,
                                                                          taxonomy_id=taxonomy_id,
                                                                          template_uuid=template_uuid,
                                                                          refresh_mode=refresh_mode,
                                                                          refresh_frequency=refresh_frequency,
                                                                          refresh_unit=refresh_unit,
                                                                          tag_history=tag_history,
                                                                          tag_stream=tag_stream,
                                                                          overwrite=overwrite,
                                                                          old_config_uuid=old_config_uuid)

        return new_config_uuid

    def update_restore_config(self, group_key, owner_user_id, old_config_uuid, config_status, source_template_uuid,
                              source_template_id, source_template_project, source_template_region, target_template_uuid,
                              target_template_id, target_template_project, target_template_region,
                              metadata_export_location, tag_history, tag_stream, overwrite=False):

        self.db.collection('restore_configs').document(old_config_uuid).update({
            'config_status': ConfigStatus.INACTIVE
        })

        new_config_uuid = self.write_restore_config(group_key, owner_user_id, config_status, source_template_uuid,
                                                    source_template_id,
                                                    source_template_project, source_template_region,
                                                    target_template_uuid, target_template_id, target_template_project,
                                                    target_template_region, metadata_export_location, tag_history,
                                                    tag_stream, overwrite=False)  # the overwrite bit feels like a bug

        return new_config_uuid


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("tagengine.ini")

    te = TagEngineUtils()
    te.write_template('quality_template', config['DEFAULT']['PROJECT'], config['DEFAULT']['REGION'],
                      ConfigStatus.ACTIVE)
