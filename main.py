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
import datetime
import logging.config
import os
from typing import List

from flask import Flask, render_template, request, jsonify, json, session

import DataCatalogUtils as dc
import JobManager as jobm
import Resources as res
import TagEngineUtils as te
import Resources as res
import BackupFileParser as bfp
import CsvParser as cp
import constants

import JobManager as jobm
import TaskManager as taskm
import UserUtils as uu
import constants

config = configparser.ConfigParser()
config.read("tagengine.ini")
logging.config.dictConfig(
    {
        "version": 1,
        "root": {
            "handlers": ["console"],
            "level": "DEBUG"
        },
        "handlers": {
            "console": {
                "formatter": "std_out",
                "class": "logging.StreamHandler",
                "level": "DEBUG"
            }
        },
        "formatters": {
            "std_out": {
                "format": "%(asctime)s : %(levelname)s : %(module)s : %(funcName)s : %(lineno)d : : %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S.%s"
            }
        },
    }
)

app = Flask(__name__)
app.secret_key = config["DEFAULT"].get("APP_SECRET_KEY")
teu = te.TagEngineUtils()

data_domains = list(map(lambda x: x.strip(), config["DEFAULT"].get("DATA_DOMAINS", "").split(",")))
user_utils = uu.UserUtils(data_domains, config["DEFAULT"].get("ADMIN_EMAIL"), config["DEFAULT"].get("DOMAIN"))

# handles create requests from API and on-demand update requests from API (i.e. config contains refresh_mode = ON_DEMAND)
jm = jobm.JobManager(config['DEFAULT']['TAG_ENGINE_PROJECT'], config['DEFAULT']['QUEUE_REGION'],
                     config['DEFAULT']['INJECTOR_QUEUE'], "/_split_work")
tm = taskm.TaskManager(config['DEFAULT']['TAG_ENGINE_PROJECT'], config['DEFAULT']['QUEUE_REGION'],
                       config['DEFAULT']['WORK_QUEUE'], "/_run_task")


def get_group_from_group_key(group_key: str, groups: List[dict]) -> dict:
    return next(group for group in groups if group["group_key"] == group_key)


"""
Headers from IAP:
('X-Goog-Authenticated-User-Id', 'accounts.google.com:LONG_ID')
('X-Goog-Authenticated-User-Email', 'accounts.google.com:USER_EMAIL')
('X-Goog-Iap-Jwt-Assertion', 'VERY_LONG_HASH_OF_JWT_ASSERTION_KEY')
"""


@app.before_request
def get_groups():
    """
    Method for refreshing groups on each request, if it's not present in the session
    """
    if not session.get("groups") or not session.get("user_id"):
        user_id = request.headers.get("X-Goog-Authenticated-User-Id")
        if user_id and not session.get("user_id"):
            session["user_id"] = user_id
            session["user_email"] = request.headers.get("X-Goog-Authenticated-User-Email")
            session["user_jwt_assertion"] = request.headers.get("X-Goog-Iap-Jwt-Assertion")
        # HACK: for development environment, we cannot use IAP, so get the user_id from env var
        if os.environ.get("FLASK_ENV") == "development":
            if not session.get("user_id") and os.environ.get("USE_USER_ID"):
                session["user_id"] = os.environ.get("USE_USER_ID")
            if not session.get("user_email") and os.environ.get("USE_USER_EMAIL"):
                session["user_email"] = os.environ.get("USE_USER_EMAIL")
            if not session.get("user_jwt_assertion") and os.environ.get("USE_USER_ASSERTION"):
                session["user_jwt_assertion"] = os.environ.get("USE_USER_ASSERTION")
        if session.get('user_id'):
            logging.info(f"Getting groups for user {session['user_id']}")
            session["groups"] = user_utils.get_groups_for_user(session["user_id"])
            logging.info(f"Got groups ({session['groups']}) for user {session['user_id']}")


# #################### UI METHODS #################
# TODO: This might benefit from splitting into flask Blueprints
#  https://flask.palletsprojects.com/en/2.1.x/blueprints/#blueprints
@app.route("/")
def homepage():
    # Default settings are per user
    exists, settings = teu.read_default_settings(session["user_id"])
    if exists:
        template_id = settings['template_id']
        project_id = settings['project_id']
        region = settings['region']
        group = settings['group']
    else:
        template_id = ""
        project_id = ""
        region = ""
        group = ""

    # [END default_settings]
    # [START render_template]
    return render_template(
        'index.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        group=group,
        groups=session["groups"]
    )


@app.route("/default_settings<int:saved>", methods=["GET"])
def default_settings(saved):
    exists, settings = teu.read_default_settings(session["user_id"])
    if exists:
        template_id = settings['template_id']
        project_id = settings['project_id']
        region = settings['region']
        group = settings['group']
    else:
        template_id = ""
        project_id = ""
        region = ""
        group = ""

    # [END default_settings]
    # [START render_template]
    return render_template(
        'default_settings.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        group=group,
        groups=session['groups'],
        settings=saved)
    # [END render_template]


@app.route("/coverage_settings<int:saved>")
def coverage_settings(saved):
    groups_coverage_settings = teu.read_coverage_settings_for_groups(session["groups"])
    # [END report_settings]
    # [START render_template]
    return render_template(
        'coverage_settings.html',
        groups_coverage_settings=groups_coverage_settings,
        settings=saved)
    # [END render_template]


@app.route("/tag_history_settings<int:saved>", methods=["GET"])
def tag_history_settings(saved):
    groups_with_tag_history_settings = teu.read_tag_history_settings_for_groups(session["groups"])
    # [END tag_history_settings]
    # [START render_template]
    return render_template(
        'tag_history_settings.html',
        groups_with_tag_history_settings=groups_with_tag_history_settings,
        settings=saved)
    # [END render_template]


@app.route("/tag_stream_settings<int:saved>", methods=["GET"])
def tag_stream_settings(saved):
    groups_with_tag_stream = teu.read_tag_stream_settings_for_groups(session["groups"])

    # [END tag_stream_settings]
    # [START render_template]
    return render_template(
        'tag_stream_settings.html',
        groups_with_tag_stream=groups_with_tag_stream,
        settings=saved)
    # [END render_template]


@app.route("/set_default", methods=['POST'])
def set_default():
    template_id = request.form['template_id'].rstrip()
    project_id = request.form['project_id'].rstrip()
    region = request.form['region'].rstrip()
    group = get_group_from_group_key(request.form['group'].rstrip(), session['groups'])
    logging.info(f"Got set_default values: {request.form}")
    if template_id == "":
        template_id = None
    if project_id == "":
        project_id = None
    if region == "":
        region = None
    if group is not None or template_id is not None or project_id is not None or region is not None:
        teu.write_default_settings(session["user_id"], template_id, project_id, region, group)

    return default_settings(1)


@app.route("/set_tag_history", methods=['POST'])
def set_tag_history():
    group_key = request.form["group_key"].rstrip()
    enabled = request.form['enabled'].rstrip()
    project_id = request.form['project_id'].rstrip()
    region = request.form['region'].rstrip()
    dataset = request.form['dataset'].rstrip()

    print("group_key: " + group_key)
    print("enabled: " + enabled)
    print("project_id: " + project_id)
    print("region: " + region)
    print("dataset: " + dataset)

    if enabled == "on":
        enabled = True
    else:
        enabled = False

    # TODO: Get rid of magic strings
    if project_id == "{your_project_id}":
        project_id = None
    if region == "{your_region}":
        region = None
    if dataset == "{your_dataset}":
        dataset = None

    # can't be enabled if either of the required fields are NULL
    if enabled and (project_id is None or region is None or dataset is None):
        enabled = False
    # TODO: Use method
    group = next(group for group in session["groups"] if group["group_key"] == group_key)
    if group is not None and (project_id is not None or region is not None or dataset is not None):
        teu.write_tag_history_settings(group, session["user_id"], enabled, project_id, region, dataset)

        return tag_history_settings(1)
    else:
        return tag_history_settings(0)


@app.route("/set_tag_stream", methods=['POST'])
def set_tag_stream():
    group_key = request.form["group_key"].rstrip()
    enabled = request.form['enabled'].rstrip()
    project_id = request.form['project_id'].rstrip()
    topic = request.form['topic'].rstrip()

    print("group_key: " + group_key)
    print("enabled: " + enabled)
    print("project_id: " + project_id)
    print("topic: " + topic)

    if enabled == "on":
        enabled = True
    else:
        enabled = False

    if project_id == "":
        project_id = None
    if topic == "":
        topic = None

    # can't be enabled if either the required fields are NULL
    if enabled and (project_id is None or topic is None):
        enabled = False
    # TODO: User method
    group = next(group for group in session["groups"] if group["group_key"] == group_key)
    if group is not None and (project_id is not None or topic is not None):
        teu.write_tag_stream_settings(group, session["user_id"], enabled, project_id, topic)

        return tag_stream_settings(1)
    else:
        return tag_stream_settings(0)


@app.route("/set_coverage", methods=['POST'])
def set_coverage():
    group_key = request.form["group_key"].rstrip()
    project_ids = request.form['project_ids'].rstrip()
    datasets = request.form['datasets'].rstrip()
    tables = request.form['tables'].rstrip()

    print("project_ids: " + project_ids)
    print("datasets: " + datasets)
    print("tables: " + tables)

    if project_ids == "":
        project_ids = None
    if datasets == "":
        datasets = None
    if tables == "":
        tables = None
    group = next(group for group in session["groups"] if group["group_key"] == group_key)
    if group is not None and (project_ids is not None or datasets is not None or tables is not None):
        teu.write_coverage_settings(group, session["user_id"], project_ids, datasets, tables)

    return coverage_settings(1)


@app.route("/coverage_report")
def coverage_report():
    report = teu.generate_coverage_report(session['user_email'], session["groups"])

    return render_template(
        "coverage_report.html",
        report=report, )


# TO DO: re-implement this method using the DC API
@app.route("/coverage_details<string:res>", methods=['GET'])
def coverage_details(res):
    print("res: " + res)

    project_id, resource, *rest = res.split('.')

    configs = teu.read_configs_on_res(res)
    return render_template(
        'view_tags_on_res.html',
        resource=res,
        project_id=project_id,
        configs=configs)


# [START search_template]
@app.route('/search_template', methods=['POST'])
def search_template():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    group_key = request.form['group']
    # TODO: Use method
    group = next(group for group in session["groups"] if group["group_key"] == group_key)

    dcu = dc.DataCatalogUtils(session["user_email"], template_id, project_id, region)
    try:
        fields = dcu.get_template()
    except PermissionError as pe:
        return render_template("error.html", message=str(pe))

    # print("fields: " + str(fields))
    # [END search_template]
    # [START render_template]
    return render_template(
        'tag_template.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        group=group,
        fields=fields)
    # [END render_template]


@app.route('/choose_action', methods=['GET'])
def choose_action():
    template_id = request.args.get('template_id')
    project_id = request.args.get('project_id')
    region = request.args.get('region')
    group_key = request.args.get('group')
    group = next(group for group in session["groups"] if group["group_key"] == group_key)

    dcu = dc.DataCatalogUtils(session['user_email'], template_id, project_id, region)
    fields = dcu.get_template()
    # print("fields: " + str(fields))
    # [END search_template]
    # [START render_template]
    return render_template(
        'tag_template.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        group=group,
        fields=fields)
    # [END render_template]


# [START view_configs]
@app.route('/view_configs', methods=['GET'])
def view_configs():
    template_id = request.args.get('template_id')
    project_id = request.args.get('project_id')
    region = request.args.get('region')
    group_key = request.args.get('group')
    # TODO: Use method
    group = next(group for group in session["groups"] if group["group_key"] == group_key)

    print("template_id: " + str(template_id))
    print("project_id: " + str(project_id))
    print("region: " + str(region))

    dcu = dc.DataCatalogUtils(session['user_email'], template_id, project_id, region)
    template_fields = dcu.get_template()

    history_enabled, history_settings = teu.read_tag_history_settings()
    stream_enabled, stream_settings = teu.read_tag_stream_settings()

    configs = teu.read_configs(template_id, project_id, region)

    # print('configs: ', configs)

    return render_template(
        'view_configs.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        configs=configs,
        group=group,
    )
    # [END render_template]


# [START display_selected_action]
@app.route('/display_selected_action', methods=['POST'])
def display_selected_action():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    action = request.form['action']
    group = get_group_from_group_key(request.form['group'], session['groups'])

    print("template_id: " + str(template_id))
    print("project_id: " + str(project_id))
    print("region: " + str(region))
    print("action: " + str(action))

    dcu = dc.DataCatalogUtils(session['user_email'], template_id, project_id, region)
    template_fields = dcu.get_template()

    history_enabled, history_settings = teu.read_tag_history_settings(group)
    stream_enabled, stream_settings = teu.read_tag_stream_settings(group)

    if action == "View and Edit Configs":

        configs = teu.read_configs(template_id, project_id, region)

        print('configs: ', configs)

        return render_template(
            'view_configs.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            configs=configs)

    elif action == "Create Static Config":
        return render_template(
            'static_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template_fields,
            group=group,
            current_time=datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)

    elif action == "Create Dynamic Config":
        return render_template(
            'dynamic_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template_fields,
            group=group,
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)

    elif action == "Create Entry Config":
        return render_template(
            'entry_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            group=group,
            fields=template_fields,
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)

    elif action == "Create Glossary Config":
        return render_template(
            'glossary_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            group=group,
            fields=template_fields,
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)

    elif action == "Create Sensitive Config":
        return render_template(
            'sensitive_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            group=group,
            fields=template_fields,
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)

    elif action == "Create Restore Config":
        return render_template(
            'restore_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            group=group,
            fields=template_fields,
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)

    elif action == "Create Import Config":
        return render_template(
            'import_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            group=group,
            fields=template_fields,
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)

    # [END render_template]


@app.route('/update_config', methods=['POST'])
def update_config():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    config_uuid = request.form['config_uuid']
    config_type = request.form['config_type']

    # print("template_id: " + str(template_id))
    # print("project_id: " + str(project_id))
    # print("region: " + str(region))
    # print("config_uuid: " + str(config_uuid))
    # print("config_type: " + str(config_type))

    config = teu.read_config(config_uuid, config_type)
    print("config: " + str(config))

    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template_fields = dcu.get_template()
    # print("fields: " + str(template_fields))

    enabled, settings = teu.read_tag_history_settings()

    if enabled:
        tag_history = 1
    else:
        tag_history = 0

    print("tag_history: " + str(tag_history))

    enabled, settings = teu.read_tag_stream_settings()

    if enabled:
        tag_stream = 1
    else:
        tag_stream = 0

    print("tag_stream: " + str(tag_stream))

    if config_type == "STATIC":
        # [END update_tag]
        # [START render_template]
        return render_template(
            'update_static_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template_fields,
            config=config,
            current_time=datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            display_tag_history_option=tag_history,
            display_tag_stream_option=tag_stream)

    if config_type == "DYNAMIC":
        # [END display_action]
        # [START render_template]
        return render_template(
            'update_dynamic_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template_fields,
            config=config,
            display_tag_history_option=tag_history,
            display_tag_stream_option=tag_stream)

    if config_type == "ENTRY":
        # [END display_action]
        # [START render_template]
        return render_template(
            'update_entry_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template_fields,
            config=config,
            display_tag_history_option=tag_history,
            display_tag_stream_option=tag_stream)

    if config_type == "GLOSSARY":
        # [END display_action]
        # [START render_template]
        return render_template(
            'update_glossary_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template_fields,
            config=config,
            display_tag_history_option=tag_history,
            display_tag_stream_option=tag_stream)

    if config_type == "SENSITIVE":
        # [END display_action]
        # [START render_template]
        return render_template(
            'update_sensitive_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template_fields,
            config=config,
            display_tag_history_option=tag_history,
            display_tag_stream_option=tag_stream)

    if config_type == "RESTORE":
        # [END display_action]
        # [START render_template]
        return render_template(
            'update_restore_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            config=config,
            display_tag_history_option=tag_history,
            display_tag_stream_option=tag_stream)
    # [END render_template]


@app.route('/process_update_static_config', methods=['POST'])
def process_update_static_config():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    old_config_uuid = request.form['config_uuid']
    included_uris = request.form['included_uris'].rstrip()
    excluded_uris = request.form['excluded_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency'].rstrip()
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']
    overwrite = True  # this option overwrites existing tags

    job_creation = constants.SUCCESS

    # print("action: " + str(action))

    dcu = dc.DataCatalogUtils(session['user_email'], template_id, project_id, region)
    template_fields = dcu.get_template()

    if action == "Submit Changes":

        fields = []

        selected_fields = request.form.getlist("selected")
        # print("selected_fields: " + str(selected_fields))

        for selected_field in selected_fields:
            selected_type = request.form.get(selected_field + "_datatype")

            if selected_type == 'bool':
                selected_value = request.form.get(selected_field)

                if selected_value.lower() == 'true':
                    selected_value = True
                else:
                    selected_value = False
            else:
                selected_value = request.form.get(selected_field)

            # print(selected_field + ", " + str(selected_value) + ", " + selected_type)

            for template_field in template_fields:
                if template_field['field_id'] != selected_field:
                    continue

                is_required = template_field['is_required']
                field = {'field_id': selected_field, 'field_value': selected_value, 'field_type': selected_type,
                         'is_required': is_required}
                fields.append(field)
                break

        # print('fields: ' + str(fields))

        tag_history = False

        if "tag_history" in request.form:
            tag_history_option = request.form.get("tag_history")

            if tag_history_option == "selected":
                tag_history = True

        tag_stream = False

        if "tag_stream" in request.form:
            tag_stream_option = request.form.get("tag_stream")

            if tag_stream_option == "selected":
                tag_stream = True

        template_exists, template_uuid = teu.read_tag_template(template_id, project_id, region)
        new_config_uuid = teu.update_config(old_config_uuid, 'STATIC', 'PENDING', fields, included_uris, excluded_uris, \
                                            template_uuid, refresh_mode, refresh_frequency, refresh_unit, tag_history,
                                            tag_stream, overwrite)

        # print('new_config_uuid: ' + new_config_uuid)

        if isinstance(new_config_uuid, str):
            job_uuid = jm.create_job(new_config_uuid, 'STATIC')
        else:
            job_uuid = None

        if job_uuid is None:
            job_creation = constants.ERROR

    template_fields = dcu.get_template()
    configs = teu.read_configs(template_id, project_id, region)

    # print('template_fields: ' + str(template_fields))

    # [END process_update_static_tag]
    # [START render_template]
    return render_template(
        'view_configs.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        fields=template_fields,
        configs=configs,
        status=job_creation)


@app.route('/process_update_dynamic_config', methods=['POST'])
def process_update_dynamic_config():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    old_config_uuid = request.form['config_uuid']
    included_uris = request.form['included_uris'].rstrip()
    excluded_uris = request.form['excluded_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency'].rstrip()
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']

    job_creation = constants.SUCCESS

    # print('old_config_uuid: ' + old_config_uuid)
    # print("action: " + str(action))

    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template_fields = dcu.get_template()

    if action == "Submit Changes":

        fields = []

        selected_fields = request.form.getlist("selected")
        print("selected_fields: " + str(selected_fields))

        for selected_field in selected_fields:
            query_expression = request.form.get(selected_field).replace('\t', '').replace('\r', '').replace('\n',
                                                                                                            ' ').strip()
            print('query_expression: ' + query_expression)
            selected_field_type = request.form.get(selected_field + "_datatype")
            print(selected_field + ", " + query_expression + ", " + selected_field_type)

            for template_field in template_fields:
                if template_field['field_id'] != selected_field:
                    continue

                is_required = template_field['is_required']

                field = {'field_id': selected_field, 'query_expression': query_expression,
                         'field_type': selected_field_type, \
                         'is_required': is_required}
                fields.append(field)
                break

        # print('fields: ' + str(fields))

        tag_history = False

        if "tag_history" in request.form:
            tag_history_option = request.form.get("tag_history")

            if tag_history_option == "selected":
                tag_history = True

        tag_stream = False

        if "tag_stream" in request.form:
            tag_stream_option = request.form.get("tag_stream")

            if tag_stream_option == "selected":
                tag_stream = True

        template_exists, template_uuid = teu.read_tag_template(template_id, project_id, region)
        new_config_uuid = teu.update_config(old_config_uuid, 'DYNAMIC', 'PENDING', fields, included_uris, excluded_uris, \
                                            template_uuid, refresh_mode, refresh_frequency, refresh_unit, \
                                            tag_history, tag_stream)

        job_uuid = jm.create_job(new_config_uuid, 'DYNAMIC')

        if job_uuid is None:
            job_creation = constants.ERROR

    template_fields = dcu.get_template()
    # print('template_fields: ' + str(template_fields))

    configs = teu.read_configs(template_id, project_id, region)

    # [END process_update_dynamic_config]
    # [START render_template]
    return render_template(
        'view_configs.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        fields=template_fields,
        configs=configs,
        status=job_creation)


@app.route('/process_update_entry_config', methods=['POST'])
def process_update_entry_config():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    old_config_uuid = request.form['config_uuid']
    included_uris = request.form['included_uris'].rstrip()
    excluded_uris = request.form['excluded_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency'].rstrip()
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']

    job_creation = constants.SUCCESS

    # print('old_config_uuid: ' + old_config_uuid)
    # print("action: " + str(action))

    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template_fields = dcu.get_template()

    if action == "Submit Changes":

        fields = []

        selected_fields = request.form.getlist("selected")
        print("selected_fields: " + str(selected_fields))

        for selected_field in selected_fields:
            selected_field_type = request.form.get(selected_field + "_datatype")
            print(selected_field + ", " + selected_field_type)

            for template_field in template_fields:
                if template_field['field_id'] != selected_field:
                    continue

                is_required = template_field['is_required']

                field = {'field_id': selected_field, 'field_type': selected_field_type, 'is_required': is_required}
                fields.append(field)
                break

        # print('fields: ' + str(fields))

        tag_history = False

        if "tag_history" in request.form:
            tag_history_option = request.form.get("tag_history")

            if tag_history_option == "selected":
                tag_history = True

        tag_stream = False

        if "tag_stream" in request.form:
            tag_stream_option = request.form.get("tag_stream")

            if tag_stream_option == "selected":
                tag_stream = True

        template_exists, template_uuid = teu.read_tag_template(template_id, project_id, region)
        new_config_uuid = teu.update_config(old_config_uuid, 'ENTRY', 'PENDING', fields, included_uris, excluded_uris, \
                                            template_uuid, refresh_mode, refresh_frequency, refresh_unit, \
                                            tag_history, tag_stream)

        job_uuid = jm.create_job(new_config_uuid, 'ENTRY')

        if job_uuid == None:
            job_creation = constants.ERROR

    template_fields = dcu.get_template()
    # print('template_fields: ' + str(template_fields))

    configs = teu.read_configs(template_id, project_id, region)

    # [END process_update_dynamic_config]
    # [START render_template]
    return render_template(
        'view_configs.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        fields=template_fields,
        configs=configs,
        status=job_creation)


@app.route('/process_update_glossary_config', methods=['POST'])
def process_update_glossary_config():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    old_config_uuid = request.form['config_uuid']
    mapping_table = request.form['mapping_table'].rstrip()
    included_uris = request.form['included_uris'].rstrip()
    excluded_uris = request.form['excluded_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency'].rstrip()
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']
    overwrite = True
    job_creation = constants.SUCCESS

    # print('old_config_uuid: ' + old_config_uuid)
    # print("action: " + str(action))

    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template_fields = dcu.get_template()

    if action == "Submit Changes":

        fields = []

        selected_fields = request.form.getlist("selected")
        print("selected_fields: " + str(selected_fields))

        for selected_field in selected_fields:
            selected_field_type = request.form.get(selected_field + "_datatype")
            print(selected_field + ", " + selected_field_type)

            for template_field in template_fields:
                if template_field['field_id'] != selected_field:
                    continue

                is_required = template_field['is_required']

                field = {'field_id': selected_field, 'field_type': selected_field_type, 'is_required': is_required}
                fields.append(field)
                break

        # print('fields: ' + str(fields))

        tag_history = False

        if "tag_history" in request.form:
            tag_history_option = request.form.get("tag_history")

            if tag_history_option == "selected":
                tag_history = True

        tag_stream = False

        if "tag_stream" in request.form:
            tag_stream_option = request.form.get("tag_stream")

            if tag_stream_option == "selected":
                tag_stream = True

        template_exists, template_uuid = teu.read_tag_template(template_id, project_id, region)
        new_config_uuid = teu.update_config(old_config_uuid, 'GLOSSARY', 'PENDING', fields, included_uris,
                                            excluded_uris, \
                                            template_uuid, refresh_mode, refresh_frequency, refresh_unit, \
                                            tag_history, tag_stream, overwrite, mapping_table)

        job_uuid = jm.create_job(new_config_uuid, 'GLOSSARY')

        if job_uuid == None:
            job_creation = constants.ERROR

    template_fields = dcu.get_template()
    # print('template_fields: ' + str(template_fields))

    configs = teu.read_configs(template_id, project_id, region)

    # [END process_update_dynamic_config]
    # [START render_template]
    return render_template(
        'view_configs.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        fields=template_fields,
        configs=configs,
        status=job_creation)


@app.route('/process_update_sensitive_config', methods=['POST'])
def process_update_sensitive_config():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    old_config_uuid = request.form['config_uuid']
    dlp_dataset = request.form['dlp_dataset'].rstrip()
    mapping_table = request.form['mapping_table'].rstrip()
    included_uris = request.form['included_uris'].rstrip()
    excluded_uris = request.form['excluded_uris'].rstrip()

    policy_tags = request.form['policy_tags']
    if policy_tags == "true":
        create_policy_tags = True
        taxonomy_id = request.form['taxonomy_id'].rstrip()
    else:
        create_policy_tags = False
        taxonomy_id = None

    taxonomy_id = request.form['taxonomy_id'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency'].rstrip()
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']
    overwrite = True
    job_creation = constants.SUCCESS

    # print('old_config_uuid: ' + old_config_uuid)
    # print("action: " + str(action))

    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template_fields = dcu.get_template()

    if action == "Submit Changes":

        tag_history = False

        if "tag_history" in request.form:
            tag_history_option = request.form.get("tag_history")

            if tag_history_option == "selected":
                tag_history = True

        tag_stream = False

        if "tag_stream" in request.form:
            tag_stream_option = request.form.get("tag_stream")

            if tag_stream_option == "selected":
                tag_stream = True

        template_exists, template_uuid = teu.read_tag_template(template_id, project_id, region)

        new_config_uuid = teu.update_sensitive_config(old_config_uuid, 'PENDING', dlp_dataset, mapping_table,
                                                      included_uris, excluded_uris, \
                                                      create_policy_tags, taxonomy_id, template_uuid, refresh_mode,
                                                      refresh_frequency, refresh_unit, \
                                                      tag_history, tag_stream, overwrite)

        # print('new_config_uuid: ', new_config_uuid)

        job_uuid = jm.create_job(new_config_uuid, 'SENSITIVE')

        if job_uuid == None:
            job_creation = constants.ERROR

    template_fields = dcu.get_template()
    # print('template_fields: ' + str(template_fields))

    configs = teu.read_configs(template_id, project_id, region)

    # [END process_update_dynamic_config]
    # [START render_template]
    return render_template(
        'view_configs.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        fields=template_fields,
        configs=configs,
        status=job_creation)


@app.route('/process_update_restore_config', methods=['POST'])
def process_update_restore_config():
    source_template_id = request.form['source_template_id']
    source_template_project = request.form['source_template_project']
    source_template_region = request.form['source_template_region']

    target_template_id = request.form['target_template_id']
    target_template_project = request.form['target_template_project']
    target_template_region = request.form['target_template_region']

    metadata_export_location = request.form['metadata_export_location']

    config_uuid = request.form['config_uuid']

    action = request.form['action']
    overwrite = True  # this option overwrites existing tags

    job_creation = constants.SUCCESS

    # print("action: " + str(action))

    dcu = dc.DataCatalogUtils(target_template_id, target_template_project, target_template_region)
    template_fields = dcu.get_template()

    if action == "Submit Changes":

        tag_history = False

        if "tag_history" in request.form:
            tag_history_option = request.form.get("tag_history")

            if tag_history_option == "selected":
                tag_history = True

        tag_stream = False

        if "tag_stream" in request.form:
            tag_stream_option = request.form.get("tag_stream")

            if tag_stream_option == "selected":
                tag_stream = True

        template_exists, source_template_uuid = teu.read_tag_template(source_template_id, source_template_project,
                                                                      source_template_region)
        template_exists, target_template_uuid = teu.read_tag_template(target_template_id, target_template_project,
                                                                      target_template_region)

        new_config_uuid = teu.update_restore_config(config_uuid, 'PENDING', source_template_uuid, source_template_id, \
                                                    source_template_project, source_template_region,
                                                    target_template_uuid, \
                                                    target_template_id, target_template_project, target_template_region, \
                                                    metadata_export_location, tag_history, tag_stream, overwrite=True)

        # print('new_config_uuid: ' + new_config_uuid)

        if isinstance(new_config_uuid, str):
            job_uuid = jm.create_job(new_config_uuid, 'RESTORE')
        else:
            job_uuid = None

        if job_uuid == None:
            job_creation = constants.ERROR

    configs = teu.read_configs(target_template_id, target_template_project, target_template_region)

    # print('template_fields: ' + str(template_fields))

    # [END process_update_static_tag]
    # [START render_template]
    return render_template(
        'view_configs.html',
        template_id=target_template_id,
        project_id=target_template_project,
        region=target_template_region,
        configs=configs)


@app.route('/process_static_config', methods=['POST'])
def process_static_config():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    included_uris = request.form['included_uris'].rstrip()
    excluded_uris = request.form['excluded_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency'].rstrip()
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']
    group = get_group_from_group_key(request.form['group'], session['groups'])

    print('included_uris: ' + included_uris)
    print('excluded_uris: ' + excluded_uris)

    dcu = dc.DataCatalogUtils(session['user_email'], template_id, project_id, region)
    template = dcu.get_template()

    if action == "Cancel Changes":
        return render_template(
            'tag_template.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            group=group,
            fields=template)

    fields = []

    selected_fields = request.form.getlist("selected")
    print("selected_fields: " + str(selected_fields))

    for selected_field in selected_fields:
        selected_type = request.form.get(selected_field + "_datatype")

        if selected_type == 'bool':
            selected_value = request.form.get(selected_field)

            if selected_value.lower() == 'true':
                selected_value = True
            else:
                selected_value = False
        else:
            selected_value = request.form.get(selected_field)

        # print(selected_field + ", " + str(selected_value) + ", " + selected_type)

        for template_field in template:

            if template_field['field_id'] != selected_field:
                continue

            is_required = template_field['is_required']
            field = {'field_id': selected_field, 'field_value': selected_value, 'field_type': selected_type,
                     'is_required': is_required}
            fields.append(field)
            break

    # print('fields: ' + str(fields))

    tag_history_option = False
    tag_history_enabled = "OFF"

    if "tag_history" in request.form:
        tag_history = request.form.get("tag_history")

        if tag_history == "selected":
            tag_history_option = True
            tag_history_enabled = "ON"

    tag_stream_option = False
    tag_stream_enabled = "OFF"

    if "tag_stream" in request.form:
        tag_stream = request.form.get("tag_stream")

        if tag_stream == "selected":
            tag_stream_option = True
            tag_stream_enabled = "ON"

    template_uuid = teu.write_tag_template(template_id, project_id, region)
    config_uuid, included_uris_hash = teu.write_static_config('PENDING', fields, included_uris, excluded_uris,
                                                              template_uuid, refresh_mode, \
                                                              refresh_frequency, refresh_unit, tag_history_option,
                                                              tag_stream_option)

    if isinstance(config_uuid, str):
        job_uuid = jm.create_job(config_uuid, 'STATIC')

    if job_uuid != None:
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR

    # [END process_static_tag]
    # [START render_template]
    return render_template(
        'submitted_static_config.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        fields=fields,
        included_uris=included_uris,
        excluded_uris=excluded_uris,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=job_creation)
    # [END render_template]


@app.route('/process_dynamic_config', methods=['POST'])
def process_dynamic_config():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    included_uris = request.form['included_uris'].rstrip()
    excluded_uris = request.form['excluded_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency']
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']
    group = get_group_from_group_key(request.form['group'], session['groups'])

    # print('included_uris: ' + included_uris)
    # print('excluded_uris: ' + excluded_uris)
    # print('refresh_mode: ' + refresh_mode)
    # print('refresh_frequency: ' + refresh_frequency)
    # print('refresh_unit: ' + refresh_unit)

    dcu = dc.DataCatalogUtils(session['user_email'], template_id, project_id, region)
    template = dcu.get_template()

    if action == "Cancel Changes":
        return render_template(
            'tag_template.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            group=group,
            fields=template)

    fields = []

    selected_fields = request.form.getlist("selected")
    # print("selected_fields: " + str(selected_fields))

    for selected_field in selected_fields:
        query_expression = request.form.get(selected_field).replace('\t', '').replace('\r', '').replace('\n',
                                                                                                        ' ').strip()
        # print("query_expression: " + query_expression)
        selected_field_type = request.form.get(selected_field + "_datatype")
        # print("selected_field_type: " + selected_field_type)
        print(selected_field + ", " + query_expression + ", " + selected_field_type)

        for template_field in template:

            if template_field['field_id'] != selected_field:
                continue

            is_required = template_field['is_required']
            field = {'field_id': selected_field, 'query_expression': query_expression,
                     'field_type': selected_field_type, \
                     'is_required': is_required}
            fields.append(field)
            break

    # print('fields: ' + str(fields))

    tag_history_option = False
    tag_history_enabled = "OFF"

    if "tag_history" in request.form:
        tag_history = request.form.get("tag_history")

        if tag_history == "selected":
            tag_history_option = True
            tag_history_enabled = "ON"

    tag_stream_option = False
    tag_stream_enabled = "OFF"

    if "tag_stream" in request.form:
        tag_stream = request.form.get("tag_stream")

        if tag_stream == "selected":
            tag_stream_option = True
            tag_stream_enabled = "ON"

    template_uuid = teu.write_tag_template(template_id, project_id, region)
    config_uuid, included_uris_hash = teu.write_dynamic_config('PENDING', fields, included_uris, excluded_uris,
                                                               template_uuid, \
                                                               refresh_mode, refresh_frequency, refresh_unit, \
                                                               tag_history_option, tag_stream_option)
    if isinstance(config_uuid, str):
        job_uuid = jm.create_job(config_uuid, 'DYNAMIC')
    else:
        job_uuid = None

    if job_uuid != None:
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR

    # [END process_dynamic_tag]
    # [START render_template]
    return render_template(
        'submitted_dynamic_config.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        fields=fields,
        included_uris=included_uris,
        included_uris_hash=included_uris_hash,
        excluded_uris=excluded_uris,
        refresh_mode=refresh_mode,
        refresh_frequency=refresh_frequency,
        refresh_unit=refresh_unit,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=job_creation)
    # [END render_template]


@app.route('/process_entry_config', methods=['POST'])
def process_entry_config():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    included_uris = request.form['included_uris'].rstrip()
    excluded_uris = request.form['excluded_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency']
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']

    # print('included_uris: ' + included_uris)
    # print('excluded_uris: ' + excluded_uris)
    # print('refresh_mode: ' + refresh_mode)
    # print('refresh_frequency: ' + refresh_frequency)
    # print('refresh_unit: ' + refresh_unit)

    dcu = dc.DataCatalogUtils(session['user_email'], template_id, project_id, region)
    template = dcu.get_template()

    if action == "Cancel Changes":
        return render_template(
            'tag_template.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template)

    fields = []

    selected_fields = request.form.getlist("selected")
    # print("selected_fields: " + str(selected_fields))

    for selected_field in selected_fields:
        selected_field_type = request.form.get(selected_field + "_datatype")
        # print(selected_field + ", " + selected_field_type)

        for template_field in template:

            if template_field['field_id'] != selected_field:
                continue

            is_required = template_field['is_required']
            field = {'field_id': selected_field, 'field_type': selected_field_type, \
                     'is_required': is_required}
            fields.append(field)
            break

    # print('fields: ' + str(fields))

    tag_history_option = False
    tag_history_enabled = "OFF"

    if "tag_history" in request.form:
        tag_history = request.form.get("tag_history")

        if tag_history == "selected":
            tag_history_option = True
            tag_history_enabled = "ON"

    tag_stream_option = False
    tag_stream_enabled = "OFF"

    if "tag_stream" in request.form:
        tag_stream = request.form.get("tag_stream")

        if tag_stream == "selected":
            tag_stream_option = True
            tag_stream_enabled = "ON"

    template_uuid = teu.write_tag_template(template_id, project_id, region)
    config_uuid, included_uris_hash = teu.write_entry_config('PENDING', fields, included_uris, excluded_uris,
                                                             template_uuid, \
                                                             refresh_mode, refresh_frequency, refresh_unit, \
                                                             tag_history_option, tag_stream_option)
    if isinstance(config_uuid, str):
        job_uuid = jm.create_job(config_uuid, 'ENTRY')
    else:
        job_uuid = None

    if job_uuid != None:
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR

    # [END process_dynamic_tag]
    # [START render_template]
    return render_template(
        'submitted_entry_config.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        fields=fields,
        included_uris=included_uris,
        included_uris_hash=included_uris_hash,
        excluded_uris=excluded_uris,
        refresh_mode=refresh_mode,
        refresh_frequency=refresh_frequency,
        refresh_unit=refresh_unit,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=job_creation)
    # [END render_template]


@app.route('/process_glossary_config', methods=['POST'])
def process_glossary_config():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    mapping_table = request.form['mapping_table'].rstrip()
    included_uris = request.form['included_uris'].rstrip()
    excluded_uris = request.form['excluded_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency']
    refresh_unit = request.form['refresh_unit']
    overwrite = True  # set to true as we are creating a new glossary config
    action = request.form['action']

    # print('included_uris: ' + included_uris)
    # print('excluded_uris: ' + excluded_uris)
    # print('refresh_mode: ' + refresh_mode)
    # print('refresh_frequency: ' + refresh_frequency)
    # print('refresh_unit: ' + refresh_unit)

    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template = dcu.get_template()

    if action == "Cancel Changes":
        return render_template(
            'tag_template.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template)

    fields = []

    selected_fields = request.form.getlist("selected")
    # print("selected_fields: " + str(selected_fields))

    for selected_field in selected_fields:
        selected_field_type = request.form.get(selected_field + "_datatype")
        # print(selected_field + ", " + selected_field_type)

        for template_field in template:

            if template_field['field_id'] != selected_field:
                continue

            is_required = template_field['is_required']
            field = {'field_id': selected_field, 'field_type': selected_field_type, \
                     'is_required': is_required}
            fields.append(field)
            break

    # print('fields: ' + str(fields))

    tag_history_option = False
    tag_history_enabled = "OFF"

    if "tag_history" in request.form:
        tag_history = request.form.get("tag_history")

        if tag_history == "selected":
            tag_history_option = True
            tag_history_enabled = "ON"

    tag_stream_option = False
    tag_stream_enabled = "OFF"

    if "tag_stream" in request.form:
        tag_stream = request.form.get("tag_stream")

        if tag_stream == "selected":
            tag_stream_option = True
            tag_stream_enabled = "ON"

    template_uuid = teu.write_tag_template(template_id, project_id, region)

    config_uuid, included_uris_hash = teu.write_glossary_config('PENDING', fields, mapping_table, included_uris,
                                                                excluded_uris, template_uuid, \
                                                                refresh_mode, refresh_frequency, refresh_unit, \
                                                                tag_history_option, tag_stream_option, overwrite)
    if isinstance(config_uuid, str):
        job_uuid = jm.create_job(config_uuid, 'GLOSSARY')
    else:
        job_uuid = None

    if job_uuid != None:
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR

    # [END process_dynamic_tag]
    # [START render_template]
    return render_template(
        'submitted_glossary_config.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        fields=fields,
        mapping_table=mapping_table,
        included_uris=included_uris,
        included_uris_hash=included_uris_hash,
        excluded_uris=excluded_uris,
        refresh_mode=refresh_mode,
        refresh_frequency=refresh_frequency,
        refresh_unit=refresh_unit,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=job_creation)
    # [END render_template]


@app.route('/process_sensitive_config', methods=['POST'])
def process_sensitive_config():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    dlp_dataset = request.form['dlp_dataset'].rstrip()
    mapping_table = request.form['mapping_table'].rstrip()
    included_uris = request.form['included_uris'].rstrip()
    excluded_uris = request.form['excluded_uris'].rstrip()

    # policy tag inputs
    policy_tags = request.form['policy_tags']
    if policy_tags == "true":
        create_policy_tags = True
        taxonomy_id = request.form['taxonomy_id'].rstrip()
    else:
        create_policy_tags = False
        taxonomy_id = None

    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency']
    refresh_unit = request.form['refresh_unit']
    overwrite = True  # set to true as we are creating a new sensitive config
    action = request.form['action']

    # print('included_uris: ' + included_uris)
    # print('excluded_uris: ' + excluded_uris)
    # print('refresh_mode: ' + refresh_mode)
    # print('refresh_frequency: ' + refresh_frequency)
    # print('refresh_unit: ' + refresh_unit)

    dcu = dc.DataCatalogUtils(session['user_email'], template_id, project_id, region)
    template = dcu.get_template()

    if action == "Cancel Changes":
        return render_template(
            'tag_template.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template)

    fields = []

    selected_fields = request.form.getlist("selected")
    # print("selected_fields: " + str(selected_fields))

    for selected_field in selected_fields:
        selected_field_type = request.form.get(selected_field + "_datatype")
        # print(selected_field + ", " + selected_field_type)

        for template_field in template:

            if template_field['field_id'] != selected_field:
                continue

            is_required = template_field['is_required']
            field = {'field_id': selected_field, 'field_type': selected_field_type, \
                     'is_required': is_required}
            fields.append(field)
            break

    # print('fields: ' + str(fields))

    tag_history_option = False
    tag_history_enabled = "OFF"

    if "tag_history" in request.form:
        tag_history = request.form.get("tag_history")

        if tag_history == "selected":
            tag_history_option = True
            tag_history_enabled = "ON"

    tag_stream_option = False
    tag_stream_enabled = "OFF"

    if "tag_stream" in request.form:
        tag_stream = request.form.get("tag_stream")

        if tag_stream == "selected":
            tag_stream_option = True
            tag_stream_enabled = "ON"

    template_uuid = teu.write_tag_template(template_id, project_id, region)

    config_uuid, included_uris_hash = teu.write_sensitive_config('PENDING', fields, dlp_dataset, mapping_table,
                                                                 included_uris, excluded_uris, \
                                                                 create_policy_tags, taxonomy_id, \
                                                                 template_uuid, \
                                                                 refresh_mode, refresh_frequency, refresh_unit, \
                                                                 tag_history_option, tag_stream_option, overwrite)
    if isinstance(config_uuid, str):
        job_uuid = jm.create_job(config_uuid, 'SENSITIVE')
    else:
        job_uuid = None

    if job_uuid is not None:
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR

    # [END process_dynamic_tag]
    # [START render_template]
    return render_template(
        'submitted_sensitive_config.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        fields=fields,
        dlp_dataset=dlp_dataset,
        mapping_table=mapping_table,
        included_uris=included_uris,
        included_uris_hash=included_uris_hash,
        excluded_uris=excluded_uris,
        policy_tags=policy_tags,
        taxonomy_id=taxonomy_id,
        refresh_mode=refresh_mode,
        refresh_frequency=refresh_frequency,
        refresh_unit=refresh_unit,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=job_creation)
    # [END render_template]


@app.route('/process_restore_config', methods=['POST'])
def process_restore_config():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']

    source_template_id = request.form['source_template_id']
    source_template_project = request.form['source_template_project']
    source_template_region = request.form['source_template_region']

    target_template_id = request.form['target_template_id']
    target_template_project = request.form['target_template_project']
    target_template_region = request.form['target_template_region']

    metadata_export_location = request.form['metadata_export_location']

    action = request.form['action']

    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template = dcu.get_template()

    if action == "Cancel Changes":
        return render_template(
            'tag_template.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template)

    tag_history_option = False
    tag_history_enabled = "OFF"

    if "tag_history" in request.form:
        tag_history = request.form.get("tag_history")

        if tag_history == "selected":
            tag_history_option = True
            tag_history_enabled = "ON"

    tag_stream_option = False
    tag_stream_enabled = "OFF"

    if "tag_stream" in request.form:
        tag_stream = request.form.get("tag_stream")

        if tag_stream == "selected":
            tag_stream_option = True
            tag_stream_enabled = "ON"

    source_template_uuid = teu.write_tag_template(source_template_id, source_template_project, source_template_region)
    target_template_uuid = teu.write_tag_template(target_template_id, target_template_project, target_template_region)

    overwrite = True

    config_uuid = teu.write_restore_config('PENDING', source_template_uuid, source_template_id, source_template_project,
                                           source_template_region, \
                                           target_template_uuid, target_template_id, target_template_project,
                                           target_template_region, \
                                           metadata_export_location, \
                                           tag_history_option, tag_stream_option, overwrite)

    if isinstance(config_uuid, str):
        job_uuid = jm.create_job(config_uuid, 'RESTORE')
    else:
        job_uuid = None

    if job_uuid != None:
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR

    # [END process_restore_tag]
    # [START render_template]
    return render_template(
        'submitted_restore_config.html',
        source_template_id=source_template_id,
        source_template_project=source_template_project,
        source_template_region=source_template_region,
        target_template_id=target_template_id,
        target_template_project=target_template_project,
        target_template_region=target_template_region,
        metadata_export_location=metadata_export_location,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=job_creation)
    # [END render_template]


@app.route('/process_import_config', methods=['POST'])
def process_import_config():
    # template_id = request.form['template_id']
    # project_id = request.form['project_id']
    # region = request.form['region']

    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']

    metadata_import_location = request.form['metadata_import_location']

    action = request.form['action']

    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    template = dcu.get_template()

    if action == "Cancel Changes":
        return render_template(
            'tag_template.html',
            template_id=template_id,
            project_id=template_project,
            region=template_region,
            fields=template)

    tag_history_option = False
    tag_history_enabled = "OFF"

    if "tag_history" in request.form:
        tag_history = request.form.get("tag_history")

        if tag_history == "selected":
            tag_history_option = True
            tag_history_enabled = "ON"

    tag_stream_option = False
    tag_stream_enabled = "OFF"

    if "tag_stream" in request.form:
        tag_stream = request.form.get("tag_stream")

        if tag_stream == "selected":
            tag_stream_option = True
            tag_stream_enabled = "ON"

    template_uuid = teu.write_tag_template(template_id, template_project, template_region)

    overwrite = True

    config_uuid = teu.write_import_config('PENDING', template_uuid, template_id, template_project, template_region, \
                                          metadata_import_location, \
                                          tag_history_option, tag_stream_option, overwrite)

    if isinstance(config_uuid, str):
        job_uuid = jm.create_job(config_uuid, 'IMPORT')
    else:
        job_uuid = None

    if job_uuid != None:
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR

    # [END process_restore_tag]
    # [START render_template]
    return render_template(
        'submitted_import_config.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        metadata_import_location=metadata_import_location,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=job_creation)
    # [END render_template]


##################### INTERNAL METHODS #################

def get_refresh_parameters(json):
    refresh_mode = json['refresh_mode']
    refresh_frequency = ''
    refresh_unit = ''

    if refresh_mode == 'AUTO':
        if 'refresh_frequency' in json:
            refresh_frequency = json['refresh_frequency']
        else:
            print(
                "config request must include a refresh_frequency when refresh_mode is set to AUTO. This is a required parameter.")
            resp = jsonify(success=False)
            return resp

    if refresh_mode == 'AUTO':
        if 'refresh_unit' in json:
            refresh_unit = json['refresh_unit']
        else:
            print(
                "config request must include a refresh_unit when refresh_mode is set to AUTO. This is a required parameter.")
            resp = jsonify(success=False)
            return resp

    return refresh_mode, refresh_frequency, refresh_unit


##################### API METHODS #################

"""
Args:
    template_id: tag template to use
    project_id: tag template's Google Cloud project 
    region: tag template's region 
    fields: list of all the template field names to include in the tag (no need to include the field type)
    included_uris: The paths to the resources (either in BQ or GCS) 
    excluded_uris: The paths to the resources to exclude (optional)
    refresh_mode: AUTO or ON_DEMAND
    refresh_frequency: positive integer
    refresh_unit: minutes or hours
    tag_history: true if tag history is on, false otherwise 
    tag_stream: true if tag stream is on, false otherwise
Returns:
    job_uuid 
"""


@app.route("/dynamic_create", methods=['POST'])
def dynamic_create():
    json = request.get_json(force=True)
    print('json: ' + str(json))

    template_id = json['template_id']
    project_id = json['project_id']
    region = json['region']

    # print('template_id: ' + template_id)
    # print('project_id: ' + project_id)
    # print('region: ' + region)

    template_uuid = teu.write_tag_template(template_id, project_id, region)

    dcu = dc.DataCatalogUtils(session['user_email'], template_id, project_id, region)
    included_fields = json['fields']
    fields = dcu.get_template(included_fields)

    excluded_uris = json['excluded_uris']
    included_uris = json['included_uris']

    refresh_mode, refresh_frequency, refresh_unit = get_refresh_parameters(json)

    tag_history = json['tag_history']
    tag_stream = json['tag_stream']

    config_uuid, included_uris_hash = teu.write_dynamic_config('PENDING', fields, included_uris, excluded_uris,
                                                               template_uuid, \
                                                               refresh_mode, refresh_frequency, refresh_unit, \
                                                               tag_history, tag_stream)

    if isinstance(config_uuid, str):
        job_uuid = jm.create_job(config_uuid, 'DYNAMIC')
    else:
        job_uuid = None

    return jsonify(job_uuid=job_uuid)


@app.route("/static_create", methods=['POST'])
def static_create():
    """
    Args:
        template_id: tag template to use
        project_id: tag template's Google Cloud project
        region: tag template's region
        fields: list of all the template field names to include in the tag (no need to include the field type)
        included_uris: The paths to the resources (either in BQ or GCS)
        excluded_uris: The paths to the resources to exclude (optional)
        refresh_mode: AUTO or ON_DEMAND
        refresh_frequency: positive integer
        refresh_unit: minutes or hours
        tag_history: true if tag history is on, false otherwise
        tag_stream: true if tag stream is on, false otherwise
    Returns:
        job_uuid
    """
    json = request.get_json(force=True)
    print('json: ' + str(json))

    template_id = json['template_id']
    project_id = json['project_id']
    region = json['region']

    # print('template_id: ' + template_id)
    # print('project_id: ' + project_id)
    # print('region: ' + region)

    template_uuid = teu.write_tag_template(template_id, project_id, region)

    dcu = dc.DataCatalogUtils(session['user_email'], template_id, project_id, region)
    included_fields = json['fields']
    fields = dcu.get_template(included_fields)

    excluded_uris = json['excluded_uris']
    included_uris = json['included_uris']
    tag_history = json['tag_history']
    tag_stream = json['tag_stream']

    refresh_mode, refresh_frequency, refresh_unit = get_refresh_parameters(json)

    # since we are creating a new config, we are overwriting any previously created tags
    overwrite = True

    config_uuid, included_uris_hash = teu.write_static_config('PENDING', fields, included_uris, excluded_uris,
                                                              template_uuid, \
                                                              refresh_mode, refresh_frequency, refresh_unit, \
                                                              tag_history, tag_stream, overwrite)

    if isinstance(config_uuid, str):
        job_uuid = jm.create_job(config_uuid, 'STATIC')
    else:
        job_uuid = None

    return jsonify(job_uuid=job_uuid)


@app.route("/entry_create", methods=['POST'])
def entry_create():
    """
    Args:
        template_id: file metadata tag template id
        project_id: tag template's Google Cloud project
        region: tag template's region
        fields: list of all the template field names to include in the tag (no need to include the field type)
        included_uris: The paths to the resources (either in BQ or GCS)
        excluded_uris: The paths to the resources to exclude (optional)
        refresh_mode: AUTO or ON_DEMAND
        refresh_frequency: positive integer
        refresh_unit: minutes or hours
        tag_history: true if tag history is on, false otherwise
        tag_stream: true if tag stream is on, false otherwise
    Returns:
        job_uuid
    """
    json = request.get_json(force=True)
    print('json: ' + str(json))

    template_id = json['template_id']
    project_id = json['project_id']
    region = json['region']

    # print('template_id: ' + template_id)
    # print('project_id: ' + project_id)
    # print('region: ' + region)

    template_uuid = teu.write_tag_template(template_id, project_id, region)

    dcu = dc.DataCatalogUtils(session['user_email'], template_id, project_id, region)
    included_fields = json['fields']
    fields = dcu.get_template(included_fields)

    included_uris = json['included_uris']

    if 'excluded_uris' in json:
        excluded_uris = json['excluded_uris']
    else:
        excluded_uris = ''

    refresh_mode, refresh_frequency, refresh_unit = get_refresh_parameters(json)

    tag_history = json['tag_history']
    tag_stream = json['tag_stream']

    config_uuid, included_uris_hash = teu.write_entry_config('PENDING', fields, included_uris, excluded_uris,
                                                             template_uuid, \
                                                             refresh_mode, refresh_frequency, refresh_unit, \
                                                             tag_history, tag_stream)

    if isinstance(config_uuid, str):
        job_uuid = jm.create_job(config_uuid, 'ENTRY')
    else:
        job_uuid = None

    return jsonify(job_uuid=job_uuid)


@app.route("/glossary_create", methods=['POST'])
def glossary_create():
    """
    Args:
        template_id: enterprise dictionary tag template id
        project_id: tag template's Google Cloud project
        region: tag template's region
        fields: list of all the template field names to include in the tag (no need to include the field type)
        mapping_table: The path to the mapping table in BQ. This is required.
        included_uris: The path(s) to the resources (either in BQ or GCS)
        excluded_uris: The path(s) to the resources to exclude (optional)
        refresh_mode: AUTO or ON_DEMAND
        refresh_frequency: positive integer
        refresh_unit: minutes or hours
        tag_history: true if tag history is on, false otherwise
        tag_stream: true if tag stream is on, false otherwise
    Returns:
        job_uuid
    """
    json = request.get_json(force=True)
    print('json: ' + str(json))

    template_id = json['template_id']
    project_id = json['project_id']
    region = json['region']

    # print('template_id: ' + template_id)
    # print('project_id: ' + project_id)
    # print('region: ' + region)

    template_uuid = teu.write_tag_template(template_id, project_id, region)

    dcu = dc.DataCatalogUtils(session['user_email'], template_id, project_id, region)
    included_fields = json['fields']
    fields = dcu.get_template(included_fields)

    # validate mapping_table field
    if 'mapping_table' in json:
        mapping_table = json['mapping_table']
    else:
        print("glossary_config request doesn't include mapping_table field. This is a required parameter.")
        resp = jsonify(success=False)
        return resp

    included_uris = json['included_uris']

    if 'excluded_uris' in json:
        excluded_uris = json['excluded_uris']
    else:
        excluded_uris = ''

    refresh_mode, refresh_frequency, refresh_unit = get_refresh_parameters(json)

    tag_history = json['tag_history']
    tag_stream = json['tag_stream']
    overwrite = True

    config_uuid, included_uris_hash = teu.write_glossary_config('PENDING', fields, mapping_table, included_uris,
                                                                excluded_uris, template_uuid, \
                                                                refresh_mode, refresh_frequency, refresh_unit, \
                                                                tag_history, tag_stream, overwrite)

    if isinstance(config_uuid, str):
        job_uuid = jm.create_job(config_uuid, 'GLOSSARY')
    else:
        job_uuid = None

    return jsonify(job_uuid=job_uuid)


"""
Args:
    template_id: data attribute tag template id
    project_id: tag template's Google Cloud project 
    region: tag template's region 
    fields: list of all the template field names to include in the tag (no need to include the field type)
    dlp_dataset: The path to the dataset in BQ in which the DLP findings tables are stored
    mapping_table: The path to the mapping table in BQ. This is required. 
    included_uris: The path(s) to the resources (either in BQ or GCS) 
    excluded_uris: The path(s) to the resources to exclude (optional)
    create_policy_tags: true if this request should also create the policy tags on the sensitive columns, false otherwise
    taxonomy_id: The fully-qualified path to the policy tag taxonomy (projects/[PROJECT]/locations/[REGION]/taxonomies/[TAXONOMY_ID])
    refresh_mode: AUTO or ON_DEMAND
    refresh_frequency: positive integer
    refresh_unit: minutes or hours
    tag_history: true if tag history is on, false otherwise 
    tag_stream: true if tag stream is on, false otherwise
Returns:
    job_uuid 
"""


@app.route("/sensitive_create", methods=['POST'])
def sensitive_create():
    json = request.get_json(force=True)
    print('json: ' + str(json))

    template_id = json['template_id']
    project_id = json['project_id']
    region = json['region']

    # print('template_id: ' + template_id)
    # print('project_id: ' + project_id)
    # print('region: ' + region)

    template_uuid = teu.write_tag_template(template_id, project_id, region)

    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    included_fields = json['fields']
    fields = dcu.get_template(included_fields)

    # validate dlp_dataset parameter
    if 'dlp_dataset' in json:
        dlp_dataset = json['dlp_dataset']
    else:
        print("sensitive_config request doesn't include a dlp_dataset field. This is a required parameter.")
        resp = jsonify(success=False)
        return resp

    # validate mapping_table parameter
    if 'mapping_table' in json:
        mapping_table = json['mapping_table']
    else:
        print("sensitive_config request doesn't include a mapping_table field. This is a required parameter.")
        resp = jsonify(success=False)
        return resp

    included_uris = json['included_uris']

    if 'excluded_uris' in json:
        excluded_uris = json['excluded_uris']
    else:
        excluded_uris = ''

    # validate create_policy_tags parameter
    if 'create_policy_tags' in json:
        create_policy_tags = json['create_policy_tags']
    else:
        print("sensitive_config request doesn't include a create_policy_tags field. This is a required parameter.")
        resp = jsonify(success=False)
        return resp

    if create_policy_tags:
        if 'taxonomy_id' in json:
            taxonomy_id = json['taxonomy_id']
        else:
            print(
                "sensitive_config request must include a taxonomy_id when the create_policy_tags field is true. This is a required parameter.")
            resp = jsonify(success=False)
            return resp

    refresh_mode, refresh_frequency, refresh_unit = get_refresh_parameters(json)

    tag_history = json['tag_history']
    tag_stream = json['tag_stream']
    overwrite = True

    config_uuid, included_uris_hash = teu.write_sensitive_config('PENDING', fields, dlp_dataset, mapping_table,
                                                                 included_uris, excluded_uris, \
                                                                 create_policy_tags, taxonomy_id, template_uuid, \
                                                                 refresh_mode, refresh_frequency, refresh_unit, \
                                                                 tag_history, tag_stream, overwrite)

    if isinstance(config_uuid, str):
        job_uuid = jm.create_job(config_uuid, 'SENSITIVE')
    else:
        job_uuid = None

    return jsonify(job_uuid=job_uuid)


"""
Args:
    source_template_id: The tag template id whose tags are to be restored
    source_template_project: The source tag template's project id 
    source_template_region: The source tag template's region 
    target_template_id: The tag template id whose tags are to be restored
    target_template_project: The source tag template's project id 
    target_template_region: The source tag template's region
    metadata_export_location: The path to the export files on GCS (Cloud Storage)
    tag_history: true if tag history is on, false otherwise 
    tag_stream: true if tag stream is on, false otherwise
Returns:
    job_uuid 
"""


@app.route("/restore_create", methods=['POST'])
def restore_create():
    json = request.get_json(force=True)
    print('json: ' + str(json))

    source_template_id = json['source_template_id']
    source_template_project = json['source_template_project']
    source_template_region = json['source_template_region']

    target_template_id = json['target_template_id']
    target_template_project = json['target_template_project']
    target_template_region = json['target_template_region']

    print('source_template_id: ', source_template_id)
    print('source_template_project: ', source_template_project)
    print('source_template_region: ', source_template_region)

    print('target_template_id: ', target_template_id)
    print('target_template_project: ', target_template_project)
    print('target_template_region: ', target_template_region)

    source_template_uuid = teu.write_tag_template(source_template_id, source_template_project, source_template_region)
    target_template_uuid = teu.write_tag_template(target_template_id, target_template_project, target_template_region)

    if 'metadata_export_location' in json:
        metadata_export_location = json['metadata_export_location']
    else:
        print(
            "restore config type requires the metadata_export_location parameter. Please this parameter to the json object.")
        resp = jsonify(success=False)
        return resp

    tag_history = json['tag_history']
    tag_stream = json['tag_stream']
    overwrite = True

    config_uuid = teu.write_restore_config('PENDING', source_template_uuid, source_template_id, source_template_project,
                                           source_template_region, \
                                           target_template_uuid, target_template_id, target_template_project,
                                           target_template_region, \
                                           metadata_export_location, \
                                           tag_history, tag_stream, overwrite)

    if isinstance(config_uuid, str):
        job_uuid = jm.create_job(config_uuid, 'RESTORE')
    else:
        job_uuid = None

    return jsonify(job_uuid=job_uuid)


@app.route("/import_create", methods=['POST'])
def import_create():
    json = request.get_json(force=True)
    print('json: ' + str(json))

    template_id = json['template_id']
    template_project = json['template_project']
    template_region = json['template_region']

    print('template_id: ', template_id)
    print('template_project: ', template_project)
    print('template_region: ', template_region)

    template_uuid = teu.write_tag_template(template_id, template_project, template_region)

    if 'metadata_import_location' in json:
        metadata_export_location = json['metadata_import_location']
    else:
        print(
            "import config type requires the metadata_import_location parameter. Please add this parameter to the json object.")
        resp = jsonify(success=False)
        return resp

    metadata_import_location = json['metadata_import_location']

    tag_history = json['tag_history']
    tag_stream = json['tag_stream']
    overwrite = True

    config_uuid = teu.write_import_config('PENDING', template_uuid, template_id, template_project, template_region, \
                                          metadata_import_location, tag_history, tag_stream, overwrite)

    if isinstance(config_uuid, str):
        job_uuid = jm.create_job(config_uuid, 'IMPORT')
    else:
        job_uuid = None

    return jsonify(job_uuid=job_uuid)


@app.route("/ondemand_updates", methods=['POST'])
def ondemand_updates():
    """
    Args:
        template_id: tag template to use
        project_id: tag template's Google Cloud project
        region: tag template's region
        included_uris: tag config's included uris or
        included_uris_hash: tag config's md5 hash value (in place of the included_uris)
    Note: caller must provide either the included_uris_hash or included_uris
    Returns:
        job_uuid = unique identifer for job
    """
    json = request.get_json(force=True)
    template_id = json['template_id']
    project_id = json['project_id']
    region = json['region']

    template_exists, template_uuid = teu.read_tag_template(template_id, project_id, region)

    # validate request
    if not template_exists:
        print("tag_template " + template_id + " doesn't exist")
        resp = jsonify(success=False)
        return resp

    if 'included_uris' in json:
        included_uris = json['included_uris']
        success, config = teu.lookup_config_by_included_uris(template_uuid, included_uris, None)

    elif 'included_uris_hash' in json:
        included_uris_hash = json['included_uris_hash']
        success, config = teu.lookup_config_by_included_uris(template_uuid, None, included_uris_hash)

    else:
        resp = jsonify(success=False,
                       message="Request is missing the required field included_uris or included_uris_hash.")
        return resp

    if success != True:
        print("config not found " + str(config))
        resp = jsonify(success=False, message="Config not found.")
        return resp

    # validate the matching config, i.e. make sure the scheduling mode is set to ON_DEMAND and not AUTO
    if config['refresh_mode'] == 'AUTO':
        print("config == AUTO: " + str(config))
        resp = jsonify(success=False,
                       message="Config has refresh_mode='AUTO'. Please update your config to refresh_mode='ON_DEMAND' prior to calling this method.")
        return resp

    # config is valid, create the job
    if isinstance(config['config_uuid'], str):
        job_uuid = jm.create_job(config['config_uuid'], config['config_type'])
    else:
        job_uuid = None

    return jsonify(job_uuid=job_uuid)

    # [END ondemand_updates]


@app.route("/get_job_status", methods=['POST'])
def get_job_status():
    """
    Args:
        job_uuid = unique identifer for job
    Returns:
        job_status = one of (PENDING, RUNNING, COMPLETE, ERROR)
        task_count = number of tasks associates with this jobs
        tasks_ran = number of tasks that have run
        tasks_completed = number of tasks which have completed
        tasks_failed = number of tasks which have failed
    """
    json = request.get_json(force=True)
    job_uuid = json['job_uuid']

    job = jm.get_job_status(job_uuid)
    print('job: ', job)

    if job is None:
        return jsonify(success=False, message="job_uuid " + job_uuid + " cannot be found.")

    elif job['job_status'] == 'COMPLETED':
        return jsonify(success=True, job_status=job['job_status'], task_count=job['task_count'],
                       tasks_ran=job['tasks_ran'], \
                       tasks_completed=job['tasks_completed'], tasks_failed=job['tasks_failed'])
    else:
        return jsonify(job_status=job['job_status'], task_count=job['task_count'], tasks_ran=job['tasks_ran'], \
                       tasks_completed=job['tasks_completed'], tasks_failed=job['tasks_failed'])


"""
Method called by Cloud Scheduler entry to update the tags from all scheduled configs which are set to auto updated
Args:
    None
Returns:
    True if the request succeeded, False otherwise
"""


@app.route("/scheduled_auto_updates", methods=['POST'])
def scheduled_auto_updates():
    try:
        print('*** enter scheduled_auto_updates ***')

        jobs = []

        ready_configs = teu.read_ready_configs()

        for config_uuid, config_type in ready_configs:

            print('ready config: ', config_uuid, ', ', config_type)

            if isinstance(config_uuid, str):
                teu.update_config_status(config_uuid, 'PENDING')
                teu.increment_version_next_run(config_uuid, config_type)
                job_uuid = jm.create_job(config_uuid, config_type)
                jobs.append(job_uuid)

        print('created jobs: ' + str(jobs))
        resp = jsonify(success=True, job_ids=json.dumps(jobs))

    except Exception as e:
        print('failed scheduled_auto_updates {}'.format(e))
        resp = jsonify(success=False, message='failed scheduled_auto_updates ' + str(e))

    return resp


################ INTERNAL PROCESSING METHODS #################

@app.route("/_split_work", methods=['POST'])
def _split_work():
    print('*** enter _split_work ***')

    json = request.get_json(force=True)
    print('json: ', json)

    job_uuid = json['job_uuid']
    config_uuid = json['config_uuid']
    config_type = json['config_type']

    config = teu.read_config(config_uuid, config_type)

    print('config: ', config)

    if config == {}:
        resp = jsonify(success=False)
        return resp

    if 'included_uris' in config:
        uris = list(res.Resources.get_resources(config.get('included_uris'), config.get('excluded_uris', None)))
        jm.record_num_tasks(job_uuid, len(uris))
        jm.update_job_running(job_uuid)
        tm.create_config_uuid_tasks(job_uuid, config_uuid, config_type, uris)

    if config_type == 'RESTORE':
        bkp_files = list(res.Resources.get_resources(config.get('metadata_export_location'), None))

        # print('bkp_files: ', bkp_files)
        extracted_tags = []

        for bkp_file in bkp_files:
            extracted_tags.append(bfp.BackupFileParser.extract_tags(config.get('source_template_id'),
                                                                    config.get('source_template_project'), \
                                                                    bkp_file))

    if config_type == 'IMPORT':
        csv_files = list(res.Resources.get_resources(config.get('metadata_import_location'), None))
        # print('csv_files: ', csv_files)

        extracted_tags = []

        for csv_file in csv_files:
            extracted_tags.extend(cp.CsvParser.extract_tags(csv_file))

    if config_type == 'RESTORE' or config_type == 'IMPORT':

        # no tags were extracted from the CSV files
        if extracted_tags == [[]]:
            resp = jsonify(success=False)
            return resp

        jm.record_num_tasks(job_uuid, len(extracted_tags))
        jm.update_job_running(job_uuid)
        tm.create_tag_extract_tasks(job_uuid, config_uuid, config_type, extracted_tags)

    teu.update_config_status(config_uuid, config_type, 'RUNNING')

    resp = jsonify(success=True)
    return resp


@app.route("/_run_task", methods=['POST'])
def _run_task():
    print('*** enter _run_task ***')

    creation_status = constants.ERROR

    json = request.get_json(force=True)
    job_uuid = json['job_uuid']
    config_uuid = json['config_uuid']
    config_type = json['config_type']
    shard_uuid = json['shard_uuid']
    task_uuid = json['task_uuid']

    if 'uri' in json:
        uri = json['uri']
    else:
        uri = None
        # print('uri: ', uri)

    if 'tag_extract' in json:
        tag_extract = json['tag_extract']
        # print('tag_extract: ', tag_extact)
    else:
        tag_extract = None

    tm.update_task_status(shard_uuid, task_uuid, 'RUNNING')

    # retrieve config
    config = teu.read_config(config_uuid, config_type)
    # print('read config: ', config)

    # validate config
    if config_type == 'RESTORE':
        if 'target_template_id' not in config:
            resp = jsonify(success=False)
            return resp
        if 'source_template_id' not in config:
            resp = jsonify(success=False)
            return resp
    else:
        if 'template_uuid' not in config:
            resp = jsonify(success=False)
            return resp

    if config_type == 'RESTORE':
        dcu = dc.DataCatalogUtils(config['target_template_id'], config['target_template_project'],
                                  config['target_template_region'])
    elif config_type == 'IMPORT':
        dcu = dc.DataCatalogUtils(config['template_id'], config['template_project'], config['template_region'])
    else:
        tem_config = teu.read_template_config(config['template_uuid'])
        dcu = dc.DataCatalogUtils(tem_config['template_id'], tem_config['project_id'], tem_config['region'])

    if config_type == 'DYNAMIC':
        creation_status = dcu.apply_dynamic_config(config['fields'], uri, config['config_uuid'], \
                                                   config['template_uuid'], config['tag_history'], \
                                                   config['tag_stream'])
    if config_type == 'STATIC':
        creation_status = dcu.apply_static_config(config['fields'], uri, config['config_uuid'], \
                                                  config['template_uuid'], config['tag_history'], \
                                                  config['tag_stream'], config['overwrite'])
    if config_type == 'ENTRY':
        creation_status = dcu.apply_entry_config(config['fields'], uri, config['config_uuid'], \
                                                 config['template_uuid'], config['tag_history'], \
                                                 config['tag_stream'])
    if config_type == 'GLOSSARY':
        creation_status = dcu.apply_glossary_config(config['fields'], config['mapping_table'], uri,
                                                    config['config_uuid'], \
                                                    config['template_uuid'], config['tag_history'], \
                                                    config['tag_stream'], config['overwrite'])
    if config_type == 'SENSITIVE':
        creation_status = dcu.apply_sensitive_config(config['fields'], config['dlp_dataset'], config['mapping_table'], \
                                                     uri, config['create_policy_tags'], config['taxonomy_id'],
                                                     config['config_uuid'], \
                                                     config['template_uuid'], config['tag_history'], \
                                                     config['tag_stream'], config['overwrite'])
    if config_type == 'RESTORE':
        creation_status = dcu.apply_restore_config(config['config_uuid'], tag_extract, \
                                                   config['tag_history'], config['tag_stream'], config['overwrite'])

    if config_type == 'IMPORT':
        creation_status = dcu.apply_import_config(config['config_uuid'], tag_extract, \
                                                  config['tag_history'], config['tag_stream'], config['overwrite'])

    if creation_status == constants.SUCCESS:
        tm.update_task_status(shard_uuid, task_uuid, 'COMPLETED')
    else:
        tm.update_task_status(shard_uuid, task_uuid, 'FAILED')

    # fan-in
    is_success, is_failed, pct_complete = jm.calculate_job_completion(job_uuid)

    if pct_complete == 100 and is_success:
        teu.update_config_status(config_uuid, config_type, 'ACTIVE')
        teu.update_scheduling_status(config_uuid, config_type, 'READY')
        teu.update_overwrite_flag(config_uuid, config_type)
        resp = jsonify(success=True)
    elif pct_complete == 100 and is_failed:
        teu.update_config_status(config_uuid, config_type, 'ERROR')
        jm.update_job_failed(job_uuid)
        resp = jsonify(success=False)
    else:
        teu.update_config_status(config_uuid, config_type, 'PROCESSING: {}% complete'.format(pct_complete))
        resp = jsonify(success=True)

    return resp


# [END _run_task]

####################### TEST METHOD ####################################

@app.route("/ping", methods=['GET'])
def ping():
    return "Tag Engine is alive"


# [END ping]

@app.errorhandler(500)
def server_error(e):
    # Log the error and stacktrace.
    # logging.exception('An error occurred during a request.')
    return 'An internal error occurred: ' + str(e), 500


# [END app]


if __name__ == "__main__":
    app.run()
