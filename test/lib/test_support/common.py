import copy
import datetime
import json
import tempfile
import uuid
from configparser import ConfigParser
import os

import requests
import yaml
from kafka import KafkaConsumer

from test_support import test_utils
from test_support.service_client import ServiceClient
from test_support.test_cases import CASE_01
from test_support.constants import (
    KAFKA_TOPIC,
    TOKEN2, TOKEN_SERVICE, TEST_COL_SCHEMA, TEST_COL_WS_OBJ_VER, TEST_COL_DATA_LINK, TEST_COL_NODE_EDGE, TEST_COL_NODES,
    TEST_COL_VER_EDGE, TEST_COL_VERSION, TEST_COL_SAMPLE, TEST_PWD, TEST_USER, TEST_DB_NAME, TOKEN_WS_READ_ADMIN,
)
from test_support.test_utils import assert_ms_epoch_close_to_now
from SampleService.core.user import UserID

VER = "0.2.1"


def replace_acls(url, sample_id, token, acls, as_admin=0, debug=False):
    response = requests.post(
        url,
        headers=get_authorized_headers(token),
        json=make_rpc(
            "replace_sample_acls",
            [{"id": sample_id, "acls": acls, "as_admin": as_admin}],
        ),
    )
    if debug:
        print(response.text)
    assert response.ok is True

    assert response.json() == make_result(None)


def assert_acl_contents(url, sample_id, token, expected, as_admin=0, print_resp=False):
    params = [{"id": sample_id, "as_admin": as_admin}]
    result = rpc_call_result(url, token, "get_sample_acls", params)

    for key in ["admin", "write", "read"]:
        assert sorted(result[key]) == sorted(expected[key])

    for key in ["owner", "public_read"]:
        assert result[key] == expected[key]


def get_authorized_headers(token):
    headers = {"accept": "application/json"}
    if token is not None:
        headers["authorization"] = token
    return headers


def check_kafka_messages(kafka_host, expected_msgs, topic=KAFKA_TOPIC, print_res=False):
    kc = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_host,
        auto_offset_reset="earliest",
        group_id="foo",
    )  # quiets warnings

    try:
        res = kc.poll(timeout_ms=2000)  # 1s not enough? Seems like a lot
        if print_res:
            print(res)
        assert len(res) == 1
        assert next(iter(res.keys())).topic == topic
        records = next(iter(res.values()))
        assert len(records) == len(expected_msgs)
        for i, r in enumerate(records):
            assert json.loads(r.value) == expected_msgs[i]
        # Need to commit here? doesn't seem like it
    finally:
        kc.close()


def clear_kafka_messages(kafka_host, topic=KAFKA_TOPIC):
    kc = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_host,
        auto_offset_reset="earliest",
        group_id="foo",
    )  # quiets warnings

    try:
        kc.poll(timeout_ms=2000)  # 1s not enough? Seems like a lot
        # Need to commit here? doesn't seem like it
    finally:
        kc.close()


def _validate_sample_as_admin(url):
    ret = requests.post(
        url,
        headers=get_authorized_headers(TOKEN2),
        json={
            "method": "SampleService.validate_samples",
            "version": "1.1",
            "id": "67",
            "params": [
                {
                    "samples": [
                        {
                            "name": "mysample",
                            "node_tree": [
                                {
                                    "id": "root",
                                    "type": "BioReplicate",
                                    "meta_controlled": {"foo": {"bar": "baz"}},
                                    "meta_user": {"a": {"b": "c"}},
                                }
                            ],
                        }
                    ]
                }
            ],
        },
    )
    assert ret.ok is True
    ret_json = ret.json()["result"][0]
    assert "mysample" not in ret_json["errors"]


def get_current_epochmillis():
    return round(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)


def create_generic_sample(url, token):
    ret = requests.post(
        url,
        headers=get_authorized_headers(token),
        json=make_rpc("create_sample", [CASE_01]),
    )
    assert ret.ok is True
    assert ret.json()["result"][0]["version"] == 1
    return ret.json()["result"][0]["id"]


def rpc_call(url, token, method, params, debug=False):
    response = requests.post(
        url, headers=get_authorized_headers(token), json=make_rpc(method, params)
    )
    if debug:
        print("[rpc_call]", params, response.text)
    return response


def rpc_call_result(url, token, method, params, debug=False):
    response = rpc_call(url, token, method, params, debug)
    assert response.ok is True
    payload = response.json()
    assert "result" in payload
    result = response.json()["result"]

    # The result may be either null or an array of one element
    # containing the result data.
    if result is None:
        return result

    assert type(result) is list
    assert len(result) == 1
    return result[0]


def rpc_call_error(url, token, method, params, debug=False):
    response = rpc_call(url, token, method, params, debug)
    assert response.status_code == 500
    return response.json()["error"]


def assert_error_rpc_call(url, token, method, params, expected_message, debug=False):
    response = rpc_call(url, token, method, params, debug)
    if debug:
        print("[assert_error_rpc_call]", expected_message)
    assert response.status_code == 500
    error = response.json()["error"]
    if debug:
        print("[assert_error_rpc_call]", error["message"], expected_message)
    assert error["message"] == expected_message
    return error


def assert_result_rpc_call(url, token, method, params, expected_result, debug=False):
    result = rpc_call_result(url, token, method, params, debug)
    assert result == expected_result
    return result


def _request_fail(url, method, token, params, expected):
    ret = requests.post(
        url,
        headers=get_authorized_headers(token),
        json={
            "method": "SampleService." + method,
            "version": "1.1",
            "id": "42",
            "params": [params],
        },
    )
    assert ret.status_code == 500
    assert ret.json()["error"]["message"] == expected


def create_link(url, token, params, expected_user, debug=False):
    result = rpc_call_result(url, token, "create_data_link", [params], debug)
    link = result["new_link"]
    link_id = link["linkid"]
    uuid.UUID(link_id)  # check the ID is a valid UUID
    del link["linkid"]
    created = link["created"]
    assert_ms_epoch_close_to_now(created)
    del link["created"]
    assert link == {
        "id": params["id"],
        "version": params["version"],
        "node": params["node"],
        "upa": params["upa"],
        "dataid": params.get("dataid"),
        "createdby": expected_user,
        "expiredby": None,
        "expired": None,
    }
    return link_id


def assert_result_create_link(url, token, params, expected_user, debug=False):
    result = rpc_call_result(url, token, "create_data_link", [params], debug)
    link = result["new_link"]
    link_id = link["linkid"]
    uuid.UUID(link_id)  # check the ID is a valid UUID
    del link["linkid"]
    created = link["created"]
    assert_ms_epoch_close_to_now(created)
    del link["created"]
    assert link == {
        "id": params["id"],
        "version": params["version"],
        "node": params["node"],
        "upa": params["upa"],
        "dataid": params.get("dataid"),
        "createdby": expected_user,
        "expiredby": None,
        "expired": None,
    }
    return link_id


def create_link_assert_result(url, token, params, expected_user, debug=False):
    result = rpc_call_result(url, token, "create_data_link", [params], debug)

    link = copy.deepcopy(result["new_link"])

    uuid.UUID(link["linkid"])  # check the ID is a valid UUID
    del link["linkid"]

    assert_ms_epoch_close_to_now(link["created"])
    del link["created"]

    assert link == {
        "id": params["id"],
        "version": params["version"],
        "node": params["node"],
        "upa": params["upa"],
        "dataid": params.get("dataid"),
        "createdby": expected_user,
        "expiredby": None,
        "expired": None,
    }
    return result["new_link"]


def assert_error_create_link(url, token, params, expected_error_message):
    error = rpc_call_error(url, token, "create_data_link", [params])
    error["message"] == expected_error_message


def get_sample(url, token, sample_id, version=1, as_admin=False):
    return rpc_call(
        url,
        token,
        "get_sample",
        [{"id": str(sample_id), "version": version, "as_admin": as_admin}],
    )


def get_sample_result(url, token, sample_id, version=1, as_admin=False):
    return rpc_call_result(
        url,
        token,
        "get_sample",
        [{"id": str(sample_id), "version": version, "as_admin": as_admin}],
    )


def make_rpc(func, params):
    return {
        "method": f"SampleService.{func}",
        "version": "1.1",
        "id": "123",
        "params": params,
    }


def make_result(result):
    return {"version": "1.1", "id": "123", "result": result}


def make_error(error):
    return {"version": "1.1", "id": "123", "error": error}


#
# Create Sample
#


def create_duplicate_samples(url, token, sample, sample_count, debug=False):
    sample_ids = []
    for i in range(0, sample_count):
        result = rpc_call_result(
            url, token, "create_sample", [{"sample": sample}], debug
        )
        assert result["version"] == 1
        sample_ids.append(result["id"])
    return sample_ids


def create_sample(url, token, sample, expected_version=1, debug=False):
    result = rpc_call_result(url, token, "create_sample", [{"sample": sample}], debug)
    assert result["version"] == expected_version
    return result["id"]


def create_sample_result(url, token, sample, expected_version=1):
    result = rpc_call_result(url, token, "create_sample", [{"sample": sample}])
    assert result["version"] == expected_version
    return result


def create_sample_assert_result(
    url, token, params, expectations={"version": 1}, debug=False
):
    sample_service = ServiceClient("SampleService", url=url, token=token)
    result = sample_service.call_assert_result("create_sample", params, debug=debug)
    assert result["version"] == expectations["version"]
    return result


def create_sample_assert_error(url, token, params, expectations=None, debug=False):
    sample_service = ServiceClient("SampleService", url=url, token=token)
    error = sample_service.call_assert_error("create_sample", params, debug=debug)
    if expectations is not None:
        assert error["message"] == expectations["message"]
    return error


def create_sample_error(url, token, sample):
    return rpc_call_error(url, token, "create_sample", [{"sample": sample}])


def assert_create_sample(url, token, params, expected_version=1, debug=False):
    result = rpc_call_result(url, token, "create_sample", [params], debug)
    assert result["version"] == expected_version
    return result["id"]


def assert_fail_create_sample(url, token, params, error_message):
    response = requests.post(
        url,
        headers=get_authorized_headers(token),
        json=make_rpc("create_sample", [params]),
    )
    assert response.status == 500
    assert response.json()["error"]["message"] == error_message


def assert_get_sample(url, token, username, sample_id, sample_version, params):
    response = requests.post(
        url,
        headers=get_authorized_headers(token),
        json=make_rpc("get_sample", [{"id": sample_id, "version": sample_version}]),
    )
    assert response.ok is True
    result = response.json()["result"][0]

    # Test that the save data is sane, but remove it before an exact
    # dict match
    assert_ms_epoch_close_to_now(result["save_date"])
    del result["save_date"]

    expected = copy.deepcopy(params["sample"])
    expected["id"] = sample_id
    expected["user"] = username
    expected["version"] = sample_version
    expected["node_tree"][0]["parent"] = None

    assert result == expected


def sample_params_to_sample(params, update):
    """
    Given a sample params dict, and a set of fields to update, returns a dict
    which should be the same as that returned from the sample service for a
    sample created by said params.
    """
    expected = copy.deepcopy(params["sample"])
    expected["id"] = update["id"]
    expected["version"] = update["version"]
    expected["user"] = update["user"]
    expected["node_tree"][0]["parent"] = None
    return expected


def get_sample_assert_result(url, token, params, expected, debug=False):
    sample_service = ServiceClient("SampleService", url=url, token=token)
    sample = sample_service.call_assert_result("get_sample", params, debug=debug)

    # Test that the save data is sane, but remove it before an exact
    # dict match
    sample_to_compare = copy.deepcopy(sample)
    assert_ms_epoch_close_to_now(sample_to_compare["save_date"])
    del sample_to_compare["save_date"]

    assert sample_to_compare == expected
    return sample


def get_sample_assert_error(url, token, params, expected=None, debug=False):
    sample_service = ServiceClient("SampleService", url=url, token=token)
    error = sample_service.call_assert_error("get_sample", params, debug=debug)
    if expected is not None:
        assert expected["message"] == error["message"]
        # and more to come?
    return error


def get_samples_assert_result(url, token, params, expected, debug=False):
    sample_service = ServiceClient("SampleService", url=url, token=token)
    samples = sample_service.call_assert_result("get_samples", params, debug=debug)

    # Test that the save data is sane, but remove it before an exact
    # dict match
    samples_to_compare = copy.deepcopy(samples)
    for sample in samples_to_compare:
        assert_ms_epoch_close_to_now(sample["save_date"])
        del sample["save_date"]

    assert samples_to_compare == expected
    return samples


def get_samples_assert_error(url, token, params, expected=None, debug=False):
    sample_service = ServiceClient("SampleService", url=url, token=token)
    error = sample_service.call_assert_error("get_samples", params, debug=debug)
    if expected is not None:
        assert expected["message"] == error["message"]
        # and more to come?
    return error


def update_acls_fail(url, token, params, expected):
    return assert_error_rpc_call(url, token, "update_sample_acls", [params], expected)


def update_acls(url, token, params, debug=False):
    return assert_result_rpc_call(
        url, token, "update_sample_acls", [params], None, debug
    )


def update_samples_acls_fail(url, token, params, expected_message):
    return assert_error_rpc_call(
        url, token, "update_samples_acls", [params], expected_message
    )


def update_samples_acls(url, token, params):
    return assert_result_rpc_call(url, token, "update_samples_acls", [params], None)


def make_expected_sample(case, sample_id, user, version=1):
    expected = copy.deepcopy(case["sample"])
    expected["id"] = sample_id
    expected["user"] = user
    expected["version"] = version
    expected["node_tree"][0]["parent"] = None
    return expected


def get_sample_node_id(sample_params):
    return sample_params["sample"]["node_tree"][0]["id"]

def u(user):
    return UserID(user)


def dt(timestamp):
    return datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)


def make_uuid():
    return uuid.uuid4()


def nw():
    return datetime.datetime.fromtimestamp(1, tz=datetime.timezone.utc)


def now_fun(now=None):
    if now is None:
        now = datetime.datetime.now(tz=datetime.timezone.utc)

    def now_funny():
        return now

    return now_funny


def sorted_dict(d):
    return dict(sorted(d.items()))


def get_links_from_sample_set_assert_error(
    url, token, params, expected_message=None, debug=False
):
    sample_service = ServiceClient("SampleService", url=url, token=token)
    error = sample_service.call_assert_error(
        "get_data_links_from_sample_set", params, debug=debug
    )

    if expected_message is not None:
        assert expected_message == error["message"]
    return error
