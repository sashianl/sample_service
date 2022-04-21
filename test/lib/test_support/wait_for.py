import json
from time import sleep, time

import requests


def wait_for_arangodb(url, tries=30, interval=1, verbose=False):
    if tries < 1:
        print(f"🛑  Sorry, 'tries' must be greater than 0; {tries} was provided")
        return False

    if interval <= 0:
        print(f"🛑  Sorry, 'interval' must be greater than 0; {interval} was provided")
        return False

    print(f"🕰  Waiting for ArangoDB to be available at {url}")
    last_result = None
    start_at = time()
    try_count = 0
    while try_count < tries:
        print(f"🍀 Attempt {try_count}")
        try:
            response = requests.get(f"{url}/_admin/server/mode")
            status = response.json()
            if not status.get("error") and status.get("mode") == "default":
                elapsed = time() - start_at
                print(
                    f"💖 ArangoDB successfully detected after {format_elapsed(elapsed)}s"
                )
                return True
            else:
                print("😬 ArangoDB mode not yet correct")
                last_result = status
        except Exception as ex:
            last_result = ex
            print("😬 Error fetching ArangoDB server mode, continuing")
            if verbose:
                print(f"😬 Error is: {str(ex)}")
        try_count += 1
        sleep(interval)
    elapsed = time() - start_at

    if isinstance(last_result, Exception):
        print("❌ Final attempt failed due to an error")
        print(f"❌ Error is: {str(last_result)}")
    else:
        print("❌ Final attempt failed due to invalid server mode")
        print(f"❌ mode: {last_result.get('mode')}, error: {last_result.get('error')}")

    print(
        (
            "🛑  Attempts to wait for ArangoDB exhausted after "
            f"{format_elapsed(elapsed)} seconds ({tries} tries)"
        )
    )


def format_elapsed(elapsed):
    return "{0:.3f}".format(elapsed)


def wait_for_sample_service(url, tries=30, interval=1, verbose=False):
    if tries < 1:
        print(f"🛑  Sorry, 'tries' must be greater than 0; {tries} was provided")
        return False

    if interval <= 0:
        print(f"🛑  Sorry, 'interval' must be greater than 0; {interval} was provided")
        return False

    print(f"🕰  Waiting for SampleService to be available at {url}")
    last_result = None
    start_at = time()
    try_count = 0
    while try_count < tries:
        print(f"🍀 Attempt {try_count}")
        try:
            response = requests.post(
                url,
                json.dumps(
                    {
                        "id": "123",
                        "version": "1.1",
                        "method": "SampleService.status",
                        "params": [],
                    }
                ),
            )

            result = response.json()["result"][0]
            if result.get("state") == "OK":
                elapsed = time() - start_at
                print(
                    f"💖 SampleService successfully detected after {format_elapsed(elapsed)}s"
                )
                return True
            else:
                print("😬 Service not yet OK")
                last_result = result
        except Exception as ex:
            last_result = ex
            print("😬 Error fetching SampleService status, continuing")
            if verbose:
                print(f"😬 Error is: {str(ex)}")
        try_count += 1
        sleep(interval)
    elapsed = time() - start_at

    if isinstance(last_result, Exception):
        print("❌ Final attempt failed due to an error")
        print(f"❌ Error is: {str(last_result)}")
    else:
        print("❌ Final attempt failed due to a bad server response")
        print(f"❌ {json.dumps(last_result)}")

    print(
        (
            "🛑 Attempts to wait for SampleService exhausted after "
            f"{format_elapsed(elapsed)} seconds ({tries} tries)"
        )
    )
    return False
