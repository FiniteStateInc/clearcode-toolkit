import time


def publish_to_algolia_with_retry(algolia_index, document, max_retries=5):
    retry = 0

    while retry < max_retries:
        try:
            algolia_index.save_object(document)
            return
        except Exception as exc:
            retry += 1
            sleep_time = 2 ** retry
            time.sleep(sleep_time)