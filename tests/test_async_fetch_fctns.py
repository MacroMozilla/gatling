import unittest
import json
from gatling.utility.async_fetch_fctns import sync_fetch_http


def log_response(name, url, method, rtype, status, size, result):
    """
    Unified logging helper for test results.
    Prints a concise summary of each request and response.

    :param name: Test name or label.
    :param url: Target URL.
    :param method: HTTP method used (GET, POST, etc.).
    :param rtype: Expected return type ('text', 'json', or 'bytes').
    :param status: HTTP response status code.
    :param size: Response body size in bytes.
    :param result: Response content.
    """
    print(f"\n[{name}]")
    print(f"  URL: {url}")
    print(f"  Method: {method}")
    print(f"  Return type: {rtype}")
    print(f"  Status: {status}")
    print(f"  Size: {size} bytes")

    # Show a short preview of the response content
    if isinstance(result, (bytes, bytearray)):
        preview = result[:50]  # limit byte preview length
        print(f"  Preview (bytes): {preview!r}")
    elif isinstance(result, (dict, list)):
        snippet = json.dumps(result, ensure_ascii=False)[:100]
        print(f"  Preview (json): {snippet!r}")
    else:
        print(f"  Preview (text): {str(result)[:100]!r}")


class TestSyncFetchHttp(unittest.TestCase):
    """
    Unit tests for sync_fetch_http().
    Covers both GET and POST requests with 'text', 'json', and 'bytes' response types.
    """

    def test_get_text(self):
        """GET request returning HTML text."""
        url = "https://httpbin.org/html"
        method = "GET"
        rtype = "text"
        result, status, size = sync_fetch_http(url, method=method, rtype=rtype)
        self.assertEqual(status, 200)
        log_response("GET text", url, method, rtype, status, size, result)

    def test_get_json(self):
        """GET request returning JSON data."""
        url = "https://httpbin.org/get"
        method = "GET"
        rtype = "json"
        result, status, size = sync_fetch_http(url, method=method, rtype=rtype)
        self.assertEqual(status, 200)
        log_response("GET json", url, method, rtype, status, size, result)

    def test_get_bytes(self):
        """GET request returning binary data (PNG image)."""
        url = "https://httpbin.org/image/png"
        method = "GET"
        rtype = "bytes"
        result, status, size = sync_fetch_http(url, method=method, rtype=rtype)
        self.assertEqual(status, 200)
        log_response("GET bytes", url, method, rtype, status, size, result)

    def test_post_text(self):
        """POST request returning text (form-encoded)."""
        url = "https://httpbin.org/post"
        method = "POST"
        rtype = "text"
        result, status, size = sync_fetch_http(url, method=method, data={"k": "v"}, rtype=rtype)
        self.assertEqual(status, 200)
        log_response("POST text", url, method, rtype, status, size, result)

    def test_post_json(self):
        """POST request returning JSON response."""
        url = "https://httpbin.org/post"
        method = "POST"
        rtype = "json"
        data = json.dumps({"a": 1, "b": 2})
        headers = {"Content-Type": "application/json"}
        result, status, size = sync_fetch_http(
            url, method=method, data=data, headers=headers, rtype=rtype
        )
        self.assertEqual(status, 200)
        log_response("POST json", url, method, rtype, status, size, result)

    def test_post_bytes(self):
        """POST request returning binary response."""
        url = "https://httpbin.org/post"
        method = "POST"
        rtype = "bytes"
        payload = b"test-binary-data"
        result, status, size = sync_fetch_http(url, method=method, data=payload, rtype=rtype)
        self.assertEqual(status, 200)
        log_response("POST bytes", url, method, rtype, status, size, result)


if __name__ == "__main__":
    # Run all tests with detailed output
    unittest.main(verbosity=2)
