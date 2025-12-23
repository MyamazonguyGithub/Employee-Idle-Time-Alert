from collections import deque
from dataclasses import dataclass, field
import random
import requests
import time
from datetime import datetime
from .throttler import RequestThrottler

# If you are working with only one file, do not use the import statement above.
# Instead, replace the import statement with the entire code snippet from the throttler.py file.

@dataclass
class _AirtableThrottlerDefaultsBase:
    """Default values for the AirtableThrottler class."""
    max_requests_in_window: int = 5  # requests per second
    rate_limit_window: int = 1  # in seconds
    throttle_start_percentage: float = 0.50  # Start throttling at 60% of the limit
    full_throttle_percentage: float = 0.70  # Fully throttle at 80% of the limit


@dataclass
class AirtableThrottler(_AirtableThrottlerDefaultsBase, RequestThrottler):
    """
    A specialized throttler for Airtable API requests that handles dynamic rate limiting
    based on the specified limits and response status codes.
    """
    throttle_trigger_count: int = field(init=False)
    full_throttle_trigger_count: int = field(init=False)
    request_timestamps: deque = field(default_factory=deque, init=False)
    total_requests_made: int = field(default=0, init=False)
    window_start_time: float = field(default_factory=time.time, init=False)
    

    def __post_init__(self):
        """Initialize the API key and calculate throttle thresholds."""
        self._recalculate_throttle_thresholds()
        self.is_server_providing_request_position = False
        self.is_leaky_bucket = False       

    def _get_retry_after_seconds(self, retry_after_value):
        """Convert Retry-After value to seconds."""
        try:
            # If Retry-After is a timestamp (HTTP-date)
            retry_after_date = datetime.strptime(retry_after_value, '%a, %d %b %Y %H:%M:%S GMT')
            retry_after_seconds = (retry_after_date - datetime.now()).total_seconds()
        except ValueError:
            # If Retry-After is in seconds
            retry_after_seconds = int(retry_after_value)
        
        return max(0, retry_after_seconds)

    def _make_request(self, method, url, headers=None, params=None, data=None, json=None, retries=3, backoff_factor=2):
        """Make a request with retries using exponential backoff and jitter."""
        headers = headers or {}
        params = params or {}
        data = data or {}
        json = json or {}

        method_map = {
            'GET': requests.get,
            'POST': requests.post,
            'PUT': requests.put,
            'PATCH': requests.patch,
            'DELETE': requests.delete
        }

        if method not in method_map:
            raise ValueError("Unsupported HTTP method")

        for attempt in range(retries):
            self._throttle()

            # Make the request
            try:
                response = method_map[method](url, headers=headers, params=params, data=data, json=json)             
                response.raise_for_status()
                self._record_request()
                return response

            # Handle HTTP errors
            except requests.exceptions.HTTPError as http_err:
                print((http_err.response).json())
                if not self._is_transient_error(http_err.response.status_code, http_err.response):
                    raise

                retry_after = None
                if 'Retry-After' in http_err.response.headers:
                    retry_after = self._get_retry_after_seconds(http_err.response.headers['Retry-After'])

                if retry_after:
                    print(f"Received 429: Retrying after {retry_after} seconds")
                    time.sleep(retry_after)
                else:
                    print("Received 429: No Retry-After header, waiting for 30 seconds")
                    time.sleep(30 + random.uniform(0, 1))

                if attempt < retries - 1:
                    sleep_time = ((backoff_factor ** attempt) * 30) + random.uniform(0, 1)
                    print(f"Retrying in {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
                else:
                    raise

            except requests.exceptions.RequestException:
                if attempt < retries:
                    sleep_time = (backoff_factor ** (attempt + 1)) + random.uniform(0, 1)
                    print(f"Request failed, retrying in {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
                else:
                    raise
