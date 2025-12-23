from dataclasses import InitVar, dataclass, field
import math
import time
from collections import deque
import random
import requests

@dataclass
class _RequestThrottlerDefaultsBase:
    """Default values for the RequestThrottler class."""
    max_requests_in_window: int = 10  # requests per window
    rate_limit_window: int = 1  # in seconds
    throttle_start_percentage: float = 0.75  # Start throttling at 75% of the limit
    full_throttle_percentage: float = 0.90  # Fully throttle at 90% of the limit

@dataclass
class RequestThrottler(_RequestThrottlerDefaultsBase):
    """
    A class that throttles requests with exponential backoff, jitter, and dynamic throttling thresholds.

    Attributes:
        max_requests_in_window (int): The maximum number of requests allowed within a single time window.
        rate_limit_window (int): The duration of the time window in seconds. Default is 1 second.
        throttle_start_percentage (float): The percentage of the max requests in the window at which throttling begins.
                                            Default is 0.75 (75%).
        full_throttle_percentage (float): The percentage of the max requests in the window at which full throttling occurs.
                                          Default is 0.90 (90%).
    """
    
    throttle_trigger_count: int = field(init=False)
    full_throttle_trigger_count: int = field(init=False)
    request_timestamps: deque = field(default_factory=deque, init=False)
    total_requests_made: int = field(default=0, init=False)
    window_start_time: float = field(default_factory=time.time, init=False)
    request_position: int = field(default=0, init=False)
    is_server_providing_request_position: bool = field(default=False, init=False)
    is_leaky_bucket: bool = field(default=True, init=False)

    def __post_init__(self):
        """Calculate when throttling should start after initialization."""
        self._recalculate_throttle_thresholds()

    def _recalculate_throttle_thresholds(self):
        """Recalculate the throttle and full throttle trigger counts based on the current rate limits."""
        self.throttle_trigger_count = math.ceil(self.max_requests_in_window * self.throttle_start_percentage)
        self.full_throttle_trigger_count = math.ceil(self.max_requests_in_window * self.full_throttle_percentage)

    def _throttle(self):
        """Handle the throttling logic before making a request."""
        current_time = time.time()
        
        # Remove old request timestamps that are outside the current time window
        while self.request_timestamps and self.request_timestamps[0] < current_time - self.rate_limit_window:
            self.request_timestamps.popleft()

        time_elapsed = current_time - self.window_start_time
        time_remaining = abs(self.rate_limit_window - time_elapsed)

        # Reset window start time if the current window has expired
        if time_remaining <= 0:
            self.window_start_time = current_time

        # Get the position of the current request in the throttling window
        if not self.is_server_providing_request_position:
            self.request_position = len(self.request_timestamps)

        # Apply throttling if within the throttle range
        if self.request_position >= self.throttle_trigger_count and self.request_position < self.full_throttle_trigger_count:
            remaining_requests = self.full_throttle_trigger_count - self.request_position
            
            if self.is_leaky_bucket:
                time_to_wait = min(time_remaining / max(remaining_requests, 1), self.rate_limit_window)
                print(f"\033[93m[Throttling] Waiting {time_to_wait:.2f} seconds before making the next request.\033[0m")
            else:
                time_to_wait = min(time_remaining, self.rate_limit_window)
                print(f"\033[93m[Throttling] Waiting {time_to_wait:.2f} seconds before making the next request.\033[0m")

            time.sleep(time_to_wait)

        # Fully throttle if at the last position in the throttle range
        if self.request_position == self.full_throttle_trigger_count - 1:
            time_to_wait = time_remaining * 1.1  # Add an extra 10% delay as cushion
            if time_to_wait > 0:
                print(f"\033[93m[Full Throttle] Waiting {time_to_wait:.2f} seconds to consume remaining time.\033[0m")
                time.sleep(time_to_wait)

        # Apply exponential backoff if the request count exceeds the full throttle trigger count
        if self.request_position >= self.full_throttle_trigger_count:
            if time_elapsed < self.rate_limit_window:
                backoff_time = (self.rate_limit_window - time_elapsed) * 1.5
                print(f"\033[93m[Backoff] Exponential Backoff: Waiting {backoff_time:.2f} seconds before proceeding.\033[0m")
                time.sleep(backoff_time)

    def _record_request(self):
        """Record the current time as a request timestamp and update the total request count."""
        self.request_timestamps.append(time.time())
        self.total_requests_made += 1
        
        # Reset window start time if this is the first request in a new cycle
        if len(self.request_timestamps) == 1:
            self.window_start_time = time.time()

    def _is_transient_error(self, status_code, response):
        """Determine if the error is transient and worth retrying."""
        transient_errors = {408, 429}
        if status_code in transient_errors:
            return True
        if 500 <= status_code < 600:
            return True
        if status_code == 403 and 'Retry-After' in response.headers:
            return True
        return False
    
    def _make_request(self, method, url, headers=None, params=None, data=None, json=None, retries=3, backoff_factor=2):
        """Make a request with retries using exponential backoff and jitter."""
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
                params_dict = {
                    'headers': headers or None,
                    'params': params or None,
                    'data': data or None,
                    'json': json or None
                }
                filtered_params = {k: v for k, v in params_dict.items() if v}
                response = method_map[method](url, **filtered_params)

                try:
                    response.raise_for_status()
                except Exception as e:
                    raise e
                self._record_request()
                return response
    
            # Handle HTTP errors
            except requests.exceptions.HTTPError as http_err:
                print(f"HTTPError: {http_err}")
                if not self._is_transient_error(http_err.response.status_code, http_err.response):
                    raise

                if 'Retry-After' in http_err.response.headers:
                    retry_after = int(http_err.response.headers['Retry-After'])
                    print(f"Response has Retry-After header. ")
                    print(f"Retrying after {retry_after} seconds.")
                    time.sleep(retry_after)
                elif attempt < retries:
                    sleep_time = (backoff_factor ** (attempt + 1)) + random.uniform(0, 1)
                    time.sleep(sleep_time)
                else:
                    raise
    
            except requests.exceptions.RequestException as req_err:
                print(f"RequestException: {req_err}")
                if attempt < retries:
                    sleep_time = (backoff_factor ** attempt + 1) + random.uniform(0, 1)
                    time.sleep(sleep_time)
                else:
                    raise

    def throttled_get(self, url, headers=None, params=None):
        """Throttled GET request."""
        return self._make_request('GET', url, headers=headers, params=params)

    def throttled_post(self, url, data=None, json=None, headers=None, params=None):
        """Throttled POST request."""
        return self._make_request('POST', url, headers=headers, params=params, data=data, json=json)

    def throttled_put(self, url, data=None, headers=None, params=None):
        """Throttled PUT request."""
        return self._make_request('PUT', url, headers=headers, params=params, data=data)

    def throttled_patch(self, url, data=None, headers=None, params=None):
        """Throttled PATCH request."""
        return self._make_request('PATCH', url, headers=headers, params=params, data=data)

    def throttled_delete(self, url, headers=None, params=None):
        """Throttled DELETE request."""
        return self._make_request('DELETE', url, headers=headers, params=params)
