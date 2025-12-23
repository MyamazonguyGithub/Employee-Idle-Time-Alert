from dataclasses import InitVar, dataclass, field
import random
import time
import requests
from throttlers.python.throttler import RequestThrottler

# If you are working with only one file, do not use the import statement above.
# Instead, replace the import statement with the entire code snippet from the throttler.py file.
@dataclass
class _AsanaThrottlerDefaultsBase:
    """Default values for the AsanaThrottler class."""
    max_requests_in_window: int = 1500 # requests per window
    rate_limit_window: int = 60  # in seconds
    throttle_start_percentage: float = 0.75 # Start throttling at 75% of the limit
    full_throttle_percentage: float = 0.90 # Fully throttle at 90% of the limit

@dataclass
class _AsanaThrottlerBase:
    """Base class for the AsanaThrottler class."""
    primary_api_key: str
    backup_api_keys: list

@dataclass
class AsanaThrottler(_AsanaThrottlerDefaultsBase, RequestThrottler, _AsanaThrottlerBase):
    """
    A specialized throttler for Asana API requests that handles multiple API keys and dynamically
    adjusts rate limits based on the rate-limit headers returned by Asana.
    """
    primary_api_key: InitVar[str]
    backup_api_keys: InitVar[list]
    current_api_key: str = field(init=False)

    def __post_init__(self, primary_api_key, backup_api_keys):
        """Initialize the throttler with the primary API key."""
        super().__post_init__()
        self.current_api_key = primary_api_key
        self.backup_api_keys = backup_api_keys
        self.is_server_providing_request_position = False
        self.is_leaky_bucket = False

    def _switch_api_key(self):
        """Switch to a random backup API key when the current key is rate-limited."""
        all_keys = [self.current_api_key] + self.backup_api_keys
        available_keys = [key for key in all_keys if key != self.current_api_key]
        if available_keys:
            self.current_api_key = random.choice(available_keys)

    def _calculate_backoff_time(self, attempt):
        return (self.backoff_factor ** attempt) + random.uniform(0, 1)

    def _make_request(self, method, url, headers=None, params=None, data=None, json=None, retries=3, backoff_factor=2):
        """Make a request with retries using exponential backoff, jitter, and dynamic API key switching."""
        headers = headers or {}
        headers['authorization'] = f'Bearer {self.current_api_key}'

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

            try:
                response = method_map[method](url, headers=headers, params=params, data=data)
                response.raise_for_status()
                self._record_request()
                return response

            except requests.exceptions.HTTPError as http_err:
                if http_err.response.status_code == 429 or 500 <= http_err.response.status_code < 600:
                    self._switch_api_key()
                    retry_after = int(http_err.response.headers.get('Retry-After', 0))
                    if retry_after:
                        print(f"Rate limit hit. Switching API key. Retrying after {retry_after} seconds.")
                        time.sleep(retry_after)
                    else:
                        backoff_time = self._calculate_backoff_time(attempt)
                        print(f"Rate limit hit. Switching API key. Retrying after {backoff_time} seconds.")
                        time.sleep(backoff_time)
                else:
                    raise

            except requests.exceptions.RequestException as req_err:
                self._switch_api_key()
                if attempt < retries - 1:
                    time.sleep((backoff_factor ** attempt) + random.uniform(0, 1))
                else:
                    raise
