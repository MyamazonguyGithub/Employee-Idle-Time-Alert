from dataclasses import InitVar, dataclass, field
import random
import time
import requests
from throttlers.python.throttler import RequestThrottler

# If you are working with only one file, do not use the import statement above.
# Instead, replace the import statement with the entire code snippet from the throttler.py file.

@dataclass
class _HubSpotThrottlerDefaultsBase:
    """Default values for the HubSpotThrottler class."""
    max_requests_in_window: int = 190  # per window
    rate_limit_window: int = 10  # in seconds
    throttle_start_percentage: float = 0.75 # Start throttling at 75% of the limit
    full_throttle_percentage: float = 0.90 # Fully throttle at 90% of the limit

@dataclass
class _HubSpotThrottlerBase:
    """Base class for the HubSpotThrottler class."""
    primary_api_key: str
    backup_api_keys: list
    
@dataclass
class HubSpotThrottler(_HubSpotThrottlerDefaultsBase, RequestThrottler, _HubSpotThrottlerBase):
    """
    A specialized throttler for HubSpot API requests that handles multiple API keys and dynamically
    adjusts rate limits based on the rate-limit headers returned by HubSpot.
    """
    primary_api_key: InitVar[str]
    backup_api_keys: InitVar[list]
    current_api_key: str = field(init=False)

    def __post_init__(self, primary_api_key, backup_api_keys):
        """Initialize the throttler with the primary API key."""
        super().__post_init__()
        self.current_api_key = primary_api_key
        self.backup_api_keys = backup_api_keys
        self.is_server_providing_request_position = True
        self.is_leaky_bucket = False

    def _switch_api_key(self):
        """Switch to a random backup API key when the current key is rate-limited."""
        all_keys = [self.current_api_key] + self.backup_api_keys
        available_keys = [key for key in all_keys if key != self.current_api_key]
        if available_keys:
            self.current_api_key = random.choice(available_keys)

    def _update_rate_limits(self, response):
        """Update the rate limits based on HubSpot's response headers."""
        if 'X-HubSpot-RateLimit-Interval-Milliseconds' in response.headers:
            self.rate_limit_window = int(response.headers['X-HubSpot-RateLimit-Interval-Milliseconds']) / 1000
        
        # Recalculate thresholds based on the updated rate limits
        self._recalculate_throttle_thresholds()

    def _make_request(self, method, url, headers=None, params=None, data=None, json=None, retries=4, backoff_factor=3):
        """Make a request with retries using exponential backoff, jitter, and dynamic API key switching."""
        headers = headers or {}
        headers['Authorization'] = f'Bearer {self.current_api_key}'

        for attempt in range(retries):
            self._throttle()

            try:
                response = super()._make_request(
                    method, url, headers=headers, params=params, data=data, json=json, retries=3
                )
                self.request_position = int(response.headers.get('X-HubSpot-RateLimit-Max', '150')) - int(response.headers.get('X-HubSpot-RateLimit-Remaining', '150'))
                self._record_request()
                self._update_rate_limits(response)
                return response

            except requests.exceptions.HTTPError as http_err:
                if http_err.response.status_code == 429:
                    self._switch_api_key()
                    retry_after = int(http_err.response.headers.get('Retry-After', 0))
                    time.sleep(retry_after if retry_after else (backoff_factor ** attempt) + random.uniform(0, 1))
                else:
                    raise

            except requests.exceptions.RequestException:
                if attempt < retries - 1:
                    time.sleep((backoff_factor ** attempt) + random.uniform(0, 1))
                else:
                    raise
