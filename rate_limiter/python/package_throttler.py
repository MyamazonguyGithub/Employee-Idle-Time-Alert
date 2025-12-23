from collections import deque
from dataclasses import dataclass, field
from pprint import pprint
from requests.exceptions import HTTPError, ConnectionError, Timeout
import math
import random
import time

@dataclass
class _PackageThrottlerDefaultsBase:
    """Default values for the PackageThrottler class."""
    max_operations_in_window: int = 10  # operations per window
    rate_limit_window: int = 1  # in seconds
    throttle_start_percentage: float = 0.75  # Start throttling at 75% of the limit
    full_throttle_percentage: float = 0.90  # Fully throttle at 90% of the limit
    base_backoff_delay: float = 10.0  # Base delay in seconds for backoff

@dataclass
class _PackageThrottlerBase:
    """Base class for the PackageThrottler class."""
    transient_exceptions: tuple

@dataclass
class PackageThrottler(_PackageThrottlerDefaultsBase, _PackageThrottlerBase):
    """
    A class that throttles operations with exponential backoff, jitter, and dynamic throttling thresholds.
    
    Attributes:
        max_operations_in_window (int): The maximum number of operations allowed within a single time window.
        rate_limit_window (int): The duration of the time window in seconds. Default is 1 second.
        throttle_start_percentage (float): The percentage of the max operations in the window at which throttling begins.
                                            Default is 0.75 (75%).
        full_throttle_percentage (float): The percentage of the max operations in the window at which full throttling occurs.
                                          Default is 0.90 (90%).
        base_backoff_delay (float): The base delay in seconds for exponential backoff.
    """

    throttle_trigger_count: int = field(init=False)
    full_throttle_trigger_count: int = field(init=False)
    operation_timestamps: deque = field(default_factory=deque, init=False)
    total_operations_made: int = field(default=0, init=False)
    window_start_time: float = field(default_factory=time.time, init=False)
    operation_position: int = field(default=0, init=False)
    is_server_providing_operation_position: bool = field(default=False, init=False)
    is_leaky_bucket: bool = field(default=True, init=False)

    def __post_init__(self):
        """Calculate when throttling should start after initialization."""
        self._recalculate_throttle_thresholds()

    def _recalculate_throttle_thresholds(self):
        """Recalculate the throttle and full throttle trigger counts based on the current rate limits."""
        self.throttle_trigger_count = math.ceil(self.max_operations_in_window * self.throttle_start_percentage)
        self.full_throttle_trigger_count = math.ceil(self.max_operations_in_window * self.full_throttle_percentage)

    def _throttle(self):
        """Handle the throttling logic before making an operation."""
        current_time = time.time()
        
        # Remove old operation timestamps that are outside the current time window
        while self.operation_timestamps and self.operation_timestamps[0] < current_time - self.rate_limit_window:
            self.operation_timestamps.popleft()

        time_elapsed = current_time - self.window_start_time
        time_remaining = abs(self.rate_limit_window - time_elapsed)

        # Reset window start time if the current window has expired
        if time_remaining <= 0:
            self.window_start_time = current_time

        # Get the position of the current operation in the throttling window
        if not self.is_server_providing_operation_position:
            self.operation_position = len(self.operation_timestamps)

        # Apply throttling if within the throttle range
        if self.operation_position >= self.throttle_trigger_count and self.operation_position < self.full_throttle_trigger_count:
            remaining_operations = self.full_throttle_trigger_count - self.operation_position
            
            if self.is_leaky_bucket:
                time_to_wait = min(time_remaining / max(remaining_operations, 1), self.rate_limit_window)
                print(f"\033[93m[Throttling] Waiting {time_to_wait:.2f} seconds before making the next operation.\033[0m")
            else:
                time_to_wait = min(time_remaining, self.rate_limit_window)
                print(f"\033[93m[Throttling] Waiting {time_to_wait:.2f} seconds before making the next operation.\033[0m")

            time.sleep(time_to_wait)

        # Fully throttle if at the last position in the throttle range
        if self.operation_position == self.full_throttle_trigger_count - 1:
            time_to_wait = time_remaining * 1.1  # Add an extra 10% delay as cushion
            if time_to_wait > 0:
                print(f"\033[93m[Full Throttle] Waiting {time_to_wait:.2f} seconds to consume remaining time.\033[0m")
                time.sleep(time_to_wait)

        # Apply exponential backoff if the operation count exceeds the full throttle trigger count
        if self.operation_position >= self.full_throttle_trigger_count:
            if time_elapsed < self.rate_limit_window:
                backoff_time = (self.rate_limit_window - time_elapsed) * 1.5
                print(f"\033[93m[Backoff] Exponential Backoff: Waiting {backoff_time:.2f} seconds before proceeding.\033[0m")
                time.sleep(backoff_time)

    def _record_operation(self):
        """Record the current time as an operation timestamp and update the total operation count."""
        self.operation_timestamps.append(time.time())
        self.total_operations_made += 1
        
        # Reset window start time if this is the first operation in a new cycle
        if len(self.operation_timestamps) == 1:
            self.window_start_time = time.time()


    def _is_transient_error(self, exception):
        """Determine if the error is transient and worth retrying."""
        if isinstance(exception, (Timeout, ConnectionError)):
            print(f"\033[91mIs transient error: Connection\033[0m")
            return True  # Retry for connection-related errors

        if isinstance(exception, HTTPError):
            print(f"\033[91mIs transient error: HTTP\033[0m")
            status_code = exception.response.status_code
            if status_code in {429, 503}:  # Rate limiting or temporary unavailability
                return True
            if 500 <= status_code < 600:  # Retry for server errors
                return True
            
        if self.transient_exceptions and isinstance(exception, self.transient_exceptions):
            print(f"\033[91mIs transient error: Custom\033[0m")
            return True

        # Customize with additional checks as needed for your client
        print(f"\033[91mIs not transient error\033[0m")
        return False

    
    def _make_operation(self, method, *args, **kwargs):
        """Make an operation with retries using exponential backoff, jitter, and base delay only for transient errors."""
        retries = kwargs.pop('retries', 3)
        backoff_factor = kwargs.pop('backoff_factor', 2)
    
        for attempt in range(retries):
            self._throttle()
    
            # Make the operation
            try:
                response = method(*args, **kwargs)
                self._record_operation()
                return response
    
            # Handle transient errors with exponential backoff and jitter
            except Exception as err:
                print(f"OperationError: {err}")
                if self._is_transient_error(err):
                    backoff_time = self.base_backoff_delay * (backoff_factor ** attempt) + random.uniform(0, 1)
                    print(f"\033[93m[Rate Limit Hit] Backoff: Waiting {backoff_time:.2f} seconds before retrying.\033[0m")
                    time.sleep(backoff_time)
                else:
                    raise

    def execute_with_throttle(self, client_instance, operation, *args, **kwargs):
        """Throttle and execute a client operation."""
        method = getattr(client_instance, operation, None)
        if method is None:
            raise ValueError(f"The client instance does not support the operation: {operation}")
        return self._make_operation(method, *args, **kwargs)
