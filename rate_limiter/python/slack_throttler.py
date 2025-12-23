from dataclasses import dataclass
from throttlers.python.throttler import RequestThrottler


@dataclass
class SlackThrottler(RequestThrottler):
    """
    A specialized throttler for Slack API requests that handles dynamic rate limiting

    Attributes:
        max_requests_in_window (int): The maximum number of requests allowed within a single time window.
        rate_limit_window (int): The duration of the time window in seconds. Default is 1 second.
        throttle_start_percentage (float): The percentage of the max requests in the window at which throttling begins.
                                            Default is 0.75 (75%).
        full_throttle_percentage (float): The percentage of the max requests in the window at which full throttling occurs.
                                          Default is 0.90 (90%).
    """

    pass
