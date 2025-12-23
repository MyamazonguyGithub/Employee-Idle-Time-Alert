# API Rate Limiter

## Overview

The API Rate Limiter project provides tools to help manage and control the rate at which API requests are made, ensuring compliance with rate limits and preventing overwhelming the server. The project includes classes for throttling requests, applying exponential backoff, and handling retries for transient errors.

## Table of Contents

- [RequestThrottler](#requestthrottler)
- [HubSpotThrottler](#hubspotthrottler)
- [TimeDoctorThrottler](#timedoctorthrottler)
- [AsanaThrottler](#asanathrottler)
- [SlackThrottler](#slackthrottler)
- [AirtableThrottler](#airtablethrottler)
- [PackageThrottler](#packagethrottler)

## Classes

### RequestThrottler

The `RequestThrottler` class is designed to manage API request rate limiting by controlling the number of requests made within a specified time window. It supports dynamic throttling thresholds, exponential backoff, and jitter to ensure compliance with rate limits and prevent overwhelming the target server.

#### Initialization - RequestThrottler

```python
throttler = RequestThrottler(
    max_requests_in_window=150, 
    rate_limit_window=10, 
    throttle_start_percentage=0.75, 
    full_throttle_percentage=0.90
)
```

- **`max_requests_in_window` (int)**: Maximum number of requests allowed within the time window.
- **`rate_limit_window` (int, optional)**: The time window in seconds. Default is 1 second.
- **`throttle_start_percentage` (float, optional)**: Percentage of the request limit at which throttling begins. Default is 75%.
- **`full_throttle_percentage` (float, optional)**: Percentage of the request limit at which full throttling is applied. Default is 90%.

#### Methods - RequestThrottler

- **`throttled_get(url, headers=None, params=None)`**:
  - Makes a GET request with throttling applied.
  - **Parameters**:
    - `url` (str): The URL for the GET request.
    - `headers` (dict, optional): Headers to include in the request.
    - `params` (dict, optional): Query parameters to include in the request.

- **`throttled_post(url, data=None, headers=None, params=None)`**:
  - Makes a POST request with throttling applied.
  - **Parameters**:
    - `url` (str): The URL for the POST request.
    - `data` (dict, optional): Data to include in the body of the POST request.
    - `headers` (dict, optional): Headers to include in the request.
    - `params` (dict, optional): Query parameters to include in the request.

- **`throttled_put(url, data=None, headers=None, params=None)`**:
  - Makes a PUT request with throttling applied.
  - **Parameters**:
    - `url` (str): The URL for the PUT request.
    - `data` (dict, optional): Data to include in the body of the PUT request.
    - `headers` (dict, optional): Headers to include in the request.
    - `params` (dict, optional): Query parameters to include in the request.

- **`throttled_patch(url, data=None, headers=None, params=None)`**:
  - Makes a PATCH request with throttling applied.
  - **Parameters**:
    - `url` (str): The URL for the PATCH request.
    - `data` (dict, optional): Data to include in the body of the PATCH request.
    - `headers` (dict, optional): Headers to include in the request.
    - `params` (dict, optional): Query parameters to include in the request.

- **`throttled_delete(url, headers=None, params=None)`**:
  - Makes a DELETE request with throttling applied.
  - **Parameters**:
    - `url` (str): The URL for the DELETE request.
    - `headers` (dict, optional): Headers to include in the request.
    - `params` (dict, optional): Query parameters to include in the request.

#### Example Usage - RequestThrottler

```python
throttler = RequestThrottler(max_requests_in_window=20, rate_limit_window=20)

for idx in range(50):
    response = throttler.throttled_get('https://example.com/api/resource')
    print(f"Making request {idx + 1}")
    print(response.status_code)
```

This example shows how to use the `RequestThrottler` class to make GET requests while managing the rate at which they are sent. The throttler automatically applies the necessary delays and backoff to ensure compliance with rate limits.

### HubSpotThrottler

The `HubSpotThrottler` class extends the functionality of `RequestThrottler` specifically for managing API requests to the HubSpot API. It dynamically adjusts to HubSpot's rate limits, introduces delays during high traffic to avoid hitting those limits, and switches between primary and backup API keys if a rate limit is reached.

#### Initialization - HubSpotThrottler

```python
hubspot_requester = HubSpotThrottler(
    primary_api_key='your_primary_api_key',
    backup_api_keys=['your_backup_api_key_1', 'your_backup_api_key_2']
)
```

- **`primary_api_key` (str)**: The primary API key used for making requests.
- **`backup_api_keys` (list)**: A list of backup API keys to use if the primary key hits the rate limit.

**Note**: You do not need to manually set `max_requests_in_window` and `rate_limit_window` for HubSpot, as these values are automatically retrieved from HubSpot's response headers. However, you can configure the other parameters like `throttle_start_percentage` and `full_throttle_percentage` as needed.

#### Methods - HubSpotThrottler

The `HubSpotThrottler` class has the same methods as the `RequestThrottler` class:

- **`throttled_get(url, headers=None, params=None)`**
- **`throttled_post(url, data=None, json=None, headers=None, params=None)`**
- **`throttled_put(url, data=None, headers=None, params=None)`**
- **`throttled_patch(url, data=None, headers=None, params=None)`**
- **`throttled_delete(url, headers=None, params=None)`**

#### Example Usage - HubSpotThrottler

```python
hubspot_requester = HubSpotThrottler(
    primary_api_key='your_primary_api_key',
    backup_api_keys=['your_backup_api_key_1', 'your_backup_api_key_2']
)

for idx in range(50):
    response = hubspot_requester.throttled_get('https://api.hubapi.com/crm/v3/objects/companies/19740367123')
    print(f"Making request {idx + 1}")
    print(response.status_code)
```

This example shows how to use the `HubSpotThrottler` class to manage API requests to HubSpot, introducing delays as needed and switching API keys if the rate limit is reached to ensure uninterrupted operation.

### TimeDoctorThrottler

The `TimeDoctorThrottler` class extends the functionality of `RequestThrottler` specifically for managing API requests to the Time Doctor API. It is designed to handle high traffic scenarios by introducing delays between requests when a large burst is detected, thereby reducing the likelihood of hitting the API rate limit. Additionally, if the rate limit is exceeded, the throttler is designed to switch to backup API keys to ensure continuous processing of requests.

#### Initialization - TimeDoctorThrottler

```python
time_doctor_requester = TimeDoctorThrottler()
```

**Note**: The limits such as `max_requests_in_window`, `rate_limit_window`, `throttle_start_percentage`, and `full_throttle_percentage` are pre-configured and do not require manual setup.

#### Methods - TimeDoctorThrottler

The `TimeDoctorThrottler` class includes the following methods, inherited and customized from `RequestThrottler`:

- **`throttled_get(url, headers=None, params=None)`**
- **`throttled_post(url, data=None, json=None, headers=None, params=None)`**
- **`throttled_put(url, data=None, headers=None, params=None)`**
- **`throttled_patch(url, data=None, headers=None, params=None)`**
- **`throttled_delete(url, headers=None, params=None)`**

Additionally, the `TimeDoctorThrottler` class includes a private method:

- **`_get_api_key()`**:
  - Retrieves the API key from the Time Doctor API. This method handles the login process and ensures that the appropriate token is obtained for authenticating requests.
  - **Returns**:
    - `str`: The API key for authenticating subsequent requests.

#### Example Usage - TimeDoctorThrottler

```python
time_doctor_requester = TimeDoctorThrottler()

for idx in range(1000):
    response = time_doctor_requester.throttled_get('https://api2.timedoctor.com/api/1.0/endpoint')
    print(f"Making request {idx + 1}")
    print(response.status_code)
```

This example demonstrates how to use the `TimeDoctorThrottler` class to manage API requests to the Time Doctor API. The throttler automatically introduces delays and switches to backup API keys as needed to prevent hitting the rate limit.

### AsanaThrottler

The `AsanaThrottler` class extends the functionality of `RequestThrottler` specifically for managing API requests to the Asana API. It dynamically adjusts the delay between requests based on the `Retry-After` headers provided by Asana, ensuring that rate limits are respected. Like the `HubSpotThrottler`, it also has the ability to switch between primary and backup API keys when a rate limit is reached.

#### Initialization - AsanaThrottler

```python
asana_requester = AsanaThrottler(
    primary_api_key='your_primary_api_key',
    backup_api_keys=['your_backup_api_key_1', 'your_backup_api_key_2']
)
```

- **`primary_api_key` (str)**: The primary API key used for making requests.
- **`backup_api_keys` (list)**: A list of backup API keys to use if the primary key hits the rate limit.

**Note**: The `AsanaThrottler` automatically handles `Retry-After` headers provided by Asana and adjusts the request delays accordingly. The `max_requests_in_window` and `rate_limit_window` parameters are not required for this class, as the delay is dynamically set based on the `Retry-After` headers.

#### Methods - AsanaThrottler

The `AsanaThrottler` class inherits the following methods from `RequestThrottler`:

- **`throttled_get(url, headers=None, params=None)`**
- **`throttled_post(url, data=None, json=None, headers=None, params=None)`**
- **`throttled_put(url, data=None, headers=None, params=None)`**
- **`throttled_patch(url, data=None, headers=None, params=None)`**
- **`throttled_delete(url, headers=None, params=None)`**

#### Example Usage - AsanaThrottler

```python
asana_requester = AsanaThrottler(
    primary_api_key=getenv('ASANA_API_KEY_1'),
    backup_api_keys=[
        getenv('ASANA_API_KEY_2'),
        getenv('ASANA_API_KEY_3')
    ]
)

for idx in range(200):
    response = asana_requester.throttled_get(
      'https://app.asana.com/api/1.0/users/me',
      headers={"accept": "application/json"}
    )
    print(f"Making request {idx + 1}")
    print(response.status_code)
    print()
```

This example shows how to use the `AsanaThrottler` class to manage API requests to Asana. The throttler will automatically apply delays based on the `Retry-After` headers returned by Asana and switch to backup API keys if the rate limit is reached.

### SlackThrottler

The `SlackThrottler` class extends the functionality of `RequestThrottler` specifically for managing API requests to the Slack API. It handles dynamic rate limiting, introducing delays between requests to ensure compliance with Slack's rate limits. The `SlackThrottler` class is designed to manage the number of API requests within a specified time window, preventing the server from being overwhelmed.

#### Initialization - SlackThrottler

```python
from throttler import SlackThrottler

slack_requester = SlackThrottler(
    max_requests_in_window=50, 
    rate_limit_window=60
)
```

- **`max_requests_in_window` (int)**: Maximum number of requests allowed within the time window.
- **`rate_limit_window` (int, optional)**: The time window in seconds. Default is 1 second.
- **`throttle_start_percentage` (float, optional)**: Percentage of the request limit at which throttling begins. Default is 75%.
- **`full_throttle_percentage` (float, optional)**: Percentage of the request limit at which full throttling is applied. Default is 90%.

#### Methods - SlackThrottler

The `SlackThrottler` class inherits the following methods from `RequestThrottler`:

- **`throttled_get(url, headers=None, params=None)`**
- **`throttled_post(url, data=None, json=None, headers=None, params=None)`**
- **`throttled_put(url, data=None, headers=None, params=None)`**
- **`throttled_patch(url, data=None, headers=None, params=None)`**
- **`throttled_delete(url, headers=None, params=None)`**

#### Example Usage - SlackThrottler

```python
slack_requester = SlackThrottler(max_requests_in_window=20, rate_limit_window=60)

for idx in range(100):
    response = slack_requester.throttled_get('https://slack.com/api/conversations.list')
    print(f"Making request {idx + 1}")
    print(response.status_code)
```

This example demonstrates how to use the `SlackThrottler` class to manage API requests to Slack. The throttler automatically applies the necessary delays and backoff to ensure compliance with Slack's rate limits while handling a high volume of requests.

### AirtableThrottler

The `AirtableThrottler` class extends the functionality of `RequestThrottler` specifically for managing API requests to the Airtable API. It is designed to handle dynamic rate limiting based on specified limits and response status codes, ensuring compliance with Airtable's rate limits while managing retries with exponential backoff for transient errors.

#### Initialization - AirtableThrottler

```python
airtable_requester = AirtableThrottler()
```

**Note**: The limits such as `max_requests_in_window`, `rate_limit_window`, `throttle_start_percentage`, and `full_throttle_percentage` are pre-configured based on typical Airtable API usage patterns. However, these values can be adjusted as needed to better fit specific use cases.

#### Methods - AirtableThrottler

The `AirtableThrottler` class includes the following methods, inherited and customized from `RequestThrottler`:

- **`throttled_get(url, headers=None, params=None)`**
- **`throttled_post(url, data=None, json=None, headers=None, params=None)`**
- **`throttled_put(url, data=None, headers=None, params=None)`**
- **`throttled_patch(url, data=None, headers=None, params=None)`**
- **`throttled_delete(url, headers=None, params=None)`**

#### Example Usage - AirtableThrottler

```python
airtable_requester = AirtableThrottler()

for idx in range(10):
    response = airtable_requester.throttled_get('https://api.airtable.com/v0/appXXXXXXXXX/YourTableName')
    print(f"Making request {idx + 1}")
    print(response.status_code)
```

This example demonstrates how to use the `AirtableThrottler` class to manage API requests to Airtable. The throttler automatically applies the necessary delays, handles retries with exponential backoff, and manages the request rate to ensure compliance with Airtable's rate limits while processing a high volume of requests.

### PackageThrottler

The `PackageThrottler` class is a robust solution for managing operation throttling with built-in support for exponential backoff, jitter, and dynamic throttling thresholds. It is designed to control the rate at which operations are performed, ensuring compliance with rate limits and reducing the likelihood of overwhelming the server or hitting rate limits. This class is particularly useful in scenarios where you need to manage a high volume of operations efficiently while handling transient errors gracefully.

#### Initialization - PackageThrottler

```python
gspread_throttler = PackageThrottler(
    transient_exceptions=(gspread.exceptions.APIError),
    max_operations_in_window=60,
    rate_limit_window=60
)
```

- **`transient_exceptions` (tuple)**: A tuple of exceptions that should be treated as transient and retried with exponential backoff.
- **`max_operations_in_window` (int)**: Maximum number of operations allowed within the time window.
- **`rate_limit_window` (int, optional)**: The time window in seconds. Default is 1 second.
- **`throttle_start_percentage` (float, optional)**: Percentage of the operation limit at which throttling begins. Default is 75%.
- **`full_throttle_percentage` (float, optional)**: Percentage of the operation limit at which full throttling is applied. Default is 90%.
- **`base_backoff_delay` (float, optional)**: Base delay in seconds for exponential backoff. Default is 10 seconds.

#### Methods - PackageThrottler

The `PackageThrottler` class includes several methods to manage operation throttling effectively:

- **`execute_with_throttle(client_instance, operation, *args, **kwargs)`**:
  - Throttles and executes a client operation, handling retries with exponential backoff for transient errors.
  - **Parameters**:
    - `client_instance` (object): The client instance on which the operation is to be performed.
    - `operation` (str): The operation to be executed on the client instance.
    - `args` (tuple): Positional arguments to pass to the operation.
    - `kwargs` (dict): Keyword arguments to pass to the operation.

#### Example Usage - PackageThrottler

```python
from pprint import pprint
from python.package_throttler import PackageThrottler
from python.throttler import RequestThrottler
import gspread
import requests

# Define the throttlers
gspread_throttler = PackageThrottler(
    transient_exceptions=(gspread.exceptions.APIError),
    max_operations_in_window=60,
    rate_limit_window=60,
).execute_with_throttle

throttled_get = RequestThrottler(
    max_requests_in_window=60,
    rate_limit_window=60,
).throttled_get


# Define the service account
service_account = gspread_throttler(
    gspread, 
    'service_account_from_dict', 
    {
      # Your service account credentials
    }
)

# Define the spreadsheet and worksheet
spread = gspread_throttler(
    service_account, 
    'open_by_key',
    '1Bd6axAIQ3K-6sy40pYtBSb7TVjO6q6zo4qYmZw0F8QI'
)

sheet1 = gspread_throttler(
    spread, 
    'get_worksheet_by_id',
    0
)

# Get 1000 random users and append them to the sheet
for _ in range(1000):
    response = throttled_get('https://randomuser.me/api/')
    user = response.json().get('results', {})[0]
    name = f"{user.get('name', {}).get('first', '')} {user.get('name', {}).get('last', '')}"
    email = user.get('email', '')
    phone = user.get('phone', '')
    gender = user.get('gender', '')

    gspread_throttler(
        sheet1,
        'append_row',
        values = [name, email, phone, gender],
        value_input_option = 'USER_ENTERED'
    )
```

This example demonstrates how to use the `PackageThrottler` class to manage a high volume of operations, such as interacting with the Google Sheets API (`gspread`) and making API requests. The throttler automatically applies the necessary delays, retries with exponential backoff for transient errors, and handles operations efficiently to prevent hitting rate limits.
