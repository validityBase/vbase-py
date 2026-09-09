vBase Python SDK
===========================

.. automodule:: vbase
   :members:
   :undoc-members:
   :show-inheritance:

Forwarder errors
----------------

Forwarder HTTP failures with a structured API response raise
``ForwarderAPIError``. It remains a subclass of ``requests.HTTPError`` and
preserves the original response while exposing ``status_code``, ``code``,
``message``, ``details``, and ``response_payload`` for application and test
error reporting. Non-structured HTTP failures continue to raise the original
``requests.HTTPError``.
