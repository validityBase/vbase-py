vBase Python SDK
===========================

.. automodule:: vbase
   :members:
   :undoc-members:
   :show-inheritance:

Forwarder errors
----------------

HTTP failures whose response uses the ``application/problem+json`` media type
and the vBase `RFC 9457 <https://www.rfc-editor.org/rfc/rfc9457>`_ schema raise
``ProblemDetailsError``. It remains a subclass of ``requests.HTTPError`` and
preserves the original response. The standard members are exposed as ``type``,
``title``, ``status``, ``detail``, and ``instance``; additional members are in
``extensions``. The required vBase ``code`` and optional ``details`` extensions
have convenience properties. Clients should branch primarily on ``type``.
The validated document is available as ``error.problem`` and can be serialized
with ``error.problem.to_dict()``.

The required ``type`` and optional ``instance`` members must be valid RFC 3986
URI references; ``instance`` may be omitted. The optional vBase ``details``
extension may also be omitted, but when present it must be a JSON object.
Responses that violate these constraints, contain invalid required vBase
members, use non-Problem-Details content, or have a status mismatch continue to
raise the original ``requests.HTTPError`` instead of being interpreted as a
trusted structured error.

The canonical machine-readable contract is the
:download:`public vBase Problem Details JSON Schema <problem-details.schema.json>`
published with this SDK. Forwarder implementations and downstream applications
must conform to this public schema.
