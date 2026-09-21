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
The required ``code`` extension must be an uppercase machine-readable identifier
made up of ASCII letters, digits, and underscores, with at most 64 characters.
Responses that violate these constraints, contain invalid required vBase
members, use non-Problem-Details content, or have a status mismatch continue to
raise the original ``requests.HTTPError`` instead of being interpreted as a
trusted structured error.

The :download:`vBase SDK Problem Details consumer profile
<problem-details.schema.json>` documents the forward-compatible response shape
accepted by this SDK. It intentionally does not enumerate producer-specific
error codes or detail fields; each API owns that authoritative contract.
