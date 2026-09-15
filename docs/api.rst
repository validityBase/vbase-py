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
``extensions``. The optional vBase ``code`` and ``details`` extensions also
have convenience properties. Clients should branch primarily on ``type``.
The validated document is available as ``error.problem`` and can be serialized
with ``error.problem.to_dict()``.

Malformed, non-Problem-Details, and status-mismatched HTTP failures continue
to raise the original ``requests.HTTPError`` instead of being interpreted as a
trusted structured error.
