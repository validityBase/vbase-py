"""Process-local write ownership shared by service instances and async workers."""

import threading
import weakref
from dataclasses import dataclass, field
from functools import wraps
from typing import Callable

_registry_lock = threading.Lock()
_locks: weakref.WeakValueDictionary = weakref.WeakValueDictionary()


@dataclass
class UserWriteState:
    """Confirmed nonce floor and ownership retained by live service instances."""

    lock: object = field(default_factory=threading.RLock)
    next_nonce: int | None = None


def serialize_user_write(method: Callable) -> Callable:
    """Hold through nonce lookup, submission and receipt processing.

    This queue cannot coordinate other processes or writers using the app API.
    """

    @wraps(method)
    def queued(service, *args, **kwargs):
        endpoint = getattr(service, "forwarder_url", None) or service.node_rpc_url
        key = (endpoint.rstrip("/"), service.get_default_user().lower())
        with _registry_lock:
            lock = _locks.get(key)
            if lock is None:
                lock = UserWriteState()
                _locks[key] = lock
        service._user_write_state = lock
        with lock.lock:
            return method(service, *args, **kwargs)

    return queued
