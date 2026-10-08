"""The cloud-sync logger shared by the facade and its owners.

Its name stays ``utils.cloud_sync`` so log records keep their channel.

Relocated from ``utils/cloud_sync.py`` (the compatibility facade) in Stage S4
of the cloud-sync extraction. ``logging.getLogger`` returns the same logger
the facade bound with ``getLogger(__name__)``.
"""

from __future__ import annotations

import logging

logger = logging.getLogger("utils.cloud_sync")
