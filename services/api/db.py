"""Database connection factory shared by API and investigation tools."""

import asyncio
import sys

from psycopg import AsyncConnection
from psycopg.rows import dict_row

from ship_analytics.config import get_settings

if sys.platform == "win32" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def connection() -> AsyncConnection[dict[str, object]]:
    settings = get_settings()
    kwargs: dict[str, object] = {}
    if settings.pg_sslmode:
        kwargs["sslmode"] = settings.pg_sslmode
    if settings.pg_ssl_root_cert:
        kwargs["sslrootcert"] = settings.pg_ssl_root_cert
    return await AsyncConnection.connect(settings.database_url, row_factory=dict_row, **kwargs)
