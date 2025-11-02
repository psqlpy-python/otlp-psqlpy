import re
import typing as t

import psqlpy
import wrapt  # type: ignore[import-untyped]
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import (
    BaseInstrumentor,  # type: ignore[attr-defined]
)
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.semconv.trace import (
    DbSystemValues,
    NetTransportValues,
    SpanAttributes,
)
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import Status, StatusCode

from otlp_psqlpy.package import _instruments
from otlp_psqlpy.version import __version__

CONNECTION_METHODS = [
    "execute",
    "execute_batch",
    "execute_many",
    "fetch",
    "fetch_row",
    "fetch_val",
]
TRANSACTION_METHODS = [
    "begin",
    "commit",
    "rollback",
    "execute",
    "execute_batch",
    "execute_many",
    "fetch",
    "fetch_row",
    "fetch_val",
    "pipeline",
    "create_savepoint",
    "rollback_savepoint",
    "release_savepoint",
]
CURSOR_METHODS = [
    "__anext__",
    "execute",
    "fetchone",
    "fetchmany",
    "fetchall",
]


def _construct_span(
    instance: t.Union[psqlpy.Connection, psqlpy.Transaction, psqlpy.Cursor],
    query: str,
    parameters: t.Sequence[t.Any],
    prepared: t.Optional[bool] = None,
) -> dict[str, t.Any]:
    """Get network and database attributes from instance."""
    span_attributes = {
        SpanAttributes.DB_SYSTEM: DbSystemValues.POSTGRESQL.value,
    }

    dbname = getattr(instance, "dbname", None)
    if dbname:
        span_attributes[SpanAttributes.DB_NAME] = dbname
    user = getattr(instance, "user", None)
    if user:
        span_attributes[SpanAttributes.DB_USER] = user

    hosts = getattr(instance, "hosts", [])
    host_addrs = getattr(instance, "host_addrs", [])
    ports = getattr(instance, "ports", [])

    if hosts:
        span_attributes[SpanAttributes.SERVER_ADDRESS] = ", ".join(hosts)
        span_attributes[SpanAttributes.SERVER_PORT] = ", ".join(
            [str(port) for port in ports],
        )
        span_attributes[SpanAttributes.NETWORK_TRANSPORT] = (
            NetTransportValues.IP_TCP.value
        )

    elif host_addrs:
        span_attributes[SpanAttributes.SERVER_ADDRESS] = ", ".join(host_addrs)
        span_attributes[SpanAttributes.SERVER_PORT] = ", ".join(
            [str(port) for port in ports],
        )
        span_attributes[SpanAttributes.NETWORK_TRANSPORT] = (
            NetTransportValues.IP_TCP.value
        )

    if query is not None:
        span_attributes[SpanAttributes.DB_STATEMENT] = query

    if parameters is not None and len(parameters) > 0:
        span_attributes["db.statement.parameters"] = str(parameters)

    if prepared is not None:
        span_attributes["db.statement.prepared"] = str(prepared)

    return span_attributes


def _retrieve_parameter_from_args_or_kwargs(
    parameter_name: str,
    parameter_index: int,
    args: t.Sequence[t.Any],
    kwargs: t.Mapping[t.Any, t.Any],
) -> t.Optional[t.Any]:
    return (
        args[parameter_index]
        if len(args) > parameter_index
        else kwargs.get(parameter_name, None)
    )


class PSQLPyPGInstrumentor(BaseInstrumentor):
    """Instrumentor for PSQLPy."""

    _leading_comment_remover = re.compile(r"^/\*.*?\*/")
    _tracer = None

    def __init__(self, capture_parameters: bool = False) -> None:
        super().__init__()
        self.capture_parameters = capture_parameters

    def instrumentation_dependencies(self) -> t.Collection[str]:  # noqa: D102
        return _instruments

    def _instrument(self, **kwargs: t.Any) -> None:
        tracer_provider = kwargs.get("tracer_provider")
        self._tracer = trace.get_tracer(
            __name__,
            __version__,
            tracer_provider,
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )

        for method in CONNECTION_METHODS:
            wrapt.wrap_function_wrapper(
                "psqlpy",
                f"Connection.{method}",
                self._do_execute,
            )

        for method in TRANSACTION_METHODS:
            wrapt.wrap_function_wrapper(
                "psqlpy",
                f"Transaction.{method}",
                self._do_execute,
            )

        for method in CURSOR_METHODS:
            wrapt.wrap_function_wrapper(
                "psqlpy",
                f"Cursor.{method}",
                self._do_cursor_execute,
            )

    def _uninstrument(self, **__: t.Any) -> None:
        for cls, methods in [
            (
                psqlpy.Connection,
                CONNECTION_METHODS,
            ),
            (psqlpy.Transaction, TRANSACTION_METHODS),
            (psqlpy.Cursor, CURSOR_METHODS),
        ]:
            for method_name in methods:
                unwrap(cls, method_name)

    async def _do_execute(self, func, instance, args, kwargs):
        exception = None
        params = getattr(instance, "_params", {})
        name = args[0] if args else params.get("database", "postgresql")

        try:
            # Strip leading comments so we get the operation name.
            name = self._leading_comment_remover.sub("", name).split()[0]
        except IndexError:
            name = ""

        with self._tracer.start_as_current_span(
            name,
            kind=SpanKind.CLIENT,
        ) as span:
            if span.is_recording():
                span_attributes = _construct_span(
                    instance,
                    _retrieve_parameter_from_args_or_kwargs(
                        parameter_name="querystring",
                        parameter_index=0,
                        args=args,
                        kwargs=kwargs,
                    ),
                    _retrieve_parameter_from_args_or_kwargs(
                        parameter_name="parameters",
                        parameter_index=1,
                        args=args,
                        kwargs=kwargs,
                    ),
                    _retrieve_parameter_from_args_or_kwargs(
                        parameter_name="prepared",
                        parameter_index=2,
                        args=args,
                        kwargs=kwargs,
                    ),
                )
                for attribute, value in span_attributes.items():
                    span.set_attribute(attribute, value)

            try:
                result = await func(*args, **kwargs)
            except Exception as exc:  # pylint: disable=W0703
                exception = exc
                raise
            finally:
                if span.is_recording() and exception is not None:
                    span.set_status(Status(StatusCode.ERROR))

        return result

    async def _do_cursor_execute(self, func, instance, args, kwargs):
        """Wrap cursor based functions. For every call this will generate a new span."""
        exception = None

        stop = False
        with self._tracer.start_as_current_span(
            f"CURSOR",
            kind=SpanKind.CLIENT,
        ) as span:
            if span.is_recording():
                span_attributes = _construct_span(
                    instance,
                    instance.querystring,
                    instance.parameters,
                )
                for attribute, value in span_attributes.items():
                    span.set_attribute(attribute, value)

            try:
                result = await func(*args, **kwargs)
            except StopAsyncIteration:
                # Do not show this exception to the span
                stop = True
            except Exception as exc:  # pylint: disable=W0703
                exception = exc
                raise
            finally:
                if span.is_recording() and exception is not None:
                    span.set_status(Status(StatusCode.ERROR))

        if not stop:
            return result
        raise StopAsyncIteration
