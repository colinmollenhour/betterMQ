# BetterMQ HTTP load generator

Run `bettermq-loadgen --help` for ingest load options. Outstanding task state is
bounded by `--concurrency`, including completed request bookkeeping.

`--connect-timeout-ms` defaults to 5000 milliseconds.
`--request-timeout-ms` defaults to 30000 milliseconds and covers the entire
request, including reading the response body. Both must be greater than zero.
Raise the request deadline explicitly for deliberate long-running fault tests;
do not disable it. Transport/deadline failures are counted as failed requests
and messages in the final report and cause a nonzero exit after the run drains.
A server may have accepted a request whose response timed out, so use stable
idempotency keys when comparing broker acceptance against generator failures.
