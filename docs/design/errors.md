# Errors

Error messages returned from the API may be general errors without a specific structure to
the error string or messages that have error codes embedded in the error string. The latter
*usually* indicate that the user/client has sent bad input, while the former indicate a server
error. A message with an error code has the following structure:

```text
Sample service error code <error code> <error type>: <message>
```

There is a 1:1 mapping from error code to error type; error type is simply a more readable
version of the error code. The error type **may change** for an error code, but the error code
for a specific error will not.

The current error codes are:

| Code | Message |  Meaning |
|------|---------|----------|
| 20000 | Unauthorized | | |
| 30000 | Missing input parameter |  |
| 30001 | Illegal input parameter |  |
| 30010 | Metadata validation failed |  |
| 40000 | Concurrency violation |  |
| 50000 | No such user |  |
| 50010 | No such sample |  |
| 50020 | No such sample version |  |
| 50030 | No such sample node |  |
| 50050 | No such data link |  |
| 60000 | Data link exists for data ID |  |
| 60010 | Too many data links |  |
| 100000 | unsupported operation |  |

## Generic JSON-RPC Errors

Errors with the form of the request, or the programming of a method, may result in "standard" JSON-RPC errors.

The word *standard* is in quotes because this service utilizes [JSON-RPC 1.1](https://jsonrpc.org/historical/json-rpc-1-1-wd.html), which never actually settled on standard error codes (and was never actually released as a JSON-RPC standard). Rather, KBase utilizes error codes from [JSON-RPC 2.0](https://www.jsonrpc.org/specification).

The error codes are:

| Code | Message |  Meaning |
|------|---------|----------|
| -32700 | Parse error | Invalid JSON was received by the server. An error occurred on the server while parsing the JSON text. |
| -32600 | Invalid Request | The JSON sent is not a valid Request object. |
| -32601 | Method not found  | The method does not exist / is not available. |
| -32602 | Invalid params | Invalid method parameter(s). |
| -32603 | Internal error | Internal JSON-RPC error. |
| -32000 to -32099| Server error | Reserved for implementation-defined server-errors. |
