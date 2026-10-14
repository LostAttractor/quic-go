# Request completion inspection

`ReceiveStream.TryReadEOF` and `Stream.TryReadEOF` expose a nonblocking
completion check. A clean EOF requires a known final offset and consumption of
all preceding bytes. An out-of-order FIN with unread data is not EOF. Resets,
local cancellation, and connection errors remain distinguishable from a clean
end. A successful check acknowledges stream completion once, like `Read`.

The HTTP/3 request body exposes the same method. It confirms that no DATA or
unread trailer frame remains before reporting EOF. The method follows the same
single-reader ownership rule as `Read`; it does not change deadlines or wait
for peer input.

The dae MITM adapter uses this contract to preserve `Content-Length: 0` for
confirmed empty requests and to forward open requests without waiting for FIN.
Framing policy belongs to dae, outside this library. The receive-stream tests
cover open streams, data gaps, delayed FIN, buffered data, cancellation, and
single completion notification; dae's integration tests cover HTTP/3 forwarding.
