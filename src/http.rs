pub const HTTP1_INTERNAL_SERVER_ERROR: &[u8] =
    b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
pub const HTTP1_NOT_FOUND_ERROR: &[u8] =
    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
pub const HTTP1_BAD_REQUEST_ERROR: &[u8] =
    b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
pub const HTTP1_CONTENT_NOT_FOUND_ERROR: &[u8] =
    b"HTTP/1.1 413 Content Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
pub const HTTP1_CONTENT_TOO_LARGE_ERROR: &[u8] =
    b"HTTP/1.1 413 Content Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
pub const HTTP1_OK_RESPONSE: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nConnection: close\r\n\r\n";

pub fn is_get_request(request: &httparse::Request) -> bool {
    matches!(request.method, Some(ref m) if m.eq_ignore_ascii_case("get"))
}
