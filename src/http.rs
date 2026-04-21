use crate::buffer_pool::BUFFER_POOL_ITEM_SIZE;
use arrayvec::ArrayVec;

const RESPONSE_HEADER_BUFFER_SIZE: usize = 256;
pub type HttpHeaderBuffer = ArrayVec<u8, RESPONSE_HEADER_BUFFER_SIZE>;
// compile time assertion that response headers fit in our buffer pool
const _: () = assert!(BUFFER_POOL_ITEM_SIZE > RESPONSE_HEADER_BUFFER_SIZE);

pub fn write_static_internal_server_error(buf: &mut HttpHeaderBuffer) {
    write_static_response_header(
        buf,
        b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n",
    );
}

pub fn write_static_bad_request_error(buf: &mut HttpHeaderBuffer) {
    write_static_response_header(
        buf,
        b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n",
    );
}

pub fn write_static_content_not_found_error(buf: &mut HttpHeaderBuffer) {
    write_static_response_header(
        buf,
        b"HTTP/1.1 413 Content Too Large\r\nContent-Length: 0\r\n\r\n",
    );
}

pub fn write_static_content_too_large_error(buf: &mut HttpHeaderBuffer) {
    write_static_response_header(
        buf,
        b"HTTP/1.1 413 Content Too Large\r\nContent-Length: 0\r\n\r\n",
    );
}

pub fn write_static_ok_response(buf: &mut HttpHeaderBuffer) {
    write_static_response_header(buf, b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n");
}

pub fn is_get_request(request: &httparse::Request) -> bool {
    matches!(request.method, Some(ref m) if m.eq_ignore_ascii_case("get"))
}

fn write_static_response_header(buf: &mut HttpHeaderBuffer, response: &[u8]) {
    assert!(RESPONSE_HEADER_BUFFER_SIZE > response.len());
    unsafe {
        buf.try_extend_from_slice(response).unwrap_unchecked();
    }
}
