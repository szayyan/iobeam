use std::borrow::Cow;

use crate::buffer_pool::BUFFER_POOL_ITEM_SIZE;
use arrayvec::ArrayVec;
use percent_encoding::percent_decode_str;

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
    write_static_response_header(buf, b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
}

pub fn write_static_content_too_large_error(buf: &mut HttpHeaderBuffer) {
    write_static_response_header(
        buf,
        b"HTTP/1.1 413 Content Too Large\r\nContent-Length: 0\r\n\r\n",
    );
}

pub fn write_dynamic_ok_response(buf: &mut HttpHeaderBuffer, path: &str, content_length: usize) {
    // doesn't cover edge cases like Path::new(..).extension() does
    // but good enough for our usecase and more performant
    let content_type = path
        .rfind('.')
        .map(|s| mime_guess::from_ext(&path[s + 1..]).first_raw())
        .flatten()
        .unwrap_or("application/octet-stream");

    let mut cl_buf = itoa::Buffer::new();
    let cl_str = cl_buf.format(content_length);

    buf.try_extend_from_slice(b"HTTP/1.1 200 OK\r\nContent-Type: ")
        .unwrap();
    buf.try_extend_from_slice(content_type.as_bytes()).unwrap();
    buf.try_extend_from_slice(b"\r\nContent-Length: ").unwrap();
    buf.try_extend_from_slice(cl_str.as_bytes()).unwrap();
    buf.try_extend_from_slice(b"\r\n\r\n").unwrap();
}

pub fn is_get_request(request: &httparse::Request) -> bool {
    matches!(request.method, Some(ref m) if m.eq_ignore_ascii_case("get"))
}

fn write_static_response_header(buf: &mut HttpHeaderBuffer, response: &[u8]) {
    buf.try_extend_from_slice(response).expect("Attempted to write static header response greater than the size of the response header buffer");
}

pub fn decode_http_request_path(path: &str) -> anyhow::Result<Cow<'_, str>> {
    let path = path.trim_start_matches('/');
    Ok(percent_decode_str(path).decode_utf8()?)
}
