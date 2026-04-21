use arrayvec::ArrayVec;

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

const OK_RESP_CHUNK_1: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Type: ";
const OK_RESP_CHUNK_2: &[u8] = b"\r\nContent-Length: ";
const OK_RESP_CHUNK_3: &[u8] = b"\r\n\r\n";

const OK_RESP_HEADER_MAX_LEN: usize = {
    let content_type_max_len = 64; // assumption
    let content_length_max_len = 20; // u64::MAX
    OK_RESP_CHUNK_1.len()
        + OK_RESP_CHUNK_2.len()
        + OK_RESP_CHUNK_3.len()
        + content_type_max_len
        + content_length_max_len
};

pub fn construct_ok_response_headers(
    content_length: usize,
) -> ArrayVec<u8, OK_RESP_HEADER_MAX_LEN> {
    let mut binding = itoa::Buffer::new();
    let content_length_bytes = binding.format(content_length).as_bytes();
    let mut byte_array = ArrayVec::<u8, OK_RESP_HEADER_MAX_LEN>::new();
    byte_array.try_extend_from_slice(OK_RESP_CHUNK_1).unwrap();
    byte_array.try_extend_from_slice(b"text/html").unwrap();
    byte_array.try_extend_from_slice(OK_RESP_CHUNK_2).unwrap();
    byte_array
        .try_extend_from_slice(content_length_bytes)
        .unwrap();
    byte_array.try_extend_from_slice(OK_RESP_CHUNK_3).unwrap();
    return byte_array;
}

pub fn is_get_request(request: &httparse::Request) -> bool {
    matches!(request.method, Some(ref m) if m.eq_ignore_ascii_case("get"))
}
