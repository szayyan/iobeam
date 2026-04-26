fn main() {
    #[cfg(not(target_os = "linux"))]
    compile_error!(
        "iobeam requires Linux as it depends on io_uring. Windows and macOS are not supported."
    );
}
