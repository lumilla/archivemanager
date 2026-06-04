// i am regretting my choice of Rust already, but here we are.
// (STATUS_HEAP_CORRUPTION is an error I am getting intermittently and I have no idea why)
fn main() {
    slint_build::compile("src/app.slint").unwrap();
}