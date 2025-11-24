// https://docs.rs/wit-bindgen/latest/wit_bindgen/macro.generate.html#options-to-generate
wit_bindgen::generate!({
	path: "./src/zela.wit",
	// this breaks with async functions
	// but then async functions break with wasm32-wasip2 (the linker doesn't like async functions)
	ownership: Borrowing { duplicate_if_necessary: true },
	pub_export_macro: true,
});
